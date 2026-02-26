package main

import (
	"context"
	"fmt"
	"log"
	"my-cron/common"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// // 跟worker保持一致的结构体
//
//	type Job struct {
//		Name     string `json:"name"`
//		Command  string `json:"command"`
//		CronExpr string `json:"cronExpr"`
//	}
var client *clientv3.Client

func main() {
	//第五天的代码，day8进行重写，构建Master API Server
	// //拨打电话
	// client, err := clientv3.New(clientv3.Config{Endpoints: []string{common.EtcdEndpoints}})
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer client.Close()
	// //1.构造一个结构体，每两秒打印一次hello
	// job1 := common.Job{
	// 	Name:     "job",
	// 	Command:  "echo hello",
	// 	CronExpr: "*/2 * * * * * ",
	// }
	// //转成Json字符串
	// val, _ := json.Marshal(job1)
	// //2.发送任务（PUT）
	// client.Put(context.TODO(), "/cron/jobs/job12", string(val))
	// fmt.Println("任务已发送")
	// //3、模拟5秒后删除任务
	// time.Sleep(5 * time.Second)
	// client.Delete(context.TODO(), "/cron/jobs/job12")
	// fmt.Println("任务已删除")

	//day8代码
	//1、初始化Etcd连接
	var err error
	client, err = clientv3.New(clientv3.Config{Endpoints: common.GetEtcdEndpoints(),
		DialTimeout: 5 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	//2、启动Gin路由
	r := gin.Default()

	//3、定义路由
	r.POST("/job/save", handleJobSave)     //增/改
	r.POST("/job/delete", handleJobDelete) //删
	r.GET("/job/list", handleJobList)      //查

	//4、启动服务
	fmt.Println("Master API 服务已启动")
	if err := r.Run(":50012"); err != nil {
		log.Fatal(err)
	}
}

// ----处理函数----
// POST /job/save
// Body: {"name":"job1", "command":"echo hello", "cronExpr":"*/2 * * * * *"}
func handleJobSave(c *gin.Context) {
	//解析参数到结构体
	var job common.Job
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err,
		})
		return
	}

	//存入Etcd
	//Key：/cron/jobs/job1
	//value：JSON字符串
	kv := clientv3.NewKV(client)
	//使用common包里的Bytes（）方法序列化
	jobBytes := job.Bytes()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := kv.Put(ctx, common.JobSaveDir+job.Name, string(jobBytes))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	//返回存入成功
	c.JSON(http.StatusOK, gin.H{
		"msg":  "success",
		"data": job,
	})
}

// 删除任务接口
// POST:/job/delete
//
//body:name=job1(表单格式)
func handleJobDelete(c *gin.Context) {
	name := c.PostForm("name")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "name can not be empty!",
		})
		return
	}
	//从Etcd删除
	kv := clientv3.NewKV(client)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := kv.Delete(ctx, common.JobSaveDir+name)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	//返回删除成功
	c.JSON(http.StatusOK, gin.H{
		"msg":  "delete success",
		"data": common.JobSaveDir + name,
	})
}

// 列出所有任务的接口
// GET /job/list
func handleJobList(c *gin.Context) {
	kv := clientv3.NewKV(client)
	//读取 /cron/jobs/目录下的全部文件
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	getResp, err := kv.Get(ctx, common.JobSaveDir, clientv3.WithPrefix())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": err.Error(),
		})
		return
	}
	//解析结果
	var jobList []*common.Job
	for _, kvPair := range getResp.Kvs {
		job := &common.Job{}
		job, err = common.UnpackJob(kvPair.Value)
		if err == nil {
			jobList = append(jobList, job)
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"msg":   "success",
		"count": len(jobList),
		"data":  jobList,
	})
}
