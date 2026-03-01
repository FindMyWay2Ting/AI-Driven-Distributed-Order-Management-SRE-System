package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"my-cron/common"
	"net/http"
	"os/exec"

	"strings"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/robfig/cron/v3"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// // 1、定义任务结构体（通信协议）
// type Job struct {
// 	Name     string `json:"name"`     //任务名
// 	Command  string `json:"command"`  //shell命令
// 	CronExpr string `json:"cronExpr"` //cron表达式（比如*/5 * * * * *）
// }

// 全局变量
var (
	G_jobMap     map[string]cron.EntryID //记录任务名->Cron内部ID的映射
	G_cron       *cron.Cron              //调度器
	G_workerPool *ants.Pool              //协程池
	G_logger     *zap.Logger             //结构化日志
)

// 初始化函数
func initEnv() {
	var err error
	//1 初始化协程池，限制最大并发100，防止大量任务把Worker打挂
	//如果池子满了，Submit不会阻塞等待，而是直接返回ants.ErrPoolOverload
	G_workerPool, err = ants.NewPool(100, ants.WithNonblocking(true))
	if err != nil {
		log.Fatal("协程池初始化失败", err)
	}
	//2 初始化生产级日志
	G_logger, _ = zap.NewProduction()

	if err := InitLogSink(); err != nil {
		log.Fatal("MongoDB连接失败", err)
	}
}

// 核心执行逻辑
func executeJob(job *common.Job, client *clientv3.Client) {
	// 1. 构造日志对象 (保持不变)
	jobLog := &common.JobLog{
		JobName:      job.Name,
		Command:      job.Command,
		Output:       "",
		PlanTime:     0,
		ScheduleTime: time.Now(),
		StartTime:    time.Now(),
	}
	jobLog.StartTime = jobLog.StartTime.In(time.FixedZone("CTC", 8*3600))
	jobLog.ScheduleTime = jobLog.ScheduleTime.In(time.FixedZone("CTC", 8*3600))

	if job.Type == "http" {
		jobLog.Command = job.HttpUrl
	}

	// Zap 日志
	G_logger.Info("开始执行任务", zap.String("jobName", job.Name), zap.String("type", job.Type))

	startTime := time.Now()
	var err error
	var output string // 用于接收执行结果

	// 2. 重试循环
	maxLoop := 1 + job.RetryCount
	if maxLoop <= 0 {
		maxLoop = 1
	}

	for i := 0; i < maxLoop; i++ {
		// 重试间隔
		if i > 0 {
			G_logger.Warn("任务执行失败，正在重试...",
				zap.String("jobName", job.Name),
				zap.Int("retry", i),
				zap.Error(err), // 把上一次的错误打出来
			)
			time.Sleep(time.Duration(job.RetryInterval) * time.Millisecond)
		}

		// 执行任务
		switch job.Type {
		case "http":
			output, err = runHttpJob(job)
		case "shell":
			// 假设你把 runShellJob 改成了返回 (string, error)
			// output, err = runShellJob(job)

			// 如果没改，暂时还用原来的
			output, err = runShellJob(job)

		default:
			err = fmt.Errorf("不支持的type：%s", job.Type)
		}
		// 成功则跳出
		if err == nil {
			break
		}
	}

	// 3. 结果回填
	cost := time.Since(startTime)
	jobLog.EndTime = time.Now()
	jobLog.EndTime = jobLog.EndTime.In(time.FixedZone("CTC", 8*3600))

	jobLog.Output = output //  回填 Output

	if err != nil {
		jobLog.Err = err.Error()
		// 失败时，Output 保持为空或者记录部分信息
	} else {
		jobLog.Err = ""
	}

	// 4. 异步落库
	G_logSink.Append(jobLog)

	// 5. 最终日志 (Zap)
	if err != nil {
		G_logger.Error("任务最终执行失败，正在从Etcd中下线该任务", //  这才是真正的最终失败
			zap.String("jobName", job.Name),
			zap.Error(err),
			zap.Int("retried", maxLoop), // 记录重试了多少次
			zap.Duration("cost", cost),
		)
		//[新增] 触发死信警告
		//将任务名、命令和具体错误信息推送到Kafka
		G_logSink.SendDLQ(job.Name, job.Command, err.Error())

		// [核心修改]：如果重试次数用尽依然失败，自动下线任务，防止死循环执行
		_, delErr := client.Delete(context.TODO(), common.JobSaveDir+job.Name)
		if delErr != nil {
			G_logger.Error("下线任务失败", zap.String("jobName", job.Name), zap.Error(delErr))
		} else {
			G_logger.Info("任务已自动下线", zap.String("jobName", job.Name))
		}
	} else {
		G_logger.Info("任务最终执行成功",
			zap.String("jobName", job.Name),
			zap.String("result", output),
			zap.Duration("cost", cost),
		)
	}
}

// 具体执行http请求
func runHttpJob(job *common.Job) (string, error) {
	//设置超时
	timeout := 5000 //默认5秒
	if job.Timeout > 0 {
		timeout = job.Timeout
	} //记录超时时间，覆盖默认值
	client := &http.Client{Timeout: time.Duration(timeout * int(time.Millisecond))}
	req, err := http.NewRequest(job.HttpMethod, job.HttpUrl, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req) //向job.Httpurl发送请求
	if err != nil {
		return "", err
	}
	defer resp.Body.Close() //结束对响应体的读取并释放资源
	//无论状态码是多少，Body里通常都有业务相关信息
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("读取响应体失败:%v", err)
	}
	output := string(bodyBytes) //把[]Bytes转为string

	//如果状态码不是2XX，就认为是错误
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return output, fmt.Errorf("HTTP状态码错误:%d", resp.StatusCode)
	}
	return output, nil
}

// 具体执行shell命令
func runShellJob(job *common.Job) (string, error) {
	cmd := exec.Command("/bin/sh", "-c", job.Command)
	output, err := cmd.CombinedOutput() //这里包含了标准输出+标准错误
	return string(output), err
}

// 调度器回调：枪锁 ->扔进协程池
// 注意：参数改为了 *common.job，因为我们需要里面的RetryCount等信息
// 这是一个“任务包装器”：他把普通的任务包装成带有“分布式锁”的任务
// jobName：任务名（job1）
// client：etcd客户端
// kv：Etcd的kv操作接口（用于事务）
// lease：Etcd的租约接口
func createJobWithLock(job *common.Job, client *clientv3.Client) func() {
	return func() {
		//---0. 加超时控制，防止卡死Cron协程---
		//抢锁和申请租约必须快，2秒搞不定就放弃，别浪费资源
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		//----1.上锁逻辑----
		//创建一个5秒的租约lease，表示如果我抢到锁，但5秒后锁自动失效，防止死锁。
		lease := clientv3.NewLease(client)
		leaseGrantResp, err := lease.Grant(ctx, 5)
		if err != nil {
			fmt.Println("创建租约失败", err)
			return
		}
		leaseID := leaseGrantResp.ID

		//准备抢锁的Key：/cron/lock/{{jobName}}
		lockKey := common.JobLockDir + job.Name

		//2.启动事务抢锁（Txn
		//逻辑：如果Key不存在，createRevison==0
		//			那么写入Key，并绑定租约（Put）
		//			否则抢锁失败
		kv := clientv3.NewKV(client) //从 “总客户端”里拿出一个专门负责 KV 读写的“子模块/句柄”
		txn := kv.Txn(ctx)
		txnResp, err := txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
			Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID))).
			//opPut是构造一个写入的描述，由commit一次性原子执行
			Else(
			//抢不到锁什么都不做
			).Commit()
		//抢不到锁或发生错误
		if err != nil || !txnResp.Succeeded {
			lease.Revoke(context.TODO(), leaseID) //立即清理租约
			return
		}
		//----2.5开始续租（keepAlive），保证长任务不丢锁
		kaCtx, kaCancel := context.WithCancel(context.Background())

		kaCh, err := lease.KeepAlive(kaCtx, leaseID)
		if err != nil {
			//续约失败，同时解决KaCh为nil的情况
			kaCancel()
			//释放租约（会删除lockKey）
			lease.Revoke(context.TODO(), leaseID)
			return
		}
		// 3.消费 keepalive 响应，避免 channel 堆积（推荐保留）
		go func() {
			for range kaCh {
				// 正常情况下会持续收到响应；这里不打印，避免刷屏
			}
		}()
		//====4.非阻塞提交 + 失败还锁====
		err = G_workerPool.Submit(func() {
			//进入worker线程内部
			defer func() {
				kaCancel()                            //停止续约
				lease.Revoke(context.TODO(), leaseID) //释放锁
			}()
			executeJob(job, client)
		})
		//5.如果池子满了(ants.ErrPoolOverload),放弃任务换锁
		if err != nil {
			G_logger.Warn("worker协程池满，放弃任务，立即换锁",
				zap.String("job", job.Name))
			//停止续约
			kaCancel()
			//立即换锁，Etcd会通知其他空闲节点来抢
			lease.Revoke(context.TODO(), leaseID)
		}
	}

}

func main() {
	//初始化环境
	initEnv()
	//初始化全局变量
	G_jobMap = make(map[string]cron.EntryID)
	//开启秒级支持（默认cron是分钟级，开启后支持秒级 */2 * * * * *）
	G_cron = cron.New(cron.WithSeconds())
	G_cron.Start() //启动调度器（他会在后台运行）
	G_logger.Info("Worker已经启动", zap.Int("pool_size", 100))
	//连接etcd
	client, err := clientv3.New(clientv3.Config{Endpoints: common.GetEtcdEndpoints()})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	//2、启动监听
	watchChan := client.Watch(context.Background(), common.JobSaveDir, clientv3.WithPrefix())
	//3、循环处理事件
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			//提取任务名：/cron/jobs/job1 -> job1
			jobKey := string(event.Kv.Key)
			jobName := strings.TrimPrefix(string(jobKey), "/cron/jobs/")
			if jobName == "" {
				G_logger.Warn("非法任务Key，jobName为空", zap.String("key", string(event.Kv.Key)))
				continue
			}

			switch event.Type {
			case mvccpb.PUT: //任务来了（新增或修改）
				//反序列化JSON
				var job common.Job
				err := json.Unmarshal(event.Kv.Value, &job) //这里的value就是{"name":"job1","command":"echo hello","cronExpr":"*/2 * * * * *"}
				if err != nil {
					G_logger.Error("任务反序列化失败", zap.Error(err))
					continue
				}

				//校验job.Name与jobName一致
				if job.Name != "" && job.Name != jobName {
					G_logger.Warn("任务Name与Key不一致，已以Key为准",
						zap.String("keyName", jobName),
						zap.String("valueName", job.Name),
					)
				}
				job.Name = jobName

				//A. 检查是不是更新任务？如果是，先把旧的删了
				if oldID, exists := G_jobMap[jobName]; exists {
					G_cron.Remove(oldID)
				}
				//B. 添加新任务到Cron
				//传入&job指针
				cronJobFun := createJobWithLock(&job, client)
				newID, _ := G_cron.AddFunc(job.CronExpr, cronJobFun)
				//C. 记录ID到Map，方便下次删除
				G_jobMap[jobName] = newID
				G_logger.Info("任务已调度",
					zap.String("job", jobName),
					zap.String("cron", job.CronExpr),
					zap.String("type", job.Type))
			case mvccpb.DELETE: //删除任务
				if oldID, exist := G_jobMap[jobName]; exist {
					G_cron.Remove((oldID))    //从etcd里删除该任务
					delete(G_jobMap, jobName) //将map该任务的映射删除
					G_logger.Info("任务已停止", zap.String("job", jobName))
				}
			}

		}
	}
}
