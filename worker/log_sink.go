// 数据库 IO 是慢操作。如果每个任务都阻塞等待写库，Worker 的吞吐量会暴跌。
// 我设计了一个 LogSink，利用 Channel + 独立协程 异步写库，
// 甚至可以做批量插入（Batch Insert）优化。
package main

import (
	"context"
	"fmt"
	"my-cron/common"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// 全局单例
var G_logSink *LogSink

// LogSink负责存储日志
type LogSink struct {
	client         *mongo.Client       //MongoDB 客户端连接
	collection     *mongo.Collection   //MongoDB 的某一个集合（表），相当于 MySQL 里的一张表
	logChan        chan *common.JobLog //把日志丢进 channel，由后台goroutine统一写
	autoCommitChan chan *common.JobLog //存放需要 自动落库 / 批量提交 的日志
}

// InitLogSink 初始化日志汇聚点
func InitLogSink() error {
	//1 建立连接（设置超时时间）
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	//连接本地MongoDB
	// 修改为读取环境变量，如果没有则默认 localhost (兼容本地开发)
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}
	//2 选择数据库和表（cron-log）
	G_logSink = &LogSink{
		client:     client,
		collection: client.Database("cron").Collection("log"),
		logChan:    make(chan *common.JobLog, 1000), //缓冲Channel，防阻塞
	}
	//创建TTL索引（自动删除7天前日志）
	//这是一个后台操作，只需执行一次，重复执行也没事
	indexModel := mongo.IndexModel{
		Keys: bson.D{{Key: "startTime", Value: 1}}, // 索引基于 startTime 字段
		Options: options.Index().
			SetExpireAfterSeconds(7 * 24 * 3600), // 设置过期时间：7天 (秒)
	}
	_, err = G_logSink.collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		fmt.Println("警告：创建TTL索引失败:", err)
	} else {
		fmt.Println("成功创建日志TTL索引，日志将保留7天")
	}

	//3 启动后台协程，专门处理日志写入
	go G_logSink.writeLoop()
	fmt.Println("MongoDB日志模块初始化成功")
	return nil
}

// Append发送日志（对外暴露的方法）
func (sink *LogSink) Append(log *common.JobLog) {
	select {
	case sink.logChan <- log:
	default:
		// 极端情况：如果日志写的太慢，channel 满了，为了不阻塞任务执行，
		// 这里选择丢弃日志 (或者打印一条错误)
		fmt.Println("警告：日志队列已满，丢弃日志", log.JobName)
	}
}

// writeLoop后台写入
func (sink *LogSink) writeLoop() {
	for log := range sink.logChan {
		//插入MongoDB
		_, err := sink.collection.InsertOne(context.TODO(), log)
		if err != nil {
			fmt.Println("写入日志失败")
		}
	}
}
