/*
Kafka重构版：
核心变化有三个：
存储介质变了：从直接写 MongoDB 变成先写 Kafka
结构设计变了：LogSink 字段、初始化逻辑、后台协程都变了
职责边界变了：原来是“日志落库模块”，现在是“日志投递到 Kafka 的生产者模块”，落库可以交给独立消费服务做
*/

package main

import (
	"encoding/json"
	"fmt"
	"my-cron/common"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// 全局单例
var G_logSink *LogSink

// LogSink 负责将日志发送到 Kafka (重构版)
/*
原版
拿着 mongo.Client 和 collection，直接操作 Mongo
主通道 logChan，预留了 autoCommitChan 做批量写
偏向“数据库 DAO + 异步队列”的设计
《----------------》
重构版
不再关心数据库，只关心 Kafka 生产者和 Topic
没有内部 chan 了，借用 sarama.AsyncProducer 自带的缓冲和协程
LogSink 的职责从“写库”转成“发消息”
*/
type LogSink struct {
	producer sarama.AsyncProducer // 改为：Kafka 异步生产者
	topic    string               // Kafka Topic 名字
}

// InitLogSink 初始化日志汇聚点
func InitLogSink() error {
	//1、读取环境变量中的Kafka地址，没有则用本地默认值
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "127.0.0.1:9092"
	}
	brokers := strings.Split(brokersEnv, ",")

	//2、配置Sarma 生产者参数
	config := sarama.NewConfig()
	//WaitForAll 标识等待所有副本同步完成才返回，保证数据的最高可靠性
	config.Producer.RequiredAcks = sarama.WaitForAll
	//随机分区发送，均匀打散流量
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//异步模式下，开启捕获Error用于监控，关闭Success提高性能
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true

	//3、创建异步生产者（高吞吐的核心）
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("连接kafka失败:%v", err)
	}
	//4、启动后台协程，专门处理异步投递失败的错误日志
	go func() {
		for err := range producer.Errors() {

			/*
				if G_logger != nil (首选方案：专业日志)： 如果全局的 Zap 日志对象 (G_logger) 已经成功初始化了，
				我们就使用它来打出结构化的高级日志。
				这对应着代码里的 G_logger.Error("写入 Kafka 失败", zap.Error(err))。
				else (兜底方案：控制台打印)： 如果 G_logger 还没来得及初始化，或者因为某种异常变成了空指针 (nil)，
				为了防止程序直接崩溃（在 Go 语言中，调用空指针的方法会直接引发 Panic 导致程序死掉），
				我们就退而求其次，使用最基础的 fmt.Println(...) 把错误强行打印到控制台。
			*/
			if G_logger != nil {
				G_logger.Error("写入Kafka失败", zap.Error(err))
			} else {
				fmt.Println("写入Kafka失败", err)
			}
		}
	}()
	//5、实例化单例
	G_logSink = &LogSink{
		producer: producer,
		topic:    "cron_logs", //把日志统一投递到这个Topic
	}
	fmt.Println("kafka 日志生产者初始化成功，Brokers:", brokers)
	return nil
}

// Append发送日志（对外暴露的方法）
func (sink *LogSink) Append(jobLog *common.JobLog) {
	//1、将结构体序列化为JSON
	bytes, err := json.Marshal(jobLog)
	if err != nil {
		if G_logger != nil {
			G_logger.Error("日志序列化失败", zap.Error(err))
		}
		return
	}
	//2、构造kafka消息
	msg := &sarama.ProducerMessage{
		Topic: sink.topic,
		Value: sarama.ByteEncoder(bytes),
	}
	//3、异步发送到Kafka（非阻塞，像发子弹一样快）
	select {
	case sink.producer.Input() <- msg:
		//发送指令已经提交给sarama内部协程
	default:
		//极端情况kafka彻底宕机导致sarama内部channel满载
		if G_logger != nil {
			G_logger.Warn("Kafka发送队列已满，丢弃日志", zap.String("jobName", jobLog.JobName))
		}
	}
}

// 为了让Worker拥有把彻底失败的任务扔进死信队列的能力，我们需要改两个文件，先改log_sink.go，添加发送死信的方法
// SendDLQ 发送消息到死信队列
func (sink *LogSink) SendDLQ(jobName string, command string, errMsg string) {
	// 1. 构造死信警告内容
	dlqMsg := map[string]interface{}{
		"jobName": jobName,
		"command": command,
		"errMsg":  errMsg,
		"time":    time.Now().Local().Format(time.RFC3339),
	}
	bytes, _ := json.Marshal(dlqMsg)
	// 2. 投递到专门的死信Topic (cron_dead_letters)
	msg := &sarama.ProducerMessage{
		Topic: "cron_dead_letters",
		Value: sarama.ByteEncoder(bytes),
	}
	// 3. 异步发送，绝不阻塞worker原有流程
	select {
	case sink.producer.Input() <- msg:
		// 提交成功
		if G_logger != nil {
			G_logger.Info("✅ 死信已成功投递到 Kafka", zap.String("job", jobName))
		}
	case <-time.After(1 * time.Second):
		// 如果等了 3 秒 Kafka 还没接收（严重网络故障），才做丢弃处理
		if G_logger != nil {
			G_logger.Error("❌ 死信队列投递超时(3秒)！Kafka可能发生阻塞", zap.String("job", jobName))
		}
	}
}
