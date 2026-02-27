package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"my-cron/common"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	fmt.Println("Log-Consumer微服务正在启动...")
	//1、初始化MongoDB连接
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "127.0.0.1:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal("连接MongoDB失败", err) //mongo.Connect() 会创建客户端并准备连接池。真正尝试连接是在你第一次执行需要和服务器交互的操作时发生，比如 Ping() 或 InsertOne()。
	}
	collection := client.Database("cron").Collection("log") //只是告诉 Mongo 驱动你想操作哪张表。
	fmt.Println("✅MongoDB连接成功")

	//2、初始化 Kafka消费者组
	brokensEnv := os.Getenv("KAFKA_BROKERS")
	if brokensEnv == "" {
		brokensEnv = "127.0.0.1:9092"
	}
	brokers := strings.Split(brokensEnv, ",")

	config := sarama.NewConfig()
	/*
		设置Kafka消费者组的分区再均衡策略：
		Kafka 的 Consumer Group 在发生成员变化时需要把分区重新分配。选择RoundRobin（轮询分配）
		GroupStrategies 是一个列表，列表里的元素是实现了 BalanceStrategy 接口的策略对象，每一个对象表示一种分区再均衡算法。
		RoundRobin 策略的行为：如果有 N 个消费者和 M 个分区。它会按照顺序把分区轮流分配给每个消费者。
	*/
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest //从最新消息开始消费
	//创建消费者组（Consumer Group）
	consumerGroup, err := sarama.NewConsumerGroup(brokers, "cron-log-group", config)
	if err != nil {
		log.Fatal("创建 Kafka 消费者组失败:", err)
	}
	fmt.Println("✅ Kafka 消费者组 [cron-log-group] 启动成功")
	//3、运行消费逻辑
	ctx2, cancel2 := context.WithCancel(context.Background())
	handler := &logConsumerHandler{collection: collection}
	go func() {
		for {
			//Consumer会一直阻塞，知道出错或ctx被cancel
			if err := consumerGroup.Consume(ctx2, []string{"cron_logs"}, handler); err != nil {
				log.Printf("消费出错: %v\n", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	//4、退出机制
	/*
		捕获 Ctrl+C 或 SIGTERM
		停止消费流程
		通知 Kafka 离开 consumer group
		关闭消费者
		断开 client
	*/
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	fmt.Println("收到退出信号，正在关闭消费者...")
	cancel2()
	consumerGroup.Close()
	client.Disconnect(context.TODO())
}

// ==========================================
// 核心消费逻辑：实现 sarama.ConsumerGroupHandler 接口
// ==========================================
type logConsumerHandler struct {
	collection *mongo.Collection
}

/*
Kafka Consumer Group生命周期回调函数，Sarama消费者组必须实现

		type ConsumerGroupHandler interface {
	    Setup(ConsumerGroupSession) error
	    Cleanup(ConsumerGroupSession) error
	    ConsumeClaim(ConsumerGroupSession, ConsumerGroupClaim) error
	}
*/
func (h *logConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}
func (h *logConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 是真正的消费循环（面试高光时刻！）
func (h *logConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	//申请一个切片，作为批处理的缓冲池
	batch := make([]interface{}, 0, 100)
	//设置一个2秒定时器
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil //channel被关闭
			}
			//1、解析Kafka里的JSON消息
			var jobLog common.JobLog
			if err := json.Unmarshal(msg.Value, &jobLog); err == nil {
				batch = append(batch, jobLog)
			}
			//2、标记消息已读，提交Offset
			session.MarkMessage(msg, "")
			//[高并发优化] 容量出发：攒满100条，立刻打包存入MongoDB
			if len(batch) >= 100 {
				h.collection.InsertMany(context.TODO(), batch)
				fmt.Printf("触发批量写入%d条日志到MongoDB\n", len(batch))
				batch = batch[:0] // 清空切片复用内存
			}
		case <-ticker.C:
			//[高并发优化] 时间触发：哪怕没有100条，也插入
			if len(batch) > 0 {
				h.collection.InsertMany(context.TODO(), batch)
				fmt.Printf("⏱️ [时间触发] 批量写入 %d 条日志到 MongoDB\n", len(batch))
				batch = batch[:0]
			}
		case <-session.Context().Done():
			// 优雅退出时，把池子里剩下的最后一点日志存进去，绝不弄丢一条数据
			if len(batch) > 0 {
				h.collection.InsertMany(context.TODO(), batch)
			}
			return nil
		}
	}
}
