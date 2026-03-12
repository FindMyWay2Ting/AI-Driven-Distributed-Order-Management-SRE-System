package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

var (
	redisClient   *redis.Client
	kafkaProducer sarama.SyncProducer //使用同步生产者确保超时消息发送成功
)

// 复用上游的事件结构
type OrderCreateEvent struct {
	OrderNo    string `json:"orderNo"`    //订单号
	CreateTime int64  `json:"createTime"` //创建时间
}

func main() {
	fmt.Println("⏱️ Timer-Service (延迟调度微服务) 正在启动...")
	//初始化Redis
	initRedis()
	//初始化kafka Producer
	initkafkaProducer()
	//创建全局context，用来控制goroutine生命周期
	ctx, cancel := context.WithCancel(context.Background())
	//启动延迟调度器（核心逻辑）
	go startDelayScheduler(ctx)
	//启动kafka消费者，监听订单创建
	go startOrderConsumer(ctx)
	//优雅退出
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm //阻塞等待退出信号
	fmt.Println("收到退出信号，正在关闭服务器...")
	cancel()
	time.Sleep(1 * time.Second) //等待所有goroutine退出
	redisClient.Close()
	kafkaProducer.Close()
}

// ///////////////////////////////////////////////////
// 初始化 Redis
// ///////////////////////////////////////////////////
func initRedis() {
	//从环境变量中读取redis地址
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6377"
	}
	//创建redis客户端
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	//测试连接
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatal("redis连接失败:", err)
	}
	fmt.Println("✅ Redis 连接成功")
}

// ///////////////////////////////////////////////////
// 初始化 Kafka Producer
// ///////////////////////////////////////////////////
func initkafkaProducer() {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "127.0.0.1:9092"
	}
	brokers := strings.Split(brokersEnv, ",")
	config := sarama.NewConfig()
	//kafka需要全部副本确认
	config.Producer.RequiredAcks = sarama.WaitForAll
	//开启成功返回
	config.Producer.Return.Successes = true
	//随即分区
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	var err error
	kafkaProducer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal("❌ Kafka 生产者初始化失败:", err)
	}
	fmt.Println("✅ Kafka 同步生产者初始化成功")
}

// ///////////////////////////////////////////////////
// Kafka 消费者
// 监听订单创建事件
// ///////////////////////////////////////////////////
func startOrderConsumer(ctx context.Context) {
	brokersEnv := os.Getenv("KAFKA_BROKERS")

	if brokersEnv == "" {
		brokersEnv = "127.0.0.1:9092"
	}

	brokers := strings.Split(brokersEnv, ",")
	condif := sarama.NewConfig()

	//kafka Reblance策略
	condif.Consumer.Group.Rebalance.GroupStrategies =
		[]sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	//从最新的消息开始消费
	condif.Consumer.Offsets.Initial = sarama.OffsetNewest

	//创建消费者群组
	consumerGroup, err := sarama.NewConsumerGroup(
		brokers,
		"timer-service-group",
		condif,
	)
	if err != nil {
		log.Fatal("❌ 创建 Kafka 消费者组失败:", err)
	}
	handler := &OrderConsumerHandler{}
	for {
		//持续消费
		if err := consumerGroup.Consume(
			ctx,
			[]string{"order_create_events"},
			handler,
		); err != nil {
			log.Printf("消费者异常: %v\n", err)
		}
		if ctx.Err() != nil {
			return
		} //如果ctx超时或被关闭，就退出
	}
}

// ///////////////////////////////////////////////////
// Kafka Consumer Handler
// ///////////////////////////////////////////////////
type OrderConsumerHandler struct{}

func (h *OrderConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *OrderConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *OrderConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event OrderCreateEvent
		//解析kafka消息
		if err := json.Unmarshal(msg.Value, &event); err == nil {
			//计算订单过期时间
			//测试：30秒
			//生产：30分钟
			expireTime := time.Now().Add(20 * time.Second).Unix() //表示到期的时间
			//写入Redisu延迟队列
			//key：delay_orders
			//Score:过期时间
			//Member：订单号
			err := redisClient.ZAdd(context.Background(), "delayed_orders", redis.Z{Score: float64(expireTime), Member: event.OrderNo}).Err()
			if err != nil {
				log.Printf("❌ 订单 %s 写入 Redis 失败: %v\n",
					event.OrderNo, err)
			} else {
				fmt.Printf(
					"📥 订单进入延迟队列 [%s]，过期时间: %d\n",
					event.OrderNo,
					expireTime,
				)
			}

		}
		//标记kafka消息已经消费
		session.MarkMessage(msg, "")
	}
	return nil
}

// ///////////////////////////////////////////////////
// 延迟调度器
// 每秒扫描 Redis ZSet
// ///////////////////////////////////////////////////
func startDelayScheduler(ctx context.Context) {
	//每秒执行一次
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().Unix()
			//查询所有过期订单
			orders, err := redisClient.ZRangeArgsWithScores(context.Background(),
				redis.ZRangeArgs{
					Key:     "delayed_orders",
					Start:   "-inf",
					Stop:    strconv.FormatInt(now, 10), //10进制
					ByScore: true,
				}).Result() //查询score在[-∞,now]之间的元素
			if err != nil {
				log.Printf("❌ 查询 Redis 失败: %v\n", err)
				continue
			}
			if len(orders) == 0 {
				continue
			}
			for _, order := range orders {
				orderNo := order.Member.(string) //取出订单号
				//删除订单（防止重复处理）
				removed, err := redisClient.ZRem(
					context.Background(),
					"delayed_orders",
					orderNo,
				).Result()
				if err != nil || removed == 0 {
					continue //如果删除失败就跳过，即使redis服务器断了，没删除成功，下一秒轮询的时候也可以查到
				} //只有成功删除 Redis 的那个实例才有资格继续处理订单，这样就实现了分布式锁效果
				//发送kafka消息
				msg := &sarama.ProducerMessage{
					Topic: "order_timeout_jobs",
					Value: sarama.StringEncoder(orderNo),
				}
				_, _, err = kafkaProducer.SendMessage(msg)
				if err != nil {
					log.Printf(
						"❌ 订单 %s 推送失败: %v\n",
						orderNo,
						err,
					)
					//发送失败重新放回Redis
					redisClient.ZAdd(
						context.Background(),
						"delayed_orders",
						redis.Z{
							Score:  float64(now), //重新放回去，并标记立马超时
							Member: orderNo,
						},
					)
				} else {
					fmt.Printf(
						"🚀 订单 [%s] 已超时，派发 Worker\n",
						orderNo,
					)
				}
			}
		}
	}
}
