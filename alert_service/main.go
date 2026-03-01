package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	fmt.Println("🚨 Alert-Service (告警微服务) 正在启动...")
	//1、获取kafka地址
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "127.0.0.1:9092"
	}
	brokers := strings.Split(brokersEnv, ",")
	//2、初始化kafka消费者组
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetOldest //每次消费最早的消息，保证消息不会漏
	consumerGroup, err := sarama.NewConsumerGroup(brokers, "cron-alert-group", config)
	if err != nil {
		log.Fatal("消创建Kafka告警消费者组失败:", err)
	}
	//3、运行告警监听
	ctx, cancel := context.WithCancel(context.Background())
	handler := &AlertConsumerHandler{}
	go func() {
		for {
			//专门监听死信队列Topic
			if err := consumerGroup.Consume(ctx, []string{"cron_dead_letters"}, handler); err != nil {
				log.Printf("告警消费出错: %v\n", err)
			}
			if ctx.Err() != nil {
				return //如果外部发出取消信号，就退出这个 goroutine，不再继续消费。
			}
		}
	}()
	fmt.Println("🛡️  正在监听死信队列 [cron_dead_letters]...")

	//4、优雅退出
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	cancel()
	consumerGroup.Close()
}

// ==========================================
// 告警消费逻辑
// ==========================================
type AlertConsumerHandler struct{}

func (h *AlertConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *AlertConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *AlertConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		//1、解析死信内容
		var dlqData map[string]interface{}
		json.Unmarshal(msg.Value, &dlqData)
		jobName := dlqData["jobName"]
		errMsg := dlqData["errMsg"]
		timeStr := dlqData["time"]
		// 2. 模拟触发企业微信/飞书/钉钉的 Webhook
		sendWebhookAlert(jobName.(string), errMsg.(string), timeStr.(string))
		// 3. 标记消息已处理
		session.MarkMessage(msg, "")
	}
	return nil
}

// sendWebhookAlert 模拟发送飞书/钉钉卡片
func sendWebhookAlert(jobName, errMsg, timeStr string) {
	fmt.Println("\n==============================================")
	fmt.Printf("🔴 【严重告警】定时任务执行彻底失败！\n")
	fmt.Printf("⏰ 失败时间: %s\n", timeStr)
	fmt.Printf("💣 任务名称: %s\n", jobName)
	fmt.Printf("❌ 错误根因: %s\n", errMsg)
	fmt.Printf("🛠️  处理建议: 请立即检查业务代码或服务器状态，考虑人工介入补偿！\n")
	fmt.Println("==============================================\n")
}
