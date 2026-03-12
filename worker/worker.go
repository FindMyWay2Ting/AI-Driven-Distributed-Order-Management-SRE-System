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
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ==========================================
// 1. 数据模型 (与 Mock Server 保持一致)
// ==========================================
type Order struct {
	ID        uint   `gorm:"primaryKey"`
	OrderNo   string `gorm:"type:varchar(32);uniqueIndex"` //订单号全局唯一
	UserID    int    `gorm:"index"`                        // 属于哪个用户
	ProductID int    `gorm:"index"`                        // 买了什么商品
	// index:idx_status_created 表示和 CreatedAt 组成联合索引，
	// 适合按状态 + 创建时间查询，比如扫描超时未支付订单。
	Status    int       `gorm:"index:idx_status_created"` //0:未支付，1：已支付；2：已关闭（包括超时取消）
	CreatedAt time.Time `gorm:"index:idx_status_created"` //创建时间
}

// Inventory库存表
type Inventory struct {
	ID        uint `gorm:"primaryKey"`
	ProductID int  `gorm:"uniqueIndex"`
	Count     int  //剩余库存
}

var (
	db            *gorm.DB
	logger        *zap.Logger
	kafkaProducer sarama.SyncProducer // 用于发送死信
)

func main() {
	// 1. 初始化生产级结构化日志 (解决痛点3)
	logger, _ = zap.NewProduction()
	defer logger.Sync()
	logger.Info("👷 Worker Node (企业级消费者) 正在启动...")
	//初始化数据库连接
	initDB()
	//初始化kafka生产者
	initKafkaProducer()
	//启动kafka消费者
	ctx, cancel := context.WithCancel(context.Background())
	go startWorkerConsumer(ctx)

	//优雅退出
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	fmt.Println("收到退出信号。正在安全退出...")
	cancel()
	time.Sleep(1 * time.Second)
}

// ==========================================
//
//	initDB 初始化 MySQL 连接
//
// ==========================================
func initDB() {
	dsn := "root:123456@tcp(mysql:3306)/cron-demo?charset=utf8mb4&parseTime=True&loc=Local"
	var err error
	//重试15次，容器启动时，比Mysql启动更早，重试等待mysql也启动
	for i := 0; i < 15; i++ {
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err == nil {
			break
		}
		fmt.Printf("等待 MySQL 启动 (%d/15)...\n", i+1)
		time.Sleep(2 * time.Second)
	}
	//如果重试多次失败，直接退出程序
	if err != nil {
		log.Fatal("❌ MySQL 连接失败:", err)
	}
	fmt.Println("✅ MySQL 连接成功")
}
func initKafkaProducer() {
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "127.0.0.1:9092"
	}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	var err error
	kafkaProducer, err = sarama.NewSyncProducer(strings.Split(brokersEnv, ","), config)
	if err != nil {
		log.Fatal("❌ 死信 Kafka 生产者初始化失败:", err)
	}
}

// ==========================================
// 2. Kafka 消费逻辑
// ==========================================

func startWorkerConsumer(ctx context.Context) {
	//读取kafka地址
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "127.0.0.1:9092"
	}
	brokers := strings.Split(brokersEnv, ",")
	//创建config配置
	config := sarama.NewConfig()
	//消费者再均衡策略（轮询分配分区）
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	//关键配置：关闭自动提交offset，因为我们希望“业务成功后再提交 Offset”，避免消息先提交、业务却失败的情况。否则可能造成消息丢失（Kafka 以为消费成功了，但数据库没处理成功）。
	config.Consumer.Offsets.AutoCommit.Enable = false
	//如果没有已提交offset，则从最早的消息开始消费
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	//创建消费者组
	consumerGroup, err := sarama.NewConsumerGroup(brokers, "order-cancel-group", config)
	if err != nil {
		log.Fatal("❌ 创建 Kafka Worker 消费者组失败:", err)
	}
	//自定义消费者处理器
	handler := &workerConsumerHandler{}
	fmt.Println("🎧 正在监听超时任务队列 [order_timeout_jobs]...")
	//持续消费
	for {
		// Consume 会阻塞，直到：
		// 1. 本轮消费结束
		// 2. 发生 rebalance
		// 3. ctx 被取消
		if err := consumerGroup.Consume(ctx, []string{"order_timeout_jobs"}, handler); err != nil {
			log.Printf("Worker 消费出错: %v\n", err)
		}
		//如果上下文已取消，说明程序正在退出，直接返回
		if ctx.Err() != nil {
			return
		}
	}
}

// ==========================================
// 3. 核心业务：订单取消与库存回滚
// ==========================================
// WorkerConsumerHandler 实现 Sarama 的 ConsumerGroupHandler 接口
type workerConsumerHandler struct{}

func (h *workerConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *workerConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *workerConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// claim.Messages() 会不断返回当前分区分配到的消息
	for msg := range claim.Messages() {
		orderNo := string(msg.Value)
		var processErr error
		// 【重试机制】最多重试 3 次
		for i := 1; i <= 3; i++ {
			processErr = processTimeoutOrder(orderNo)
			if processErr == nil {
				break // 成功则跳出重试循环
			}
			logger.Warn("订单取消失败，准备重试", zap.String("orderNo", orderNo), zap.Int("retry", i), zap.Error(processErr))
			time.Sleep(time.Duration(i*50) * time.Millisecond) // 退避策略
		}
		// 调用具体业务逻辑处理订单取消

		if processErr == nil {
			// 只有彻底成功了，才标记消息，等待后台批量提交
			session.MarkMessage(msg, "")
			session.Commit()
			logger.Info("✅ 订单取消成功", zap.String("orderNo", orderNo))
			fmt.Printf("✅ 订单 [%s] 取消成功，已提交 Offset\n", orderNo)
		} else {
			//死信队列DLQ
			logger.Error("❌ 重试3次依然失败，打入死信队列", zap.String("orderNo", orderNo))
			sendToDLQ(orderNo, processErr.Error())
			// 打入死信队列后，这笔消息就算“处理完”了，标记 Offset 跳过它，防止堵塞整个分区
			session.MarkMessage(msg, "")
			session.Commit()
		}
	}
	return nil
}

// 封装一个满足alert_service格式的死信数据
func sendToDLQ(orderNo string, errMsg string) {
	dlqData := map[string]interface{}{
		"jobName": "OrderCancelJob_" + orderNo, //对齐 AlertService 的解析字段
		"errMsg":  errMsg,
		"time":    time.Now().Format(time.RFC3339),
	}
	bytes, _ := json.Marshal(dlqData)
	msg := &sarama.ProducerMessage{
		Topic: "cron_dead_letters", //发送到死信topic
		Value: sarama.StringEncoder(bytes),
	}
	kafkaProducer.SendMessage(msg)
}

// processTimeoutOrder 执行具体的数据库事务操作
func processTimeoutOrder(orderNo string) error {
	if strings.HasPrefix(orderNo, "VBUG_") {
		return fmt.Errorf("外部系统发货接口响应超时，API 熔断")
	}
	// 🔥 【Chaos 注入点】遇到 BUG_ 单号，直接模拟数据库死锁异常
	if strings.HasPrefix(orderNo, "BUG_") {
		return fmt.Errorf("数据库执行异常: gorm lock wait timeout exceeded; try restarting transaction")
	}
	//开启数据库事务
	tx := db.Begin()
	//防止panic导致事务悬挂，确保异常回滚
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	var order Order
	// ==========================================
	// 悲观锁：锁住订单行，防止并发修改
	// ==========================================
	// 等价 SQL：
	// SELECT * FROM orders WHERE order_no = ? FOR UPDATE
	//
	// 为什么要加锁？
	// 假设一个订单正好在“系统自动取消”和“用户扫码支付”同时发生：
	// - 如果不加锁，可能出现状态竞争：
	//   系统把订单改为已关闭，用户支付线程又把它改为已支付
	// - 使用 FOR UPDATE 后，同一时刻只有一个事务能修改这条订单记录
	//   从而避免并发一致性问题
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("order_no = ?", orderNo).First(&order).Error; err != nil {
		tx.Rollback()
		if err == gorm.ErrRecordNotFound {
			// 数据库里根本没这个订单，说明是历史遗留的脏消息，或者已经被物理删除了
			fmt.Printf("⚠️ 找不到订单 [%s]，可能已被物理删除，作为正常情况跳过\n", orderNo)
			return nil // 返回 nil 告诉外层成功了，直接把这条消息消费掉！
		}
		return fmt.Errorf("找不到订单或加锁失败: %v", err)
	}
	// ==========================================
	// 幂等性检查
	// ==========================================
	// 只有“待支付(0)”状态的订单才能取消
	//
	// 如果订单已经变成：
	// - 1 已支付
	// - 2 已关闭
	// 就说明这条超时消息重复了，或者订单已被其他流程处理
	//
	// 这里直接返回 nil，而不是报错：
	// 因为这不属于系统异常，而是“无需重复处理”的正常情况
	// 外层会继续提交 Offset，避免消息反复重试
	if order.Status != 0 {
		tx.Rollback()
		fmt.Printf("⚠️ 订单 [%s] 状态为 %d，无需取消，直接跳过\n", orderNo, order.Status)
		return nil
	}
	// ==========================================
	// 1. 更新订单状态为 2（已关闭）
	// ==========================================
	order.Status = 2
	if err := tx.Save(&order).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("更新订单状态失败: %v", err)
	}
	// ==========================================
	// 2. 回滚库存
	// ==========================================
	// 等价 SQL：
	// UPDATE inventories
	// SET count = count + 1
	// WHERE product_id = 101
	//
	// 使用 UpdateColumn + gorm.Expr 的好处：
	// - 在数据库层做原子自增
	// - 避免“先查再改”带来的并发覆盖问题
	// 实际生产中这里应从 Order_Item 明细表里拉取出真正的商品 ID 和数量。
	// 这里用伪代码逻辑展示：假设我们反查到这个订单购买了实际的 ProductID
	// 🔥 动态获取实际商品的 ProductID 退库存

	if err := tx.Model(&Inventory{}).Where("product_id = ?", order.ProductID).UpdateColumn("count", gorm.Expr("count + ?", 1)).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("退还库存失败: %v", err)
	}
	// ==========================================
	// 3. 提交事务
	// ==========================================
	// 只有走到这里，说明：
	// - 订单状态修改成功
	// - 库存回滚成功
	// - 整个业务事务完成
	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("事务提交失败：%v", err)
	}
	return nil
}
