/*
这段代码模拟了一个电商订单系统。 关键技术点：
复合索引：idx_status_created，保证查询不扫全表。
Limit 限制：每次只取 100 条，防止内存爆炸。
悲观锁：clause.Locking{Strength: "UPDATE"}，防止并发修改。
*/
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 1 定义数据模型
// Order订单表
// 关键优化：建立联合索引 idx_status_created,加速查询
type Order struct {
	ID        uint      `gorm:"primaryKey"`
	OrderNo   string    `gorm:"type:varchar(32);uniqueIndex"`
	UserID    int       `gorm:"index"`                    // 属于哪个用户
	ProductID int       `gorm:"index"`                    // 买了什么商品
	Status    int       `gorm:"index:idx_status_created"` //0:未支付，1：已支付；2：已关闭;3:挂起订单，由Agent决策
	CreatedAt time.Time `gorm:"index:idx_status_created"` //创建时间
}

// Inventory库存表
type Inventory struct {
	ID        uint `gorm:"primaryKey"`
	ProductID int  `gorm:"uniqueIndex"`
	Count     int  //剩余库存
}

// 本地消息表模型outbox
type EventOutbox struct {
	ID       uint      `grom:"primaryKey"`
	OrderNo  string    `grom:"index"`
	Payload  string    `grom:"type:text"` //存放要发送到kafka的json
	Status   int       `grom:"index"`     //0待发送，1已发送
	CreateAt time.Time `grom:"index"`
}

// 商品表，用于区分商品类型（AI Agent 判断不同规则的核心依据）
type Product struct {
	ID       uint   `gorm:"primaryKey"`
	Name     string `gorm:"type:varchar(64)"`
	Category string `gorm:"type:varchar(32)"` // "electronics"(数码), "seafood"(生鲜), "virtual"(虚拟)
}

var db *gorm.DB //声明一个全局的数据库连接句柄（指针），指向 GORM 封装后的数据库操作对象
func initDB() {
	//连接Docker里的MySQL（服务名为mysql）
	dsn := "root:123456@tcp(mysql:3306)/cron-demo?charset=utf8mb4&parseTime=True&loc=Local"
	var err error
	//重试机制：等待MySQL容器完全启动
	for i := 0; i < 15; i++ {
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err == nil {
			break
		}
		fmt.Printf("等待MysQL启动(%d/15)...%v\n", i+1, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("MySQL启动失败:", err)
	}

	// 自动建表加入 Product
	db.AutoMigrate(&Order{}, &Inventory{}, &EventOutbox{})
	//初始化测试数据（如果没有数据的话）
	var count int64
	db.Model(&Product{}).Count(&count) //统计Product表中有多少记录，把结果放进count
	if count == 0 {
		fmt.Println("正在初始化多类目商品与库存数据...")

		// 1. 初始化商品档案 (涵盖了普通商品、生鲜、虚拟物品)
		db.Create(&Product{ID: 101, Name: "iPhone 15 Pro", Category: "electronics"})
		db.Create(&Product{ID: 102, Name: "波士顿大龙虾", Category: "seafood"})
		db.Create(&Product{ID: 103, Name: "王者荣耀点券", Category: "virtual"})

		// 2. 初始化库存
		db.Create(&Inventory{ProductID: 101, Count: 1000})
		db.Create(&Inventory{ProductID: 102, Count: 500})
		db.Create(&Inventory{ProductID: 103, Count: 9999})
	}
}

func main() {
	initDB()
	// 启动后台消息中继器
	// 它会不断扫描 event_outboxes 表，把待发送消息投递到 Kafka
	go startOutboxRelay()

	r := gin.Default()
	r.POST("/trade/create_order", handleCreateOrder)
	//核心接口：关闭超时订单
	r.POST("/trade/close_timeout", handleCloseTimeout)
	//辅助接口：重置数据（方便测试）
	r.POST("/trade/reset", handleReset)
	//查询订单详情
	r.GET("/trade/order", handleQueryOrder)
	//用户主动取消订单
	r.POST("trade/cancel", handleUserCancel)
	//刻意制造“故障订单”的接口
	r.POST("/trade/create_bug_order", handleCreateBugOrder)
	r.POST("/internal/force_inventory_sync", handleForceInventorySync)
	r.POST("/internal/mark_manual_review", handleMarkManualReview)
	r.POST("/trade/create_virtual_bug", handleCreateVirtualBug)
	fmt.Println("Mock业务服务启动：8877")
	r.Run(":8877")
}
func handleCreateBugOrder(c *gin.Context) {
	// 刻意制造一个 BUG_ 开头的单号
	orderNo := fmt.Sprintf("BUG_%d", time.Now().UnixNano())
	now := time.Now()
	//强制插入一条待支付订单
	db.Create(&Order{
		OrderNo:   orderNo,
		UserID:    888,
		ProductID: 102,
		Status:    0,
		CreatedAt: now,
	})
	//直接塞入给Outbox发给kafka走延迟调度链路
	payloadMap := map[string]interface{}{
		"orderNo":    orderNo,
		"createTime": now.Unix(),
	}
	payloadBytes, _ := json.Marshal(payloadMap)
	db.Create(&EventOutbox{
		OrderNo:  orderNo,
		Payload:  string(payloadBytes),
		Status:   0,
		CreateAt: now,
	})
	c.JSON(http.StatusOK, gin.H{
		"msg":     "💣 故障订单注入成功，2分钟后观察 Worker 和 DLQ 表现！",
		"orderNo": orderNo,
	})
}
func handleCreateOrder(c *gin.Context) {
	// 生成全局唯一订单号
	// 用 UnixNano 可以避免高并发下订单号冲突
	orderNo := fmt.Sprintf("ORDER_%d", time.Now().UnixNano())

	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	// 扣库存（悲观锁实现：update where count > 0）
	// 这样可以防止库存为 0 还扣成功，也能避免超卖：
	// SQL 实际执行： UPDATE inventory SET count = count - 1 WHERE product_id = 101 AND count > 0
	// RowsAffected=0 表示库存不足，事务需要回滚
	res := tx.Model(&Inventory{}).Where("product_id = ? AND count > 0", 101).
		UpdateColumn("count", gorm.Expr("count - ?", 1))
	if res.RowsAffected == 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{"error": "库存不足"})
		return
	}
	// 插入订单记录
	// status=0 表示“待支付”，用于后续延迟取消判断
	order := Order{
		OrderNo:   orderNo,
		Status:    0,
		CreatedAt: time.Now(),
	}
	// 写入订单失败 → 事务整体回滚
	if err := tx.Create(&order).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建订单失败"})
		return
	}
	// ------------------------------------------------
	// 插入 Outbox 消息表
	// ------------------------------------------------
	// 这里是整个架构的关键：
	// 不直接发 Kafka，而是先把“待发送消息”写入数据库
	// 并且和订单写入放在同一个事务中
	//
	// 这样就保证了：
	// - 订单成功 => 消息记录一定成功
	// - 消息记录失败 => 订单也回滚
	//
	// 从而实现“业务数据”和“消息事件”的本地强一致
	now := time.Now()
	payloadMap := map[string]interface{}{
		"orderNo":    orderNo,
		"createTime": now.Unix(),
	}
	payloadBytes, _ := json.Marshal(payloadMap)
	outbox := EventOutbox{
		OrderNo:  orderNo,
		Payload:  string(payloadBytes),
		Status:   0,
		CreateAt: now,
	}
	// 如果写消息表失败，则整个事务回滚
	if err := tx.Create(&outbox).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "写入消息表失败",
		})
		return
	}
	// ------------------------------------------------
	// 步骤 4：提交事务
	// ------------------------------------------------
	// 到这里，数据库里已经同时有：
	// 1. 扣减后的库存
	// 2. 新订单
	// 3. 一条待发送消息
	//
	// 即使此时应用突然挂掉，
	// 后台恢复后仍然能从 event_outboxes 中继续补发消息
	tx.Commit()
	// 返回下单成功
	c.JSON(http.StatusOK, gin.H{
		"msg":     "下单成功",
		"orderNo": orderNo,
	})
}

// ==========================================
// 3. 后台消息中继器 (Outbox Relay)
// ==========================================

// startOutboxRelay 后台中继器
// 职责：
// 1. 周期性扫描 event_outboxes 表里 status=0 的消息
// 2. 投递到 Kafka
// 3. 发送成功后，把 status 改成 1
func startOutboxRelay() {
	//读取kafka地址
	brokersEnv := os.Getenv("KAFKA_BROKERS")
	if brokersEnv == "" {
		brokersEnv = "127.0.0.1:9092"
	}
	brokers := strings.Split(brokersEnv, ",")
	config := sarama.NewConfig()
	// 等待所有副本确认
	// 可靠性最高，性能稍差
	// 含义：Kafka 真正确认落盘/复制后才算发送成功
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 同步生产者必须开启
	// 否则 SendMessage 拿不到成功结果
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal("Outbox 生产者初始化失败:", err)
	}
	defer producer.Close()
	fmt.Println("✅ 消息中继器 (Outbox Relay) 已启动")
	// 每 500ms 轮询一次数据库
	ticker := time.NewTicker(500 * time.Microsecond)
	for range ticker.C {
		var pendingEvents []EventOutbox
		//查出最多1000条待发送消息
		if err := db.Where("status = ?", 0).
			Order("id ASC").Limit(1000).Find(&pendingEvents).Error; err != nil {
			continue
		}
		//没有待发消息就跳过
		if len(pendingEvents) == 0 {
			continue
		}
		//逐条发送到kafka
		for _, event := range pendingEvents {
			msg := &sarama.ProducerMessage{
				Topic: "order_create_events",
				Value: sarama.StringEncoder(event.Payload),
			}
			//同步发送
			_, _, err := producer.SendMessage(msg)
			if err != nil {
				fmt.Printf("⚠️ 消息 [%s] 发送失败，等待下轮重试: %v\n", event.OrderNo, err)
				// 注意：
				// 这里不把 status 改成 1
				// 所以下次轮询还会继续发
				// 这就实现了“至少投递一次”
				continue
			}
			//kafka发送成功后，更新数据库状态为已发送
			db.Model(&EventOutbox{}).Where("id = ?", event.ID).Update("status", 1)
			// 到这里说明：
			// 数据库里的这条消息已经完成投递
		}
	}

}

// 核心业务逻辑
func handleCloseTimeout(c *gin.Context) {
	//获取分片参数（默认不分片0/1）
	shardIdStr := c.DefaultQuery("shard_id", "0")
	totalSharIStr := c.DefaultQuery("total_shards", "1")
	shardId, _ := strconv.Atoi(shardIdStr)
	totalSharId, _ := strconv.Atoi(totalSharIStr)

	//定义超时订单：30分钟前的订单

	expireTime := time.Now().Add(30 * time.Minute)
	//开启事务
	tx := db.Begin()
	//遇到Panic自动回滚
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	var timeoutOrders []Order
	//[第一层防御]：使用索引+Limit分批查询，避免全表扫描
	//这里的Find只是为了拿到ID，还没有加锁
	if err := tx.Select("id"). //只查ID减少网络传输
					Where("status = ? AND created_at < ? AND id % ?= ?", 0, expireTime, totalSharId, shardId).
					Order("created_at ASC").
					Limit(1000). //每次处理100条
					Find(&timeoutOrders).Error; err != nil {
		tx.Rollback()
		c.JSON(500, gin.H{
			"error": err.Error()})
		return
	}
	if len(timeoutOrders) == 0 {
		tx.Rollback() //释放事务
		c.JSON(200, gin.H{"msg": "没有超时订单"})
		return
	}
	processed := []string{}
	//逐条处理（也可以批量Update，但逐条处理更能模拟复杂的业务补偿逻辑）
	for _, tempOrder := range timeoutOrders {
		var order Order
		//[悲观锁]SELECT...FOR UPDATE
		//锁定这一行，防止用户在关闭订单的同时支付
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&order, tempOrder.ID).Error; err != nil {
			continue //锁不到可能被别人处理了，跳过
		}
		//双重检查
		if order.Status != 0 {
			continue
		}
		//1 改状态
		order.Status = 2 //关闭订单
		tx.Save(&order)
		//2 还库存
		tx.Model(&Inventory{}).Where("product_id = ?", 101).
			UpdateColumn("count", gorm.Expr("count+?", 1))
		processed = append(processed, order.OrderNo)
	}
	//提交事务
	tx.Commit()
	c.JSON(200, gin.H{
		"msg":       "处理成功",
		"processed": processed,
	})
}

// 重置数据接口，方便反复测试
func handleReset(c *gin.Context) {
	db.Exec("TRUNCATE TABLE orders")
	db.Exec("TRUNCATE TABLE event_outboxes")
	db.Exec("UPDATE inventories SET count=10000 WHERE product_id=101")
	//重新插入超时订单

	c.JSON(200, gin.H{"msg": "数据已重置"})
}

// ==========================================
// Agent 专用接口：查询订单状态
// ==========================================
func handleQueryOrder(c *gin.Context) {
	orderNo := c.Query("order_no")
	var order Order
	if err := db.Where("order_no = ?", orderNo).First(&order).Error; err != nil {
		c.JSON(404, gin.H{"error": "订单不存在!"})
		return
	}
	//将状态翻译为可读字符串，方便大模型理解
	// 把数据库的 int 状态翻译成大模型容易理解的文字
	statusStr := "未知状态"
	switch order.Status {
	case 0:
		statusStr = "待支付"
	case 1:
		statusStr = "已支付"
	case 2:
		statusStr = "已关闭/已取消"
	case 3:
		statusStr = "人工挂起复核中"
	}
	c.JSON(200, gin.H{
		"order_no":    order.OrderNo,
		"status":      statusStr,
		"productID":   order.ProductID,
		"status_code": order.Status,
		"create_at":   order.CreatedAt.Format(time.RFC3339),
	})
}

// ==========================================
// Agent 专用接口：用户主动取消订单
// ==========================================
func handleUserCancel(c *gin.Context) {
	var req struct {
		OrderNo string `json:"order_no"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.OrderNo == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "参数错误，需要提供 order_no",
		})
		return
	}
	tx := db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	var order Order
	//悲观锁，锁住订单，防止定时任务和用户同时取消
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("order_no = ?", req.OrderNo).First(&order).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusNotFound, gin.H{
			"error": "订单不存在",
		})
		return
	}
	//业务校验，只有待支付状态才能取消
	if order.Status != 0 {
		tx.Rollback()
		c.JSON(http.StatusBadRequest, gin.H{
			"error": fmt.Sprintf("当前订单状态不支持取消，状态码: %d", order.Status),
		})
		return
	}
	//到这里说明找到的订单是可以取消的
	//改状态
	order.Status = 2
	if err := tx.Save(&order).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "取消失败，系统异常"})
		return
	}
	//退库存
	if err := tx.Model(&Inventory{}).Where("product_id = ?", 101).
		UpdateColumn("count", gorm.Expr("count + ?", 1)).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "退库存失败"})
		return
	}
	//到这里说明可以成功退货了
	tx.Commit()
	c.JSON(http.StatusOK, gin.H{
		"msg":      "订单取消成功，库存已退还",
		"order_no": req.OrderNo,
	})
}

// ==========================================
// Agent 专属工具 1：强制库存对账 (修复锁超时)
// ==========================================
func handleForceInventorySync(c *gin.Context) {
	var req struct {
		ProductID int `json:"product_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
		return
	}

	// 模拟运维强制对齐库存的过程：直接给对应商品库存 +1
	if err := db.Model(&Inventory{}).Where("product_id = ?", req.ProductID).
		UpdateColumn("count", gorm.Expr("count + ?", 1)).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "强制对账失败"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"msg": fmt.Sprintf("✅ 商品 [%d] 强制库存对账完毕，已安全释放资源", req.ProductID)})
}

// ==========================================
// Agent 专属工具 2：转交人工挂起 (处理生鲜异常等)
// ==========================================
func handleMarkManualReview(c *gin.Context) {
	var req struct {
		OrderNo string `json:"order_no"`
		Reason  string `json:"reason"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "参数错误"})
		return
	}

	// 将订单状态修改为 3（代表人工挂起异常状态）
	if err := db.Model(&Order{}).Where("order_no = ?", req.OrderNo).
		Update("status", 3).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "挂起失败"})
		return
	}

	fmt.Printf("⚠️ 订单 [%s] 已被 AI 挂起转交人工，理由: %s\n", req.OrderNo, req.Reason)
	c.JSON(http.StatusOK, gin.H{"msg": "✅ 订单已被成功挂起，等待人工介入"})
}

// ==========================================
// 专门生成虚拟商品故障单
// ==========================================
func handleCreateVirtualBug(c *gin.Context) {
	orderNo := fmt.Sprintf("VBUG_%d", time.Now().UnixNano())
	now := time.Now()

	// ProductID 103 是王者荣耀点券（虚拟商品）
	db.Create(&Order{
		OrderNo:   orderNo,
		UserID:    888,
		ProductID: 103,
		Status:    0,
		CreatedAt: now,
	})

	payloadBytes, _ := json.Marshal(map[string]interface{}{"orderNo": orderNo, "createTime": now.Unix()})
	db.Create(&EventOutbox{
		OrderNo:  orderNo,
		Payload:  string(payloadBytes),
		Status:   0,
		CreateAt: now,
	})

	c.JSON(200, gin.H{
		"msg":     "🎮 虚拟商品故障单注入成功，准备观察 AI 不同的排障路线！",
		"orderNo": orderNo,
	})
}
