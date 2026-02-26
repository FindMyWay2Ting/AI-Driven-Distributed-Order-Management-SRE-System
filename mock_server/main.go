/*
这段代码模拟了一个电商订单系统。 关键技术点：
复合索引：idx_status_created，保证查询不扫全表。
Limit 限制：每次只取 100 条，防止内存爆炸。
悲观锁：clause.Locking{Strength: "UPDATE"}，防止并发修改。
*/
package main

import (
	"fmt"
	"log"
	"strconv"
	"time"

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
	Status    int       `gorm:"index:idx_status_created"` //0:未支付，1：已支付；2：已关闭
	CreatedAt time.Time `gorm:"index:idx_status_created"` //创建时间
}

// Inventory库存表
type Inventory struct {
	ID        uint `gorm:"primaryKey"`
	ProductID int  `gorm:"uniqueIndex"`
	Count     int  //剩余库存
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
	//自动建表
	db.AutoMigrate(&Order{}, &Inventory{})
	//初始化测试数据（如果没有数据的话）
	var count int64
	db.Model(&Order{}).Count(&count) //统计Order表中有多少记录，把结果放进count
	if count == 0 {
		fmt.Println("正在初始化数据...")
		//1、初始化库存
		db.Create(&Inventory{ProductID: 101, Count: 100})
		//插入一些测试订单
		for i := 0; i < 5; i++ {
			db.Create(&Order{
				OrderNo:   fmt.Sprintf("TIMEOUT_%d", i),
				Status:    0,
				CreatedAt: time.Now().Add(-1 * time.Hour),
			})
		}
	}
}

func main() {
	initDB()
	r := gin.Default()
	//核心接口：关闭超时订单
	r.POST("/trade/close_timeout", handleCloseTimeout)
	//辅助接口：重置数据（方便测试）
	r.POST("/trade/reset", handleReset)
	fmt.Println("Mock业务服务启动：8877")
	r.Run(":8877")
}

// 核心业务逻辑
func handleCloseTimeout(c *gin.Context) {
	//获取分片参数（默认不分片0/1）
	shardIdStr := c.DefaultQuery("shard_id", "0")
	totalSharIStr := c.DefaultQuery("total_shards", "1")
	shardId, _ := strconv.Atoi(shardIdStr)
	totalSharId, _ := strconv.Atoi(totalSharIStr)

	//定义超时订单：30分钟前的订单

	expireTime := time.Now().Add(-30 * time.Minute)
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
	db.Exec("UPDATE inventories SET count=100 WHERE product_id=101")
	//重新插入超时订单
	now := time.Now()
	for i := 0; i < 5; i++ {
		db.Create(&Order{OrderNo: fmt.Sprintf("TIMEOUT_%d", i),
			Status:    0,
			CreatedAt: now.Add(-1 * time.Hour),
		})
	}
	c.JSON(200, gin.H{"msg": "数据已重置"})
}
