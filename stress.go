package main

import (
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 定义简单的Order结构，只为了插数据
type Order struct {
	OrderNo   string    `gorm:"type:varchar(32);uniqueIndex"`
	Status    int       `gorm:"index:idx_status_created"`
	CreatedAt time.Time `gorm:"index:idx_status_created"`
}

func main() {
	// 🔥 注意：这里要连宿主机的 3307 端口
	dsn := "root:123456@tcp(127.0.0.1:3307)/cron-demo?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("连接数据库失败，请检查 Docker 是否启动，端口是否是 3307")
	}

	fmt.Println("开始制造“订单灾难”...")
	const TOTAL_ORDERS = 50000 // 插入 1万 条
	const BATCH_SIZE = 1000    // 每批 500 条

	var orders []Order
	// 设置一个超时时间：2小时前创建的（绝对超时了）
	createTime := time.Now().Add(-10 * time.Hour)

	for i := 0; i < TOTAL_ORDERS; i++ {
		orders = append(orders, Order{
			OrderNo:   fmt.Sprintf("STRESS_%d_%d", time.Now().UnixNano(), i),
			Status:    0, // 0 = 未支付
			CreatedAt: createTime,
		})

		// 批量插入
		if len(orders) >= BATCH_SIZE {
			result := db.Table("orders").Create(&orders)
			if result.Error != nil {
				fmt.Printf("插入失败: %v\n", result.Error)
			} else {
				fmt.Printf("已生成 %d / %d 条超时订单\n", i+1, TOTAL_ORDERS)
			}
			orders = []Order{} // 清空切片
		}
	}
	fmt.Println("✅ 10,000 条超时订单已就位！请启动 Worker 开始清理！")
}
