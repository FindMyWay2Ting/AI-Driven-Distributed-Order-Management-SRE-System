package main

import (
	"fmt"
	"time"
)

func main() {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	fmt.Println(time.Now().In(loc).Add(-30 * time.Minute))
}
