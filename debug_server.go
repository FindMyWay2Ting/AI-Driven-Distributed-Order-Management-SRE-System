package main

import (
	"fmt"
	"net/http"
	"time"
)

func main() {
	// 模拟一个不稳定的接口
	http.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("[%s] 收到 Worker 请求!\n", time.Now().Format("15:04:05"))

		// 故意返回 500 错误，测试 Worker 会不会重试
		// 如果你想测试成功，就把这里改成 200
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Oops! Server Error"))
	})

	fmt.Println("调试服务器启动，监听 :8888 ...")
	http.ListenAndServe(":8888", nil)
}
