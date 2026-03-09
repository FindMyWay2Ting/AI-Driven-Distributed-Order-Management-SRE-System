#==== 第一阶段：构建（builder）===
FROM golang:1.25-alpine AS builder

WORKDIR /app

# 复制依赖并下载
COPY go.mod go.sum ./
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod download

# 复制所有源码
COPY . .

# 🚀 编译 Mock Server (业务下单与发送 Kafka 消息入口)
RUN go build -o mock-bin ./mock_server/mock_server_main.go
# 🚀 [Day 2 新增] 编译 Timer Service 
RUN go build -o timer-bin ./timer_service/TimerService.go
# 🚀 [Day 3 新增] 编译 Worker
RUN go build -o worker-bin ./worker/worker.go
RUN go build -o alert-bin ./alert_service/alert_service_main.go



#===第二阶段：运行（runner）===
FROM alpine:latest
WORKDIR /root/

# 从第一阶段把编译好的二进制文件拷过来
COPY --from=builder /app/mock-bin .
COPY --from=builder /app/timer-bin .
# 🚀 [Day 3 新增] 把 Worker 二进制拷过来
COPY --from=builder /app/worker-bin .
COPY --from=builder /app/alert-bin .
CMD ["./mock-bin"]