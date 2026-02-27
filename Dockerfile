#==== 第一阶段：构建（builder）===
#使用官方Go镜像作为构建环境
FROM golang:1.25-alpine AS builder

#设置工作目录
WORKDIR /app

#复制go.mod和go.sum并下载依赖
#这一步可以利用docker缓存，加速构建
COPY go.mod go.sum ./
#设置国内代理，防止下载慢
ENV GOPROXY=https://goproxy.cn,direct
RUN go mod download

#复制源码
COPY . .
#编译Master
RUN go build -o master-bin ./master/master.go

#编译Worker
RUN go build -o worker-bin ./worker/

#编译Mock Server
RUN go build -o mock-bin ./mock_server/main.go
 # 编译Log-Consumer
 RUN go build -o consumer-bin ./log_consumer/main.go
#===第二阶段：运行（runner）===
#使用极小的Alpine镜像（5MB）
FROM alpine:latest

#设置工作目录
WORKDIR /root/
#从第一阶段把编译好的两个二进制文件拷过来
COPY --from=builder /app/master-bin .
COPY --from=builder /app/worker-bin .
COPY --from=builder /app/mock-bin .
COPY --from=builder /app/consumer-bin .
#这一步仅仅是说明，具体运行哪个由docker-compose决定
CMD ["./master-bin"]