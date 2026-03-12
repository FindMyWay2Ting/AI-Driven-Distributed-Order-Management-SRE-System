# AI-Driven Distributed Order Management & SRE System
本项目是一个模拟电商核心链路的后端系统，采用 Go 语言开发。除了实现高可用的下单、延迟任务调度和超时自动取消功能外，还创新性地引入了 AI Agent (SRE Agent)，利用 RAG (检索增强生成) 技术实现微服务故障的智能诊断与自主修复。

## 🚀 核心架构与功能
系统由以下五个核心模块组成：


#### Mock Business Server: 业务入口，利用 Outbox Pattern (本地消息表) 保证订单数据与 Kafka 消息的事务一致性 。


#### Timer Service: 基于 Redis ZSet 实现的分布式延迟队列，负责千万级订单的超时扫描与调度 。

#### Worker Node: 分布式消费者，执行订单状态翻转与库存回滚。具备 3次本地重试 与 死信队列 (DLQ) 容错机制。


#### AI-SRE Service: 智能运维中枢。监听死信队列，结合 Qdrant 向量数据库 检索 SOP 手册，驱动大模型执行自主排障 。

#### RAG Ingester: 自动化知识入库工具，将运维 SOP 文本向量化并持久化至 Qdrant。

## 🛠️ 技术栈
语言: Go (Gin, GORM, Sarama)

中间件: Kafka (消息解耦), Redis (延迟任务), MySQL (持久化)

AI 技术: Qwen (LLM), DashScope (Embedding), Qdrant (Vector DB)

基础设施: Docker Compose (全环境容器化编排)

## 🌟 核心亮点
1. 强一致性保证 (Transactional Outbox)
通过在一个事务中同时操作业务表和消息表，解决了分布式系统中“数据库成功但消息发送失败”的经典难题，保证了数据的最终一致性 。

2. 高可靠延迟调度
放弃了不可靠的内存定时器，采用 Redis ZSet 结合分布式抢占逻辑。即使服务集群中部分节点挂掉，任务依然能被其他节点接管，不重不漏 。

3. AI 驱动的故障自愈 (SRE Agent)
当 Worker 遇到数据库死锁（Lock Wait Timeout）或业务逻辑冲突时，AI Agent 会通过 RAG 召回专业 SOP，自主决定是“强制同步库存”还是“挂起人工复核”，实现了分钟级的无人值守排障 。

## 📦 快速开始
1. 环境变量准备
```Bash

export DASHSCOPE_API_KEY='你的阿里云DashScope Key'
```
2. 启动基础架构
```Bash

docker-compose up -d
```
3. 初始化 RAG 知识库
```Bash

go run rag_ingester.go
```
4. 注入测试数据 (模拟故障)
```Bash

# 创建一个会导致死锁报错的异常订单
curl -X POST http://localhost:8877/trade/create_bug_order
```
## 📂 项目目录结构
​```text

├── mock_server/           # 业务逻辑中心：包含订单创建、Outbox Relay 线程及本地消息表处理 

├── timer_service/         # 延迟调度微服务：基于 Redis ZSet 实现的分布式任务扫描与 Kafka 派发 

├── worker/         # 任务执行单元：负责消费超时任务、执行退库存事务及异常重试逻辑
├── alert_service/         # 智能自愈模块：AI Agent 监听死信队列，结合 RAG 进行故障排障 

├── rag_ingester.go        # 知识入库脚本：将运维 SOP 文本向量化并持久化至 Qdrant

├── Dockerfile             # 容器化构建定义，支持多阶段构建以优化镜像体积

└── docker-compose.yaml    # 全量服务编排：一键启动 MySQL, Redis, Kafka, Qdrant 及业务组件
​```
