package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/sashabaranov/go-openai"
	"github.com/sashabaranov/go-openai/jsonschema"
)

func main() {
	fmt.Println("🚨 AI-Alert-Service (智能排障微服务) 正在启动...")
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
	fmt.Println("🛡️  AI Agent 正在监听死信队列 [cron_dead_letters]...")
	//4、优雅退出
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	cancel()
	consumerGroup.Close()
}

// ==========================================
// 告警消费与 AI 唤醒逻辑
// ==========================================
type AlertConsumerHandler struct{}

func (h *AlertConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *AlertConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *AlertConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	apiKey := os.Getenv("DASHSCOPE_API_KEY")
	for msg := range claim.Messages() {

		var dlqData map[string]interface{}
		if err := json.Unmarshal(msg.Value, &dlqData); err != nil {
			session.MarkMessage(msg, "")
			continue
		}

		jobName, _ := dlqData["jobName"].(string)
		errMsg, _ := dlqData["errMsg"].(string)

		// 从 jobName 提取出真实的单号 (比如从 OrderCancelJob_BUG_123 提取 BUG_123)
		orderNo := strings.Replace(jobName, "OrderCancelJob_", "", 1)

		fmt.Printf("\n==============================================\n")
		fmt.Printf("🔴 收到死信告警！订单号: %s\n", orderNo)
		fmt.Printf("❌ 错误信息: %s\n", errMsg)
		fmt.Printf("🤖 正在唤醒 AI Agent 进行自主排障...\n")
		fmt.Printf("🔍 正在检索排障 SOP...\n")
		//1、向量化错误信息
		vector := getEmbedding(errMsg, apiKey)
		//2、从向量库召回SOP
		sopContext := searchSOPFromQdrant(vector)
		fmt.Printf("📚 召回参考知识: \n%s\n", sopContext)

		fmt.Printf("🤖 正在唤醒 AI Agent 进行自主排障...\n")
		// 3. 把单号、错误和 SOP 一起喂给大模型
		aiReport := runSREAgent(orderNo, errMsg, sopContext)
		//4、将报告推送给团队
		sendWebhookAlert(orderNo, errMsg, aiReport)
		session.MarkMessage(msg, "")
		fmt.Printf("==============================================\n\n")
	}
	return nil
}

// ==========================================
// AI Agent 核心 ReAct 循环 (思考 -> 调用工具 -> 总结)
// ==========================================
func runSREAgent(orderNo string, errMsg string, sopContext string) string {
	apiKey := os.Getenv("DASHSCOPE_API_KEY")
	if apiKey == "" {
		fmt.Println("⚠️ 未配置 DASHSCOPE_API_KEY，Agent 无法启动")
		return fmt.Sprintf("⚠️ 未配置 DASHSCOPE_API_KEY，Agent 无法启动")
	}
	config := openai.DefaultConfig(apiKey)
	config.BaseURL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
	client := openai.NewClientWithConfig(config)
	// System Prompt
	systemPrompt := fmt.Sprintf(`你是一个高级微服务 SRE（网站可靠性工程师）Agent。
系统抛出了一个死信队列错误。你需要负责诊断并修复它。

【内部运维 SOP 手册】
以下是系统根据报错自动检索到的排障 SOP，请务必严格按照 SOP 的指导进行操作：
%s

【你的权限与工具】
你可以使用提供的工具查询订单详情、强制同步库存，或者将订单挂起。
执行要求：
1. 必须先调用 query_order_status 查清订单状态和商品ID。
2. 严格遵循上方 SOP 的修复动作！不要自己瞎猜！如果 SOP 说不要退库存，就绝对不要退！`, sopContext)
	userMessage := fmt.Sprintf("请帮我处理这个异常单号：%s，系统报错日志是：%s", orderNo, errMsg)
	messages := []openai.ChatCompletionMessage{
		{
			Role:    openai.ChatMessageRoleSystem,
			Content: systemPrompt,
		},
		{
			Role:    openai.ChatMessageRoleUser,
			Content: userMessage,
		},
	}
	ctx := context.Background()
	for {
		req := openai.ChatCompletionRequest{
			Model:    "qwen3.5-flash",
			Messages: messages,
			Tools:    getSRETools(),
		}
		resp, err := client.CreateChatCompletion(ctx, req)
		if err != nil {
			return fmt.Sprintf("API 调用失败: %v\n", err)
		}
		msg := resp.Choices[0].Message
		messages = append(messages, msg)
		if len(msg.ToolCalls) == 0 {
			// 🚀 核心修改：不再只是 Print，而是把这篇完整的报告返回出去！
			return msg.Content
		}
		for _, toolCall := range msg.ToolCalls {
			funcName := toolCall.Function.Name
			fmt.Printf("⚡ AI 决定使用工具: [%s]\n", funcName)
			var args map[string]interface{}
			json.Unmarshal([]byte(toolCall.Function.Arguments), &args)
			var toolResult string
			switch funcName {
			case "query_order_status":
				toolResult = executeQueryOrder(args["order_no"].(string))
			case "force_inventory_sync":
				toolResult = executeForceInventorySync(int(args["product_id"].(float64)))
			case "mark_manual_review":
				toolResult = executeMarkManualReview(args["order_no"].(string), args["reason"].(string))
			case "cancel_order":
				toolResult = executeCancelOrder(args["order_no"].(string))
			default:
				toolResult = `{"error":"未知工具"}`
			}
			fmt.Printf("📦 工具返回数据: %s\n", toolResult)
			messages = append(messages, openai.ChatCompletionMessage{
				Role:       openai.ChatMessageRoleTool,
				Content:    toolResult,
				ToolCallID: toolCall.ID,
			})
		}
	}
}

// ==========================================
// 定义 AI 的工具箱 (Schema)
// ==========================================
func getSRETools() []openai.Tool {
	return []openai.Tool{
		{
			Type: openai.ToolTypeFunction,
			Function: &openai.FunctionDefinition{
				Name:        "query_order_status",
				Description: "诊断工具：查询订单的当前状态、所属用户和购买的商品 ID。",
				Parameters: jsonschema.Definition{
					Type: jsonschema.Object,
					Properties: map[string]jsonschema.Definition{
						"order_no": {Type: jsonschema.String},
					},
					Required: []string{"order_no"},
				},
			},
		},
		{
			Type: openai.ToolTypeFunction,
			Function: &openai.FunctionDefinition{
				Name:        "force_inventory_sync",
				Description: "修复工具：当发现数据库死锁或锁超时（lock wait timeout）时，强制对该商品进行库存 +1 对账。警告：仅在确认为锁超时时使用！",
				Parameters: jsonschema.Definition{
					Type: jsonschema.Object,
					Properties: map[string]jsonschema.Definition{
						"product_id": {Type: jsonschema.Integer, Description: "需要对账的商品 ID"},
					},
					Required: []string{"product_id"},
				},
			},
		},
		{
			Type: openai.ToolTypeFunction,
			Function: &openai.FunctionDefinition{
				Name:        "mark_manual_review",
				Description: "挂起工具：将订单状态修改为人工复核状态。常用于生鲜类商品报错，或修复动作完成后的兜底记录。",
				Parameters: jsonschema.Definition{
					Type: jsonschema.Object,
					Properties: map[string]jsonschema.Definition{
						"order_no": {Type: jsonschema.String},
						"reason":   {Type: jsonschema.String, Description: "挂起的原因或修复备注"},
					},
					Required: []string{"order_no", "reason"},
				},
			},
		},
		{
			Type: openai.ToolTypeFunction,
			Function: &openai.FunctionDefinition{
				Name:        "cancel_order",
				Description: "取消工具：用于直接取消订单并退库存。仅在 SOP 明确指示可以取消订单（如虚拟商品发放失败）时使用。",
				Parameters: jsonschema.Definition{
					Type: jsonschema.Object,
					Properties: map[string]jsonschema.Definition{
						"order_no": {Type: jsonschema.String},
					},
					Required: []string{"order_no"},
				},
			},
		},
	}
}

// ==========================================
// 工具箱底层的 HTTP 请求实现
// ==========================================
// 注意：Docker 内部服务互相调用，使用服务名 mock-services
const MockServerURL = "http://mock-services:8877"

func executeQueryOrder(orderNo string) string {
	resp, err := http.Get(MockServerURL + "/trade/order?order_no=" + orderNo)
	if err != nil {
		return fmt.Sprintf(`{"error": "%v"}`, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}
func executeForceInventorySync(productID int) string {
	payload := map[string]interface{}{
		"productID": productID,
	}
	jsonPayload, _ := json.Marshal(payload)
	resp, err := http.Post(MockServerURL+"/internal/force_inventory_sync", "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Sprintf(`{"error": "%v"}`, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}
func executeMarkManualReview(orderNo, reason string) string {
	payload := map[string]string{"order_no": orderNo, "reason": reason}
	jsonPayload, _ := json.Marshal(payload)
	resp, err := http.Post(MockServerURL+"/internal/mark_manual_review", "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Sprintf(`{"error": "%v"}`, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}
func executeCancelOrder(orderNo string) string {
	payload := map[string]string{"order_no": orderNo}
	jsonPayload, _ := json.Marshal(payload)
	resp, err := http.Post(MockServerURL+"/trade/cancel", "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Sprintf(`{"error": "%v"}`, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

// ==========================================
// RAG 组件 1：调用阿里云 DashScope 把错误日志变向量
// ==========================================
// ==========================================
// 调用阿里云 DashScope Embedding 接口
// ==========================================
func getEmbedding(text string, apiKey string) []float64 {
	url := "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings"
	//请求体：使用embedding-v2模型，输出维度为153
	payload := map[string]interface{}{
		"model": "text-embedding-v2",
		"input": []string{text},
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Embedding 请求失败: %v", err)
		return nil
	}
	defer resp.Body.Close()
	var result struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	//respBody, _ := io.ReadAll(resp.Body)
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || len(result.Data) == 0 {
		log.Printf("解析 Embedding 响应失败或为空")
		return nil
	}
	return result.Data[0].Embedding
}

// ==========================================
// RAG 组件 2：去 Qdrant 搜寻最相关的 SOP
// ==========================================
func searchSOPFromQdrant(vector []float64) string {
	if len(vector) == 0 {
		return "未检索到相关SOP"
	}
	url := "http://qdrant:6333/collections/sops/points/search"
	payload := map[string]interface{}{
		"vector":       vector,
		"limit":        2, //召回最多相关的两条SOP
		"with_payload": true,
	}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("⚠️ Qdrant 搜索失败: %v\n", err)
		return "向量库连接异常，请凭借通用知识排障。"
	}
	defer resp.Body.Close()
	var result struct {
		Result []struct {
			Score   float64                `json:"score"`
			Payload map[string]interface{} `json:"payload"`
		} `json:"result"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	if len(result.Result) == 0 {
		return "未匹配到高相关度 SOP。"
	}
	// 组装召回的文本
	var sopContext strings.Builder //高效拼接字符串。
	for i, item := range result.Result {
		//设置一个阈值，避免干扰
		if item.Score > 0.3 {
			sopContext.WriteString(fmt.Sprintf("\n--- SOP %d (匹配度: %.2f) ---\n", i+1, item.Score))
			sopContext.WriteString(fmt.Sprintf("标题: %v\n", item.Payload["title"]))
			sopContext.WriteString(fmt.Sprintf("内容: %v\n", item.Payload["content"]))
		}
	}
	return sopContext.String()

}

// ==========================================
// 模拟发送企业办公群 Webhook 卡片
// ==========================================
func sendWebhookAlert(orderNo, errMsg, aiReport string) {
	fmt.Println("\n========================================================")
	fmt.Println("🔔 [Webhook] 触发企业微信 / 钉钉机器人告警推送")
	fmt.Println("========================================================")
	fmt.Printf("🔴 故障等级: P1 (核心链路异常)\n")
	fmt.Printf("💣 关联单号: %s\n", orderNo)
	fmt.Printf("❌ 原始报错: %s\n", errMsg)
	fmt.Println("--------------------------------------------------------")
	fmt.Println("✨ 【AI 自动排障与修复报告】")
	fmt.Println(aiReport)
	fmt.Println("--------------------------------------------------------")
	fmt.Println("🔗 状态: 已处理完毕，无需人工起夜介入。")
	fmt.Println("========================================================\n")
}
