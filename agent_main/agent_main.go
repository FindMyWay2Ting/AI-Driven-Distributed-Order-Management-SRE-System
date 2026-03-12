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

	"github.com/sashabaranov/go-openai"
	"github.com/sashabaranov/go-openai/jsonschema"
)

// 1. 定义我们手写的 RAG 规则
var policyDoc = `
【退款规则】
1. 订单状态为“待支付”或“0”的，可以退订。
2. 订单状态为“已支付”或“1”的，需扣除 10% 手续费。
3. 生鲜类商品（如海鲜、水果）一旦下单，不论状态，绝对不支持退款。
`

func main() {
	//初始化大模型客户端
	apiKey := os.Getenv("DASHSCOPE_API_KEY")
	if apiKey == "" {
		log.Fatal("请设置 DASHSCOPE_API_KEY")
	}
	config := openai.DefaultConfig(apiKey)
	config.BaseURL = "https://dashscope.aliyuncs.com/compatible-mode/v1"
	client := openai.NewClientWithConfig(config)
	// 模拟用户对话：用户要退海鲜，但给了一个订单号
	// 你可以在这里把 ORDER_xxx 替换成你 Day 1 用 curl 真实创建的待支付订单号测试！
	userMessage := "帮我把 ORDER_1773150636996251169 退订吧，我不想要这个东西了。"
	fmt.Printf("👤 用户: %s\n\n", userMessage)
	// 2. 组装 System Prompt (将 RAG 检索到的规则注入进去)
	systemPrompt := fmt.Sprintf(`你是一个高级电商售后 Agent。
你的任务是帮用户处理订单问题。
这是你检索到的公司最新业务规则（RAG）：
%s
请务必遵循规则。你需要先查询订单状态，再决定下一步操作。`, policyDoc)
	message := []openai.ChatCompletionMessage{
		{
			Role:    openai.ChatMessageRoleSystem,
			Content: systemPrompt,
		},
		{
			Role:    openai.ChatMessageRoleUser,
			Content: userMessage,
		},
	}
	//第一次请求大模型让他思考
	fmt.Println("🤖 Agent 正在思考并决定行动路线...")
	ctx := context.Background()
	for {
		req := openai.ChatCompletionRequest{
			Model:    "qwen3.5-flash",
			Messages: message,
			Tools:    getTools(),
		}

		resp, err := client.CreateChatCompletion(ctx, req)
		if err != nil {
			log.Fatalf("API 调用失败: %v", err)
		}

		msg := resp.Choices[0].Message
		message = append(message, msg)

		// 没有工具调用，说明已经是最终回复
		if len(msg.ToolCalls) == 0 {
			fmt.Printf("\n🎉 最终回复: %s\n", msg.Content)
			break
		}

		// 执行工具
		for _, toolCall := range msg.ToolCalls {
			funcName := toolCall.Function.Name
			fmt.Printf("⚡ 触发动作: %s\n", funcName)

			var args map[string]interface{}
			if err := json.Unmarshal([]byte(toolCall.Function.Arguments), &args); err != nil {
				log.Fatalf("解析工具参数失败: %v", err)
			}

			orderNo, ok := args["order_no"].(string)
			if !ok {
				log.Fatalf("order_no 参数缺失或不是字符串")
			}

			var toolResult string
			switch funcName {
			case "query_order_status":
				toolResult = executeQueryOrder(orderNo)
			case "cancel_order":
				toolResult = executeCancelOrder(orderNo)
			default:
				toolResult = fmt.Sprintf(`{"error":"unknown tool: %s"}`, funcName)
			}

			fmt.Printf("📦 后端返回真实数据: %s\n\n", toolResult)

			message = append(message, openai.ChatCompletionMessage{
				Role:       openai.ChatMessageRoleTool,
				Content:    toolResult,
				ToolCallID: toolCall.ID,
			})
		}

		fmt.Println("🤖 Agent 继续思考...")
	}

}

// ==========================================
// 定义大模型的“说明书” (Schema)
// ==========================================
func getTools() []openai.Tool {
	return []openai.Tool{
		{
			Type: openai.ToolTypeFunction,
			Function: &openai.FunctionDefinition{
				Name:        "query_order_status",
				Description: "查询订单的当前状态。",
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
				Name:        "cancel_order",
				Description: "取消用户的订单。注意：必须确保符合退款规则才能调用此工具！",
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
// HTTP 封装：真实调用你的 Mock Server
// ==========================================
func executeQueryOrder(orderNo string) string {
	resp, err := http.Get("http://localhost:8877/trade/order?order_no=" + orderNo)
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
	resp, err := http.Post("http://localhost:8877/trade/cancel", "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Sprintf(`{"error": "%v"}`, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}
