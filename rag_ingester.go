package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
)

// 1. 准备真实的运维 SOP 数据 (这就是我们的 RAG 知识库)
var sops = []map[string]interface{}{
	{
		"id":    1,
		"title": "SOP_01: 数据库锁超时 (gorm lock wait timeout)",
		"content": `故障现象：Worker 执行订单取消时，报 "gorm lock wait timeout exceeded" 错误。
排障与修复指南：
1. 这是由于高并发下多个事务争抢同一行订单记录导致的死锁或等待超时。
2. 修复动作：切勿直接回滚库存！请先调用 【force_inventory_sync】 工具，传入该订单对应的 product_id 进行库存对齐。
3. 对齐完成后，调用 【mark_manual_review】 工具，将订单状态挂起转交人工复核，挂起原因为"锁超时，已强制对账"。`,
	},
	{
		"id":    2,
		"title": "SOP_02: 生鲜类商品超时取消失败",
		"content": `故障现象：包含生鲜类商品（Category: seafood）的订单在超时调度时发生异常。
排障与修复指南：
1. 生鲜类商品具有极强的时效性，一旦创建订单并进入发货流程，绝对不允许系统自动强制退订。
2. 修复动作：发现类目为 seafood 的死信报错时，禁止调用任何退库存工具。
3. 必须立即调用 【mark_manual_review】 工具，将订单状态设为 3（人工挂起），并在理由中注明 "生鲜商品超时失败，请求人工介入判断生鲜损耗"。`,
	},
	{
		"id":    3,
		"title": "SOP_03: 虚拟物品库存不足或发放失败",
		"content": `故障现象：虚拟商品（Category: virtual）扣库存成功，但外部系统发货失败打入死信。
排障与修复指南：
1. 虚拟商品（如点券、充值卡）一般是立刻发放的。如果到了这一步说明外部 API 熔断。
2. 修复动作：可以直接调用系统底层工具取消该订单。`,
	},
}

func main() {
	apiKey := os.Getenv("DASHSCOPE_API_KEY")
	if apiKey == "" {
		log.Fatal("请先设置环境变量 DASHSCOPE_API_KEY (export DASHSCOPE_API_KEY='你的key')")
	}
	fmt.Println("🧹 1. 正在初始化 Qdrant 向量数据库集合 [sops]...")
	initQdrantCollection()
	fmt.Println("🧠 2. 正在调用 DashScope 向量化 SOP 并入库...")
	for _, sop := range sops {
		text := fmt.Sprintf("%s\n%s", sop["title"], sop["content"])
		//获取文本向量
		vector := getEmbedding(text, apiKey)
		if vector == nil {
			log.Fatalf("获取向量失败: %s", sop["title"])
		}
		//存入qdrant
		insertToQdrant(sop["id"].(int), vector, sop)
		fmt.Printf("✅ 成功入库: %s\n", sop["title"])
	}
	fmt.Println("🎉 RAG 知识库初始化完毕！Agent 已经准备好学习这些知识了。")
}

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
// 操作 Qdrant 向量数据库 (HTTP API)
// ==========================================
func initQdrantCollection() {
	// 每次运行脚本前，先删掉旧的集合，方便我们反复测试
	reqDelete, _ := http.NewRequest("DELETE", "http://localhost:6333/collections/sops", nil)
	http.DefaultClient.Do(reqDelete)
	// 创建新集合，Vector Size 必须和大模型的输出一致（text-embedding-v2 是 1536 维）
	// 使用余弦相似度 (Cosine) 算法
	payload := []byte(`{
		"vectors": {
			"size": 1536,
			"distance": "Cosine"
		}
	}`)
	req, _ := http.NewRequest("PUT", "http://localhost:6333/collections/sops", bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode >= 400 {
		log.Fatalf("创建 Qdrant Collection 失败，请确保 Docker 里的 Qdrant 已启动: %v", err)
	}
}
func insertToQdrant(id int, verctor []float64, payload map[string]interface{}) {
	url := "http://localhost:6333/collections/sops/points"
	data := map[string]interface{}{
		"points": []map[string]interface{}{
			{
				"id":      id,
				"vector":  verctor,
				"payload": payload, //把原文作为 Payload 存进去，召回时大模型才能看见文本
			},
		},
	}
	body, _ := json.Marshal(data)
	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		log.Fatalf("插入点数据失败: %s", string(respBody))
	}

}
