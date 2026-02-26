package common

import (
	"encoding/json"
	"time"
)

// 我们需要一个结构体来描述一次任务执行的完整历史
type JobLog struct {
	JobName      string    `json:"jobName" bson:"jobName"`           //任务名字
	Command      string    `json:"command" bson:"command"`           //脚本/命令
	Err          string    `json:"err" bson:"err"`                   //错误原因
	Output       string    `json:"output" bson:"output"`             //脚本输出
	PlanTime     int64     `json:"planTime" bson:"planTime"`         //计划调度时间（秒）
	ScheduleTime time.Time `json:"scheduleTime" bson:"scheduleTime"` //实际调度时间
	StartTime    time.Time `json:"startTime" bson:"startTime"`       //任务执行开始时间
	EndTime      time.Time `json:"endTime" bson:"endTime"`           //任务执行结束之间
} //json是给前端展示用的，bson是给MongoDB用的

// 定义任务结构体（通信协议）
type Job struct {
	Name string `json:"name"` //任务名
	//--- 执行配置 ---
	Type       string `json:"type"`       //任务类型：http或shell
	Command    string `json:"command"`    //shell命令（Type=shell时用）
	HttpUrl    string `json:"httpUrl"`    //回调地址（Type=http时用）
	HttpMethod string `json:"httpMethod"` //GET,POST
	Timeout    int    `json:"timeout"`    //超时时间（毫秒）

	//---调度配置 ---
	CronExpr string `json:"cronExpr"` //cron表达式（比如*/5 * * * * *）

	//--- 容错配置（企业级关键）---
	RetryCount    int `json:"retryCount"`    //允许失败重试次数（例如3）
	RetryInterval int `json:"retryInterval"` //重试间隔（毫秒）
}

// 还可以封装两个辅助函数，方便序列化
func (j *Job) Bytes() []byte {
	b, _ := json.Marshal(j)
	return b
}
func UnpackJob(value []byte) (*Job, error) {
	var job Job
	err := json.Unmarshal(value, &job)
	return &job, err
}
