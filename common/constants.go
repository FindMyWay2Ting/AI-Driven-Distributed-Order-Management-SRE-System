package common

import "os"

//在docker里可以通过环境变量覆盖
func GetEtcdEndpoints() []string {
	if env := os.Getenv("ETCD_ENDPOINTS"); env != "" {
		return []string{env}
	}
	//默认值，用于本地开发
	return []string{"127.0.0.1:2379"}
}

//把 Etcd 的地址、任务的路径前缀都抽出来。
const (
	//JobSaveDir任务保存目录
	JobSaveDir = "/cron/jobs/"

	//JobLockDir分布式锁目录
	JobLockDir = "/cron/lock/"
)
