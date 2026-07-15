package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"doc-pipeline-go/internal/model"

	"github.com/redis/go-redis/v9"
)

// Dispatcher — Redis Stream 任务调度器
// 使用 Redis Stream + Consumer Group 保证 at-least-once 交付
type Dispatcher struct {
	rdb        *redis.Client
	streamName string
	group      string
}

func NewDispatcher(rdb *redis.Client, streamName, group string) *Dispatcher {
	d := &Dispatcher{rdb: rdb, streamName: streamName, group: group}

	// 创建 Consumer Group（幂等）
	if err := rdb.XGroupCreateMkStream(context.Background(), streamName, group, "0").Err(); err != nil {
		log.Printf("[DISPATCHER] group already exists: %v", err)
	}

	return d
}

// EnqueueTask ★ 将任务推入 Redis Stream
// 任务入队后自动清点 Consumer Group
func (d *Dispatcher) EnqueueTask(ctx context.Context, task *model.Task) error {
	taskData, _ := json.Marshal(task)

	// 缓存完整任务数据
	d.rdb.Set(ctx, "task:"+task.ID+":data", taskData, 24*time.Hour)

	// 推入 Stream
	return d.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: d.streamName,
		Values: map[string]interface{}{
			"task_id": task.ID,
		},
	}).Err()
}

// ClaimTask ★ Worker 从 Stream 取任务（阻塞读取）
// Consumer Group 保证任务不重复分配
func (d *Dispatcher) ClaimTask(ctx context.Context, block time.Duration) (*model.Task, error) {
	streams, err := d.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    d.group,
		Consumer: "worker",
		Streams:  []string{d.streamName, ">"},
		Count:    1,
		Block:    block,
	}).Result()
	if err != nil || len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, fmt.Errorf("no task")
	}

	msg := streams[0].Messages[0]
	taskID, _ := msg.Values["task_id"].(string)

	// 加载完整任务数据
	data, err := d.rdb.Get(ctx, "task:"+taskID+":data").Result()
	if err != nil {
		return nil, fmt.Errorf("task data not found")
	}

	var task model.Task
	json.Unmarshal([]byte(data), &task)

	// ACK 消息
	d.rdb.XAck(ctx, d.streamName, d.group, msg.ID)

	return &task, nil
}

// Requeue 重试任务
func (d *Dispatcher) Requeue(ctx context.Context, task *model.Task) {
	data, _ := json.Marshal(task)
	d.rdb.Set(ctx, "task:"+task.ID+":data", data, 24*time.Hour)
	d.EnqueueTask(ctx, task)
}

// FetchFile 从 Minio 或本地获取文件数据
// 简化版：直接从内存读取（生产环境应改为 Minio SDK）
func (d *Dispatcher) FetchFile(ctx context.Context, path string) ([]byte, error) {
	data, err := d.rdb.Get(ctx, "file:"+path).Bytes()
	if err != nil {
		return nil, fmt.Errorf("file not found in minio cache: %s", path)
	}
	return data, nil
}

// PendingTasks 查看待处理任务数
func (d *Dispatcher) PendingTasks(ctx context.Context) (int64, error) {
	return d.rdb.XLen(ctx, d.streamName).Result()
}
