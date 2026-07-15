package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"doc-pipeline-go/internal/model"

	"github.com/redis/go-redis/v9"
)

// Pipeline 流水线编排器 — 串联所有 Stage
// 特点：流式处理，阶段间通过 io.Reader 传递，避免全量加载
type Pipeline struct {
	stages    []Stage
	rdb       *redis.Client
	mu        sync.RWMutex
	taskStore map[string]*model.Task       // 内存中的任务状态
	wsClients map[string]chan model.ProgressEvent // taskID → ws channel
}

func New(rdb *redis.Client, stages ...Stage) *Pipeline {
	return &Pipeline{
		stages:    stages,
		rdb:       rdb,
		taskStore: make(map[string]*model.Task),
		wsClients: make(map[string]chan model.ProgressEvent),
	}
}

// Run ★ 执行完整流水线：Parser → OCR → Classify → Index
// 每个阶段完成后推送 WebSocket 进度
func (p *Pipeline) Run(ctx context.Context, task *model.Task, rawData []byte) error {
	p.mu.Lock()
	p.taskStore[task.ID] = task
	p.mu.Unlock()

	task.Status = model.StatusProcessing
	p.emitProgress(task, 0)

	var reader io.Reader = bytes.NewReader(rawData)

	for _, stage := range p.stages {
		task.CurrentStage = stage.Name()
		p.emitProgress(task, p.stageProgress(stage.Name()))

		var err error
		reader, err = stage.Process(ctx, task, reader)
		if err != nil {
			log.Printf("[PIPELINE] stage %s error: %v", stage.Name(), err)
			task.Status = model.StatusFailed
			task.Error = err.Error()
			p.emitProgress(task, 1.0)
			return fmt.Errorf("stage %s: %w", stage.Name(), err)
		}
	}

	task.Status = model.StatusDone
	task.Progress = 1.0
	now := time.Now()
	task.CompletedAt = &now
	p.emitProgress(task, 1.0)

	// Redis Stream: acknowledge message
	if p.rdb != nil {
		p.rdb.Del(ctx, "task:"+task.ID+":data")
	}
	return nil
}

// SubscribeWS 订阅任务进度
func (p *Pipeline) SubscribeWS(taskID string) (<-chan model.ProgressEvent, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch := make(chan model.ProgressEvent, 10)
	p.wsClients[taskID] = ch

	// 发送当前状态（如果有）
	if task, ok := p.taskStore[taskID]; ok {
		ch <- model.ProgressEvent{
			TaskID:   taskID,
			Stage:    task.CurrentStage,
			Progress: task.Progress,
			Status:   task.Status,
			Time:     time.Now(),
		}
	}

	cancel := func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if ch, ok := p.wsClients[taskID]; ok {
			delete(p.wsClients, taskID)
			close(ch)
		}
	}
	return ch, cancel
}

// GetTask 获取任务状态
func (p *Pipeline) GetTask(taskID string) *model.Task {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.taskStore[taskID]
}

func (p *Pipeline) emitProgress(task *model.Task, progress float64) {
	task.Progress = progress
	task.UpdatedAt = time.Now()

	event := model.ProgressEvent{
		TaskID:   task.ID,
		Stage:    task.CurrentStage,
		Progress: progress,
		Status:   task.Status,
		Error:    task.Error,
		Time:     time.Now(),
	}

	p.mu.RLock()
	if ch, ok := p.wsClients[task.ID]; ok {
		select {
		case ch <- event:
		default:
		}
	}
	p.mu.RUnlock()
}

func (p *Pipeline) stageProgress(s model.PipelineStage) float64 {
	total := len(p.stages)
	idx := 0
	for i, stage := range p.stages {
		if stage.Name() == s {
			idx = i
			break
		}
	}
	return float64(idx) / float64(total)
}
