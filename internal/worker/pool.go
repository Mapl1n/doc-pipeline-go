package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"doc-pipeline-go/internal/model"
	"doc-pipeline-go/internal/pipeline"
)

// Pool — Go Worker Pool
// 每个 Worker 从 Redis Stream 消费任务，执行完整流水线
type Pool struct {
	count    int
	pipeline *pipeline.Pipeline
	wg       sync.WaitGroup
	stopCh   chan struct{}
}

func NewPool(count int, p *pipeline.Pipeline) *Pool {
	return &Pool{count: count, pipeline: p, stopCh: make(chan struct{})}
}

// Start 启动所有 Worker
func (p *Pool) Start(ctx context.Context, dispatcher *Dispatcher) {
	for i := 0; i < p.count; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i, dispatcher)
	}
	log.Printf("[POOL] %d workers started", p.count)
}

// Stop 优雅停止
func (p *Pool) Stop() {
	close(p.stopCh)
	p.wg.Wait()
	log.Println("[POOL] all workers stopped")
}

func (p *Pool) worker(ctx context.Context, id int, dispatcher *Dispatcher) {
	defer p.wg.Done()
	log.Printf("[WORKER %d] ready", id)

	for {
		select {
		case <-p.stopCh:
			log.Printf("[WORKER %d] stopping", id)
			return
		default:
		}

		// 从 Redis Stream 拉取任务
		task, err := dispatcher.ClaimTask(ctx, 5*time.Second)
		if err != nil {
			continue // no tasks available
		}

		log.Printf("[WORKER %d] processing: %s (%s)", id, task.Filename, task.MimeType)

		// Fetch file from Minio and run pipeline
		data, err := dispatcher.FetchFile(ctx, task.MinioPath)
		if err != nil {
			log.Printf("[WORKER %d] fetch error: %v; retrying (count=%d)", id, err, task.RetryCount)

			if task.RetryCount < 3 {
				task.RetryCount++
				task.Status = model.StatusRetry
				dispatcher.Requeue(ctx, task)
			} else {
				task.Status = model.StatusFailed
				task.Error = fmt.Sprintf("fetch-failed: %v", err)
			}
			continue
		}

		// 执行流水线
		if err := p.pipeline.Run(ctx, task, data); err != nil {
			log.Printf("[WORKER %d] pipeline error: %v", id, err)

			if task.RetryCount < 3 {
				task.RetryCount++
				dispatcher.Requeue(ctx, task)
			}
		}
	}
}

// Metrics 返回 Worker 指标
func (p *Pool) Metrics() map[string]interface{} {
	return map[string]interface{}{
		"worker_count": p.count,
	}
}
