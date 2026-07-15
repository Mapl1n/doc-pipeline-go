package pipeline

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"doc-pipeline-go/internal/model"
)

// LocalQueue — 纯 Go channel 任务队列，替代 Redis Stream
type LocalQueue struct {
	ch     chan *model.Task
	store  map[string]*model.Task
	mu     sync.RWMutex
}

func NewLocalQueue() *LocalQueue {
	return &LocalQueue{
		ch:    make(chan *model.Task, 100),
		store: make(map[string]*model.Task),
	}
}

func (q *LocalQueue) Enqueue(task *model.Task) {
	q.mu.Lock()
	q.store[task.ID] = task
	q.mu.Unlock()
	q.ch <- task
}

func (q *LocalQueue) Dequeue(timeout time.Duration) (*model.Task, error) {
	select {
	case task := <-q.ch:
		return task, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("no task")
	}
}

func (q *LocalQueue) GetTask(id string) *model.Task {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.store[id]
}

func (q *LocalQueue) UpdateTask(task *model.Task) {
	q.mu.Lock()
	q.store[task.ID] = task
	q.mu.Unlock()
}

func (q *LocalQueue) PendingCount() int { return len(q.ch) }

// LocalStore — 内存存储，替代 ES
type LocalStore struct {
	mu   sync.RWMutex
	docs map[string]map[string]interface{}
}

func NewLocalStore() *LocalStore {
	return &LocalStore{docs: make(map[string]map[string]interface{})}
}

func (s *LocalStore) Index(id, filename, category, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.docs[id] = map[string]interface{}{
		"task_id": id, "filename": filename, "category": category,
		"text": text, "indexed_at": time.Now().Format(time.RFC3339),
	}
}

func (s *LocalStore) Search(query string, limit int) []map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	type result struct {
		doc   map[string]interface{}
		score int
	}
	var ranked []result
	for _, doc := range s.docs {
		text, _ := doc["text"].(string)
		score := strings.Count(strings.ToLower(text), strings.ToLower(query))
		if score > 0 {
			ranked = append(ranked, result{doc, score})
		}
	}
	// sort by score desc
	for i := 0; i < len(ranked); i++ {
		for j := i + 1; j < len(ranked); j++ {
			if ranked[j].score > ranked[i].score {
				ranked[i], ranked[j] = ranked[j], ranked[i]
			}
		}
	}
	if len(ranked) > limit && limit > 0 {
		ranked = ranked[:limit]
	}
	out := make([]map[string]interface{}, len(ranked))
	for i, r := range ranked {
		out[i] = r.doc
	}
	return out
}

func (s *LocalStore) DocCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.docs)
}

// LocalParser — 本地文本解析，替代 Tika
type LocalParser struct{}

func NewLocalParser() *LocalParser { return &LocalParser{} }
func (p *LocalParser) Parse(data []byte) (string, error) {
	text := string(data)
	if len(text) == 0 {
		return "", fmt.Errorf("empty file")
	}
	return text, nil
}

// StandalonePipeline — 自包含流水线
type StandalonePipeline struct {
	queue *LocalQueue
	store *LocalStore
	parser *LocalParser
	progress map[string]chan model.ProgressEvent
	pmu      sync.RWMutex
}

func NewStandalonePipeline() *StandalonePipeline {
	return &StandalonePipeline{
		queue:    NewLocalQueue(),
		store:    NewLocalStore(),
		parser:   NewLocalParser(),
		progress: make(map[string]chan model.ProgressEvent),
	}
}

func (p *StandalonePipeline) Submit(filename string, data []byte, mimeType string) *model.Task {
	task := &model.Task{
		ID:        fmt.Sprintf("task-%d", time.Now().UnixNano()),
		Filename:  filename,
		Size:      int64(len(data)),
		MimeType:  mimeType,
		Status:    model.StatusPending,
		CreatedAt: time.Now(),
	}
	p.queue.Enqueue(task)

	// Process synchronously in a goroutine
	go p.process(task, data)
	return task
}

func (p *StandalonePipeline) process(task *model.Task, data []byte) {
	p.emitProgress(task, model.StageUpload, 0.1)

	// Parse
	p.emitProgress(task, model.StageParse, 0.3)
	text, err := p.parser.Parse(data)
	if err != nil {
		task.Status = model.StatusFailed
		task.Error = err.Error()
		p.emitProgress(task, model.StageParse, 1.0)
		return
	}
	task.TextContent = text
	p.emitProgress(task, model.StageParse, 0.5)

	// Classify
	p.emitProgress(task, model.StageClassify, 0.6)
	category := classifyText(text)
	task.Category = category
	p.emitProgress(task, model.StageClassify, 0.8)

	// Index
	p.emitProgress(task, model.StageIndex, 0.9)
	p.store.Index(task.ID, task.Filename, category, text)
	task.Status = model.StatusDone
	now := time.Now()
	task.CompletedAt = &now
	p.emitProgress(task, model.StageComplete, 1.0)

	p.queue.UpdateTask(task)
	log.Printf("[PIPELINE] %s → %s (%d chars, %s)", task.Filename, category, len(text), "done")
}

func (p *StandalonePipeline) emitProgress(task *model.Task, stage model.PipelineStage, progress float64) {
	task.CurrentStage = stage
	task.Progress = progress
	task.UpdatedAt = time.Now()

	p.pmu.RLock()
	ch, ok := p.progress[task.ID]
	p.pmu.RUnlock()
	if ok {
		select {
		case ch <- model.ProgressEvent{
			TaskID: task.ID, Stage: stage, Progress: progress,
			Status: task.Status, Error: task.Error, Time: time.Now(),
		}:
		default:
		}
	}
}

func (p *StandalonePipeline) Subscribe(taskID string) (<-chan model.ProgressEvent, func()) {
	p.pmu.Lock()
	ch := make(chan model.ProgressEvent, 20)
	p.progress[taskID] = ch
	p.pmu.Unlock()
	cancel := func() {
		p.pmu.Lock()
		delete(p.progress, taskID)
		close(ch)
		p.pmu.Unlock()
	}
	return ch, cancel
}

func (p *StandalonePipeline) GetTask(id string) *model.Task { return p.queue.GetTask(id) }
func (p *StandalonePipeline) LocalStore() *LocalStore       { return p.store }
func (p *StandalonePipeline) PendingCount() int             { return p.queue.PendingCount() }

func classifyText(text string) string {
	best, max := "其他", 0
	patterns := map[string][]string{
		"合同": {"合同", "协议", "甲方", "乙方", "签订"},
		"报告": {"报告", "总结", "汇报"},
		"简历": {"简历", "工作经历", "教育经历"},
		"发票": {"发票", "增值税", "纳税人"},
	}
	for cat, kws := range patterns {
		c := 0
		for _, kw := range kws {
			c += strings.Count(text, kw)
		}
		if c > max {
			best, max = cat, c
		}
	}
	return best
}

func init() { _ = utf8.RuneCountInString }
