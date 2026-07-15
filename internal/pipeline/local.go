package pipeline

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"doc-pipeline-go/internal/model"
	"doc-pipeline-go/pkg/docparser"
)

// LocalQueue — channel-based task queue
type LocalQueue struct {
	ch    chan *model.Task
	store map[string]*model.Task
	mu    sync.RWMutex
}

func NewLocalQueue() *LocalQueue {
	return &LocalQueue{ch: make(chan *model.Task, 100), store: make(map[string]*model.Task)}
}
func (q *LocalQueue) Enqueue(task *model.Task) {
	q.mu.Lock(); q.store[task.ID] = task; q.mu.Unlock(); q.ch <- task
}
func (q *LocalQueue) Dequeue(timeout time.Duration) (*model.Task, error) {
	select { case task := <-q.ch: return task, nil; case <-time.After(timeout): return nil, fmt.Errorf("no task") }
}
func (q *LocalQueue) GetTask(id string) *model.Task { q.mu.RLock(); defer q.mu.RUnlock(); return q.store[id] }
func (q *LocalQueue) UpdateTask(task *model.Task) { q.mu.Lock(); q.store[task.ID] = task; q.mu.Unlock() }
func (q *LocalQueue) PendingCount() int { return len(q.ch) }

// LocalStore — in-memory document store
type LocalStore struct {
	mu   sync.RWMutex
	docs map[string]map[string]interface{}
}

func NewLocalStore() *LocalStore { return &LocalStore{docs: make(map[string]map[string]interface{})} }
func (s *LocalStore) Index(id, filename, category, text string) {
	s.mu.Lock(); defer s.mu.Unlock()
	s.docs[id] = map[string]interface{}{"task_id": id, "filename": filename, "category": category, "text": text, "indexed_at": time.Now().Format(time.RFC3339)}
}
func (s *LocalStore) ListAll() []map[string]interface{} {
	s.mu.RLock(); defer s.mu.RUnlock()
	list := make([]map[string]interface{}, 0, len(s.docs))
	for _, d := range s.docs {
		item := map[string]interface{}{"filename": d["filename"], "category": d["category"], "indexed_at": d["indexed_at"]}
		if t, ok := d["text"].(string); ok && len(t) > 100 {
			item["preview"] = t[:100] + "..."
		} else {
			item["preview"] = d["text"]
		}
		list = append(list, item)
	}
	return list
}

type LocalParser struct{}

func NewLocalParser() *LocalParser { return &LocalParser{} }
func (p *LocalParser) Parse(data []byte, filename string) (string, error) { return docparser.Parse(data, filename) }

// StandalonePipeline — self-contained pipeline with progress replay for WebSocket
type StandalonePipeline struct {
	queue        *LocalQueue
	store        *LocalStore
	parser       *LocalParser
	progress     map[string]chan model.ProgressEvent
	lastProgress map[string]model.ProgressEvent
	pmu          sync.RWMutex
	mu           sync.Mutex
}

func NewStandalonePipeline() *StandalonePipeline {
	return &StandalonePipeline{
		queue:        NewLocalQueue(),
		store:        NewLocalStore(),
		parser:       NewLocalParser(),
		progress:     make(map[string]chan model.ProgressEvent),
		lastProgress: make(map[string]model.ProgressEvent),
	}
}

func (p *StandalonePipeline) Submit(filename string, data []byte, mimeType string) *model.Task {
	task := &model.Task{
		ID: fmt.Sprintf("task-%d", time.Now().UnixNano()), Filename: filename,
		Size: int64(len(data)), MimeType: mimeType, Status: model.StatusPending, CreatedAt: time.Now(),
	}
	p.queue.Enqueue(task)
	go p.process(task, data)
	return task
}

func (p *StandalonePipeline) process(task *model.Task, data []byte) {
	// Add artificial delay so WebSocket has time to connect
	time.Sleep(500 * time.Millisecond)

	p.emit(task, model.StageParse, 0.2)
	text, err := p.parser.Parse(data, task.Filename)
	if err != nil {
		task.Status = model.StatusFailed; task.Error = err.Error()
		p.emit(task, model.StageParse, 1.0)
		return
	}
	task.TextContent = text

	p.emit(task, model.StageParse, 0.5)
	category := classifyText(text)
	task.Category = category

	p.emit(task, model.StageClassify, 0.7)

	p.emit(task, model.StageIndex, 0.9)
	p.store.Index(task.ID, task.Filename, category, text)

	task.Status = model.StatusDone
	now := time.Now()
	task.CompletedAt = &now
	p.emit(task, model.StageComplete, 1.0)
	p.queue.UpdateTask(task)
	log.Printf("[PIPELINE] %s -> %s (%d chars)", task.Filename, category, len(text))
}

func (p *StandalonePipeline) emit(task *model.Task, stage model.PipelineStage, progress float64) {
	task.CurrentStage = stage
	task.Progress = progress
	task.UpdatedAt = time.Now()

	evt := model.ProgressEvent{TaskID: task.ID, Stage: stage, Progress: progress, Status: task.Status, Error: task.Error, Time: time.Now()}

	// Store last event for replay
	p.pmu.Lock()
	p.lastProgress[task.ID] = evt
	if ch, ok := p.progress[task.ID]; ok {
		select { case ch <- evt: default: }
	}
	p.pmu.Unlock()
}

func (p *StandalonePipeline) Subscribe(taskID string) (<-chan model.ProgressEvent, func()) {
	ch := make(chan model.ProgressEvent, 20)

	p.pmu.Lock()
	p.progress[taskID] = ch
	// Replay last progress if task already progressed
	if last, ok := p.lastProgress[taskID]; ok {
		go func() { ch <- last }()
	}
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
	for cat, kws := range map[string][]string{
		"合同": {"合同", "协议", "甲方", "乙方", "签订", "违约责任", "付款"},
		"报告": {"报告", "总结", "汇报", "工作进展", "项目", "计划"},
		"简历": {"简历", "工作经历", "教育经历", "自我评价", "技能"},
		"发票": {"发票", "增值税", "纳税人", "发票代码", "价税合计"},
		"法律文书": {"判决书", "裁定书", "起诉状", "人民法院", "原告", "被告"},
	} {
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

func init() { _ = strings.TrimSpace; _ = utf8.RuneCountInString }
