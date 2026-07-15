package model

import "time"

// PipelineStage 流水线阶段
type PipelineStage string

const (
	StageUpload    PipelineStage = "upload"
	StageParse     PipelineStage = "parse"
	StageOCR       PipelineStage = "ocr"
	StageClassify  PipelineStage = "classify"
	StageIndex     PipelineStage = "index"
	StageComplete  PipelineStage = "complete"
)

// TaskStatus 任务状态
type TaskStatus string
const (
	StatusPending    TaskStatus = "pending"
	StatusProcessing TaskStatus = "processing"
	StatusDone       TaskStatus = "done"
	StatusFailed     TaskStatus = "failed"
	StatusRetry      TaskStatus = "retry"
)

// Task 文档处理任务
type Task struct {
	ID            string        `json:"id"`
	Filename      string        `json:"filename"`
	Size          int64         `json:"size"`
	MimeType      string        `json:"mime_type"`
	Status        TaskStatus    `json:"status"`
	Progress      float64       `json:"progress"`       // 0.0 - 1.0
	CurrentStage  PipelineStage `json:"current_stage"`
	RetryCount    int           `json:"retry_count"`
	Error         string        `json:"error,omitempty"`
	MinioPath     string        `json:"minio_path"`
	TextContent   string        `json:"-"`               // 解析文本
	OcrText       string        `json:"-"`               // OCR 文本
	Category      string        `json:"category,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
	CompletedAt   *time.Time    `json:"completed_at,omitempty"`
}

// ProgressEvent WebSocket 进度事件
type ProgressEvent struct {
	TaskID   string        `json:"task_id"`
	Stage    PipelineStage `json:"stage"`
	Progress float64       `json:"progress"`
	Status   TaskStatus    `json:"status"`
	Error    string        `json:"error,omitempty"`
	Time     time.Time     `json:"time"`
}
