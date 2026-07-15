package pipeline

import (
	"context"
	"doc-pipeline-go/internal/model"
	"io"
)

// Stage 流水线阶段接口
type Stage interface {
	Name() model.PipelineStage
	Process(ctx context.Context, task *model.Task, input io.Reader) (io.Reader, error)
}
