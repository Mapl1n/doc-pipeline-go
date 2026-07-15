package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"doc-pipeline-go/internal/model"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// IndexStage — ES 全文索引
type IndexStage struct {
	es    *elasticsearch.Client
	index string
}

func NewIndexStage(es *elasticsearch.Client, index string) *IndexStage {
	return &IndexStage{es: es, index: index}
}
func (s *IndexStage) Name() model.PipelineStage { return model.StageIndex }

func (s *IndexStage) Process(ctx context.Context, task *model.Task, input io.Reader) (io.Reader, error) {
	content, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}
	text := string(content)

	doc := map[string]interface{}{
		"task_id":   task.ID,
		"filename":  task.Filename,
		"category":  task.Category,
		"text":      text,
		"size":      task.Size,
		"indexed_at": time.Now().Format(time.RFC3339),
	}

	body, _ := json.Marshal(doc)
	req := esapi.IndexRequest{
		Index:      s.index,
		DocumentID: task.ID,
		Body:       bytes.NewReader(body),
		Refresh:    "true",
	}

	resp, err := req.Do(ctx, s.es)
	if err != nil {
		return nil, fmt.Errorf("es index: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[index] %s → ES index '%s' done", task.Filename, s.index)
	return bytes.NewReader([]byte(text)), nil
}
