package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"doc-pipeline-go/internal/model"
)

// ParserStage — Tika 文档解析
type ParserStage struct{ tikaURL string }

func NewParserStage(tikaURL string) *ParserStage {
	return &ParserStage{tikaURL: tikaURL}
}
func (s *ParserStage) Name() model.PipelineStage { return model.StageParse }

func (s *ParserStage) Process(ctx context.Context, task *model.Task, input io.Reader) (io.Reader, error) {
	log.Printf("[parse] %s (%s)", task.Filename, task.MimeType)

	client := &http.Client{Timeout: 60 * time.Second}
	body, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}

	req, _ := http.NewRequestWithContext(ctx, "PUT", s.tikaURL+"/tika", bytes.NewReader(body))
	req.Header.Set("Accept", "text/plain")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tika: %w", err)
	}
	defer resp.Body.Close()

	content, _ := io.ReadAll(resp.Body)
	text := string(content)
	if len(text) == 0 {
		return nil, fmt.Errorf("文档为空或无法解析")
	}
	task.TextContent = text
	log.Printf("[parse] %s → %d chars", task.Filename, len(text))

	// 检查是否需要 OCR（图片型 PDF）
	if len(text) < 100 && strings.Contains(task.MimeType, "pdf") {
		return nil, fmt.Errorf("文档可能为扫描件，需要OCR（word_count=%d）", len(text))
	}

	return bytes.NewReader([]byte(text)), nil
}
