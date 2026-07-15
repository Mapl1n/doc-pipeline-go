package pipeline

import (
	"bytes"
	"context"
	"io"
	"log"
	"regexp"

	"doc-pipeline-go/internal/model"
)

// ClassifyStage — 文本分类（规则引擎，可替换为 ML 模型）
// 识别档案类型：合同、报告、简历、发票、证件
type ClassifyStage struct{}

func NewClassifyStage() *ClassifyStage { return &ClassifyStage{} }
func (s *ClassifyStage) Name() model.PipelineStage { return model.StageClassify }

var patterns = map[string][]string{
	"合同":   {`合同`, `协议`, `甲方`, `乙方`, `签订`, `违约责任`},
	"报告":   {`报告`, `总结`, `汇报`, `工作进展`},
	"简历":   {`简历`, `教育经历`, `工作经历`, `自我评价`},
	"发票":   {`发票`, `增值税`, `纳税人`, `发票代码`},
	"证件":   {`身份证`, `护照`, `居民身份证`, `签发机关`},
	"法律文书": {`判决书`, `裁定书`, `起诉状`, `人民法院`},
}

func (s *ClassifyStage) Process(ctx context.Context, task *model.Task, input io.Reader) (io.Reader, error) {
	content, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}
	text := string(content)

	// 规则匹配打分
	scores := make(map[string]int)
	for category, keywords := range patterns {
		for _, kw := range keywords {
			scores[category] += len(regexp.MustCompile(kw).FindAllString(text, -1))
		}
	}

	best, max := "其他", 0
	for cat, score := range scores {
		if score > max {
			best, max = cat, score
		}
	}

	task.Category = best
	log.Printf("[classify] %s → %s (score=%d)", task.Filename, best, max)

	return bytes.NewReader(content), nil
}

// GetKeywords 获取分类的关键词
func GetKeywords(category string) []string {
	if kw, ok := patterns[category]; ok {
		return kw
	}
	return nil
}

