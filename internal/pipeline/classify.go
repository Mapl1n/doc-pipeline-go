package pipeline

import (
	"bytes"
	"context"
	"io"
	"log"
	"regexp"

	"doc-pipeline-go/internal/model"
)

// precompile regex patterns once
var (
	reContract  = regexp.MustCompile(`合同|协议|甲方|乙方|签订|违约责任`)
	reReport    = regexp.MustCompile(`报告|总结|汇报|工作进展`)
	reResume    = regexp.MustCompile(`简历|教育经历|工作经历|自我评价`)
	reInvoice   = regexp.MustCompile(`发票|增值税|纳税人|发票代码`)
	reIDCard    = regexp.MustCompile(`身份证|护照|居民身份证|签发机关`)
	reLegal     = regexp.MustCompile(`判决书|裁定书|起诉状|人民法院`)
)

type classifyPattern struct {
	category string
	re       *regexp.Regexp
}

var classifyPatterns = []classifyPattern{
	{"合同", reContract},
	{"报告", reReport},
	{"简历", reResume},
	{"发票", reInvoice},
	{"证件", reIDCard},
	{"法律文书", reLegal},
}

// ClassifyStage — 文本分类（规则引擎）
type ClassifyStage struct{}

func NewClassifyStage() *ClassifyStage { return &ClassifyStage{} }
func (s *ClassifyStage) Name() model.PipelineStage { return model.StageClassify }

func (s *ClassifyStage) Process(ctx context.Context, task *model.Task, input io.Reader) (io.Reader, error) {
	content, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}
	text := string(content)

	best, max := "其他", 0
	for _, p := range classifyPatterns {
		count := len(p.re.FindAllString(text, -1))
		if count > max {
			best, max = p.category, count
		}
	}

	task.Category = best
	log.Printf("[classify] %s => %s (score=%d)", task.Filename, best, max)

	return bytes.NewReader(content), nil
}
