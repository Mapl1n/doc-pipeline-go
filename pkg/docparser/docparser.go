package docparser

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"strings"

	"github.com/ledongthuc/pdf"
)

// Parse 从文件数据中自动识别格式并提取文本
// 支持: PDF, DOCX, TXT, HTML
func Parse(data []byte, filename string) (string, error) {
	// Extract extension safely (handle files with no extension)
	ext := ""
	if idx := strings.LastIndex(filename, "."); idx >= 0 && idx < len(filename)-1 {
		ext = strings.ToLower(filename[idx+1:])
	}

	switch ext {
	case "pdf":
		return parsePDF(data)
	case "docx", "doc":
		return parseDOCX(data)
	case "txt", "text", "md", "csv", "json", "xml", "html", "htm":
		text := string(data)
		if strings.TrimSpace(text) == "" {
			return "", fmt.Errorf("文件内容为空")
		}
		// HTML: strip tags
		if ext == "html" || ext == "htm" {
			return stripHTML(data), nil
		}
		return text, nil
	default:
		// Try as plain text first
		if isReadable(data) {
			return string(data), nil
		}
		return "", fmt.Errorf("不支持的格式 .%s，请上传 PDF/DOCX/TXT", ext)
	}
}

// parsePDF 使用纯 Go 库提取 PDF 文本
func parsePDF(data []byte) (string, error) {
	reader := bytes.NewReader(data)
	size := reader.Size()

	pdfReader, err := pdf.NewReader(reader, size)
	if err != nil {
		return "", fmt.Errorf("PDF解析失败: %w（该PDF可能是扫描件，需要OCR）", err)
	}

	var result strings.Builder
	for i := 0; i < pdfReader.NumPage(); i++ {
		page := pdfReader.Page(i + 1)
		if page.V.IsNull() {
			continue
		}
		text, err := page.GetPlainText(nil)
		if err != nil {
			continue
		}
		result.WriteString(text)
		result.WriteString("\n")
	}

	content := strings.TrimSpace(result.String())
	if content == "" {
		return "", fmt.Errorf("PDF文本提取为空，可能是扫描件图片PDF（需要OCR）")
	}
	return content, nil
}

// parseDOCX 从 ZIP/XML 结构中提取文本
func parseDOCX(data []byte) (string, error) {
	zipReader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", fmt.Errorf("DOCX解析失败: %w", err)
	}

	var docXML []byte
	for _, f := range zipReader.File {
		if f.Name == "word/document.xml" {
			rc, err := f.Open()
			if err != nil {
				return "", err
			}
			docXML, err = io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return "", err
			}
			break
		}
	}

	if docXML == nil {
		return "", fmt.Errorf("DOCX中未找到文档内容")
	}

	return extractTextFromXML(docXML)
}

// extractTextFromXML 从 Word XML 中提取 <w:t> 标签内的文本
func extractTextFromXML(xmlData []byte) (string, error) {
	decoder := xml.NewDecoder(bytes.NewReader(xmlData))
	var result strings.Builder
	inText := false

	for {
		tok, err := decoder.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Local == "t" || t.Name.Local == "p" {
				inText = true
			}
		case xml.EndElement:
			if t.Name.Local == "p" {
				result.WriteString("\n")
			}
			if t.Name.Local == "t" {
				inText = false
			}
		case xml.CharData:
			if inText {
				result.Write(t)
			}
		}
	}

	content := strings.TrimSpace(result.String())
	if content == "" {
		return "", fmt.Errorf("DOCX中未提取到文本")
	}
	return content, nil
}

// stripHTML 去除 HTML 标签
func stripHTML(data []byte) string {
	var result strings.Builder
	inTag := false
	for _, b := range string(data) {
		if b == '<' {
			inTag = true
			continue
		}
		if b == '>' {
			inTag = false
			result.WriteString(" ")
			continue
		}
		if !inTag {
			result.WriteRune(b)
		}
	}
	return strings.TrimSpace(result.String())
}

// isReadable 判断是否为可读文本（非二进制）
func isReadable(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	n := min(4096, len(data))
	bad := 0
	for _, b := range data[:n] {
		if b != 0 && b != '\n' && b != '\r' && b != '\t' && b != ' ' && b < 0x20 {
			bad++
		}
	}
	return float64(bad)/float64(n) < 0.03
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() { _ = fmt.Sprintf }
