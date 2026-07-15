package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"doc-pipeline-go/internal/pipeline"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type ProgressHandler struct {
	pipeline *pipeline.Pipeline
}

func NewProgressHandler(p *pipeline.Pipeline) *ProgressHandler {
	return &ProgressHandler{pipeline: p}
}

// StreamProgress WebSocket 实时推送处理进度
func (h *ProgressHandler) StreamProgress(c *gin.Context) {
	taskID := c.Query("task_id")
	if taskID == "" {
		c.JSON(400, gin.H{"message": "task_id required"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// 订阅进度
	ch, cancel := h.pipeline.SubscribeWS(taskID)
	defer cancel()

	for event := range ch {
		data, _ := json.Marshal(event)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("[WS] client disconnected for task %s", taskID)
			return
		}

		if event.Status == "done" || event.Status == "failed" {
			// 发送最终事件后关闭
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1000, "done"))
			return
		}
	}
}

// GetTask 查询任务状态（HTTP 轮询模式）
func (h *ProgressHandler) GetTask(c *gin.Context) {
	taskID := c.Param("task_id")
	task := h.pipeline.GetTask(taskID)
	if task == nil {
		c.JSON(404, gin.H{"message": "任务不存在"})
		return
	}
	c.JSON(200, gin.H{
		"code": 0,
		"data": task,
	})
}
