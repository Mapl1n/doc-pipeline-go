package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"doc-pipeline-go/internal/pipeline"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

type ProgressHandler struct {
	sp *pipeline.StandalonePipeline
}

func NewProgressHandler(sp *pipeline.StandalonePipeline) *ProgressHandler {
	return &ProgressHandler{sp: sp}
}

func (h *ProgressHandler) StreamProgress(c *gin.Context) {
	taskID := c.Query("task_id")
	if taskID == "" {
		c.JSON(400, gin.H{"message": "task_id required"})
		return
	}

	if h.sp == nil {
		c.JSON(200, gin.H{"message": "WebSocket not available in this mode"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ch, cancel := h.sp.Subscribe(taskID)
	defer cancel()

	for event := range ch {
		data, _ := json.Marshal(event)
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			log.Printf("[WS] client disconnected: %s", taskID)
			return
		}
		if event.Status == "done" || event.Status == "failed" {
			return
		}
	}
}
