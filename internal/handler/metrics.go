package handler

import (
	"doc-pipeline-go/internal/worker"
	"net/http"

	"github.com/gin-gonic/gin"
)

type MetricsHandler struct {
	pool       *worker.Pool
	dispatcher *worker.Dispatcher
}

func NewMetricsHandler(pool *worker.Pool, dispatcher *worker.Dispatcher) *MetricsHandler {
	return &MetricsHandler{pool: pool, dispatcher: dispatcher}
}

// Prometheus 导出 Prometheus 格式指标
func (h *MetricsHandler) Prometheus(c *gin.Context) {
	pending, _ := h.dispatcher.PendingTasks(c.Request.Context())

	c.String(http.StatusOK, `# HELP doc_pipeline_tasks_pending Pending pipeline tasks
# TYPE doc_pipeline_tasks_pending gauge
doc_pipeline_tasks_pending %d
# HELP doc_pipeline_workers Worker count
# TYPE doc_pipeline_workers gauge
doc_pipeline_workers %d
`, pending, 4) // hardcoded for now
}
