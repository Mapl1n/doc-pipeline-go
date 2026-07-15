package router

import (
	"io"
	"log"

	"doc-pipeline-go/internal/config"
	"doc-pipeline-go/internal/handler"
	"doc-pipeline-go/internal/pipeline"

	"github.com/gin-gonic/gin"
)

type StandaloneHandlers struct {
	Upload   gin.HandlerFunc
	Task     gin.HandlerFunc
	WS       gin.HandlerFunc
	Metrics  gin.HandlerFunc
}

func Setup(cfg *config.Config) (*gin.Engine, *pipeline.StandalonePipeline) {
	// Use standalone pipeline (zero external dependencies)
	sp := pipeline.NewStandalonePipeline()
	log.Println("[PIPELINE] running in standalone mode (in-memory queue + local store)")

	progressH := handler.NewProgressHandler(sp)

	r := gin.Default()
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" { c.AbortWithStatus(204); return }
		c.Next()
	})

	r.GET("/", serveWebUI)
	r.GET("/api/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok", "mode": "standalone"}) })

	api := r.Group("/api")
	{
		api.POST("/upload", func(c *gin.Context) {
			file, header, err := c.Request.FormFile("file")
			if err != nil {
				c.JSON(400, gin.H{"code": 400, "message": "请选择文件"})
				return
			}
			defer file.Close()
			data, _ := io.ReadAll(file)
			task := sp.Submit(header.Filename, data, header.Header.Get("Content-Type"))
			c.JSON(201, gin.H{"code": 0, "message": "文档已提交", "data": gin.H{
				"task_id": task.ID, "filename": task.Filename, "status": "pending",
			}})
		})

		api.GET("/tasks/:task_id", func(c *gin.Context) {
			task := sp.GetTask(c.Param("task_id"))
			if task == nil {
				c.JSON(404, gin.H{"message": "not found"})
				return
			}
			c.JSON(200, gin.H{"code": 0, "data": task})
		})

		api.GET("/ws/progress", progressH.StreamProgress)

		api.GET("/metrics", func(c *gin.Context) {
			c.String(200, "# HELP doc_pipeline_tasks_pending Pending tasks\n# TYPE doc_pipeline_tasks_pending gauge\ndoc_pipeline_tasks_pending %d\n", sp.PendingCount())
		})
		api.GET("/queue/pending", func(c *gin.Context) {
			c.JSON(200, gin.H{"pending": sp.PendingCount()})
		})
		api.GET("/documents", func(c *gin.Context) {
			docs := sp.LocalStore().ListAll()
			c.JSON(200, gin.H{"code": 0, "data": docs})
		})
	}

	return r, sp
}
