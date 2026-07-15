package handler

import (
	"context"
	"io"
	"time"

	"doc-pipeline-go/internal/model"
	"doc-pipeline-go/internal/worker"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type UploadHandler struct {
	dispatcher *worker.Dispatcher
	rdb        *redis.Client
}

func NewUploadHandler(dispatcher *worker.Dispatcher, rdb *redis.Client) *UploadHandler {
	return &UploadHandler{dispatcher: dispatcher, rdb: rdb}
}

// Upload 上传文件 → 缓存到 Redis → 入队
func (h *UploadHandler) Upload(c *gin.Context) {
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(400, gin.H{"code": 400, "message": "请选择文件"})
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		c.JSON(400, gin.H{"code": 400, "message": "读取文件失败"})
		return
	}

	taskID := uuid.New().String()

	// 缓存文件到 Redis（生产环境用 Minio SDK）
	// ⚠️ Redis 不适合存大文件 (>10MB)，生产环境务必使用 Minio
	ctx := context.Background()
	maxSize := int64(50 * 1024 * 1024) // 50MB
	if header.Size > maxSize {
		c.JSON(413, gin.H{"code": 413, "message": "文件过大，最大支持50MB"})
		return
	}
	h.rdb.Set(ctx, "file:"+taskID, data, 1*time.Hour)

	task := &model.Task{
		ID:           taskID,
		Filename:     header.Filename,
		Size:         header.Size,
		MimeType:     header.Header.Get("Content-Type"),
		Status:       model.StatusPending,
		CurrentStage: model.StageUpload,
		MinioPath:    taskID,
		CreatedAt:    time.Now(),
	}

	// 入队
	h.dispatcher.EnqueueTask(ctx, task)

	c.JSON(201, gin.H{
		"code":    0,
		"message": "文档已提交处理",
		"data": gin.H{
			"task_id":  taskID,
			"filename": task.Filename,
			"status":   "pending",
		},
	})
}
