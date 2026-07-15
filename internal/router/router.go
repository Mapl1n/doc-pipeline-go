package router

import (
	"context"
	"doc-pipeline-go/internal/config"
	"doc-pipeline-go/internal/handler"
	"doc-pipeline-go/internal/pipeline"
	"doc-pipeline-go/internal/worker"
	"log"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func Setup(cfg *config.Config) (*gin.Engine, *worker.Pool, *worker.Dispatcher) {
	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, DB: cfg.RedisDB})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis unreachable: %v", err)
	}

	// ES
	es, err := elasticsearch.NewClient(elasticsearch.Config{Addresses: []string{cfg.ESUrl()}})
	if err != nil {
		log.Fatalf("ES: %v", err)
	}

	// Pipeline
	pl := pipeline.New(rdb,
		pipeline.NewParserStage(cfg.TikaURL),
		pipeline.NewClassifyStage(),
		pipeline.NewIndexStage(es, "documents"),
	)

	// Worker Pool
	dispatcher := worker.NewDispatcher(rdb, cfg.StreamName, cfg.ConsumerGroup)
	pool := worker.NewPool(cfg.WorkerCount, pl)
	go pool.Start(context.Background(), dispatcher)

	// Handlers
	uploadH := handler.NewUploadHandler(dispatcher, rdb)
	progressH := handler.NewProgressHandler(pl)
	metricsH := handler.NewMetricsHandler(pool, dispatcher)

	r := gin.Default()
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" { c.AbortWithStatus(204); return }
		c.Next()
	})

	r.GET("/", serveWebUI)
	r.GET("/api/health", func(c *gin.Context) { c.JSON(200, gin.H{"status":"ok"}) })

	api := r.Group("/api")
	{
		api.POST("/upload", uploadH.Upload)
		api.GET("/tasks/:task_id", progressH.GetTask)
		api.GET("/ws/progress", progressH.StreamProgress)
		api.GET("/metrics", metricsH.Prometheus)
		api.GET("/queue/pending", func(c *gin.Context) {
			n, _ := dispatcher.PendingTasks(c.Request.Context())
			c.JSON(200, gin.H{"pending": n})
		})
	}

	return r, pool, dispatcher
}
