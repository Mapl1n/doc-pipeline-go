package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	ServerPort  string
	AppEnv      string
	TikaURL     string
	MinioEndpoint string
	MinioAccessKey string
	MinioSecretKey string
	MinioBucket   string
	MinioUseSSL   bool
	RedisAddr     string
	RedisDB       int
	ESHost        string
	ESPort        string
	WorkerCount   int
	MaxRetries    int
	StreamName    string
	ConsumerGroup string
}

func Load() *Config {
	return &Config{
		ServerPort:     getEnv("SERVER_PORT", "8082"),
		AppEnv:         getEnv("APP_ENV", "development"),
		TikaURL:        getEnv("TIKA_URL", "http://localhost:9998"),
		MinioEndpoint:  getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey: getEnv("MINIO_ACCESS_KEY", "minioadmin"),
		MinioSecretKey: getEnv("MINIO_SECRET_KEY", "minioadmin"),
		MinioBucket:    getEnv("MINIO_BUCKET", "documents"),
		MinioUseSSL:    false,
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		RedisDB:        getEnvInt("REDIS_DB", 0),
		ESHost:         getEnv("ES_HOST", "localhost"),
		ESPort:         getEnv("ES_PORT", "9200"),
		WorkerCount:    getEnvInt("WORKER_COUNT", 4),
		MaxRetries:     getEnvInt("MAX_RETRIES", 3),
		StreamName:     getEnv("STREAM_NAME", "doc:pipeline"),
		ConsumerGroup:  getEnv("CONSUMER_GROUP", "doc-workers"),
	}
}

func (c *Config) ESUrl() string { return fmt.Sprintf("http://%s:%s", c.ESHost, c.ESPort) }

func getEnv(k, d string) string { if v := os.Getenv(k); v != "" { return v }; return d }
func getEnvInt(k string, d int) int { if v := os.Getenv(k); v != "" { if n, e := strconv.Atoi(v); e == nil { return n } }; return d }
