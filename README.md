# 🔄 分布式文档处理流水线

基于 **Go + Redis Stream + Worker Pool** 的文档异步处理引擎。上传 → 解析 → 分类 → 索引全链路自动化，WebSocket 实时推送进度。

![Go](https://img.shields.io/badge/Go-1.23-00ADD8?logo=go)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## 💡 一句话说清

海量文档处理最怕"卡住不知道进度"。这个系统用 **Redis Stream 任务队列 + Go Worker Pool 并发处理 + WebSocket 实时进度推送**，每份文档的处理状态和进度可视化、可追踪、失败自动重试。

## 🎯 核心特性

| 能力 | 实现 |
|------|------|
| 📥 **异步处理** | 上传即返回，后台 Worker 异步执行流水线，不阻塞用户 |
| 🔗 **Pipeline 编排** | Parser → OCR → Classify → Index 四阶段流水线 |
| 👷 **Worker Pool** | 可配并发数，N 个 Worker 并行消费 Redis Stream |
| 📡 **WebSocket 进度** | 前端实时看到每阶段进度：解析中 → 分类中 → 索引中 → 完成 |
| 🔄 **断点重试** | 任务失败自动重试 3 次，Consumer Group 保证不丢消息 |
| 🏷️ **自动分类** | 规则引擎识别档案类型：合同/报告/简历/发票/法律文书 |
| 📊 **Prometheus** | 暴露指标端点，可接入 Grafana 监控 |
| ⚡ **零依赖运行** | 无 Redis/ES 时降级为 Go channel 队列 + 内存存储 |

## 🚀 快速开始

### 零依赖模式

```bash
git clone https://github.com/Mapl1n/doc-pipeline-go.git
cd doc-pipeline-go
go run ./cmd/server
# 打开 http://localhost:8082
```

上传文档 → Web 界面实时看到流水线动画进度。

### 完整模式（生产级）

```bash
docker compose up -d   # 启动 Redis + ES + Tika
go run ./cmd/server
```

## 📡 API 端点

```
POST /api/upload             上传文档 → 返回 task_id → 后台处理
GET  /api/tasks/:task_id     查询任务状态（轮询模式）
GET  /api/ws/progress         WebSocket 实时进度推送
GET  /api/metrics             Prometheus 指标
GET  /api/queue/pending      待处理任务数
GET  /api/health             服务健康检查
```

## 🏗️ 流水线架构

```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│  Upload  │ →  │  Parse   │ →  │ Classify │ →  │  Index   │
│  (Minio) │    │  (Tika)  │    │ (规则引擎)│    │  (ES)    │
└──────────┘    └──────────┘    └──────────┘    └──────────┘
      ↓              ↓              ↓              ↓
┌─────────────────────────────────────────────────────────┐
│  Redis Stream (任务队列)  ←  Consumer Group              │
│  Worker 1   Worker 2   Worker 3   Worker 4              │
│    ↓           ↓          ↓          ↓                  │
│  fetch → process → ack → next                           │
│                                                         │
│  WebSocket ←→ 前端实时进度动画                            │
└─────────────────────────────────────────────────────────┘
```

## 🔧 技术栈

| 组件 | 用途 | 降级方案 |
|------|------|---------|
| **Redis Stream** | 任务队列 + Consumer Group | Go channel |
| **Apache Tika** | 文档内容提取 | 直接读文本 |
| **ElasticSearch** | 全文索引 | 内存 map 存储 |
| **Gorilla WebSocket** | 实时进度推送 | HTTP 轮询 |
| **Worker Pool** | 并发处理控制 | 单 goroutine |

---

## 📝 License

MIT
