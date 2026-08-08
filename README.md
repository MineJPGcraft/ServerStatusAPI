# Minecraft 服务器状态监测系统

一个基于 Python 的 Minecraft 服务器状态监测系统，采用 **服务端 + 节点端** 分布式架构。

## 架构概览

```
                    ┌──────────────────────────────────────────────┐
                    │                  服务端 (Server)               │
                    │                                              │
  远程API ──────────▶  定期获取服务器列表                              │
  (getjson)         │                                              │
                    │  ┌──────────┐   ┌──────────┐   ┌──────────┐  │
                    │  │ 数据聚合  │   │ 节点认证  │   │ 过期清理  │  │
                    │  └──────────┘   └──────────┘   └──────────┘  │
                    │       ▲              ▲                        │
                    │       │              │                        │
  外部查询 ◀────────│  GET /?ip=xxx        │                        │
  (返回状态)         │                      │                        │
                    └──────────────────────┼────────────────────────┘
                                           │
                    ┌──────────────────────┼────────────────────────┐
                    │              节点端 (Node)                     │
                    │                      │                        │
                    │  ◀──── 拉取配置和服务器列表 (GET /api/node/config)
                    │  ────── 上报监测数据 (POST /api/node/report) ──▶│
                    │                                              │
                    │  ┌──────────┐  ┌──────────┐  ┌──────────┐    │
                    │  │ 监测循环  │  │ 上报循环  │  │ 配置循环  │    │
                    │  └────┬─────┘  └──────────┘  └──────────┘    │
                    │       │                                      │
                    │       ▼                                      │
                    │  mcstatus → MC服务器1, MC服务器2, ...          │
                    └──────────────────────────────────────────────┘
```

## 目录结构

```
ServerStatusAPI/
├── .github/
│   └── workflows/
│       ├── docker-node.yml       # 节点端 Docker 镜像自动构建工作流
│       └── docker-server.yml     # 服务端 Docker 镜像自动构建工作流
├── server/                       # 服务端
│   ├── server.py                 # 服务端主程序
│   ├── Dockerfile                # 服务端 Docker 镜像构建文件
│   ├── config.yaml               # 服务端配置文件
│   └── requirements.txt          # 服务端依赖
├── node/                         # 节点端
│   ├── node.py                   # 节点端主程序
│   ├── Dockerfile                # 节点端 Docker 镜像构建文件
│   ├── .env.example              # 环境变量示例
│   └── requirements.txt          # 节点端依赖
├── .dockerignore                 # Docker 构建忽略文件
├── docker-compose.yml            # 服务端+节点端 Docker Compose 部署文件
├── start_server.bat              # Windows 启动服务端脚本
├── start_node.bat                # Windows 启动节点端脚本
└── README.md
```

## 快速开始

### 1. 服务端部署

```bash
cd server
python -m venv venv
# Windows
call venv\Scripts\activate.bat
# Linux/Mac
# source venv/bin/activate

pip install -r requirements.txt
```

编辑 `config.yaml`：
- 修改 `registered_nodes` 中的节点ID和Token
- 调整 `fetch.interval`（服务器列表获取间隔）
- 调整 `node_defaults` 中的监测/上报间隔

启动：
```bash
python server.py
# 或使用批处理
..\start_server.bat
```

服务端启动后监听 `http://0.0.0.0:8000`

### 1.5 服务端 Docker 部署（推荐）

> 镜像由 GitHub Actions 自动构建并发布到 GHCR。

#### 方式一：Docker Compose（推荐，含节点端一体化部署）

```bash
# 1. 准备配置目录
mkdir -p config
cp server/config.yaml config/server-config.yaml
# 编辑配置：修改 registered_nodes 中的节点ID和Token
vi config/server-config.yaml

# 2. 编辑 docker-compose.yml 中的节点端环境变量
vi docker-compose.yml

# 3. 启动（服务端 + 节点端）
docker compose up -d

# 4. 查看日志
docker compose logs -f mcstatus-server
docker compose logs -f mcstatus-node

# 5. 停止
docker compose down
```

#### 方式二：仅部署服务端

```bash
# 1. 准备配置文件
mkdir -p config
cp server/config.yaml config/server-config.yaml
vi config/server-config.yaml  # 修改节点ID和Token

# 2. 启动
docker run -d \
  --name mcstatus-server \
  --restart unless-stopped \
  -p 8000:8000 \
  -v $(pwd)/config/server-config.yaml:/app/config.yaml:ro \
  -e CONFIG_PATH=/app/config.yaml \
  ghcr.io/mcjpg/serverstatusapi-server:latest

# 3. 验证
curl http://localhost:8000/api/health
```

### 2. 节点端部署

```bash
cd node
python -m venv venv
# Windows
call venv\Scripts\activate.bat
# Linux/Mac
# source venv/bin/activate

pip install -r requirements.txt
```

创建 `.env` 文件（参考 `.env.example`）：
```env
SERVER_URL=http://你的服务端地址:8000
DEVICE_ID=node-001
TOKEN=change-me-please
```

启动：
```bash
python node.py
# 或使用批处理
..\start_node.bat
```

### 3. 节点端 Docker 部署（推荐）

> 镜像由 GitHub Actions 自动构建并发布到 GHCR，每次推送到 main 分支或打 tag 时自动更新。

#### 方式一：Docker Compose（推荐）

```bash
# 1. 下载 docker-compose.yml
curl -O https://raw.githubusercontent.com/minejpgcraft/ServerStatusAPI/main/docker-compose.yml

# 2. 编辑环境变量
vi docker-compose.yml
# 修改 SERVER_URL / DEVICE_ID / TOKEN

# 3. 启动
docker compose up -d

# 4. 查看日志
docker compose logs -f

# 5. 停止
docker compose down
```

#### 方式二：Docker Run

```bash
docker run -d \
  --name mcstatus-node \
  --restart unless-stopped \
  -e SERVER_URL="http://your-server:8000" \
  -e DEVICE_ID="node-001" \
  -e TOKEN="change-me-please" \
  -e MAX_CONCURRENCY="50" \
  -e SERVER_TIMEOUT="10" \
  ghcr.io/mcjpg/serverstatusapi-node:latest
```

#### 镜像标签说明

| 镜像 | 标签 | 说明 | 示例 |
|------|------|------|------|
| 服务端 | `latest` | main/master 分支最新 | `ghcr.io/mcjpg/serverstatusapi-server:latest` |
| 服务端 | `v1.0.0` | 精确版本号 | `ghcr.io/mcjpg/serverstatusapi-server:v1.0.0` |
| 服务端 | `1.0` | 主次版本号 | `ghcr.io/mcjpg/serverstatusapi-server:1.0` |
| 服务端 | `sha-abc123` | 特定 commit | `ghcr.io/mcjpg/serverstatusapi-server:sha-abc123` |
| 节点端 | `latest` | main/master 分支最新 | `ghcr.io/mcjpg/serverstatusapi-node:latest` |
| 节点端 | `v1.0.0` | 精确版本号 | `ghcr.io/mcjpg/serverstatusapi-node:v1.0.0` |
| 节点端 | `1.0` | 主次版本号 | `ghcr.io/mcjpg/serverstatusapi-node:1.0` |
| 节点端 | `sha-abc123` | 特定 commit | `ghcr.io/mcjpg/serverstatusapi-node:sha-abc123` |

#### 支持的平台

| 平台 | 架构 |
|------|------|
| `linux/amd64` | x86_64（大多数服务器/VPS） |
| `linux/arm64` | ARM64（树莓派/Apple Silicon/ARM服务器） |

## API 接口文档

### 外部查询接口

**请求：**
```
GET http://服务端地址:8000/?ip=play.example.com
```

**成功响应（服务器在线）：**
```json
{
  "online": true,
  "players": {"online": 5, "max": 20},
  "delay": 43.6,
  "version": "1.20.1",
  "motd": {
    "plain": "A Minecraft Server",
    "html": "<p>A Minecraft Server</p>",
    "minecraft": "A Minecraft Server",
    "ansi": "\u001b[0mA Minecraft Server\u001b[0m"
  },
  "icon": "data:image/png;base64,..."
}
```

**失败响应（服务器离线）：**
```json
{"online": false}
```

**失败响应（不在监测列表）：**
```json
{"online": false, "error": "该服务器不在监测列表中"}
```
（HTTP 404）

### 节点配置接口

```
GET /api/node/config
Headers: X-Node-Id, X-Node-Token
```

返回：
```json
{
  "monitor_interval": 30,
  "report_interval": 60,
  "config_refresh_interval": 60,
  "servers": ["play.example.com", "play2.example.com:25565"]
}
```

### 节点上报接口

```
POST /api/node/report
Headers: X-Node-Id, X-Node-Token
Body: {"reports": [{"ip": "...", "online": true, ...}, ...]}
```

### 健康检查接口

```
GET /api/health
```

返回服务端当前监测概况、缓存/限流统计、配置状态。

### 节点状态接口

```
GET /api/nodes/status
```

检查所有注册节点的在线/离线状态。在线标准：节点在 `data_expire_minutes`（默认10分钟）内有上报数据。

返回：
```json
{
  "online": ["node-001", "node-002"],
  "offline": ["node-003"]
}
```

无需鉴权，外部可直接调用。

### 手动配置重载接口

```
POST /api/config/reload
```

手动触发配置文件重载。通常无需调用，服务端会自动检测文件变更。

## 数据聚合策略

服务端收到多个节点的上报数据后，按以下规则聚合：

| 数据项   | 聚合策略                                       |
|---------|------------------------------------------------|
| 延迟     | 去除异常值（IQR方法）后取所有在线报告的平均值       |
| 在线人数  | 取最后上报节点的数据                               |
| 最大人数  | 取最后上报节点的数据                               |
| 版本     | 取最后上报节点的数据                               |
| MOTD    | 取最后上报节点的数据                               |
| 图标     | 取最后上报节点的数据                               |
| 在线状态  | 取最后上报节点的数据                               |

每个节点的数据 **10分钟后自动丢弃**（可在配置文件中调整）。

## 配置说明

### 配置优先级（三级覆盖机制）

节点的三个间隔参数（监测/上报/配置刷新）支持三级优先级覆盖：

```
优先级（从高到低）：
  1. 节点端环境变量     ← 最高优先级，设置后始终覆盖
  2. 服务端节点单独配置  ← registered_nodes 中为该节点单独设置的值
  3. 服务端全局默认值    ← node_defaults 中的值，所有节点的兜底默认
```

```
┌─────────────────────────────────────────────────────────────────┐
│                        配置决策流程                               │
│                                                                 │
│  节点启动                                                        │
│    │                                                            │
│    ▼                                                            │
│  读取环境变量 MONITOR_INTERVAL / REPORT_INTERVAL / ...           │
│    │                                                            │
│    ├─ 已设置 → 固定使用此值（最高优先级）                           │
│    │                                                            │
│    └─ 未设置 → 从服务端拉取                                       │
│                 │                                               │
│                 ▼                                               │
│               服务端合并配置                                       │
│                 │                                               │
│                 ├─ registered_nodes 中该节点有单独配置？            │
│                 │    ├─ 是 → 使用单独配置的值                       │
│                 │    └─ 否 → 回退到 node_defaults 默认值            │
│                 │                                               │
│                 ▼                                               │
│               返回合并后的配置给节点                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 服务端 config.yaml

#### 全局配置

| 配置项 | 说明 | 默认值 |
|-------|------|-------|
| `server.host` | 监听地址 | `0.0.0.0` |
| `server.port` | 监听端口 | `8000` |
| `server.workers` | uvicorn worker进程数 | `1` |
| `server.cors_origins` | CORS 允许的来源 | `["*"]` |
| `fetch.url` | 服务器列表API地址 | `https://server-editor-api.mcjpg.dev/api/getjson` |
| `fetch.interval` | 列表获取间隔（秒） | `300` |
| `fetch.timeout` | 请求超时（秒） | `30` |
| `cache.enabled` | 是否启用查询缓存 | `true` |
| `cache.ttl` | 缓存有效期（秒） | `5` |
| `cache.max_size` | 最大缓存条目数 | `10000` |
| `rate_limit.enabled` | 是否启用限流 | `true` |
| `rate_limit.queries_per_minute` | 外部查询限流（次/分钟） | `600` |
| `rate_limit.node_requests_per_minute` | 节点请求限流（次/分钟） | `120` |

#### 节点默认配置（`node_defaults`）

所有节点的全局默认值，优先级最低：

| 配置项 | 说明 | 默认值 |
|-------|------|-------|
| `node_defaults.monitor_interval` | 监测间隔（秒） | `30` |
| `node_defaults.report_interval` | 上报间隔（秒） | `60` |
| `node_defaults.config_refresh_interval` | 配置刷新间隔（秒） | `60` |
| `node_defaults.data_expire_minutes` | 数据过期时间（分钟） | `10` |

#### 节点单独配置（`registered_nodes`）

每个节点必须包含 `id` 和 `token`，可选覆盖三个间隔参数：

```yaml
registered_nodes:
  - id: "node-001"
    token: "change-me-please"
    # 不填任何 interval → 全部走 node_defaults

  - id: "node-002"
    token: "change-me-too"
    monitor_interval: 15           # 仅覆盖监测间隔，其余走默认

  - id: "node-003"
    token: "node-003-token"
    monitor_interval: 10
    report_interval: 30
    config_refresh_interval: 120   # 全部自定义
```

### 节点端环境变量

#### 必填项

| 变量名 | 说明 | 示例 |
|-------|------|------|
| `SERVER_URL` | 服务端地址 | `http://localhost:8000` |
| `DEVICE_ID` | 节点设备ID | `node-001` |
| `TOKEN` | 节点Token | `change-me-please` |

#### 可选项（覆盖服务端配置，优先级最高）

| 变量名 | 说明 | 示例 | 不设置时 |
|-------|------|------|---------|
| `MONITOR_INTERVAL` | 监测间隔（秒） | `15` | 使用服务端下发值 |
| `REPORT_INTERVAL` | 上报间隔（秒） | `30` | 使用服务端下发值 |
| `CONFIG_REFRESH_INTERVAL` | 配置刷新间隔（秒） | `120` | 使用服务端下发值 |

#### 可选项（性能调优）

| 变量名 | 说明 | 默认值 |
|-------|------|-------|
| `MAX_CONCURRENCY` | 最大并发检测数 | `50` |
| `SERVER_TIMEOUT` | 单台服务器检测超时（秒） | `10` |
| `OFFLINE_BACKOFF` | 离线服务器退避倍数 | `3` |

#### 配置示例

**场景1：完全由服务端控制**
```env
SERVER_URL=http://localhost:8000
DEVICE_ID=node-001
TOKEN=change-me-please
# 不设置任何 INTERVAL 环境变量 → 使用服务端 config.yaml 中的配置
```

**场景2：节点端强制覆盖监测间隔**
```env
SERVER_URL=http://localhost:8000
DEVICE_ID=node-002
TOKEN=change-me-too
MONITOR_INTERVAL=10    # 无论服务端怎么配，此节点每10秒监测一次
```

**场景3：全部由环境变量控制**
```env
SERVER_URL=https://serverstatusapi.mcjpg.org
DEVICE_ID=node-003
TOKEN=node-003-token
MONITOR_INTERVAL=10
REPORT_INTERVAL=30
CONFIG_REFRESH_INTERVAL=120
```

## 配置热重载

服务端支持 **配置文件热重载**，修改 `config.yaml` 后保存即可，无需重启服务。

### 工作原理

```
保存 config.yaml
       │
       ▼
  服务端后台监听任务（每3秒检查文件 mtime）
       │
       ├─ 文件未变化 → 继续等待
       │
       └─ 文件已变化 → 重新加载 YAML
                        │
                        ▼
                    对比新旧配置差异
                        │
                        ▼
                    更新所有依赖对象：
                    ├── authenticator（节点列表 + Token + per-node 配置）
                    ├── storage（数据过期时间）
                    ├── query_cache（缓存开关 + TTL + 容量）
                    ├── query_limiter（查询限流参数）
                    ├── node_limiter（节点限流参数）
                    └── 失效全部查询缓存
                        │
                        ▼
                    日志输出变更项
```

### 支持热重载的配置项

| 配置项 | 热重载效果 |
|-------|-----------|
| `registered_nodes` | 新增/删除/修改节点立即生效，新节点可马上连接，旧节点Token失效 |
| `node_defaults.monitor_interval` | 节点下次拉取配置时获取新值 |
| `node_defaults.report_interval` | 同上 |
| `node_defaults.config_refresh_interval` | 同上 |
| `node_defaults.data_expire_minutes` | 立即更新过期时间，下次 cleanup 按新值执行 |
| `cache.enabled` / `cache.ttl` / `cache.max_size` | 立即生效，同时清空旧缓存 |
| `rate_limit.*` | 立即生效，重建限流器 |
| `fetch.url` / `fetch.interval` | 下轮获取时使用新值 |
| `server.host` / `server.port` | ⚠️ 不支持热重载，需重启 |
| `server.workers` | ⚠️ 不支持热重载，需重启 |

### 使用方式

**方式一：自动检测（推荐）**

直接修改并保存 `config.yaml`，服务端在 3 秒内自动检测并应用。

```bash
# 编辑配置
vi config.yaml
# 保存后查看服务端日志
tail -f /var/log/mcstatus-server.log
# 会看到：配置热重载完成，变更项: registered_nodes: 新增节点: {'node-004'}
```

**方式二：手动触发**

```bash
# 调用 API 手动触发重载
curl -X POST http://localhost:8000/api/config/reload
# {"status":"ok","message":"配置已重载"}
```

### Docker 环境下的热重载

Docker 部署时，将配置文件挂载为卷即可支持热重载：

```yaml
# docker-compose.yml
volumes:
  - ./config/server-config.yaml:/app/config.yaml:ro
```

```bash
# 修改宿主机上的配置文件
vi ./config/server-config.yaml

# 容器内的服务端会自动检测到文件变化并热重载
# 无需 docker restart
```

> **注意**：Docker 环境下某些挂载方式（如 NFS）可能不实时传播 mtime 变更。
> 如果自动检测不生效，使用 `curl -X POST /api/config/reload` 手动触发。

## 高并发优化

服务端内置多项高并发优化设计：

| 优化项 | 说明 |
|-------|------|
| **查询结果缓存** | LRU+TTL缓存，5秒内重复查询直接返回缓存结果，避免重复聚合计算 |
| **聚合预计算** | 数据写入时即时计算聚合结果，查询时 O(1) 读取，不再遍历排序 |
| **令牌桶限流** | 外部查询和节点请求分别限流，防止恶意请求打满CPU |
| **节点认证缓存** | 预构建查找表，O(1) 验证，不再每次遍历配置列表 |
| **分片清理** | cleanup 每批最多处理200个IP，避免长时间阻塞事件循环 |
| **连接池复用** | httpx 全局客户端复用连接池，减少TCP握手开销 |
| **CORS 支持** | 内置CORS中间件，前端可直接跨域调用 |
| **多 Worker** | 支持多进程部署：`uvicorn server:app --workers 4` |
| **配置热重载** | 自动检测 config.yaml 变更并实时应用，无需重启服务 |

### 性能预估

| 场景 | 预估QPS | 瓶颈 |
|------|---------|------|
| 单进程 + 缓存命中 | ~5000+ | 事件循环调度 |
| 单进程 + 缓存未命中 | ~2000+ | 内存读取 |
| 4 Worker + 缓存 | ~15000+ | CPU核心数 |
| 节点上报（100台服务器） | ~100/s | Pydantic解析 |

### 多进程部署

```bash
# 4 worker 进程
uvicorn server:app --host 0.0.0.0 --port 8000 --workers 4

# 或在 config.yaml 中设置
# server:
#   workers: 4
```

> **注意**：多Worker模式下各进程内存独立，数据不共享。如需跨进程共享状态，
> 可引入 Redis 作为共享存储层。当前单进程设计已可满足多数场景需求。

## CI/CD 自动构建

服务端和节点端的 Docker 镜像均通过 GitHub Actions 自动构建并发布到 GHCR。

### 工作流概览

| 工作流 | 文件 | 触发路径 | 镜像后缀 |
|-------|------|---------|---------|
| 服务端 | `.github/workflows/docker-server.yml` | `server/**` | `-server` |
| 节点端 | `.github/workflows/docker-node.yml` | `node/**` | `-node` |

### 工作流触发条件

| 触发方式 | 条件 | 产出标签 |
|---------|------|---------|
| 推送 main/master | `server/` 或 `node/` 目录有变更 | `latest` + `sha-xxx` |
| 推送 tag | tag 格式为 `v*`（如 `v1.0.0`） | `1.0.0` + `1.0` + `1` + `sha-xxx` |
| 手动触发 | Actions 页面 → Run workflow | 同 main 分支 |

### 工作流特性

- **多平台构建**：同时构建 `linux/amd64` + `linux/arm64`
- **层缓存加速**：使用 GitHub Actions 缓存，后续构建更快
- **并发取消**：同分支新构建自动取消旧的，节省资源
- **自动标签**：根据分支/tag 自动生成语义化版本标签
- **安全认证**：使用内置 `GITHUB_TOKEN`，无需额外配置密钥

### 首次使用前需要做的

1. **确保仓库 Packages 权限已开启**
   - Settings → Actions → General → Workflow permissions → Read and write permissions

2. **首次推送后到 GHCR 确认镜像可见性**
   - 服务端：`https://github.com/users/<owner>/packages/container/<owner>/serverstatusapi-server`
   - 节点端：`https://github.com/users/<owner>/packages/container/<owner>/serverstatusapi-node`
   - Package settings → Change visibility → Public（如需公开）

3. **拉取镜像**
   ```bash
   # 公开镜像无需登录
   docker pull ghcr.io/minejpgcraft/serverstatusapi-server:latest
   docker pull ghcr.io/minejpgcraft/serverstatusapi-node:latest

   # 私有镜像需先登录
   echo $GITHUB_TOKEN | docker login ghcr.io -u <username> --password-stdin
   docker pull ghcr.io/minejpgcraft/serverstatusapi-server:latest
   docker pull ghcr.io/minejpgcraft/serverstatusapi-node:latest
   ```

### 发布新版本

```bash
# 打 tag 触发版本构建（服务端和节点端同时构建）
git tag v1.0.0
git push origin v1.0.0

# 两个工作流自动并行构建并推送：
#   ghcr.io/minejpgcraft/serverstatusapi-server:1.0.0 / 1.0 / 1 / sha-xxx
#   ghcr.io/minejpgcraft/serverstatusapi-node:1.0.0  / 1.0 / 1 / sha-xxx
```

## 技术栈

- **服务端**: FastAPI + Uvicorn + httpx + PyYAML
- **节点端**: asyncio + httpx + mcstatus + dnspython + python-dotenv
- **Python**: >= 3.9

## 节点端高并发优化

节点端针对大量服务器（100~1000+台）的并发监测做了以下优化：

| 优化项 | 说明 | 默认值 |
|-------|------|-------|
| **高并发检测** | 可配置的最大并发数 | `MAX_CONCURRENCY=50` |
| **单服务器超时** | 每台服务器检测带超时控制，防止死服务器阻塞并发槽 | `SERVER_TIMEOUT=10` |
| **离线退避** | 已知离线的服务器按倍数降低检测频率，减少无效请求 | `OFFLINE_BACKOFF=3` |
| **专用线程池** | mcstatus 同步调用使用独立线程池，大小自动计算（并发×4） | 自动 |
| **SRV 记录解析** | 不含端口的 IP 先用 dnspython 手动解析 SRV 记录拼接端口，解析不出才传原始地址 | 自动 |
| **精确间隔** | sleep = interval - elapsed，确保实际周期 = 配置间隔 | 自动 |
| **结果保留** | 被退避跳过的服务器保留上一次结果，上报时数据完整 | 自动 |

### 离线退避策略

```
服务器状态      失败次数    跳过轮数    实际检测频率
─────────────────────────────────────────────────
在线            0           0           每轮都检测
离线（第1次）    1           1           每2轮检测1次
离线（第3次）    3           2           每3轮检测1次
离线（第5次）    5           3           每4轮检测1次
离线（第7次+）   7+          3（上限）    每4轮检测1次

上限 = OFFLINE_BACKOFF 值（默认3）
```

### 性能预估

| 服务器数量 | 并发数 | 预估单轮耗时 | 说明 |
|-----------|--------|-------------|------|
| 50台 | 50 | ~10s | 全部并发，一轮约等于单台超时 |
| 200台 | 50 | ~40s | 分4批，每批约10s |
| 500台 | 50 | ~100s | 分10批，离线退避后实际更少 |
| 500台 | 100 | ~50s | 调高并发，配合更大线程池 |
| 1000台 | 100 | ~100s | 离线退避生效后实际检测量减少 |

### 性能调优建议

```env
# 小规模（<50台）：默认即可
MAX_CONCURRENCY=50
SERVER_TIMEOUT=10

# 中规模（50~200台）：适当调高并发
MAX_CONCURRENCY=80
SERVER_TIMEOUT=8

# 大规模（200+台）：高并发 + 短超时 + 强退避
MAX_CONCURRENCY=100
SERVER_TIMEOUT=5
OFFLINE_BACKOFF=5
```
