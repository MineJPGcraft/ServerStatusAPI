# ============================================================
# Minecraft 服务器状态监测系统 - 节点端 Dockerfile
# ============================================================
# 多阶段构建：先安装依赖到虚拟环境，再复制到精简运行镜像
# 最终镜像约 80MB（python:3.12-slim + mcstatus 依赖）

# ---------- Stage 1: 依赖安装 ----------
FROM python:3.12-slim AS builder

# 设置工作目录
WORKDIR /build

# 先只复制 requirements.txt（利用 Docker 层缓存）
COPY node/requirements.txt .

# 安装依赖到指定目录（便于后续复制）
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---------- Stage 2: 运行镜像 ----------
FROM python:3.12-slim

# 安装最小运行时依赖（mcstatus 可能需要）
# tshark/bpf 已不需要，保持最小化
LABEL org.opencontainers.image.title="MC服务器状态监测 - 节点端" \
      org.opencontainers.image.description="Minecraft Server Status Monitor - Node" \
      org.opencontainers.image.source="https://github.com/MineJPGcraft/ServerStatusAPI" \
      org.opencontainers.image.licenses="MIT"

# 设置工作目录
WORKDIR /app

# 从 builder 阶段复制已安装的依赖
COPY --from=builder /install /usr/local

# 复制节点端代码
COPY node/node.py .
COPY node/.env.example .env.example

# 创建非 root 用户运行（安全最佳实践）
RUN useradd -m -u 1000 mcnode && chown -R mcnode:mcnode /app
USER mcnode

# 环境变量默认值（可通过 docker run -e 或 docker-compose 覆盖）
ENV SERVER_URL="https://serverstatusapi.mcjpg.org" \
    DEVICE_ID="" \
    TOKEN="" \
    MONITOR_INTERVAL="" \
    REPORT_INTERVAL="" \
    CONFIG_REFRESH_INTERVAL="" \
    MAX_CONCURRENCY="50" \
    SERVER_TIMEOUT="10" \
    OFFLINE_BACKOFF="3" \
    THREAD_POOL_SIZE="100" \
    PYTHONUNBUFFERED="1" \
    PYTHONDONTWRITEBYTECODE="1"

# 健康检查：通过尝试导入模块验证运行环境正常
HEALTHCHECK --interval=60s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import mcstatus; import httpx" || exit 1

# 入口
CMD ["python", "node.py"]
