# ============================================================
# Minecraft 服务器状态监测系统 - 服务端 Dockerfile
# ============================================================
# 多阶段构建：先安装依赖，再复制到精简运行镜像
# 最终镜像约 90MB（python:3.12-slim + FastAPI/uvicorn/httpx/pyyaml）

# ---------- Stage 1: 依赖安装 ----------
FROM python:3.12-slim AS builder

WORKDIR /build

# 先只复制 requirements.txt（利用 Docker 层缓存）
COPY server/requirements.txt .

# 安装依赖到指定目录
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---------- Stage 2: 运行镜像 ----------
FROM python:3.12-slim

LABEL org.opencontainers.image.title="MC服务器状态监测 - 服务端" \
      org.opencontainers.image.description="Minecraft Server Status Monitor - Server" \
      org.opencontainers.image.source="https://github.com/MineJPGcraft/ServerStatusAPI" \
      org.opencontainers.image.licenses="MIT"

WORKDIR /app

# 从 builder 阶段复制已安装的依赖
COPY --from=builder /install /usr/local

# 复制服务端代码和默认配置
COPY server/server.py .
COPY server/config.yaml ./config.yaml

# 创建数据目录（用于挂载外部配置）和非 root 用户
RUN mkdir -p /data && \
    useradd -m -u 1000 mcserver && \
    chown -R mcserver:mcserver /app /data
USER mcserver

# 暴露端口
EXPOSE 8000

# 环境变量默认值
ENV CONFIG_PATH="/app/config.yaml" \
    PYTHONUNBUFFERED="1" \
    PYTHONDONTWRITEBYTECODE="1"

# 健康检查：调用 /api/health 接口
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')" || exit 1

# 入口
CMD ["python", "server.py"]
