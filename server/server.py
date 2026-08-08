#!/usr/bin/env python3
"""
启动：python server.py
多进程：uvicorn server:app --host 0.0.0.0 --port 8000 --workers 4
"""

import asyncio
import logging
import os
import statistics
import time
from collections import OrderedDict, defaultdict
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# 配置加载
CONFIG_PATH = os.environ.get(
    "CONFIG_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"),
)


def load_config() -> dict:
    """加载 YAML 配置文件"""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


config = load_config()

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Server] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mcstatus-server")

# 数据模型 (Pydantic)


class ServerReport(BaseModel):
    """单台服务器的监测报告"""
    ip: str
    online: bool
    players: Optional[dict] = None
    delay: Optional[float] = None
    version: Optional[str] = None
    motd: Optional[dict] = None
    icon: Optional[str] = None


class ReportRequest(BaseModel):
    """节点上报请求体"""
    reports: list[ServerReport]


# LRU 缓存


class LRUCache:
    """带TTL的LRU缓存，用于缓存查询结果"""

    def __init__(self, max_size: int = 10000, ttl: int = 5):
        self.max_size = max_size
        self.ttl = ttl
        self._cache: OrderedDict[str, tuple[float, dict]] = OrderedDict()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[dict]:
        if key not in self._cache:
            self._misses += 1
            return None
        ts, value = self._cache[key]
        if time.time() - ts > self.ttl:
            del self._cache[key]
            self._misses += 1
            return None
        # 移到末尾（最近使用）
        self._cache.move_to_end(key)
        self._hits += 1
        return value

    def set(self, key: str, value: dict):
        self._cache[key] = (time.time(), value)
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_size:
            self._cache.popitem(last=False)

    def invalidate(self, key: str = None):
        """失效单条或全部缓存"""
        if key:
            self._cache.pop(key, None)
        else:
            self._cache.clear()

    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "ttl": self.ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{self._hits / total * 100:.1f}%" if total > 0 else "N/A",
        }


# 令牌桶限流


class TokenBucketRateLimiter:
    """
    令牌桶限流（per-IP）

    - 每个IP独立计数
    - 桶容量 = 突发上限
    - 补充速率 = 每秒令牌数
    """

    def __init__(self, rate_per_minute: int):
        self.capacity = max(rate_per_minute // 6, 10)  # 桶容量≈6秒的量
        self.refill_rate = rate_per_minute / 60.0       # 每秒补充令牌数
        self._buckets: dict[str, tuple[float, float]] = {}  # ip -> (tokens, last_refill)
        self._last_cleanup = time.time()

    def allow(self, key: str) -> bool:
        now = time.time()

        # 每5分钟清理一次不活跃的桶
        if now - self._last_cleanup > 300:
            self._cleanup_stale(now)
            self._last_cleanup = now

        if key not in self._buckets:
            self._buckets[key] = (self.capacity, now)
            return True

        tokens, last_refill = self._buckets[key]
        # 补充令牌
        elapsed = now - last_refill
        tokens = min(self.capacity, tokens + elapsed * self.refill_rate)

        if tokens >= 1:
            tokens -= 1
            self._buckets[key] = (tokens, now)
            return True
        else:
            self._buckets[key] = (tokens, now)
            return False

    def _cleanup_stale(self, now: float):
        """清理超过5分钟未访问的桶"""
        stale = [
            k for k, (_, t) in self._buckets.items()
            if now - t > 300
        ]
        for k in stale:
            del self._buckets[k]


# 数据存储与聚合（预计算）


class DataStorage:
    """
    内存数据存储 — 预计算版

    核心优化：数据写入时立即更新聚合缓存，查询时 O(1) 读取。
    避免了每次查询都遍历+排序+IQR计算的开销。
    """

    def __init__(self, expire_minutes: int):
        self.expire_seconds = expire_minutes * 60
        self.server_ips: set[str] = set()
        self.node_reports: dict[str, list[dict]] = {}  # ip -> [{node_id, timestamp, report}]
        self.active_nodes: dict[str, float] = {}        # node_id -> last_seen_timestamp

        # 预计算的聚合结果缓存: ip -> {timestamp, aggregated_data}
        self._aggregated: dict[str, tuple[float, dict]] = {}

        # 每个IP的延迟历史（用于IQR），避免每次重新提取
        self._delay_history: dict[str, list[tuple[float, float]]] = {}  # ip -> [(timestamp, delay)]

    def update_servers(self, servers_json: dict):
        """从远程JSON更新服务器列表（仅提取有ip的服务器）"""
        old_ips = self.server_ips.copy()
        new_ips: set[str] = set()

        for server in servers_json.get("servers", []):
            ip = server.get("ip")
            if ip and isinstance(ip, str) and ip.strip():
                new_ips.add(ip.strip())

        self.server_ips = new_ips

        # 清除已不在列表中的IP的所有数据
        for ip in old_ips - new_ips:
            self.node_reports.pop(ip, None)
            self._aggregated.pop(ip, None)
            self._delay_history.pop(ip, None)

        added = new_ips - old_ips
        removed = old_ips - new_ips
        if added or removed:
            logger.info(
                f"服务器列表更新: 共 {len(new_ips)} 台 "
                f"(新增 {len(added)}, 移除 {len(removed)})"
            )

    def add_report(self, node_id: str, reports: list[ServerReport]):
        """存入节点上报的监测数据，并即时更新聚合缓存"""
        now = time.time()
        self.active_nodes[node_id] = now

        for report in reports:
            if report.ip not in self.server_ips:
                continue

            ip = report.ip
            if ip not in self.node_reports:
                self.node_reports[ip] = []
            self.node_reports[ip].append({
                "node_id": node_id,
                "timestamp": now,
                "report": report,
            })

            # 即时更新延迟历史
            if report.online and report.delay is not None:
                if ip not in self._delay_history:
                    self._delay_history[ip] = []
                self._delay_history[ip].append((now, report.delay))

            # 即时更新聚合缓存
            self._recompute_aggregation(ip)

    def _recompute_aggregation(self, ip: str):
        """为单个IP重新计算聚合结果（仅在数据变更时调用）"""
        all_reports = self.node_reports.get(ip, [])
        now = time.time()

        # 过滤未过期数据
        recent = [r for r in all_reports if now - r["timestamp"] < self.expire_seconds]

        if not recent:
            self._aggregated[ip] = (now, {"online": False})
            return

        # 按时间倒序排列
        recent.sort(key=lambda r: r["timestamp"], reverse=True)

        # 筛选在线的报告
        online_reports = [r for r in recent if r["report"].online]

        if not online_reports:
            self._aggregated[ip] = (now, {"online": False})
            return

        # 取最新在线报告作为基础数据
        latest_report = online_reports[0]["report"]

        # 聚合延迟：从延迟历史中取有效数据做IQR
        delay_history = self._delay_history.get(ip, [])
        valid_delays = [
            d for ts, d in delay_history
            if now - ts < self.expire_seconds and d is not None and 0 <= d < 10000
        ]
        avg_delay = self._aggregate_delay(valid_delays)

        self._aggregated[ip] = (now, {
            "online": True,
            "players": latest_report.players,
            "delay": avg_delay,
            "version": latest_report.version,
            "motd": latest_report.motd,
            "icon": latest_report.icon,
        })

    def get_status(self, ip: str) -> Optional[dict]:
        """O(1) 查询：直接返回预计算的聚合结果"""
        if ip not in self.server_ips:
            return None

        cached = self._aggregated.get(ip)
        if cached is None:
            return {"online": False}

        ts, data = cached
        # 如果缓存时间超过过期时间，返回离线
        if time.time() - ts > self.expire_seconds:
            return {"online": False}

        return data

    def cleanup(self):
        """清理过期数据 — 分批执行避免长时间阻塞"""
        now = time.time()
        expired_count = 0
        batch_size = 200  # 每批最多处理200个IP
        processed = 0

        for ip in list(self.node_reports.keys()):
            if processed >= batch_size:
                break
            processed += 1

            original = len(self.node_reports[ip])
            self.node_reports[ip] = [
                r for r in self.node_reports[ip]
                if now - r["timestamp"] < self.expire_seconds
            ]
            expired_count += original - len(self.node_reports[ip])

            if not self.node_reports[ip]:
                del self.node_reports[ip]
                self._aggregated.pop(ip, None)
                self._delay_history.pop(ip, None)
            else:
                # 重新计算聚合（因为有些数据被清除了）
                self._recompute_aggregation(ip)

        # 清理延迟历史中的过期数据
        for ip in list(self._delay_history.keys()):
            if ip not in self.node_reports:
                self._delay_history.pop(ip, None)
                continue
            self._delay_history[ip] = [
                (ts, d) for ts, d in self._delay_history[ip]
                if now - ts < self.expire_seconds
            ]
            if not self._delay_history[ip]:
                self._delay_history.pop(ip, None)

        # 清理不活跃的节点
        for node_id in list(self.active_nodes.keys()):
            if now - self.active_nodes[node_id] > self.expire_seconds:
                del self.active_nodes[node_id]

        if expired_count > 0:
            logger.debug(f"清理过期数据: {expired_count} 条 (本轮处理 {processed} 个IP)")

    @staticmethod
    def _aggregate_delay(delays: list[float]) -> Optional[float]:
        """去除异常值后计算平均延迟（IQR方法）"""
        if not delays:
            return None

        if len(delays) <= 2:
            return round(statistics.mean(delays), 2)

        try:
            sorted_delays = sorted(delays)
            q_values = statistics.quantiles(sorted_delays, n=4)
            q1, q3 = q_values[0], q_values[2]
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            filtered = [d for d in delays if lower <= d <= upper]
            if not filtered:
                return round(statistics.mean(delays), 2)
            return round(statistics.mean(filtered), 2)
        except statistics.StatisticsError:
            return round(statistics.mean(delays), 2)

    def stats(self) -> dict:
        """返回存储统计信息"""
        return {
            "monitored_servers": len(self.server_ips),
            "servers_with_data": len(self.node_reports),
            "aggregated_cache_size": len(self._aggregated),
            "active_nodes": len(self.active_nodes),
            "active_node_ids": list(self.active_nodes.keys()),
        }


# 节点认证（带缓存）


class NodeAuthenticator:
    """
    节点认证器 + per-node 配置管理

    - 预构建查找表，O(1) 验证身份
    - 为每个节点合并 node_defaults 和 registered_nodes 中的单独配置
    - 单独配置中未填的字段自动回退到默认值
    """

    # 支持per-node覆盖的配置项
    _OVERRIDABLE_KEYS = ("monitor_interval", "report_interval", "config_refresh_interval")

    def __init__(self, registered_nodes: list[dict], defaults: dict):
        self._tokens: dict[str, str] = {}
        self._node_configs: dict[str, dict] = {}

        # 提取默认值中可覆盖的字段
        default_overrides = {
            k: defaults[k]
            for k in self._OVERRIDABLE_KEYS
            if k in defaults
        }

        for node in registered_nodes:
            node_id = node["id"]
            self._tokens[node_id] = node["token"]

            # 合并：默认值 < 节点单独配置
            merged = dict(default_overrides)
            for key in self._OVERRIDABLE_KEYS:
                if key in node and node[key] is not None:
                    merged[key] = node[key]
            self._node_configs[node_id] = merged

    def verify(self, node_id: str, token: str) -> bool:
        expected = self._tokens.get(node_id)
        if expected is None:
            return False
        return token == expected

    def get_node_config(self, node_id: str) -> Optional[dict]:
        """获取某个节点的合并后配置（默认值 + 单独覆盖）"""
        return self._node_configs.get(node_id)


# 全局实例
storage = DataStorage(config["node_defaults"]["data_expire_minutes"])
authenticator = NodeAuthenticator(
    config["registered_nodes"],
    config["node_defaults"],
)

cache_cfg = config.get("cache", {})
query_cache = LRUCache(
    max_size=cache_cfg.get("max_size", 10000),
    ttl=cache_cfg.get("ttl", 5),
) if cache_cfg.get("enabled", True) else None

rl_cfg = config.get("rate_limit", {})
query_limiter = TokenBucketRateLimiter(
    rl_cfg.get("queries_per_minute", 600)
) if rl_cfg.get("enabled", True) else None
node_limiter = TokenBucketRateLimiter(
    rl_cfg.get("node_requests_per_minute", 120)
) if rl_cfg.get("enabled", True) else None

# 后台任务

# 全局httpx客户端（复用连接池）
_http_client: Optional[httpx.AsyncClient] = None


async def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=config["fetch"].get("timeout", 30),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
    return _http_client


async def fetch_server_list():
    """从远程API获取服务器列表"""
    url = config["fetch"]["url"]
    try:
        client = await get_http_client()
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        storage.update_servers(data)
        # 服务器列表变更后失效全部查询缓存
        if query_cache:
            query_cache.invalidate()
    except httpx.HTTPStatusError as e:
        logger.error(f"获取服务器列表HTTP错误: {e.response.status_code}")
    except httpx.RequestError as e:
        logger.error(f"获取服务器列表网络错误: {e}")
    except Exception as e:
        logger.error(f"获取服务器列表异常: {e}")


async def fetch_server_list_loop():
    """定期获取服务器列表的后台循环"""
    interval = config["fetch"]["interval"]
    await fetch_server_list()
    while True:
        await asyncio.sleep(interval)
        await fetch_server_list()


async def cleanup_loop():
    """定期清理过期数据的后台循环 — 分批执行"""
    while True:
        await asyncio.sleep(60)
        storage.cleanup()
        # 清理后失效查询缓存（因为聚合结果可能变了）
        if query_cache:
            query_cache.invalidate()

# FastAPI 应用


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    fetch_task = asyncio.create_task(fetch_server_list_loop())
    cleanup_task = asyncio.create_task(cleanup_loop())
    logger.info(
        f"服务端启动 -> {config['server']['host']}:{config['server']['port']} | "
        f"列表获取间隔 {config['fetch']['interval']}s | "
        f"数据过期 {config['node_defaults']['data_expire_minutes']}min | "
        f"缓存 {'ON' if query_cache else 'OFF'} | "
        f"限流 {'ON' if query_limiter else 'OFF'}"
    )
    yield
    fetch_task.cancel()
    cleanup_task.cancel()
    for task in [fetch_task, cleanup_task]:
        try:
            await task
        except asyncio.CancelledError:
            pass
    # 关闭HTTP客户端
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
    logger.info("服务端已停止")


app = FastAPI(title="MC服务器状态监测系统", version="2.0.0", lifespan=lifespan)

# CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.get("server", {}).get("cors_origins", ["*"]),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 接口：节点获取配置和服务器列表


@app.get("/api/node/config")
async def get_node_config(
    request: Request,
    x_node_id: str = Header(..., alias="X-Node-Id"),
    x_node_token: str = Header(..., alias="X-Node-Token"),
):
    """
    节点端调用：获取监测配置和服务器IP列表

    返回的配置是 node_defaults 和 registered_nodes 中该节点单独配置的合并结果。
    节点端可进一步用环境变量覆盖返回的值（优先级最高）。
    """
    # 限流
    if node_limiter and not node_limiter.allow(x_node_id):
        raise HTTPException(status_code=429, detail="请求过于频繁")

    if not authenticator.verify(x_node_id, x_node_token):
        raise HTTPException(status_code=401, detail="认证失败：设备ID或Token错误")

    # 获取该节点的合并配置（默认值 + 节点单独覆盖）
    node_cfg = authenticator.get_node_config(x_node_id)

    return {
        "monitor_interval": node_cfg["monitor_interval"],
        "report_interval": node_cfg["report_interval"],
        "config_refresh_interval": node_cfg["config_refresh_interval"],
        "servers": sorted(storage.server_ips),
    }


# 接口：节点上报监测数据


@app.post("/api/node/report")
async def receive_report(
    body: ReportRequest,
    x_node_id: str = Header(..., alias="X-Node-Id"),
    x_node_token: str = Header(..., alias="X-Node-Token"),
):
    """节点端调用：上报本轮监测结果"""
    # 限流
    if node_limiter and not node_limiter.allow(x_node_id):
        raise HTTPException(status_code=429, detail="上报过于频繁")

    if not authenticator.verify(x_node_id, x_node_token):
        raise HTTPException(status_code=401, detail="认证失败：设备ID或Token错误")

    storage.add_report(x_node_id, body.reports)

    # 失效受影响IP的查询缓存
    if query_cache:
        for report in body.reports:
            query_cache.invalidate(report.ip)

    online_count = sum(1 for r in body.reports if r.online)
    logger.info(
        f"节点 {x_node_id} 上报 {len(body.reports)} 条数据 "
        f"(在线 {online_count})"
    )
    return {"status": "ok", "received": len(body.reports)}


# 接口：外部查询服务器状态


@app.get("/")
async def query_server(
    request: Request,
    ip: str = Query(..., description="Minecraft服务器IP地址"),
):
    """
    外部接口：查询服务器状态

    用法: GET http://host:port/?ip=play.example.com
    """
    # 限流（按客户端IP）
    client_ip = request.client.host if request.client else "unknown"
    if query_limiter and not query_limiter.allow(client_ip):
        raise HTTPException(status_code=429, detail="查询过于频繁，请稍后再试")

    # 查缓存
    if query_cache:
        cached = query_cache.get(ip)
        if cached is not None:
            return cached

    # 查存储（O(1)）
    status = storage.get_status(ip)
    if status is None:
        result = JSONResponse(
            status_code=404,
            content={"online": False, "error": "该服务器不在监测列表中"},
        )
        return result

    # 写缓存
    if query_cache:
        query_cache.set(ip, status)

    return status


# 接口：健康检查 / 系统状态


@app.get("/api/health")
async def health():
    """服务端健康检查，返回当前监测概况和缓存/限流统计"""
    return {
        "status": "ok",
        **storage.stats(),
        "cache": query_cache.stats() if query_cache else {"enabled": False},
        "rate_limit": {
            "query": {"enabled": query_limiter is not None},
            "node": {"enabled": node_limiter is not None},
        },
    }


# 启动

if __name__ == "__main__":
    workers = config.get("server", {}).get("workers", 1)
    uvicorn.run(
        "server:app",
        host=config["server"]["host"],
        port=config["server"]["port"],
        log_level="info",
        workers=workers,
    )
