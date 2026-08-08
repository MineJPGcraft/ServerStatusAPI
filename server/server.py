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
from collections import OrderedDict
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import uvicorn
import yaml
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# 配置路径
CONFIG_PATH = os.environ.get(
    "CONFIG_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"),
)


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
        self._cache.move_to_end(key)
        self._hits += 1
        return value

    def set(self, key: str, value: dict):
        self._cache[key] = (time.time(), value)
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_size:
            self._cache.popitem(last=False)

    def invalidate(self, key: str = None):
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
    def __init__(self, rate_per_minute: int):
        self.capacity = max(rate_per_minute // 6, 10)
        self.refill_rate = rate_per_minute / 60.0
        self._buckets: dict[str, tuple[float, float]] = {}
        self._last_cleanup = time.time()

    def allow(self, key: str) -> bool:
        now = time.time()
        if now - self._last_cleanup > 300:
            self._cleanup_stale(now)
            self._last_cleanup = now

        if key not in self._buckets:
            self._buckets[key] = (self.capacity, now)
            return True

        tokens, last_refill = self._buckets[key]
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
        stale = [k for k, (_, t) in self._buckets.items() if now - t > 300]
        for k in stale:
            del self._buckets[k]



# 数据存储与聚合（预计算版）



class DataStorage:
    def __init__(self, expire_minutes: int):
        self.expire_seconds = expire_minutes * 60
        self.server_ips: set[str] = set()
        self.node_reports: dict[str, list[dict]] = {}
        self.active_nodes: dict[str, float] = {}

        self._aggregated: dict[str, tuple[float, dict]] = {}
        self._delay_history: dict[str, list[tuple[float, float]]] = {}

    def update_expire(self, expire_minutes: int):
        """热重载时更新过期时间"""
        self.expire_seconds = expire_minutes * 60

    def update_servers(self, servers_json: dict):
        old_ips = self.server_ips.copy()
        new_ips: set[str] = set()

        for server in servers_json.get("servers", []):
            ip = server.get("ip")
            if ip and isinstance(ip, str) and ip.strip():
                new_ips.add(ip.strip())

        self.server_ips = new_ips

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

            if report.online and report.delay is not None:
                if ip not in self._delay_history:
                    self._delay_history[ip] = []
                self._delay_history[ip].append((now, report.delay))

            self._recompute_aggregation(ip)

    def _recompute_aggregation(self, ip: str):
        all_reports = self.node_reports.get(ip, [])
        now = time.time()

        recent = [r for r in all_reports if now - r["timestamp"] < self.expire_seconds]

        if not recent:
            self._aggregated[ip] = (now, {"online": False})
            return

        recent.sort(key=lambda r: r["timestamp"], reverse=True)
        online_reports = [r for r in recent if r["report"].online]

        if not online_reports:
            self._aggregated[ip] = (now, {"online": False})
            return

        latest_report = online_reports[0]["report"]

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
        if ip not in self.server_ips:
            return None

        cached = self._aggregated.get(ip)
        if cached is None:
            return {"online": False}

        ts, data = cached
        if time.time() - ts > self.expire_seconds:
            return {"online": False}

        return data

    def cleanup(self):
        now = time.time()
        expired_count = 0
        batch_size = 200
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
                self._recompute_aggregation(ip)

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

        for node_id in list(self.active_nodes.keys()):
            if now - self.active_nodes[node_id] > self.expire_seconds:
                del self.active_nodes[node_id]

        if expired_count > 0:
            logger.debug(f"清理过期数据: {expired_count} 条 (本轮处理 {processed} 个IP)")

    @staticmethod
    def _aggregate_delay(delays: list[float]) -> Optional[float]:
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
        return {
            "monitored_servers": len(self.server_ips),
            "servers_with_data": len(self.node_reports),
            "aggregated_cache_size": len(self._aggregated),
            "active_nodes": len(self.active_nodes),
            "active_node_ids": list(self.active_nodes.keys()),
        }



# 节点认证（带缓存）



class NodeAuthenticator:
    _OVERRIDABLE_KEYS = ("monitor_interval", "report_interval", "config_refresh_interval")

    def __init__(self, registered_nodes: list[dict], defaults: dict):
        self._tokens: dict[str, str] = {}
        self._node_configs: dict[str, dict] = {}

        default_overrides = {
            k: defaults[k]
            for k in self._OVERRIDABLE_KEYS
            if k in defaults
        }

        for node in registered_nodes:
            node_id = node["id"]
            self._tokens[node_id] = node["token"]

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
        return self._node_configs.get(node_id)



# 配置管理器（热重载核心）



class ConfigManager:
    """
    配置管理器 — 负责加载、监听和热重载 config.yaml

    工作原理：
    - 启动时加载配置文件并记录 mtime
    - 后台任务每 3 秒检查文件 mtime 是否变化
    - 检测到变化后重新加载 YAML，对比差异，更新所有依赖对象
    - 所有循环任务通过 config_manager.config 动态读取间隔值
    """

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config: dict = {}
        self._file_mtime: float = 0
        self._file_size: int = 0

    def load(self):
        """加载配置文件"""
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f) or {}
        stat = os.stat(self.config_path)
        self._file_mtime = stat.st_mtime
        self._file_size = stat.st_size

    def check_changed(self) -> bool:
        """检查配置文件是否被修改（非阻塞）"""
        try:
            stat = os.stat(self.config_path)
            return stat.st_mtime != self._file_mtime or stat.st_size != self._file_size
        except OSError:
            return False

    def reload(self) -> bool:
        """
        重新加载配置文件并更新所有依赖对象

        返回 True 表示成功重载，False 表示文件未变化或加载失败
        """
        if not self.check_changed():
            return False

        try:
            old_config = self.config
            self.load()
            new_config = self.config

            # 记录变更项
            changes = self._diff_configs(old_config, new_config)

            # 更新所有依赖对象
            self._apply_config(new_config)

            if changes:
                logger.info(f"配置热重载完成，变更项: {', '.join(changes)}")
            else:
                logger.info("配置热重载完成（格式变化但关键配置未变）")

            return True

        except yaml.YAMLError as e:
            logger.error(f"配置文件 YAML 语法错误，保持旧配置: {e}")
            # 重新加载旧 mtime 防止反复报错
            try:
                stat = os.stat(self.config_path)
                self._file_mtime = stat.st_mtime
                self._file_size = stat.st_size
            except OSError:
                pass
            return False
        except Exception as e:
            logger.error(f"配置热重载失败，保持旧配置: {e}")
            return False

    def _diff_configs(self, old: dict, new: dict) -> list[str]:
        """对比新旧配置，返回变更项描述列表"""
        changes = []

        # 对比的顶层键
        compare_keys = [
            ("server", ["host", "port", "workers", "cors_origins"]),
            ("fetch", ["url", "interval", "timeout"]),
            ("node_defaults", ["monitor_interval", "report_interval",
                               "config_refresh_interval", "data_expire_minutes"]),
            ("cache", ["enabled", "ttl", "max_size"]),
            ("rate_limit", ["enabled", "queries_per_minute",
                            "node_requests_per_minute"]),
        ]

        for section, keys in compare_keys:
            old_sec = old.get(section, {})
            new_sec = new.get(section, {})
            for key in keys:
                old_val = old_sec.get(key)
                new_val = new_sec.get(key)
                if old_val != new_val:
                    changes.append(f"{section}.{key}: {old_val}→{new_val}")

        # 节点列表变更
        old_nodes = {n["id"]: n for n in old.get("registered_nodes", [])}
        new_nodes = {n["id"]: n for n in new.get("registered_nodes", [])}
        added_nodes = set(new_nodes) - set(old_nodes)
        removed_nodes = set(old_nodes) - set(new_nodes)
        changed_nodes = {
            nid for nid in set(old_nodes) & set(new_nodes)
            if old_nodes[nid] != new_nodes[nid]
        }
        if added_nodes:
            changes.append(f"新增节点: {added_nodes}")
        if removed_nodes:
            changes.append(f"移除节点: {removed_nodes}")
        if changed_nodes:
            changes.append(f"修改节点: {changed_nodes}")

        return changes

    def _apply_config(self, cfg: dict):
        """将新配置应用到所有全局对象"""
        global authenticator, query_cache, query_limiter, node_limiter

        # 1. 更新认证器（节点列表和默认配置）
        authenticator = NodeAuthenticator(
            cfg["registered_nodes"],
            cfg["node_defaults"],
        )

        # 2. 更新数据过期时间
        storage.update_expire(cfg["node_defaults"]["data_expire_minutes"])

        # 3. 更新查询缓存
        cache_cfg = cfg.get("cache", {})
        cache_enabled = cache_cfg.get("enabled", True)
        if cache_enabled:
            new_ttl = cache_cfg.get("ttl", 5)
            new_max = cache_cfg.get("max_size", 10000)
            if query_cache is not None:
                query_cache.ttl = new_ttl
                query_cache.max_size = new_max
            else:
                query_cache = LRUCache(max_size=new_max, ttl=new_ttl)
            query_cache.invalidate()  # 配置变更后清空缓存
        else:
            query_cache = None

        # 4. 更新限流器
        rl_cfg = cfg.get("rate_limit", {})
        rl_enabled = rl_cfg.get("enabled", True)
        if rl_enabled:
            query_limiter = TokenBucketRateLimiter(
                rl_cfg.get("queries_per_minute", 600)
            )
            node_limiter = TokenBucketRateLimiter(
                rl_cfg.get("node_requests_per_minute", 120)
            )
        else:
            query_limiter = None
            node_limiter = None



# 全局实例初始化


# 配置管理器（最先初始化）
config_manager = ConfigManager(CONFIG_PATH)
config_manager.load()

# 从配置创建所有全局对象
config = config_manager.config
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

# 全局httpx客户端
_http_client: Optional[httpx.AsyncClient] = None


# 后台任务



async def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        cfg = config_manager.config
        _http_client = httpx.AsyncClient(
            timeout=cfg["fetch"].get("timeout", 30),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
    return _http_client


async def fetch_server_list():
    """从远程API获取服务器列表"""
    cfg = config_manager.config
    url = cfg["fetch"]["url"]
    try:
        client = await get_http_client()
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()
        storage.update_servers(data)
        if query_cache:
            query_cache.invalidate()
    except httpx.HTTPStatusError as e:
        logger.error(f"获取服务器列表HTTP错误: {e.response.status_code}")
    except httpx.RequestError as e:
        logger.error(f"获取服务器列表网络错误: {e}")
    except Exception as e:
        logger.error(f"获取服务器列表异常: {e}")


async def fetch_server_list_loop():
    """定期获取服务器列表 — 每轮动态读取间隔"""
    await fetch_server_list()
    while True:
        interval = config_manager.config["fetch"]["interval"]
        await asyncio.sleep(interval)
        await fetch_server_list()


async def cleanup_loop():
    """定期清理过期数据"""
    while True:
        await asyncio.sleep(60)
        storage.cleanup()
        if query_cache:
            query_cache.invalidate()


async def config_watcher_loop():
    """
    配置文件监听循环 — 每3秒检查一次 config.yaml 是否被修改

    检测到变更后自动重载配置并热更新所有依赖对象，
    无需重启服务端即可修改节点列表、间隔参数、限流设置等。
    """
    while True:
        await asyncio.sleep(3)
        try:
            config_manager.reload()
        except Exception as e:
            logger.error(f"配置监听异常: {e}")



# FastAPI 应用



@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    fetch_task = asyncio.create_task(fetch_server_list_loop())
    cleanup_task = asyncio.create_task(cleanup_loop())
    watcher_task = asyncio.create_task(config_watcher_loop())

    cfg = config_manager.config
    logger.info(
        f"服务端启动 -> {cfg['server']['host']}:{cfg['server']['port']} | "
        f"列表获取间隔 {cfg['fetch']['interval']}s | "
        f"数据过期 {cfg['node_defaults']['data_expire_minutes']}min | "
        f"缓存 {'ON' if query_cache else 'OFF'} | "
        f"限流 {'ON' if query_limiter else 'OFF'} | "
        f"配置热重载 ON (监听: {CONFIG_PATH})"
    )
    yield

    fetch_task.cancel()
    cleanup_task.cancel()
    watcher_task.cancel()
    for task in [fetch_task, cleanup_task, watcher_task]:
        try:
            await task
        except asyncio.CancelledError:
            pass

    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
    logger.info("服务端已停止")


app = FastAPI(title="MC服务器状态监测系统", version="2.1.0", lifespan=lifespan)

# CORS 中间件
cfg = config_manager.config
app.add_middleware(
    CORSMiddleware,
    allow_origins=cfg.get("server", {}).get("cors_origins", ["*"]),
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
    if node_limiter and not node_limiter.allow(x_node_id):
        raise HTTPException(status_code=429, detail="请求过于频繁")

    if not authenticator.verify(x_node_id, x_node_token):
        raise HTTPException(status_code=401, detail="认证失败：设备ID或Token错误")

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
    if node_limiter and not node_limiter.allow(x_node_id):
        raise HTTPException(status_code=429, detail="上报过于频繁")

    if not authenticator.verify(x_node_id, x_node_token):
        raise HTTPException(status_code=401, detail="认证失败：设备ID或Token错误")

    storage.add_report(x_node_id, body.reports)

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
    """外部接口：查询服务器状态"""
    client_ip = request.client.host if request.client else "unknown"
    if query_limiter and not query_limiter.allow(client_ip):
        raise HTTPException(status_code=429, detail="查询过于频繁，请稍后再试")

    if query_cache:
        cached = query_cache.get(ip)
        if cached is not None:
            return cached

    status = storage.get_status(ip)
    if status is None:
        return JSONResponse(
            status_code=404,
            content={"online": False, "error": "该服务器不在监测列表中"},
        )

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
        "config": {
            "path": CONFIG_PATH,
            "hot_reload": True,
            "registered_nodes": len(config_manager.config.get("registered_nodes", [])),
        },
    }



# 接口：手动触发配置重载



@app.post("/api/config/reload")
async def manual_reload():
    """手动触发配置重载（也可直接修改文件等待自动检测）"""
    success = config_manager.reload()
    if success:
        return {"status": "ok", "message": "配置已重载"}
    return {"status": "ok", "message": "配置未变化（文件未被修改）"}



# 启动


if __name__ == "__main__":
    cfg = config_manager.config
    workers = cfg.get("server", {}).get("workers", 1)
    uvicorn.run(
        "server:app",
        host=cfg["server"]["host"],
        port=cfg["server"]["port"],
        log_level="info",
        workers=workers,
    )
