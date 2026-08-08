#!/usr/bin/env python3
"""
节点端
"""

import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import httpx
from mcstatus import JavaServer

# 尝试加载 .env 文件
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Node] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mcstatus-node")

# 环境变量读取
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8000").rstrip("/")
DEVICE_ID = os.getenv("DEVICE_ID", "")
TOKEN = os.getenv("TOKEN", "")

if not DEVICE_ID or not TOKEN:
    logger.error("缺少必要环境变量：DEVICE_ID 和 TOKEN")
    logger.error("请设置环境变量或创建 .env 文件（参考 .env.example）")
    raise SystemExit(1)

# 可选环境变量覆盖（None 表示未设置，将使用服务端下发的值）
ENV_MONITOR_INTERVAL = _v if (_v := os.getenv("MONITOR_INTERVAL")) else None
ENV_REPORT_INTERVAL = _v if (_v := os.getenv("REPORT_INTERVAL")) else None
ENV_CONFIG_REFRESH_INTERVAL = _v if (_v := os.getenv("CONFIG_REFRESH_INTERVAL")) else None

# 性能调优环境变量
ENV_MAX_CONCURRENCY = _v if (_v := os.getenv("MAX_CONCURRENCY")) else None
ENV_SERVER_TIMEOUT = _v if (_v := os.getenv("SERVER_TIMEOUT")) else None
ENV_OFFLINE_BACKOFF = _v if (_v := os.getenv("OFFLINE_BACKOFF")) else None
ENV_THREAD_POOL_SIZE = _v if (_v := os.getenv("THREAD_POOL_SIZE")) else None


# 服务器状态追踪器（离线退避 + 失败计数）


class ServerTracker:
    """
    追踪每台服务器的检测状态，实现离线退避策略

    - 在线服务器：每轮都检测
    - 离线服务器：按 backoff 倍数降低检测频率
    - 连续失败的服务器：逐步增加跳过轮次
    """

    def __init__(self, backoff_multiplier: int = 3):
        self.backoff = backoff_multiplier
        # ip -> {"online": bool, "fail_count": int, "skip_until": float}
        self._states: dict[str, dict] = {}

    def should_check(self, ip: str, current_round: int) -> bool:
        """判断本轮是否应该检测此服务器"""
        state = self._states.get(ip)
        if state is None:
            return True  # 新服务器，总是检测

        # 在线服务器总是检测
        if state.get("online"):
            return True

        # 离线服务器：根据失败次数计算跳过轮次
        fail_count = state.get("fail_count", 0)
        # 失败次数越多，跳过轮次越多（指数退避，上限为 backoff 值）
        skip_rounds = min(self.backoff, 1 + fail_count // 2)
        last_round = state.get("last_round", 0)

        return current_round - last_round >= skip_rounds

    def update(self, ip: str, online: bool, round_num: int):
        """更新服务器状态"""
        state = self._states.get(ip, {"online": False, "fail_count": 0, "last_round": 0})
        state["online"] = online
        state["last_round"] = round_num
        if online:
            state["fail_count"] = 0
        else:
            state["fail_count"] = state.get("fail_count", 0) + 1
        self._states[ip] = state

    def remove_stale(self, active_ips: set[str]):
        """移除不再需要监测的服务器"""
        stale = set(self._states.keys()) - active_ips
        for ip in stale:
            del self._states[ip]

    def stats(self) -> dict:
        online = sum(1 for s in self._states.values() if s.get("online"))
        offline = len(self._states) - online
        return {"tracked": len(self._states), "online": online, "offline": offline}


# 节点客户端


class NodeClient:
    """节点客户端：负责拉取配置、监测MC服务器、上报数据"""

    # 三个可被环境变量覆盖的配置项名称
    _INTERVAL_KEYS = ("monitor_interval", "report_interval", "config_refresh_interval")

    def __init__(self, server_url: str, device_id: str, token: str):
        self.server_url = server_url
        self.device_id = device_id
        self.token = token
        self.headers = {
            "X-Node-Id": device_id,
            "X-Node-Token": token,
        }
        self.client = httpx.AsyncClient(timeout=30)

        # 当前生效的间隔配置（会被 fetch_config 更新）
        self.monitor_interval: int = 30
        self.report_interval: int = 60
        self.config_refresh_interval: int = 60
        self.server_ips: list[str] = []

        # 环境变量覆盖值（优先级最高，设置后始终生效）
        self._env_overrides: dict[str, Optional[int]] = {
            "monitor_interval": self._parse_int_env(ENV_MONITOR_INTERVAL),
            "report_interval": self._parse_int_env(ENV_REPORT_INTERVAL),
            "config_refresh_interval": self._parse_int_env(ENV_CONFIG_REFRESH_INTERVAL),
        }

        # 记录哪些配置项被环境变量覆盖了，用于日志提示
        active_overrides = [
            k for k, v in self._env_overrides.items() if v is not None
        ]
        if active_overrides:
            logger.info(
                f"环境变量覆盖生效: {', '.join(active_overrides)} "
                f"→ {[self._env_overrides[k] for k in active_overrides]}"
            )

        # ----- 性能参数 -----
        self.max_concurrency: int = self._parse_int_env(ENV_MAX_CONCURRENCY) or 50
        self.server_timeout: float = float(self._parse_int_env(ENV_SERVER_TIMEOUT) or 15)
        offline_backoff: int = self._parse_int_env(ENV_OFFLINE_BACKOFF) or 3
        # 线程池必须远大于并发数：超时的线程无法被杀死会继续占用线程，
        # 需要足够的冗余线程防止级联耗尽
        thread_pool_size: int = self._parse_int_env(ENV_THREAD_POOL_SIZE) or (self.max_concurrency * 4)

        # 专用线程池（避免耗尽默认线程池）
        self._executor = ThreadPoolExecutor(
            max_workers=thread_pool_size,
            thread_name_prefix="mcstatus",
        )
        logger.info(
            f"性能参数: 并发={self.max_concurrency} | "
            f"超时={self.server_timeout}s | "
            f"离线退避={offline_backoff}x | "
            f"线程池={thread_pool_size}"
        )

        # 服务器状态追踪器
        self.tracker = ServerTracker(backoff_multiplier=offline_backoff)

        # 最新一轮监测结果
        self.latest_results: list[dict] = []

        # 并发信号量
        self.semaphore = asyncio.Semaphore(self.max_concurrency)

        # 监测轮次计数器
        self._round_counter = 0

        # JavaServer 对象缓存: ip -> (JavaServer对象, 缓存时间戳)
        # 缓存 lookup() 的结果（含 SRV 解析），避免每轮重复 DNS+SRV 查询
        self._server_cache: dict[str, tuple[JavaServer, float]] = {}
        self._dns_cache_ttl = 300  # 缓存5分钟

    @staticmethod
    def _parse_int_env(val: Optional[str]) -> Optional[int]:
        """安全解析环境变量为正整数，失败返回 None"""
        if val is None:
            return None
        try:
            v = int(val)
            return v if v > 0 else None
        except (ValueError, TypeError):
            logger.warning(f"环境变量值 '{val}' 不是有效正整数，已忽略")
            return None

    def _apply_config(self, data: dict):
        """
        应用服务端下发的配置，并叠加环境变量覆盖

        优先级：环境变量 > 服务端下发值
        """
        changes = []

        for key in self._INTERVAL_KEYS:
            # 服务端下发的值
            server_val = data.get(key)
            if server_val is not None:
                server_val = int(server_val)

            # 环境变量覆盖值（优先级最高）
            env_val = self._env_overrides.get(key)
            if env_val is not None:
                final_val = env_val
                source = "env"
            else:
                final_val = server_val if server_val is not None else getattr(self, key)
                source = "server"

            old_val = getattr(self, key)
            if final_val != old_val:
                changes.append(f"{key}: {old_val}→{final_val} ({source})")
            setattr(self, key, final_val)

        old_ips = set(self.server_ips)
        self.server_ips = data.get("servers", [])
        new_ips = set(self.server_ips)

        # 清理不再监测的服务器追踪状态
        self.tracker.remove_stale(new_ips)

        if changes:
            logger.info(f"配置变更: {' | '.join(changes)}")
        if old_ips != new_ips:
            added = new_ips - old_ips
            removed = old_ips - new_ips
            logger.info(
                f"服务器列表更新: {len(self.server_ips)} 台 "
                f"(新增 {len(added)}, 移除 {len(removed)})"
            )
        logger.info(
            f"配置生效: {len(self.server_ips)} 台服务器 | "
            f"监测 {self.monitor_interval}s | 上报 {self.report_interval}s | "
            f"配置刷新 {self.config_refresh_interval}s"
        )

    # 与服务端通信

    async def fetch_config(self) -> bool:
        """从服务端拉取配置和服务器列表"""
        try:
            resp = await self.client.get(
                f"{self.server_url}/api/node/config",
                headers=self.headers,
            )
            if resp.status_code == 401:
                logger.error("认证失败，请检查 DEVICE_ID 和 TOKEN 是否正确")
                return False
            resp.raise_for_status()
            data = resp.json()

            self._apply_config(data)
            return True

        except httpx.ConnectError:
            logger.error(f"无法连接服务端: {self.server_url}")
            return False
        except httpx.HTTPStatusError as e:
            logger.error(f"获取配置HTTP错误: {e.response.status_code}")
            return False
        except Exception as e:
            logger.error(f"获取配置异常: {e}")
            return False

    async def report_data(self, results: list[dict]) -> bool:
        """向服务端上报监测数据"""
        if not results:
            logger.debug("无数据可上报")
            return False

        try:
            resp = await self.client.post(
                f"{self.server_url}/api/node/report",
                headers=self.headers,
                json={"reports": results},
            )
            if resp.status_code == 401:
                logger.error("上报数据时认证失败")
                return False
            resp.raise_for_status()
            online_count = sum(1 for r in results if r.get("online"))
            logger.info(f"上报成功: {len(results)} 条 (在线 {online_count})")
            return True
        except httpx.ConnectError:
            logger.error(f"上报数据时无法连接服务端: {self.server_url}")
            return False
        except httpx.HTTPStatusError as e:
            logger.error(f"上报数据HTTP错误: {e.response.status_code}")
            return False
        except Exception as e:
            logger.error(f"上报数据异常: {e}")
            return False

    # MC服务器监测

    async def monitor_server(self, ip: str) -> dict:
        """
        检测单台MC服务器状态
        """
        async with self.semaphore:
            # Step 1: 获取 JavaServer 对象（含 DNS+SRV 解析，带缓存）
            server = await self._resolve_server_async(ip)
            if server is None:
                return {"ip": ip, "online": False}

            # Step 2: 查询服务器状态（独立超时）
            try:
                status = await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(
                        self._executor,
                        server.status,
                    ),
                    timeout=self.server_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(f"状态查询 {ip} 超时 ({self.server_timeout}s)")
                return {"ip": ip, "online": False}
            except Exception as e:
                logger.warning(f"状态查询 {ip} 失败: {e}")
                # 解析对象可能已失效，清除缓存让下轮重新解析
                self._server_cache.pop(ip, None)
                return {"ip": ip, "online": False}

            # Step 3: 解析结果
            motd = self._parse_motd(status)
            icon = getattr(status, "icon", None)

            return {
                "ip": ip,
                "online": True,
                "players": {
                    "online": status.players.online,
                    "max": status.players.max,
                },
                "delay": round(status.latency, 2),
                "version": status.version.name,
                "motd": motd,
                "icon": icon,
            }

    async def _resolve_server_async(self, ip: str) -> Optional[JavaServer]:
        """
        异步获取 JavaServer 对象（带缓存）

        - 缓存命中时直接返回，不消耗超时预算
        - 缓存未命中时在线程池中执行 lookup()，给 30 秒超时
        - lookup() 失败/超时后清除缓存，下轮重试
        """
        now = time.time()

        # 检查缓存
        cached = self._server_cache.get(ip)
        if cached is not None:
            server_obj, ts = cached
            if now - ts < self._dns_cache_ttl:
                return server_obj

        # 缓存未命中，在线程池中执行 lookup
        try:
            server_obj = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(
                    self._executor,
                    JavaServer.lookup,
                    ip,
                ),
                timeout=30,  # DNS+SRV 解析给充足时间
            )
            self._server_cache[ip] = (server_obj, now)
            return server_obj
        except asyncio.TimeoutError:
            logger.warning(f"DNS解析 {ip} 超时 (30s)")
            return None
        except Exception as e:
            logger.warning(f"DNS解析 {ip} 失败: {e}")
            return None

    @staticmethod
    def _parse_motd(status) -> dict:
        """
        解析 MOTD 为多种格式

        优先使用 mcstatus 的 Motd 对象（新版API），
        回退到 description 字段（旧版API）。
        """
        # 新版 mcstatus：status.motd 是 Motd 对象
        motd_obj = getattr(status, "motd", None)
        if motd_obj is not None:
            try:
                return {
                    "plain": motd_obj.to_plain(),
                    "html": motd_obj.to_html(),
                    "minecraft": motd_obj.to_minecraft(),
                    "ansi": motd_obj.to_ansi(),
                }
            except Exception:
                pass  # 回退到下面的处理

        # 旧版 mcstatus：status.description 是字符串或字典
        desc = getattr(status, "description", "")
        if isinstance(desc, str):
            text = desc
        elif isinstance(desc, dict):
            # 提取纯文本
            text = desc.get("text", "")
            if not text and "extra" in desc:
                parts = []
                for part in desc["extra"]:
                    parts.append(part.get("text", ""))
                text = "".join(parts)
        else:
            text = str(desc)

        return {
            "plain": text,
            "html": f"<p>{text}</p>",
            "minecraft": text,
            "ansi": f"\x1b[0m{text}\x1b[0m",
        }

    async def monitor_all(self) -> list[dict]:
        """
        并发检测所有MC服务器
        """
        if not self.server_ips:
            logger.debug("服务器列表为空，跳过本轮监测")
            return []

        self._round_counter += 1
        round_num = self._round_counter

        # 筛选本轮需要检测的服务器（离线退避）
        to_check = [
            ip for ip in self.server_ips
            if self.tracker.should_check(ip, round_num)
        ]
        skipped = len(self.server_ips) - len(to_check)

        if not to_check:
            logger.info(
                f"第{round_num}轮: 全部 {len(self.server_ips)} 台服务器被退避跳过"
            )
            return self.latest_results  # 保留上一次的结果

        logger.info(
            f"第{round_num}轮: 开始监测 {len(to_check)}/{len(self.server_ips)} 台"
            + (f" (跳过 {skipped} 台离线)" if skipped > 0 else "")
        )

        start_time = time.time()
        results: list[dict] = []
        completed = 0
        failed = 0
        online_count = 0

        # 分批处理：每批 = max_concurrency × 2
        batch_size = self.max_concurrency * 2
        total = len(to_check)

        for i in range(0, total, batch_size):
            batch = to_check[i:i + batch_size]
            tasks = [self.monitor_server(ip) for ip in batch]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in batch_results:
                completed += 1
                if isinstance(r, Exception):
                    failed += 1
                    logger.error(f"监测任务异常: {r}")
                elif r is not None:
                    results.append(r)
                    # 更新追踪器状态
                    self.tracker.update(r["ip"], r.get("online", False), round_num)
                    if r.get("online"):
                        online_count += 1

            # 进度日志（每完成25%或批次完成时输出）
            progress = completed / total
            if total > 50 and (completed % max(total // 4, 1) == 0 or completed == total):
                elapsed = time.time() - start_time
                logger.info(
                    f"  进度: {completed}/{total} ({progress:.0%}) | "
                    f"在线 {online_count} | 耗时 {elapsed:.1f}s"
                )

        elapsed = time.time() - start_time
        offline_count = len(results) - online_count
        logger.info(
            f"第{round_num}轮完成: 在线 {online_count} / 离线 {offline_count} / "
            f"失败 {failed} / 共 {len(results)} | 耗时 {elapsed:.1f}s"
        )

        # 合并结果：本轮新结果 + 之前跳过但仍有旧数据的服务器
        if skipped > 0:
            # 保留被跳过服务器的上一次结果（标记为可能过时）
            checked_ips = {r["ip"] for r in results}
            for old_r in self.latest_results:
                if old_r["ip"] not in checked_ips:
                    results.append(old_r)

        return results
    # 运行循环

    async def monitor_loop(self):
        """
        监测循环：每隔 monitor_interval 秒检测所有服务器

        优化：精确间隔控制 — 从本轮开始到下轮开始的时间 = monitor_interval
        而非 本轮结束后 sleep monitor_interval
        """
        while True:
            cycle_start = time.time()
            try:
                results = await self.monitor_all()
                self.latest_results = results
            except Exception as e:
                logger.error(f"监测循环异常: {e}")

            # 精确间隔：减去本轮耗时
            elapsed = time.time() - cycle_start
            sleep_time = max(1, self.monitor_interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def report_loop(self):
        """上报循环：每隔 report_interval 秒上报最新结果"""
        while True:
            cycle_start = time.time()
            try:
                if self.latest_results:
                    await self.report_data(self.latest_results)
            except Exception as e:
                logger.error(f"上报循环异常: {e}")

            elapsed = time.time() - cycle_start
            sleep_time = max(1, self.report_interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def config_loop(self):
        """配置刷新循环：每隔 config_refresh_interval 秒重新拉取配置"""
        while True:
            cycle_start = time.time()
            try:
                await self.fetch_config()
            except Exception as e:
                logger.error(f"配置刷新异常: {e}")

            elapsed = time.time() - cycle_start
            sleep_time = max(1, self.config_refresh_interval - elapsed)
            await asyncio.sleep(sleep_time)

    async def run(self):
        """启动节点：初始拉取配置 + 三个并发循环"""
        logger.info(f"节点启动: ID={self.device_id} | Server={self.server_url}")

        # 初始配置拉取（带退避重试）
        retry_count = 0
        while True:
            if await self.fetch_config():
                break
            retry_count += 1
            wait = min(5 * retry_count, 60)
            logger.warning(f"初始配置获取失败，{wait}秒后重试 (第{retry_count}次)...")
            await asyncio.sleep(wait)

        # 启动三个并发循环
        tasks = [
            asyncio.create_task(self.monitor_loop(), name="monitor"),
            asyncio.create_task(self.report_loop(), name="report"),
            asyncio.create_task(self.config_loop(), name="config"),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("节点正在停止...")
        finally:
            for t in tasks:
                t.cancel()
            self._executor.shutdown(wait=False, cancel_futures=True)
            await self.client.aclose()
            logger.info("节点已停止")


# 启动入口


async def main():
    client = NodeClient(SERVER_URL, DEVICE_ID, TOKEN)
    await client.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("节点已停止 (Ctrl+C)")
