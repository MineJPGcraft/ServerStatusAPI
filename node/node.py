#!/usr/bin/env python3
"""
节点端
"""

import asyncio
import logging
import os
import time
from typing import Optional

import httpx
from mcstatus import JavaServer

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# 日志

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Node] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mcstatus-node")


# 环境变量

SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8000").rstrip("/")
DEVICE_ID = os.getenv("DEVICE_ID", "")
TOKEN = os.getenv("TOKEN", "")

if not DEVICE_ID or not TOKEN:
    logger.error("缺少必要环境变量：DEVICE_ID 和 TOKEN")
    raise SystemExit(1)

ENV_MONITOR_INTERVAL = _v if (_v := os.getenv("MONITOR_INTERVAL")) else None
ENV_REPORT_INTERVAL = _v if (_v := os.getenv("REPORT_INTERVAL")) else None
ENV_CONFIG_REFRESH_INTERVAL = _v if (_v := os.getenv("CONFIG_REFRESH_INTERVAL")) else None
ENV_MAX_CONCURRENCY = _v if (_v := os.getenv("MAX_CONCURRENCY")) else None
ENV_SERVER_TIMEOUT = _v if (_v := os.getenv("SERVER_TIMEOUT")) else None
ENV_OFFLINE_BACKOFF = _v if (_v := os.getenv("OFFLINE_BACKOFF")) else None



# 离线退避追踪器



class ServerTracker:
    def __init__(self, backoff_multiplier: int = 3):
        self.backoff = backoff_multiplier
        self._states: dict[str, dict] = {}

    def should_check(self, ip: str, current_round: int) -> bool:
        state = self._states.get(ip)
        if state is None:
            return True
        if state.get("online"):
            return True
        fail_count = state.get("fail_count", 0)
        skip_rounds = min(self.backoff, 1 + fail_count // 2)
        last_round = state.get("last_round", 0)
        return current_round - last_round >= skip_rounds

    def update(self, ip: str, online: bool, round_num: int):
        state = self._states.get(ip, {"online": False, "fail_count": 0, "last_round": 0})
        state["online"] = online
        state["last_round"] = round_num
        if online:
            state["fail_count"] = 0
        else:
            state["fail_count"] = state.get("fail_count", 0) + 1
        self._states[ip] = state

    def remove_stale(self, active_ips: set[str]):
        for ip in set(self._states.keys()) - active_ips:
            del self._states[ip]



# 节点客户端



class NodeClient:
    _INTERVAL_KEYS = ("monitor_interval", "report_interval", "config_refresh_interval")

    def __init__(self, server_url: str, device_id: str, token: str):
        self.server_url = server_url
        self.device_id = device_id
        self.token = token
        self.headers = {"X-Node-Id": device_id, "X-Node-Token": token}
        self.client = httpx.AsyncClient(timeout=30)

        self.monitor_interval: int = 30
        self.report_interval: int = 60
        self.config_refresh_interval: int = 60
        self.server_ips: list[str] = []

        self._env_overrides: dict[str, Optional[int]] = {
            "monitor_interval": self._parse_int_env(ENV_MONITOR_INTERVAL),
            "report_interval": self._parse_int_env(ENV_REPORT_INTERVAL),
            "config_refresh_interval": self._parse_int_env(ENV_CONFIG_REFRESH_INTERVAL),
        }
        active_overrides = [k for k, v in self._env_overrides.items() if v is not None]
        if active_overrides:
            logger.info(
                f"环境变量覆盖: {', '.join(active_overrides)} "
                f"→ {[self._env_overrides[k] for k in active_overrides]}"
            )

        # 性能参数
        self.max_concurrency: int = self._parse_int_env(ENV_MAX_CONCURRENCY) or 50
        self.server_timeout: float = float(self._parse_int_env(ENV_SERVER_TIMEOUT) or 15)
        offline_backoff: int = self._parse_int_env(ENV_OFFLINE_BACKOFF) or 3

        logger.info(
            f"性能参数: 并发={self.max_concurrency} | "
            f"超时={self.server_timeout}s | "
            f"离线退避={offline_backoff}x"
        )

        self.tracker = ServerTracker(backoff_multiplier=offline_backoff)
        self.latest_results: list[dict] = []
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self._round_counter = 0

        # JavaServer 对象缓存: ip -> (JavaServer, timestamp)
        self._server_cache: dict[str, tuple[JavaServer, float]] = {}
        self._cache_ttl = 300  # 5分钟

    @staticmethod
    def _parse_int_env(val: Optional[str]) -> Optional[int]:
        if val is None:
            return None
        try:
            v = int(val)
            return v if v > 0 else None
        except (ValueError, TypeError):
            logger.warning(f"环境变量值 '{val}' 不是有效正整数，已忽略")
            return None

    def _apply_config(self, data: dict):
        changes = []
        for key in self._INTERVAL_KEYS:
            server_val = data.get(key)
            if server_val is not None:
                server_val = int(server_val)
            env_val = self._env_overrides.get(key)
            if env_val is not None:
                final_val, source = env_val, "env"
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
        self.tracker.remove_stale(new_ips)

        if changes:
            logger.info(f"配置变更: {' | '.join(changes)}")
        if old_ips != new_ips:
            added = new_ips - old_ips
            removed = old_ips - new_ips
            logger.info(f"服务器列表更新: {len(self.server_ips)} 台 (新增 {len(added)}, 移除 {len(removed)})")
        logger.info(
            f"配置生效: {len(self.server_ips)} 台 | "
            f"监测 {self.monitor_interval}s | 上报 {self.report_interval}s | "
            f"配置刷新 {self.config_refresh_interval}s"
        )


    # 与服务端通信


    async def fetch_config(self) -> bool:
        try:
            resp = await self.client.get(f"{self.server_url}/api/node/config", headers=self.headers)
            if resp.status_code == 401:
                logger.error("认证失败，请检查 DEVICE_ID 和 TOKEN")
                return False
            resp.raise_for_status()
            self._apply_config(resp.json())
            return True
        except httpx.ConnectError:
            logger.error(f"无法连接服务端: {self.server_url}")
            return False
        except Exception as e:
            logger.error(f"获取配置异常: {e}")
            return False

    async def report_data(self, results: list[dict]) -> bool:
        if not results:
            return False
        try:
            resp = await self.client.post(
                f"{self.server_url}/api/node/report",
                headers=self.headers,
                json={"reports": results},
            )
            if resp.status_code == 401:
                logger.error("上报时认证失败")
                return False
            resp.raise_for_status()
            online = sum(1 for r in results if r.get("online"))
            logger.info(f"上报成功: {len(results)} 条 (在线 {online})")
            return True
        except Exception as e:
            logger.error(f"上报异常: {e}")
            return False


    # MC服务器监测（使用 mcstatus 原生 async API）


    async def _get_server(self, ip: str) -> Optional[JavaServer]:
        """
        获取 JavaServer 对象（带缓存）

        使用 JavaServer.async_lookup() 进行异步 DNS+SRV 解析。
        timeout 参数控制 TCP 连接超时，传给后续 status() 调用。
        """
        now = time.time()
        cached = self._server_cache.get(ip)
        if cached is not None:
            server_obj, ts = cached
            if now - ts < self._cache_ttl:
                return server_obj

        try:
            # async_lookup 是 classmethod，可以直接 await
            # timeout 参数设置连接超时，传递给 JavaServer 对象
            server_obj = await asyncio.wait_for(
                JavaServer.async_lookup(ip, timeout=self.server_timeout),
                timeout=30,  # 外层兜底：DNS解析本身不应超过30秒
            )
            self._server_cache[ip] = (server_obj, now)
            return server_obj
        except asyncio.TimeoutError:
            logger.warning(f"DNS解析超时: {ip}")
            return None
        except Exception as e:
            logger.warning(f"DNS解析失败: {ip} → {e}")
            return None

    async def monitor_server(self, ip: str) -> dict:
        """
        检测单台MC服务器状态

        使用 mcstatus 原生 async_status() 方法，纯异步 I/O，无需线程池。
        """
        async with self.semaphore:
            # Step 1: 异步 DNS+SRV 解析（带缓存）
            server = await self._get_server(ip)
            if server is None:
                return {"ip": ip, "online": False}

            # Step 2: 异步状态查询
            try:
                status = await asyncio.wait_for(
                    server.async_status(),
                    timeout=self.server_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(f"状态查询超时: {ip} ({self.server_timeout}s)")
                return {"ip": ip, "online": False}
            except Exception as e:
                logger.warning(f"状态查询失败: {ip} → {e}")
                # 清除缓存，下轮重新解析（可能DNS变了）
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

    @staticmethod
    def _parse_motd(status) -> dict:
        """解析 MOTD 为多种格式"""
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
                pass

        desc = getattr(status, "description", "")
        if isinstance(desc, str):
            text = desc
        elif isinstance(desc, dict):
            text = desc.get("text", "")
            if not text and "extra" in desc:
                text = "".join(p.get("text", "") for p in desc["extra"])
        else:
            text = str(desc)

        return {
            "plain": text,
            "html": f"<p>{text}</p>",
            "minecraft": text,
            "ansi": f"\x1b[0m{text}\x1b[0m",
        }


    # 批量监测


    async def monitor_all(self) -> list[dict]:
        if not self.server_ips:
            return []

        self._round_counter += 1
        round_num = self._round_counter

        to_check = [ip for ip in self.server_ips if self.tracker.should_check(ip, round_num)]
        skipped = len(self.server_ips) - len(to_check)

        if not to_check:
            logger.info(f"第{round_num}轮: 全部 {len(self.server_ips)} 台被退避跳过")
            return self.latest_results

        logger.info(
            f"第{round_num}轮: 监测 {len(to_check)}/{len(self.server_ips)} 台"
            + (f" (跳过 {skipped})" if skipped > 0 else "")
        )

        start_time = time.time()
        results: list[dict] = []
        online_count = 0
        failed = 0

        # 全部并发（信号量已控制并发数）
        tasks = [self.monitor_server(ip) for ip in to_check]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in batch_results:
            if isinstance(r, Exception):
                failed += 1
                logger.error(f"监测任务异常: {r}")
            elif r is not None:
                results.append(r)
                self.tracker.update(r["ip"], r.get("online", False), round_num)
                if r.get("online"):
                    online_count += 1

        elapsed = time.time() - start_time
        logger.info(
            f"第{round_num}轮完成: 在线 {online_count} / 离线 {len(results) - online_count} / "
            f"异常 {failed} | 耗时 {elapsed:.1f}s"
        )

        # 合并被跳过服务器的旧结果
        if skipped > 0:
            checked_ips = {r["ip"] for r in results}
            for old_r in self.latest_results:
                if old_r["ip"] not in checked_ips:
                    results.append(old_r)

        return results


    # 运行循环


    async def monitor_loop(self):
        while True:
            cycle_start = time.time()
            try:
                self.latest_results = await self.monitor_all()
            except Exception as e:
                logger.error(f"监测循环异常: {e}")
            elapsed = time.time() - cycle_start
            await asyncio.sleep(max(1, self.monitor_interval - elapsed))

    async def report_loop(self):
        while True:
            cycle_start = time.time()
            try:
                if self.latest_results:
                    await self.report_data(self.latest_results)
            except Exception as e:
                logger.error(f"上报循环异常: {e}")
            elapsed = time.time() - cycle_start
            await asyncio.sleep(max(1, self.report_interval - elapsed))

    async def config_loop(self):
        while True:
            cycle_start = time.time()
            try:
                await self.fetch_config()
            except Exception as e:
                logger.error(f"配置刷新异常: {e}")
            elapsed = time.time() - cycle_start
            await asyncio.sleep(max(1, self.config_refresh_interval - elapsed))

    async def run(self):
        logger.info(f"节点启动: ID={self.device_id} | Server={self.server_url}")

        retry_count = 0
        while True:
            if await self.fetch_config():
                break
            retry_count += 1
            wait = min(5 * retry_count, 60)
            logger.warning(f"初始配置获取失败，{wait}秒后重试 ({retry_count})...")
            await asyncio.sleep(wait)

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
            await self.client.aclose()
            logger.info("节点已停止")



# 启动


async def main():
    client = NodeClient(SERVER_URL, DEVICE_ID, TOKEN)
    await client.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("节点已停止 (Ctrl+C)")
