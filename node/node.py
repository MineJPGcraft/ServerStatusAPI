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

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Node] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("mcstatus-node")

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
        return current_round - state.get("last_round", 0) >= skip_rounds

    def update(self, ip: str, online: bool, round_num: int):
        state = self._states.get(ip, {"online": False, "fail_count": 0, "last_round": 0})
        state["online"] = online
        state["last_round"] = round_num
        state["fail_count"] = 0 if online else state.get("fail_count", 0) + 1
        self._states[ip] = state

    def remove_stale(self, active_ips: set[str]):
        for ip in set(self._states.keys()) - active_ips:
            del self._states[ip]


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
        active = [k for k, v in self._env_overrides.items() if v is not None]
        if active:
            logger.info(f"环境变量覆盖: {', '.join(active)} → {[self._env_overrides[k] for k in active]}")

        self.max_concurrency: int = self._parse_int_env(ENV_MAX_CONCURRENCY) or 50
        self.server_timeout: float = float(self._parse_int_env(ENV_SERVER_TIMEOUT) or 10)
        offline_backoff: int = self._parse_int_env(ENV_OFFLINE_BACKOFF) or 3

        # 线程池：同步 mcstatus 调用在此执行
        # 大小 = 并发 × 4，防止超时线程累积导致级联耗尽
        pool_size = self.max_concurrency * 4
        self._executor = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix="mc")

        logger.info(f"性能参数: 并发={self.max_concurrency} | 超时={self.server_timeout}s | 退避={offline_backoff}x | 线程池={pool_size}")

        self.tracker = ServerTracker(backoff_multiplier=offline_backoff)
        self.latest_results: list[dict] = []
        self.semaphore = asyncio.Semaphore(self.max_concurrency)
        self._round_counter = 0

    @staticmethod
    def _parse_int_env(val: Optional[str]) -> Optional[int]:
        if val is None:
            return None
        try:
            v = int(val)
            return v if v > 0 else None
        except (ValueError, TypeError):
            logger.warning(f"环境变量 '{val}' 非正整数，已忽略")
            return None

    def _apply_config(self, data: dict):
        changes = []
        for key in self._INTERVAL_KEYS:
            server_val = data.get(key)
            if server_val is not None:
                server_val = int(server_val)
            env_val = self._env_overrides.get(key)
            if env_val is not None:
                final, src = env_val, "env"
            else:
                final = server_val if server_val is not None else getattr(self, key)
                src = "server"
            old = getattr(self, key)
            if final != old:
                changes.append(f"{key}: {old}→{final} ({src})")
            setattr(self, key, final)

        old_ips = set(self.server_ips)
        self.server_ips = data.get("servers", [])
        new_ips = set(self.server_ips)
        self.tracker.remove_stale(new_ips)

        if changes:
            logger.info(f"配置变更: {' | '.join(changes)}")
        if old_ips != new_ips:
            logger.info(f"服务器列表: {len(self.server_ips)} 台 (新增 {len(new_ips - old_ips)}, 移除 {len(old_ips - new_ips)})")
        logger.info(f"配置生效: {len(self.server_ips)} 台 | 监测 {self.monitor_interval}s | 上报 {self.report_interval}s | 刷新 {self.config_refresh_interval}s")

    async def fetch_config(self) -> bool:
        try:
            resp = await self.client.get(f"{self.server_url}/api/node/config", headers=self.headers)
            if resp.status_code == 401:
                logger.error("认证失败，请检查 DEVICE_ID 和 TOKEN")
                return False
            resp.raise_for_status()
            self._apply_config(resp.json())
            return True
        except Exception as e:
            logger.error(f"获取配置失败: {e}")
            return False

    async def report_data(self, results: list[dict]) -> bool:
        if not results:
            return False
        try:
            resp = await self.client.post(f"{self.server_url}/api/node/report", headers=self.headers, json={"reports": results})
            if resp.status_code == 401:
                logger.error("上报时认证失败")
                return False
            resp.raise_for_status()
            online = sum(1 for r in results if r.get("online"))
            logger.info(f"上报成功: {len(results)} 条 (在线 {online})")
            return True
        except Exception as e:
            logger.error(f"上报失败: {e}")
            return False

    # ----------------------------------------------------------
    # MC服务器监测 — 直接按 mcstatus 文档调用
    # ----------------------------------------------------------

    def _check_mc_server(self, ip: str) -> dict:
        """
        同步方法：直接按 mcstatus 文档示例调用

        server = JavaServer.lookup("example.org:1234")
        status = server.status()

        不做任何额外处理，保持和文档一致。
        """
        server = JavaServer.lookup(ip)
        status = server.status()

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

    async def monitor_server(self, ip: str) -> dict:
        """
        检测单台MC服务器状态

        就是把同步的 _check_mc_server 丢到线程池跑，
        外面套一个 wait_for 做超时控制。
        """
        async with self.semaphore:
            try:
                result = await asyncio.wait_for(
                    asyncio.get_running_loop().run_in_executor(
                        self._executor,
                        self._check_mc_server,
                        ip,
                    ),
                    timeout=self.server_timeout + 20,  # 充足超时：DNS+SRV+TCP+协议
                )
                return result
            except asyncio.TimeoutError:
                logger.warning(f"监测超时: {ip} ({self.server_timeout + 20}s)")
                return {"ip": ip, "online": False}
            except Exception as e:
                logger.warning(f"监测失败: {ip} → {e}")
                return {"ip": ip, "online": False}

    @staticmethod
    def _parse_motd(status) -> dict:
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

        return {"plain": text, "html": f"<p>{text}</p>", "minecraft": text, "ansi": f"\x1b[0m{text}\x1b[0m"}

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

        logger.info(f"第{round_num}轮: 监测 {len(to_check)}/{len(self.server_ips)} 台" + (f" (跳过 {skipped})" if skipped else ""))

        start = time.time()
        results: list[dict] = []
        online_count = 0
        failed = 0

        tasks = [self.monitor_server(ip) for ip in to_check]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in batch_results:
            if isinstance(r, Exception):
                failed += 1
                logger.error(f"监测异常: {r}")
            elif r is not None:
                results.append(r)
                self.tracker.update(r["ip"], r.get("online", False), round_num)
                if r.get("online"):
                    online_count += 1

        elapsed = time.time() - start
        logger.info(f"第{round_num}轮完成: 在线 {online_count} / 离线 {len(results) - online_count} / 异常 {failed} | 耗时 {elapsed:.1f}s")

        if skipped > 0:
            checked = {r["ip"] for r in results}
            for old in self.latest_results:
                if old["ip"] not in checked:
                    results.append(old)

        return results

    async def monitor_loop(self):
        while True:
            start = time.time()
            try:
                self.latest_results = await self.monitor_all()
            except Exception as e:
                logger.error(f"监测循环异常: {e}")
            await asyncio.sleep(max(1, self.monitor_interval - (time.time() - start)))

    async def report_loop(self):
        while True:
            start = time.time()
            try:
                if self.latest_results:
                    await self.report_data(self.latest_results)
            except Exception as e:
                logger.error(f"上报循环异常: {e}")
            await asyncio.sleep(max(1, self.report_interval - (time.time() - start)))

    async def config_loop(self):
        while True:
            start = time.time()
            try:
                await self.fetch_config()
            except Exception as e:
                logger.error(f"配置刷新异常: {e}")
            await asyncio.sleep(max(1, self.config_refresh_interval - (time.time() - start)))

    async def run(self):
        logger.info(f"节点启动: ID={self.device_id} | Server={self.server_url}")

        retry = 0
        while True:
            if await self.fetch_config():
                break
            retry += 1
            wait = min(5 * retry, 60)
            logger.warning(f"初始配置失败，{wait}s 后重试 ({retry})...")
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
            self._executor.shutdown(wait=False, cancel_futures=True)
            await self.client.aclose()
            logger.info("节点已停止")


async def main():
    await NodeClient(SERVER_URL, DEVICE_ID, TOKEN).run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("节点已停止 (Ctrl+C)")
