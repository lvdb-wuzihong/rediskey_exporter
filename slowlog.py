"""Redis 慢日志采集模块"""

import logging
from datetime import datetime, timezone
from typing import Optional

import redis

logger = logging.getLogger(__name__)


class SlowLogCollector:
    """Redis 慢日志采集器"""

    def __init__(self, host: str, port: int = 6379, password: Optional[str] = None,
                 db: int = 0, socket_timeout: float = 5.0):
        self.host = host
        self.port = port
        self.addr = f"{host}:{port}"
        self._last_id = -1  # 上次采集的最大 slowlog id，用于去重
        self._client = redis.Redis(
            host=host, port=port, password=password, db=db,
            socket_timeout=socket_timeout, decode_responses=True
        )

    def get_slowlog_config(self) -> dict:
        """获取当前慢日志配置（CONFIG命令不可用时返回unknown）"""
        try:
            slowlog_log_slower_than = self._client.config_get("slowlog-log-slower-than")
            slowlog_max_len = self._client.config_get("slowlog-max-len")
            return {
                "slowlog_log_slower_than": slowlog_log_slower_than.get("slowlog-log-slower-than", "unknown"),
                "slowlog_max_len": slowlog_max_len.get("slowlog-max-len", "unknown"),
            }
        except redis.exceptions.ResponseError:
            logger.warning("CONFIG 命令不可用（云Redis可能禁用），跳过配置获取")
            return {
                "slowlog_log_slower_than": "unknown",
                "slowlog_max_len": "unknown",
            }

    def get_slowlog_len(self) -> int:
        """获取当前慢日志条数"""
        return self._client.slowlog_len()

    def collect(self, count: int = 128, min_duration_ms: float = 0) -> list[dict]:
        """
        采集慢日志

        Args:
            count: 获取的慢日志条数
            min_duration_ms: 最小耗时阈值（毫秒），低于此值的不记录

        Returns:
            慢日志列表，每条为 dict（按 id 正序，去重后）
        """
        raw_entries = self._client.slowlog_get(count)
        results = []
        max_id = self._last_id

        for entry in raw_entries:
            entry_id = entry.get("id", -1)

            # 去重：跳过上次已采集的记录
            if entry_id <= self._last_id:
                continue

            cmd = entry.get("command", "")
            if isinstance(cmd, list):
                cmd = " ".join(str(c) for c in cmd)

            start_time = entry.get("start_time", 0)
            duration_us = entry.get("duration", 0)
            duration_ms = round(duration_us / 1000, 3)

            # 自定义耗时过滤
            if duration_ms < min_duration_ms:
                continue

            record = {
                "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "type": "slowlog",
                "addr": self.addr,
                "id": entry_id,
                "cmd": cmd,
                "duration_us": duration_us,
                "duration_ms": duration_ms,
                "start_time": datetime.fromtimestamp(start_time, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ") if start_time else "unknown",
                "client_address": entry.get("client_address", "unknown"),
                "client_name": entry.get("client_name", ""),
            }
            results.append(record)

            if entry_id > max_id:
                max_id = entry_id

        # 更新去重游标
        if max_id > self._last_id:
            self._last_id = max_id

        # 按 id 正序排列
        results.sort(key=lambda r: r["id"])
        return results

    def reset(self) -> bool:
        """重置慢日志（清空已记录的慢日志）"""
        try:
            self._client.slowlog_reset()
            self._last_id = -1
            return True
        except Exception as e:
            logger.error("重置慢日志失败: %s", e)
            return False

    def close(self):
        self._client.close()
