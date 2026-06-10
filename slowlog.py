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

    def collect(self, count: int = 128) -> list[dict]:
        """
        采集慢日志

        Args:
            count: 获取的慢日志条数

        Returns:
            慢日志列表，每条为 dict
        """
        raw_entries = self._client.slowlog_get(count)
        results = []
        for entry in raw_entries:
            # slowlog_get 返回的字段: id, start_time, duration, command, client_address, client_name
            cmd = entry.get("command", "")
            if isinstance(cmd, list):
                cmd = " ".join(str(c) for c in cmd)

            start_time = entry.get("start_time", 0)
            duration_us = entry.get("duration", 0)

            record = {
                "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "type": "slowlog",
                "addr": self.addr,
                "id": entry.get("id", -1),
                "cmd": cmd,
                "duration_us": duration_us,
                "duration_ms": round(duration_us / 1000, 3),
                "start_time": datetime.fromtimestamp(start_time, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ") if start_time else "unknown",
                "client_address": entry.get("client_address", "unknown"),
                "client_name": entry.get("client_name", ""),
            }
            results.append(record)
        return results

    def reset(self) -> bool:
        """重置慢日志（清空已记录的慢日志）"""
        try:
            self._client.slowlog_reset()
            return True
        except Exception as e:
            logger.error("重置慢日志失败: %s", e)
            return False

    def close(self):
        self._client.close()
