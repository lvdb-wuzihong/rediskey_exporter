"""Redis 大Key 采集模块

通过 SCAN 遍历 + MEMORY USAGE 检测大Key
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import redis
from config import now_str

logger = logging.getLogger(__name__)

# 默认大Key阈值（字节）
DEFAULT_BIGKEY_THRESHOLD = 10 * 1024 * 1024  # 10MB


class BigKeyCollector:
    """Redis 大Key 采集器"""

    def __init__(self, host: str, port: int = 6379, password: Optional[str] = None,
                 db: int = 0, socket_timeout: float = 10.0):
        self.host = host
        self.port = port
        self.addr = f"{host}:{port}"
        self._client = redis.Redis(
            host=host, port=port, password=password, db=db,
            socket_timeout=socket_timeout, decode_responses=True
        )

    def get_info(self) -> dict:
        """获取 Redis 基本信息"""
        info = self._client.info("keyspace")
        dbsize = self._client.dbsize()
        return {"dbsize": dbsize, "keyspace": info}

    def collect(self, threshold_bytes: int = DEFAULT_BIGKEY_THRESHOLD,
                scan_count: int = 1000, max_keys: int = 0,
                scan_interval: float = 0) -> list[dict]:
        """
        扫描并采集大Key

        Args:
            threshold_bytes: 大Key阈值（字节），超过此值的key会被记录
            scan_count: 每次SCAN的count参数
            max_keys: 最多扫描的key数量，0表示不限制
            scan_interval: 每批 SCAN 之间的间隔（秒），降低对 Redis 的影响

        Returns:
            大Key记录列表
        """
        big_keys = []
        scanned = 0

        logger.info("开始扫描大Key (阈值: %d bytes / %.2f MB)", threshold_bytes, threshold_bytes / 1024 / 1024)

        cursor = 0
        while True:
            cursor, keys = self._client.scan(cursor=cursor, count=scan_count)
            for key in keys:
                try:
                    record = self._analyze_key(key)
                    if record and record["memory_bytes"] >= threshold_bytes:
                        big_keys.append(record)
                except Exception as e:
                    logger.debug("分析key失败 %s: %s", key, e)

                scanned += 1
                if max_keys > 0 and scanned >= max_keys:
                    break

                if scanned % 10000 == 0:
                    logger.info("已扫描 %d 个key, 发现 %d 个大Key", scanned, len(big_keys))

            if cursor == 0 or (max_keys > 0 and scanned >= max_keys):
                break

            # 每批 SCAN 间隔，降低 Redis 负载
            if scan_interval > 0:
                time.sleep(scan_interval)

        logger.info("扫描完成: 共扫描 %d 个key, 发现 %d 个大Key", scanned, len(big_keys))
        return big_keys

    def _analyze_key(self, key: str) -> Optional[dict]:
        """分析单个key的大小"""
        # 获取key类型
        key_type = self._client.type(key)
        if key_type == "none":
            return None

        # 获取内存占用
        memory_bytes = self._client.memory_usage(key)
        if memory_bytes is None:
            return None

        # 获取元素数量
        elements = self._get_elements(key, key_type)

        # 获取TTL
        ttl = self._client.ttl(key)

        return {
            "time": now_str(),
            "type": "bigkey",
            "addr": self.addr,
            "key": key,
            "data_type": key_type,
            "memory_bytes": memory_bytes,
            "memory_mb": round(memory_bytes / 1024 / 1024, 3),
            "elements": elements,
            "ttl": ttl,
        }

    def _get_elements(self, key: str, key_type: str) -> int:
        """获取key的元素数量"""
        try:
            if key_type == "string":
                return 1
            elif key_type == "hash":
                return self._client.hlen(key)
            elif key_type == "list":
                return self._client.llen(key)
            elif key_type == "set":
                return self._client.scard(key)
            elif key_type == "zset":
                return self._client.zcard(key)
            elif key_type == "stream":
                return self._client.xlen(key)
            else:
                return -1
        except Exception:
            return -1

    def close(self):
        self._client.close()
