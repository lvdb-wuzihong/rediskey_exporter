"""Redis 热Key 采集模块

通过 MONITOR 短时采样统计热Key（访问频率最高的Key）
也支持 OBJECT FREQ（需 Redis 配置 LFU 淘汰策略）
"""

import logging
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

import redis
from config import now_str

logger = logging.getLogger(__name__)

# MONITOR 默认采样时长（秒）
DEFAULT_SAMPLE_SECONDS = 5


class HotKeyCollector:
    """Redis 热Key 采集器"""

    def __init__(self, host: str, port: int = 6379, password: Optional[str] = None,
                 db: int = 0, socket_timeout: float = 10.0):
        self.host = host
        self.port = port
        self.addr = f"{host}:{port}"
        self._password = password
        self._db = db
        self._socket_timeout = socket_timeout
        self._client = redis.Redis(
            host=host, port=port, password=password, db=db,
            socket_timeout=socket_timeout, decode_responses=True
        )

    def check_lfu_policy(self) -> bool:
        """检查是否配置了 LFU 淘汰策略（用于 OBJECT FREQ）"""
        try:
            config = self._client.config_get("maxmemory-policy")
            policy = config.get("maxmemory-policy", "noeviction")
            return "lfu" in policy.lower()
        except redis.exceptions.ResponseError:
            logger.warning("CONFIG 命令不可用，无法检测淘汰策略，默认不使用 OBJECT FREQ")
            return False

    def collect_by_monitor(self, sample_seconds: int = DEFAULT_SAMPLE_SECONDS,
                           top_n: int = 20) -> list[dict]:
        """
        通过 MONITOR 短时采样统计热Key

        Args:
            sample_seconds: 采样时长（秒）
            top_n: 返回前N个热Key

        Returns:
            热Key记录列表
        """
        logger.info("开始 MONITOR 采样 %d 秒...", sample_seconds)

        counter = Counter()
        stop_event = threading.Event()

        def _monitor():
            """MONITOR 监听线程"""
            pubsub_client = redis.Redis(
                host=self.host, port=self.port,
                password=self._password, db=self._db,
                socket_timeout=self._socket_timeout,
                decode_responses=True
            )
            try:
                with pubsub_client.monitor() as m:
                    for cmd_info in m.listen():
                        if stop_event.is_set():
                            break
                        # 只统计读操作
                        cmd_type = cmd_info.get("command", "").upper()
                        if cmd_type in ("GET", "MGET", "HGET", "HGETALL", "HVALS",
                                        "LRANGE", "SMEMBERS", "ZRANGE", "ZRANGEBYSCORE",
                                        "GETRANGE", "STRLEN", "EXISTS", "TTL", "PTTL"):
                            key = cmd_info.get("key", "")
                            if key:
                                counter[key] += 1
            except Exception as e:
                if not stop_event.is_set():
                    logger.error("MONITOR 监听异常: %s", e)
            finally:
                pubsub_client.close()

        # 启动 MONITOR 线程
        monitor_thread = threading.Thread(target=_monitor, daemon=True)
        monitor_thread.start()

        # 等待采样时间
        time.sleep(sample_seconds)
        stop_event.set()
        monitor_thread.join(timeout=3)

        total_ops = sum(counter.values())
        logger.info("采样完成: 共捕获 %d 次读操作", total_ops)

        if not counter:
            logger.info("采样期间无读操作，未发现热Key")
            return []

        # 取 top N
        hot_keys = []
        for key, access_count in counter.most_common(top_n):
            # 获取key的详细信息
            detail = self._get_key_detail(key)
            record = {
                "time": now_str(),
                "type": "hotkey",
                "addr": self.addr,
                "key": key,
                "access_count": access_count,
                "sample_seconds": sample_seconds,
                "total_ops": total_ops,
                "access_ratio": round(access_count / total_ops, 4) if total_ops > 0 else 0,
            }
            if detail:
                record.update(detail)
            hot_keys.append(record)

        return hot_keys

    def collect_by_object_freq(self, top_n: int = 20, scan_count: int = 1000,
                                max_keys: int = 0) -> list[dict]:
        """
        通过 OBJECT FREQ 采集热Key（需要 LFU 淘汰策略）

        Args:
            top_n: 返回前N个热Key
            scan_count: 每次SCAN的count
            max_keys: 最多扫描key数量，0表示不限制

        Returns:
            热Key记录列表
        """
        if not self.check_lfu_policy():
            logger.warning("Redis 未配置 LFU 淘汰策略，OBJECT FREQ 不可用")
            return []

        logger.info("使用 OBJECT FREQ 采集热Key (LFU模式)")

        freq_list = []
        scanned = 0
        cursor = 0

        while True:
            cursor, keys = self._client.scan(cursor=cursor, count=scan_count)
            for key in keys:
                try:
                    freq = self._client.object("freq", key)
                    if freq is not None:
                        freq_list.append((key, freq))
                except Exception:
                    pass
                scanned += 1
                if max_keys > 0 and scanned >= max_keys:
                    break

            if cursor == 0 or (max_keys > 0 and scanned >= max_keys):
                break

        # 按频率排序取 top N
        freq_list.sort(key=lambda x: x[1], reverse=True)
        hot_keys = []
        for key, freq in freq_list[:top_n]:
            detail = self._get_key_detail(key)
            record = {
                "time": now_str(),
                "type": "hotkey",
                "addr": self.addr,
                "key": key,
                "object_freq": freq,
                "method": "object_freq",
            }
            if detail:
                record.update(detail)
            hot_keys.append(record)

        logger.info("扫描 %d 个key, 发现 %d 个热Key", scanned, len(hot_keys))
        return hot_keys

    def _get_key_detail(self, key: str) -> Optional[dict]:
        """获取key的辅助信息"""
        try:
            key_type = self._client.type(key)
            ttl = self._client.ttl(key)
            return {
                "data_type": key_type if key_type != "none" else "unknown",
                "ttl": ttl,
            }
        except Exception:
            return None

    def close(self):
        self._client.close()
