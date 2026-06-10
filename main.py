#!/usr/bin/env python3
"""Redis Key Analyzer - Redis 诊断守护进程

常驻采集模式：读取配置文件，周期执行慢日志/大Key/热Key采集，输出JSON日志文件。
"""

import argparse
import json
import os
import signal
import sys
import logging
import threading
import time

from config import load_config
from slowlog import SlowLogCollector
from bigkey import BigKeyCollector
from hotkey import HotKeyCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("redis_analyzer")

# 全局退出事件
_shutdown_event = threading.Event()


def _sanitize(obj):
    """将 dict 中的 bytes 递归转换为 str"""
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return obj


def _append_json(filepath: str, records: list[dict]):
    """追加写入行式 JSON 到文件"""
    if not records:
        return
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(_sanitize(record), ensure_ascii=False) + "\n")
    logger.info("写入 %d 条记录到 %s", len(records), filepath)


# ─── 采集线程 ─────────────────────────────────────────────

def _slowlog_worker(config: dict, output_path: str):
    """慢日志周期采集线程"""
    redis_cfg = config["redis"]
    slowlog_cfg = config["slowlog"]
    interval = slowlog_cfg["interval"]
    count = slowlog_cfg["count"]
    min_duration_ms = slowlog_cfg["min_duration_ms"]

    collector = SlowLogCollector(
        host=redis_cfg["host"], port=redis_cfg["port"],
        password=redis_cfg["password"], db=redis_cfg["db"],
        socket_timeout=redis_cfg["socket_timeout"],
    )

    # 首次运行：记录当前最大 id 作为游标，避免历史数据全量输出
    try:
        raw = collector._client.slowlog_get(1)
        if raw:
            collector._last_id = raw[0].get("id", -1)
            logger.info("[slowlog] 初始化游标 id=%d，跳过已有记录", collector._last_id)
    except Exception as e:
        logger.warning("[slowlog] 初始化游标失败: %s", e)

    logger.info("[slowlog] 启动，间隔=%ds, 过滤阈值=%.1fms", interval, min_duration_ms)

    while not _shutdown_event.is_set():
        try:
            records = collector.collect(count=count, min_duration_ms=min_duration_ms)
            if records:
                logger.info("[slowlog] 采集到 %d 条慢日志", len(records))
                _append_json(output_path, records)
            else:
                logger.info("[slowlog] 无新增慢日志")
        except Exception as e:
            logger.error("[slowlog] 采集异常: %s", e)

        _shutdown_event.wait(interval)

    collector.close()
    logger.info("[slowlog] 已停止")


def _bigkey_worker(config: dict, output_path: str):
    """大Key周期扫描线程"""
    redis_cfg = config["redis"]
    bigkey_cfg = config["bigkey"]
    interval = bigkey_cfg["interval"]

    collector = BigKeyCollector(
        host=redis_cfg["host"], port=redis_cfg["port"],
        password=redis_cfg["password"], db=redis_cfg["db"],
        socket_timeout=redis_cfg["socket_timeout"],
    )

    logger.info("[bigkey] 启动，间隔=%ds, 阈值=%d bytes", interval, bigkey_cfg["threshold_bytes"])

    while not _shutdown_event.is_set():
        try:
            start = time.time()
            logger.info("[bigkey] 开始扫描...")
            records = collector.collect(
                threshold_bytes=bigkey_cfg["threshold_bytes"],
                scan_count=bigkey_cfg["scan_count"],
                max_keys=bigkey_cfg["max_keys"],
                scan_interval=bigkey_cfg.get("scan_interval", 0),
            )
            elapsed = round(time.time() - start, 2)

            if records:
                logger.info("[bigkey] 发现 %d 个大Key，扫描耗时 %.1fs", len(records), elapsed)
                _append_json(output_path, records)
            else:
                logger.info("[bigkey] 未发现大Key，扫描耗时 %.1fs", elapsed)
        except Exception as e:
            logger.error("[bigkey] 扫描异常: %s", e)

        _shutdown_event.wait(interval)

    collector.close()
    logger.info("[bigkey] 已停止")


def _hotkey_worker(config: dict, output_path: str):
    """热Key周期探测线程"""
    redis_cfg = config["redis"]
    hotkey_cfg = config["hotkey"]
    interval = hotkey_cfg["interval"]
    method = hotkey_cfg["method"]

    collector = HotKeyCollector(
        host=redis_cfg["host"], port=redis_cfg["port"],
        password=redis_cfg["password"], db=redis_cfg["db"],
        socket_timeout=redis_cfg["socket_timeout"],
    )

    # 确定实际采集方式
    if method == "auto":
        if collector.check_lfu_policy():
            method = "freq"
            logger.info("[hotkey] 检测到 LFU 策略，使用 OBJECT FREQ")
        else:
            method = "monitor"
            logger.info("[hotkey] 未检测到 LFU 策略，使用 MONITOR 采样")

    logger.info("[hotkey] 启动，间隔=%ds, 方式=%s", interval, method)

    while not _shutdown_event.is_set():
        try:
            if method == "freq":
                records = collector.collect_by_object_freq(top_n=hotkey_cfg["top_n"])
            else:
                records = collector.collect_by_monitor(
                    sample_seconds=hotkey_cfg["sample_seconds"],
                    top_n=hotkey_cfg["top_n"],
                )

            if records:
                logger.info("[hotkey] 发现 %d 个热Key", len(records))
                _append_json(output_path, records)
            else:
                logger.info("[hotkey] 未发现热Key")
        except Exception as e:
            logger.error("[hotkey] 探测异常: %s", e)

        _shutdown_event.wait(interval)

    collector.close()
    logger.info("[hotkey] 已停止")


# ─── 主入口 ───────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Redis Key Analyzer - 常驻采集守护进程")
    parser.add_argument("--config", "-c", default="config.yaml", help="配置文件路径 (default: config.yaml)")
    args = parser.parse_args()

    # 加载配置
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error("加载配置失败: %s", e)
        sys.exit(1)

    redis_cfg = config["redis"]
    output_cfg = config["output"]
    log_dir = output_cfg["log_dir"]

    logger.info("=" * 50)
    logger.info("Redis Key Analyzer 启动")
    logger.info("Redis: %s:%s db=%d", redis_cfg["host"], redis_cfg["port"], redis_cfg["db"])
    logger.info("日志目录: %s", os.path.abspath(log_dir))
    logger.info("=" * 50)

    # 创建日志目录
    os.makedirs(log_dir, exist_ok=True)

    # 信号处理：优雅退出
    def _signal_handler(sig, frame):
        logger.info("收到退出信号，正在停止...")
        _shutdown_event.set()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # 启动采集线程
    threads = []

    if config["slowlog"]["enabled"]:
        t = threading.Thread(
            target=_slowlog_worker,
            args=(config, os.path.join(log_dir, output_cfg["slowlog_file"])),
            name="slowlog-worker", daemon=True,
        )
        t.start()
        threads.append(t)

    if config["bigkey"]["enabled"]:
        t = threading.Thread(
            target=_bigkey_worker,
            args=(config, os.path.join(log_dir, output_cfg["bigkey_file"])),
            name="bigkey-worker", daemon=True,
        )
        t.start()
        threads.append(t)

    if config["hotkey"]["enabled"]:
        t = threading.Thread(
            target=_hotkey_worker,
            args=(config, os.path.join(log_dir, output_cfg["hotkey_file"])),
            name="hotkey-worker", daemon=True,
        )
        t.start()
        threads.append(t)

    if not threads:
        logger.warning("所有采集器均被禁用，退出")
        sys.exit(0)

    logger.info("已启动 %d 个采集线程", len(threads))

    # 主线程等待退出信号
    try:
        while not _shutdown_event.is_set():
            _shutdown_event.wait(1)
    except KeyboardInterrupt:
        _shutdown_event.set()

    # 等待所有线程结束
    for t in threads:
        t.join(timeout=10)

    logger.info("Redis Key Analyzer 已停止")


if __name__ == "__main__":
    main()
