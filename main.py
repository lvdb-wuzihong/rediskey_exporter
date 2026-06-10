#!/usr/bin/env python3
"""Redis Key Analyzer - Redis 诊断工具（慢日志 / 大Key / 热Key）"""

import argparse
import json
import sys
import os
import logging

from slowlog import SlowLogCollector
from bigkey import BigKeyCollector, DEFAULT_BIGKEY_THRESHOLD
from hotkey import HotKeyCollector, DEFAULT_SAMPLE_SECONDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("redis_analyzer")


def _sanitize(obj):
    """将 dict 中的 bytes 递归转换为 str"""
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return obj


def output_records(records: list[dict], output_file: str | None = None):
    """输出记录到终端或文件"""
    if output_file:
        mode = "a" if os.path.exists(output_file) else "w"
        with open(output_file, mode, encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(_sanitize(record), ensure_ascii=False) + "\n")
        logger.info("已写入 %d 条记录到 %s", len(records), output_file)
    else:
        for record in records:
            print(json.dumps(_sanitize(record), ensure_ascii=False))


def cmd_slowlog(args):
    """慢日志采集子命令"""
    collector = SlowLogCollector(
        host=args.host, port=args.port, password=args.password, db=args.db
    )
    try:
        config = collector.get_slowlog_config()
        total = collector.get_slowlog_len()
        logger.info(
            "Redis %s 慢日志配置: slowlog-log-slower-than=%s, slowlog-max-len=%s, 当前记录数=%d",
            f"{args.host}:{args.port}",
            config["slowlog_log_slower_than"],
            config["slowlog_max_len"],
            total,
        )

        if total == 0:
            logger.info("当前无慢日志记录")
            return

        count = min(args.count, total)
        records = collector.collect(count=count)
        logger.info("采集到 %d 条慢日志", len(records))
        output_records(records, args.output)

        if args.reset:
            if collector.reset():
                logger.info("慢日志已重置清空")
            else:
                logger.error("慢日志重置失败")
    finally:
        collector.close()


def cmd_bigkey(args):
    """大Key采集子命令"""
    collector = BigKeyCollector(
        host=args.host, port=args.port, password=args.password, db=args.db
    )
    try:
        info = collector.get_info()
        logger.info("Redis %s 总key数: %d", f"{args.host}:{args.port}", info["dbsize"])

        records = collector.collect(
            threshold_bytes=args.threshold,
            scan_count=args.scan_count,
            max_keys=args.max_keys,
        )

        if not records:
            logger.info("未发现大Key (阈值: %d bytes)", args.threshold)
            return

        logger.info("发现 %d 个大Key", len(records))
        output_records(records, args.output)
    finally:
        collector.close()


def cmd_hotkey(args):
    """热Key采集子命令"""
    collector = HotKeyCollector(
        host=args.host, port=args.port, password=args.password, db=args.db
    )
    try:
        method = args.method
        if method == "auto":
            # 自动选择：优先 OBJECT FREQ（LFU策略），降级 MONITOR
            if collector.check_lfu_policy():
                method = "freq"
                logger.info("检测到 LFU 策略，使用 OBJECT FREQ 方式")
            else:
                method = "monitor"
                logger.info("未检测到 LFU 策略，使用 MONITOR 采样方式")

        if method == "freq":
            records = collector.collect_by_object_freq(
                top_n=args.top, scan_count=args.scan_count, max_keys=args.max_keys
            )
        else:
            records = collector.collect_by_monitor(
                sample_seconds=args.sample_seconds, top_n=args.top
            )

        if not records:
            logger.info("未发现热Key")
            return

        logger.info("发现 %d 个热Key", len(records))
        output_records(records, args.output)
    finally:
        collector.close()


def main():
    parser = argparse.ArgumentParser(
        description="Redis Key Analyzer - Redis 诊断工具"
    )
    # 公共连接参数
    parser.add_argument("--host", "-H", default="127.0.0.1", help="Redis 地址 (default: 127.0.0.1)")
    parser.add_argument("--port", "-p", type=int, default=6379, help="Redis 端口 (default: 6379)")
    parser.add_argument("--password", "-a", default=None, help="Redis 密码")
    parser.add_argument("--db", "-n", type=int, default=0, help="Redis DB (default: 0)")
    parser.add_argument("--output", "-o", default=None, help="输出文件路径（默认输出到终端）")

    subparsers = parser.add_subparsers(dest="command", help="子命令")

    # 慢日志子命令
    slowlog_parser = subparsers.add_parser("slowlog", help="采集慢日志")
    slowlog_parser.add_argument("--count", "-c", type=int, default=128, help="获取条数 (default: 128)")
    slowlog_parser.add_argument("--reset", action="store_true", help="采集后重置慢日志")

    # 大Key子命令
    bigkey_parser = subparsers.add_parser("bigkey", help="扫描大Key")
    bigkey_parser.add_argument("--threshold", "-t", type=int, default=DEFAULT_BIGKEY_THRESHOLD,
                               help=f"大Key阈值(字节) (default: {DEFAULT_BIGKEY_THRESHOLD})")
    bigkey_parser.add_argument("--scan-count", type=int, default=1000, help="每次SCAN的count (default: 1000)")
    bigkey_parser.add_argument("--max-keys", type=int, default=0, help="最多扫描key数, 0=不限制 (default: 0)")

    # 热Key子命令
    hotkey_parser = subparsers.add_parser("hotkey", help="采集热Key")
    hotkey_parser.add_argument("--method", "-m", choices=["auto", "monitor", "freq"], default="auto",
                               help="采集方式: auto=自动选择, monitor=MONITOR采样, freq=OBJECT_FREQ (default: auto)")
    hotkey_parser.add_argument("--sample-seconds", "-s", type=int, default=DEFAULT_SAMPLE_SECONDS,
                               help=f"MONITOR采样时长(秒) (default: {DEFAULT_SAMPLE_SECONDS})")
    hotkey_parser.add_argument("--top", "-t", type=int, default=20, help="返回前N个热Key (default: 20)")
    hotkey_parser.add_argument("--scan-count", type=int, default=1000, help="每次SCAN的count (default: 1000)")
    hotkey_parser.add_argument("--max-keys", type=int, default=0, help="最多扫描key数, 0=不限制 (default: 0)")

    args = parser.parse_args()

    if args.command == "slowlog":
        cmd_slowlog(args)
    elif args.command == "bigkey":
        cmd_bigkey(args)
    elif args.command == "hotkey":
        cmd_hotkey(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
