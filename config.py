"""配置加载模块"""

import logging
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# 默认配置
DEFAULTS = {
    "redis": {
        "host": "127.0.0.1",
        "port": 6379,
        "password": None,
        "db": 0,
        "socket_timeout": 10,
    },
    "slowlog": {
        "enabled": True,
        "interval": 60,
        "count": 128,
        "min_duration_ms": 0,
    },
    "bigkey": {
        "enabled": True,
        "interval": 600,
        "threshold_bytes": 10 * 1024 * 1024,
        "scan_count": 1000,
        "max_keys": 0,
    },
    "hotkey": {
        "enabled": True,
        "interval": 120,
        "method": "auto",
        "sample_seconds": 5,
        "top_n": 20,
    },
    "output": {
        "log_dir": "./logs",
        "slow_query_file": "slow_query.log",
        "bigkey_file": "bigkey.log",
        "hotkey_file": "hotkey.log",
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    """深度合并两个字典，override 覆盖 base"""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(path: str) -> dict[str, Any]:
    """
    加载 YAML 配置文件

    Args:
        path: 配置文件路径

    Returns:
        合并后的配置字典
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            user_config = yaml.safe_load(f) or {}
    except FileNotFoundError:
        logger.error("配置文件不存在: %s", path)
        raise
    except yaml.YAMLError as e:
        logger.error("配置文件解析失败: %s", e)
        raise

    # 与默认配置合并
    config = _deep_merge(DEFAULTS, user_config)

    # 校验必填字段
    _validate(config)

    logger.info("配置加载成功: %s", path)
    return config


def _validate(config: dict):
    """校验配置必填字段"""
    redis_cfg = config.get("redis", {})
    if not redis_cfg.get("host"):
        raise ValueError("配置缺少 redis.host")

    # 校验间隔合理性
    for section in ("slowlog", "bigkey", "hotkey"):
        cfg = config.get(section, {})
        if cfg.get("enabled") and cfg.get("interval", 0) < 1:
            raise ValueError(f"{section}.interval 必须 >= 1 秒")
