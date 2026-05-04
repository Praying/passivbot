"""用于配置 Passivbot 统一日志的工具函数。"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional, Sequence

TRACE_LEVEL = 5
TRACE_LEVEL_NAME = "TRACE"

DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(message)s"
DEFAULT_FORMAT_WITH_PREFIX = "%(asctime)s %(levelname)-8s [%(log_prefix)s] %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%dT%H:%M:%SZ"
_LAST_LOG_ACTIVITY_MONOTONIC = time.monotonic()
DEFAULT_LOG_FILENAME_MAX_LEN = 100


class PrefixFilter(logging.Filter):
    """为日志记录添加 log_prefix 属性的过滤器。"""

    def __init__(self, prefix: str = ""):
        super().__init__()
        self.prefix = prefix

    def filter(self, record: logging.LogRecord) -> bool:
        record.log_prefix = self.prefix
        return True


class ActivityFilter(logging.Filter):
    """追踪最近发出的日志记录的过滤器。"""

    def filter(self, record: logging.LogRecord) -> bool:
        mark_log_activity()
        return True


def mark_log_activity() -> None:
    global _LAST_LOG_ACTIVITY_MONOTONIC
    _LAST_LOG_ACTIVITY_MONOTONIC = time.monotonic()


def get_last_log_activity_monotonic() -> float:
    return _LAST_LOG_ACTIVITY_MONOTONIC


_LOG_LEVEL_ALIASES = {
    "warning": 0,
    "warn": 0,
    "w": 0,
    "info": 1,
    "i": 1,
    "debug": 2,
    "d": 2,
    "trace": 3,
    "t": 3,
}


def normalize_log_level(value, default=None):
    """返回标准化的日志级别 0-3，无效或缺失时返回默认值。"""
    if value is None:
        return default
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned in _LOG_LEVEL_ALIASES:
            return _LOG_LEVEL_ALIASES[cleaned]
        try:
            value = float(cleaned)
        except ValueError:
            return default
    try:
        level = int(float(value))
    except (TypeError, ValueError):
        return default
    return max(0, min(level, 3))


def resolve_log_level(cli_value, config_value, fallback=1):
    """从 CLI 覆盖值和配置值解析最终日志级别。"""
    cli_level = normalize_log_level(cli_value, None)
    if cli_level is not None:
        return cli_level
    cfg_level = normalize_log_level(config_value, None)
    if cfg_level is not None:
        return cfg_level
    return fallback


def _ensure_trace_level() -> None:
    """如果缺少 TRACE 日志级别，则在 logging 模块上注册它。"""
    if logging.getLevelName(TRACE_LEVEL) != TRACE_LEVEL_NAME:
        logging.addLevelName(TRACE_LEVEL, TRACE_LEVEL_NAME)
    if getattr(logging, TRACE_LEVEL_NAME, None) != TRACE_LEVEL:
        setattr(logging, TRACE_LEVEL_NAME, TRACE_LEVEL)

    if not hasattr(logging.Logger, "trace"):

        def trace(self: logging.Logger, msg: str, *args, **kwargs) -> None:
            if self.isEnabledFor(TRACE_LEVEL):
                self._log(TRACE_LEVEL, msg, args, **kwargs)

        logging.Logger.trace = trace  # type: ignore[attr-defined]


def _normalize_debug(debug: Optional[int | str]) -> int:
    level = normalize_log_level(debug, None)
    if level is None:
        return 1
    return level


def _debug_to_level(debug: int) -> int:
    if debug <= 0:
        return logging.WARNING
    if debug == 1:
        return logging.INFO
    if debug == 2:
        return logging.DEBUG
    return TRACE_LEVEL


def sanitize_log_filename(text: str, *, max_len: int = DEFAULT_LOG_FILENAME_MAX_LEN) -> str:
    """返回文件系统安全的文件名片段。"""
    sanitized = re.sub(r"[\s/\\]", "_", text)
    sanitized = re.sub(r'[<>:"|?*]', "", sanitized)
    sanitized = sanitized.strip(". ")
    if len(sanitized) > max_len:
        sanitized = sanitized[:max_len]
    return sanitized or "log"


def create_command_log_filename(
    command_args: Sequence[object], *, timestamp: Optional[datetime] = None
) -> str:
    """返回命令调用的带时间戳日志文件名。"""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
    elif timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    command_str = " ".join(str(part) for part in command_args)
    sanitized_command = sanitize_log_filename(command_str)
    prefix = timestamp.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{sanitized_command}.log"


def build_command_log_path(
    command_args: Sequence[object], log_dir: str | Path, *, timestamp: Optional[datetime] = None
) -> Path:
    """返回给定目录下命令调用的日志文件路径。"""
    return Path(log_dir).expanduser() / create_command_log_filename(command_args, timestamp=timestamp)


def update_stable_log_alias(alias_path: str | Path, target_path: str | Path) -> None:
    """将稳定的日志别名指向当前运行的带时间戳日志文件。"""
    alias = Path(alias_path).expanduser()
    target = Path(target_path).expanduser()
    alias.parent.mkdir(parents=True, exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    if alias.exists() or alias.is_symlink():
        if alias.is_dir() and not alias.is_symlink():
            raise RuntimeError(f"stable log alias path is a directory: {alias}")
        alias.unlink()
    relative_target = os.path.relpath(target, start=alias.parent)
    try:
        alias.symlink_to(relative_target)
    except OSError as exc:
        raise RuntimeError(
            f"failed to create stable live log alias {alias} -> {target}: {exc}"
        ) from exc


def configure_logging(
    debug: Optional[int | str] = 1,
    *,
    log_file: Optional[str] = None,
    current_log_file: Optional[str] = None,
    rotation: bool = False,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    stream: bool = True,
    fmt: Optional[str] = None,
    datefmt: str = DEFAULT_DATEFMT,
    prefix: Optional[str] = None,
) -> None:
    """根据 Passivbot 的调试设置初始化根日志记录器。

    Args:
        debug: 日志级别 (0=warning, 1=info, 2=debug, 3=trace)
        log_file: 可选的标准日志文件路径
        current_log_file: 可选的稳定别名路径，指向当前日志文件
        rotation: 启用日志轮转
        max_bytes: 轮转前每个日志文件的最大字节数
        backup_count: 保留的备份数量
        stream: 启用控制台输出
        fmt: 自定义日志格式（默认基于前缀）
        datefmt: 日期格式字符串
        prefix: 可选的前缀，添加到所有日志消息中（如交易所名称）
    """
    _ensure_trace_level()
    debug_level = _normalize_debug(debug)
    numeric_level = _debug_to_level(debug_level)

    # 根据前缀选择格式
    if fmt is None:
        fmt = DEFAULT_FORMAT_WITH_PREFIX if prefix else DEFAULT_FORMAT

    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    formatter.converter = time.gmtime
    handlers: list[logging.Handler] = []

    # 如果需要则创建前缀过滤器
    prefix_filter = PrefixFilter(prefix or "") if prefix else None
    activity_filter = ActivityFilter()

    if stream:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(numeric_level)
        stream_handler.addFilter(activity_filter)
        if prefix_filter:
            stream_handler.addFilter(prefix_filter)
        handlers.append(stream_handler)

    if log_file:
        path = Path(log_file).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        if current_log_file:
            update_stable_log_alias(current_log_file, path)
        if rotation:
            file_handler = RotatingFileHandler(path, maxBytes=max_bytes, backupCount=backup_count)
        else:
            file_handler = logging.FileHandler(path)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(numeric_level)
        file_handler.addFilter(activity_filter)
        if prefix_filter:
            file_handler.addFilter(prefix_filter)
        handlers.append(file_handler)

    root = logging.getLogger()
    root.setLevel(numeric_level)

    for existing in list(root.handlers):
        root.removeHandler(existing)
        existing.close()

    for handler in handlers:
        root.addHandler(handler)

    # 配置 CCXT 日志记录器仅在 TRACE 级别记录。
    # CCXT 在 DEBUG 级别记录完整的 API 请求/响应负载，过于嘈杂。
    # 根据 log_analysis_prompt.md 指南，这些负载应属于 TRACE（级别 3）。
    ccxt_logger = logging.getLogger("ccxt")
    if debug_level >= 3:
        # TRACE 模式：允许 CCXT 日志通过
        ccxt_logger.setLevel(TRACE_LEVEL)
    else:
        # DEBUG 及以下：抑制 CCXT 嘈杂的 API 负载
        # 设置为 WARNING，仅显示 CCXT 的实际警告/错误
        ccxt_logger.setLevel(logging.WARNING)


def resolve_live_log_file_settings(
    config: dict[str, Any], *, user: str, command_args: Optional[Sequence[object]] = None
) -> dict[str, Any]:
    """返回用于标准实盘文件日志的 configure_logging 关键字参数。"""
    logging_cfg = config.get("logging", {}) if isinstance(config, dict) else {}
    if not isinstance(logging_cfg, dict):
        logging_cfg = {}
    if not bool(logging_cfg.get("persist_to_file", True)):
        return {
            "log_file": None,
            "current_log_file": None,
            "rotation": False,
            "max_bytes": 10 * 1024 * 1024,
            "backup_count": 5,
        }

    log_dir = str(logging_cfg.get("dir", "logs")).strip() or "logs"
    max_bytes_mb = float(logging_cfg.get("max_bytes_mb", 10.0))
    backup_count = int(logging_cfg.get("backup_count", 5))
    effective_command_args: Sequence[object]
    if command_args:
        effective_command_args = command_args
    else:
        effective_command_args = ["passivbot live", "--user", user]
    return {
        "log_file": str(build_command_log_path(effective_command_args, log_dir)),
        "current_log_file": str(Path(log_dir).expanduser() / f"{user}.log"),
        "rotation": bool(logging_cfg.get("rotation", False)),
        "max_bytes": max(1, int(max_bytes_mb * 1024 * 1024)),
        "backup_count": max(0, backup_count),
    }
