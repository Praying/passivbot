"""
CandlestickManager: 轻量级 1m OHLCV 管理器，支持缺口标准化。

本模块提供一个最小化、自包含的实现，适配 tests/test_candlestick_manager.py
中的单元测试，同时遵循所要求的 API 和数据格式。核心功能：

- UTC 毫秒时间戳和结构化 NumPy dtype 的 K 线数据
- 缺口标准化：合成的零成交 K 线（不持久化）
- 包含两端的时间范围选择，按分钟对齐
- 收盘价/成交量/对数区间的最新 EMA 从缓存 K 线惰性计算
- 分片保存采用原子写入和 index.json 维护

示例
-------
>>> from candlestick_manager import CandlestickManager, ONE_MIN_MS
>>> cm = CandlestickManager(exchange=None, exchange_name="demo")
>>> # 直接向缓存预加载一些 K 线 (ts, o, h, l, c, bv)
>>> import time, numpy as np
>>> now = int(time.time() * 1000)
>>> base = _floor_minute(now) - 5 * ONE_MIN_MS
>>> arr = np.array([
...     (base + i * ONE_MIN_MS, 1+i, 1+i, 1+i, 1+i, float(i)) for i in range(5)
... ], dtype=CANDLE_DTYPE)
>>> cm._cache["FOO/USDT"] = arr
>>> import asyncio
>>> asyncio.run(cm.get_latest_ema_close("FOO/USDT", span=5))
1.0
"""

from __future__ import annotations

import asyncio
import calendar
import inspect
import json
import logging
import math
import os
import shutil
import sys

import time
import zlib
import atexit
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple, TypedDict, TYPE_CHECKING
import threading
from collections import OrderedDict

if TYPE_CHECKING:
    import aiohttp

import warnings
import time
from datetime import datetime, timezone

import numpy as np
import portalocker  # type: ignore

from legacy_data_migrator import (
    standardize_cache_directories,
    migrate_legacy_data_all_on_init,
    merge_duplicate_symbol_directories,
    normalize_ccxt_volume_to_base,
)

# 抑制 portalocker 的 "timeout has no effect in blocking mode" 警告
warnings.filterwarnings(
    "ignore", message="timeout has no effect in blocking mode", module="portalocker"
)

# ----- 常量和 dtype -----

ONE_MIN_MS = 60_000

_LOCK_TIMEOUT_SECONDS = 10.0
_LOCK_STALE_SECONDS = 180.0
_LOCK_BACKOFF_INITIAL = 0.1
_LOCK_BACKOFF_MAX = 2.0
_GATEIO_RECENT_1M_LIMIT_CANDLES = 9_990

# 参见: https://github.com/enarjord/passivbot/issues/547
# 在 Windows 上运行时为 True（用于文件/路径兼容命名）
windows_compatibility = (
    sys.platform.startswith("win") or os.environ.get("WINDOWS_COMPATIBILITY") == "1"
)


@dataclass
class _LockRecord:
    lock: portalocker.Lock
    count: int
    acquired_at: float
    path: str


class GapEntry(TypedDict, total=False):
    """存储在 index.json known_gaps 中的增强缺口元数据。"""

    start_ts: int  # 缺口起始时间戳 (ms)
    end_ts: int  # 缺口结束时间戳 (ms)
    retry_count: int  # 获取尝试次数（达到 3 次后标记为持久缺口）
    reason: str  # "auto_detected", "exchange_downtime", "no_archive", "fetch_failed", "manual", "no_trades"
    added_at: int  # 缺口首次检测到的时间戳 (ms)


# 标记缺口为持久缺口前的最大获取尝试次数
_GAP_MAX_RETRIES = 3

# 有效的缺口原因
GAP_REASON_AUTO = "auto_detected"
GAP_REASON_EXCHANGE_DOWNTIME = "exchange_downtime"
GAP_REASON_NO_ARCHIVE = "no_archive"
GAP_REASON_FETCH_FAILED = "fetch_failed"
GAP_REASON_MANUAL = "manual"
GAP_REASON_NO_TRADES = "no_trades"


_FIRST_OHLCV_EXCHANGE_CACHE_ALIASES = {
    "binance": "binanceusdm",
}


CANDLE_DTYPE = np.dtype(
    [
        ("ts", "int64"),
        ("o", "float32"),
        ("h", "float32"),
        ("l", "float32"),
        ("c", "float32"),
        ("bv", "float32"),
    ]
)

EMA_SERIES_DTYPE = np.dtype(
    [
        ("ts", "int64"),
        ("ema", "float32"),
    ]
)


# ----- 工具函数 -----


def _linear_interpolate(value0: float, value1: float, ratio: float) -> float:
    return float(value0 + (value1 - value0) * ratio)


def ohlcv_xm_to_1m(candle: np.void, minutes: int) -> np.ndarray:
    """将一根高级别时间周期的 OHLCV K 线展开为确定性的合成 1m K 线。"""
    if minutes <= 0:
        raise ValueError(f"minutes must be > 0, got {minutes}")

    ts = int(candle["ts"])
    o = float(candle["o"])
    h = float(candle["h"])
    l = float(candle["l"])
    c = float(candle["c"])
    bv = float(candle["bv"])

    if not all(math.isfinite(x) for x in (o, h, l, c, bv)):
        raise ValueError("all OHLCV values must be finite")
    if h < l:
        h, l = l, h
    o = min(max(o, l), h)
    c = min(max(c, l), h)

    out = np.zeros(minutes, dtype=CANDLE_DTYPE)
    out["ts"] = np.arange(ts, ts + minutes * ONE_MIN_MS, ONE_MIN_MS, dtype=np.int64)
    out["bv"] = float(bv / minutes)

    last_idx = minutes - 1
    if last_idx == 0:
        out[0]["o"] = o
        out[0]["h"] = h
        out[0]["l"] = l
        out[0]["c"] = c
        return out

    pivot_a = min(last_idx, max(1, minutes // 3))
    pivot_b = min(last_idx, max(pivot_a + 1, (2 * minutes) // 3))

    if c >= o:
        waypoints = [(0, o), (pivot_a, l), (pivot_b, h), (last_idx, c)]
        low_idx = pivot_a
        high_idx = pivot_b
    else:
        waypoints = [(0, o), (pivot_a, h), (pivot_b, l), (last_idx, c)]
        high_idx = pivot_a
        low_idx = pivot_b

    deduped = [waypoints[0]]
    for idx, value in waypoints[1:]:
        if idx > deduped[-1][0]:
            deduped.append((idx, value))
        else:
            deduped[-1] = (idx, value)

    close_path = np.empty(minutes, dtype=np.float64)
    close_path[0] = o
    for (i0, v0), (i1, v1) in zip(deduped, deduped[1:]):
        span = max(1, i1 - i0)
        for minute_idx in range(i0, i1 + 1):
            ratio = 0.0 if i1 == i0 else (minute_idx - i0) / span
            close_path[minute_idx] = min(max(_linear_interpolate(v0, v1, ratio), l), h)

    prev_close = o
    for minute_idx in range(minutes):
        minute_open = prev_close
        minute_close = float(close_path[minute_idx])
        minute_high = max(minute_open, minute_close)
        minute_low = min(minute_open, minute_close)
        if minute_idx == high_idx:
            minute_high = max(minute_high, h)
        if minute_idx == low_idx:
            minute_low = min(minute_low, l)
        out[minute_idx]["o"] = minute_open
        out[minute_idx]["h"] = minute_high
        out[minute_idx]["l"] = minute_low
        out[minute_idx]["c"] = minute_close
        prev_close = minute_close

    return out


def ohlcv_5m_to_1m(candle: np.void) -> np.ndarray:
    return ohlcv_xm_to_1m(candle, 5)


def ohlcv_15m_to_1m(candle: np.void) -> np.ndarray:
    return ohlcv_xm_to_1m(candle, 15)


def synthesize_1m_from_higher_tf(candles: np.ndarray, tf_minutes: int) -> np.ndarray:
    """将高级别时间周期的 K 线数组展开为合成的 1m OHLCV K 线。"""
    arr = _ensure_dtype(candles)
    if arr.size == 0:
        return np.empty((0,), dtype=CANDLE_DTYPE)
    if tf_minutes == 5:
        expanded = [ohlcv_5m_to_1m(row) for row in arr]
    elif tf_minutes == 15:
        expanded = [ohlcv_15m_to_1m(row) for row in arr]
    else:
        raise ValueError(f"unsupported tf_minutes={tf_minutes}")
    if not expanded:
        return np.empty((0,), dtype=CANDLE_DTYPE)
    return np.sort(np.concatenate(expanded), order="ts")


def get_caller_name(depth: int = 2, logger: Optional[logging.Logger] = None) -> str:
    """返回更有用的调用来源信息，用于调试日志。

    启发式策略：
    - 跳过 CandlestickManager 帧和常见包装帧（"one"、"<listcomp>"、asyncio 内部）
    - 优先返回包含 "passivbot" 的实例方法帧（如果存在）
    - 否则返回第一个非包装帧，格式为 module.Class.func 或 module.func
    """

    def frame_to_name(fr) -> str:
        try:
            func = getattr(fr.f_code, "co_name", "unknown")
            mod = fr.f_globals.get("__name__", None)
            cls = None
            if "self" in fr.f_locals and fr.f_locals["self"] is not None:
                cls = type(fr.f_locals["self"]).__name__
            elif "cls" in fr.f_locals and fr.f_locals["cls"] is not None:
                cls = getattr(fr.f_locals["cls"], "__name__", None)
            parts = []
            if isinstance(mod, str) and mod:
                parts.append(mod)
            if isinstance(cls, str) and cls:
                parts.append(cls)
            if isinstance(func, str) and func:
                parts.append(func)
            return ".".join(parts) if parts else "unknown"
        except Exception:
            return "unknown"

    frame = inspect.currentframe()
    target = frame
    fallback_name = "unknown"
    try:
        # Initial hop
        for _ in range(max(0, int(depth))):
            if target is None:
                break
            target = target.f_back  # type: ignore[attr-defined]
        if target is not None:
            fallback_name = frame_to_name(target)

        # Walk up to find a meaningful caller
        cur = target
        preferred: Optional[str] = None
        for _ in range(20):  # safety cap
            if cur is None:
                break
            try:
                slf = cur.f_locals.get("self") if hasattr(cur, "f_locals") else None
                is_cm = slf is not None and type(slf).__name__ == "CandlestickManager"
            except Exception:
                is_cm = False
            func = getattr(getattr(cur, "f_code", None), "co_name", "")
            mod = None
            try:
                mod = cur.f_globals.get("__name__")
            except Exception:
                mod = None

            # Skip common wrappers and asyncio internals
            skip_names = {
                "one",
                "<listcomp>",
                "<dictcomp>",
                "<lambda>",
                "_run",
                "gather",
                "create_task",
            }
            is_asyncio = isinstance(mod, str) and (
                mod.startswith("asyncio.") or mod == "asyncio.events"
            )
            if not is_cm and func not in skip_names and not is_asyncio:
                name = frame_to_name(cur)
                if isinstance(mod, str) and "passivbot" in mod and name and name != "unknown":
                    # Prefer first passivbot frame
                    preferred = name
                    break
                if name and name != "unknown" and preferred is None:
                    preferred = name
            cur = cur.f_back  # type: ignore[attr-defined]
    finally:
        try:
            del frame
        except Exception:
            pass
        try:
            del target  # type: ignore[name-defined]
        except Exception:
            pass
    return preferred or fallback_name


def _utc_now_ms() -> int:
    return int(time.time() * 1000)


def _floor_minute(ms: int) -> int:
    return (int(ms) // ONE_MIN_MS) * ONE_MIN_MS


def _ensure_dtype(a: np.ndarray) -> np.ndarray:
    if a.dtype != CANDLE_DTYPE:
        return a.astype(CANDLE_DTYPE, copy=False)
    return a


def _ts_index(a: np.ndarray) -> np.ndarray:
    """返回排序后的 ts 列作为纯 int64 数组。"""
    if a.size == 0:
        return np.empty((0,), dtype=np.int64)
    return np.asarray(a["ts"], dtype=np.int64)


def _sanitize_symbol(symbol: str) -> str:
    sanitized = symbol.replace("/", "_")
    # 参见: https://github.com/enarjord/passivbot/issues/547
    # 如果在"Windows 兼容模式"下运行，
    # 也替换 ':' 为 '_' 以确保兼容 Windows 文件命名限制。
    if windows_compatibility:
        sanitized = sanitized.replace(":", "_")
    return sanitized


def _quarantine_gateio_cache_if_stale(cache_base: str, cutoff_date: str) -> None:
    """如果任意分片早于 cutoff_date，将 gateio 缓存移动到带时间戳的备份目录。"""
    try:
        cutoff = datetime.strptime(cutoff_date, "%Y-%m-%d").date()
    except Exception:
        logging.warning(
            "Invalid GATEIO_CACHE_CUTOFF_DATE=%r; skipping gateio cache check", cutoff_date
        )
        return

    gateio_root = os.path.join(cache_base, "gateio")
    if not os.path.isdir(gateio_root):
        return

    tf_root = os.path.join(gateio_root, "1m")
    if not os.path.isdir(tf_root):
        return

    for sym in os.listdir(tf_root):
        sym_dir = os.path.join(tf_root, sym)
        if not os.path.isdir(sym_dir):
            continue
        for fname in os.listdir(sym_dir):
            if not fname.endswith(".npy"):
                continue
            try:
                day = datetime.strptime(fname[:10], "%Y-%m-%d").date()
            except Exception:
                continue
            if day < cutoff:
                stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
                backup = f"{gateio_root}_backup_{stamp}"
                logging.warning(
                    "GateIO cache has shards before %s; moving %s -> %s. "
                    "Delete backup after confirming volumes are correct.",
                    cutoff_date,
                    gateio_root,
                    backup,
                )
                try:
                    os.rename(gateio_root, backup)
                except Exception as exc:
                    logging.error("Failed to move gateio cache to backup: %s", exc)
                return


def _looks_like_daily_shard_filename(name: str) -> bool:
    if not isinstance(name, str) or not name.endswith(".npy"):
        return False
    stem = name[:-4]
    if len(stem) != 10 or stem[4] != "-" or stem[7] != "-":
        return False
    try:
        datetime.strptime(stem, "%Y-%m-%d")
    except Exception:
        return False
    return True


def _quarantine_root_level_timeframe_debris(cache_base: str) -> int:
    """隔离在交易所/时间周期根目录下发现的无效文件。

    有效的 OHLCV 布局为：
    `{cache_base}/{exchange}/{timeframe}/{symbol}/YYYY-MM-DD.npy`

    在 `{cache_base}/{exchange}/{timeframe}` 下直接发现的日分片文件
    或 index.json 文件属于旧版/损坏布局的残留，不应保留在原位。
    """
    root = Path(cache_base)
    if not root.is_dir():
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    moved = 0

    for exchange_dir in root.iterdir():
        if not exchange_dir.is_dir() or exchange_dir.name.startswith("."):
            continue
        if exchange_dir.name.startswith("_"):
            continue

        for tf_dir in exchange_dir.iterdir():
            if not tf_dir.is_dir():
                continue

            debris: List[Path] = []
            for child in tf_dir.iterdir():
                if not child.is_file():
                    continue
                if child.name == "index.json" or _looks_like_daily_shard_filename(child.name):
                    debris.append(child)

            if not debris:
                continue

            quarantine_dir = (
                root / "_quarantine_root_level" / stamp / exchange_dir.name / tf_dir.name
            )
            quarantine_dir.mkdir(parents=True, exist_ok=True)

            for child in debris:
                shutil.move(str(child), str(quarantine_dir / child.name))
                moved += 1

            logging.warning(
                "Quarantined %d invalid root-level OHLCV cache artifact(s) from %s -> %s",
                len(debris),
                tf_dir,
                quarantine_dir,
            )

    return moved


# 将时间周期字符串（如 '1m'、'5m'、'1h'、'1d'）解析为毫秒。
# 无效输入时回退到 ONE_MIN_MS。秒级向下取整到分钟。
def _tf_to_ms(s: Optional[str]) -> int:
    if not s:
        return ONE_MIN_MS
    try:
        st = s.strip().lower()
    except Exception:
        return ONE_MIN_MS
    import re

    m = re.fullmatch(r"(\d+)([smhd])", st)
    if not m:
        return ONE_MIN_MS
    n, unit = int(m.group(1)), m.group(2)
    if unit == "s":
        return max(ONE_MIN_MS, (n // 60) * ONE_MIN_MS)
    if unit == "m":
        return n * ONE_MIN_MS
    if unit == "h":
        return n * 60 * ONE_MIN_MS
    if unit == "d":
        return n * 1440 * ONE_MIN_MS
    return ONE_MIN_MS


# ----- CandlestickManager -----


class CandlestickManager:
    """管理 1m OHLCV K 线，提供简单缓存和缺口标准化。

    参数
    ----------
    exchange : Any
        CCXT 交易所实例或 None。测试传入 None 时跳过网络获取。
    exchange_name : str
        交易所名称，用于缓存目录布局。
    cache_dir : str
        磁盘缓存根目录。默认 "caches"。
    default_window_candles : int
        未提供 start_ts 时使用的默认窗口大小。
    overlap_candles : int
        从网络刷新时应用的重叠量（测试中不使用）。
    max_memory_candles_per_symbol : int
        每个交易对在内存中的最大 1m K 线数量（滚动窗口）。
    max_disk_candles_per_symbol_per_tf : int
        每个交易对+时间周期在磁盘上的最大 K 线总量（旧分片被裁剪）。
    debug : int | bool
        日志详细程度（0=警告, 1=网络信息, 2=调试, 3=跟踪）。
    """

    # 许多辅助方法同时接受 `timeframe=` 和简写 `tf=` 别名。别名保持
    # 现有调用点简洁，同时仍然推广更具描述性的名称。

    def __init__(
        self,
        exchange=None,
        exchange_name: str = "unknown",
        *,
        cache_dir: str = "caches",
        default_window_candles: int = 100,
        overlap_candles: int = 30,
        # 保留策略控制参数（基于 K 线数量）：
        max_memory_candles_per_symbol: int = 200_000,
        max_disk_candles_per_symbol_per_tf: int = 2_000_000,
        debug: int | bool = False,
        # 可选的进度日志（INFO 级别，节流）。0 禁用，推荐 30.0。
        progress_log_interval_seconds: float = 10.0,
        # 可选回调，在每次外部（网络）获取尝试时调用。
        remote_fetch_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        # 远程 ccxt 调用的可选全局并发限制器
        max_concurrent_requests: int | None = None,
        lock_timeout_seconds: float | None = None,
        # 归档获取：如果为 False，即使归档可用也仅使用 ccxt REST API。
        # 适用于实盘机器人（归档可能超时）；回测默认启用。
        archive_enabled: bool = True,
        # 可选的交易对列表，记录每页 OHLCV 范围（调试分页）。
        page_debug_symbols: Optional[Iterable[str]] = None,
    ) -> None:
        self.exchange = exchange
        # 如果未显式提供 exchange_name，从 ccxt 实例 id 推断
        if (not exchange_name or exchange_name == "unknown") and getattr(exchange, "id", None):
            self.exchange_name = str(getattr(exchange, "id"))
        else:
            self.exchange_name = exchange_name
        # 将 ccxt ID 归一化为标准缓存名称（如 "binanceusdm" -> "binance"）
        _en = self.exchange_name.lower()
        for _suffix in ("usdm", "futures"):
            if _en.endswith(_suffix):
                self.exchange_name = _en[: -len(_suffix)]
                break
        self.cache_dir = cache_dir
        self.default_window_candles = int(default_window_candles)
        self.overlap_candles = int(overlap_candles)
        self.max_memory_candles_per_symbol = int(max_memory_candles_per_symbol)
        self.max_disk_candles_per_symbol_per_tf = int(max_disk_candles_per_symbol_per_tf)
        # 归档获取：如果为 False，仅使用 ccxt REST API
        self.archive_enabled = bool(archive_enabled)
        # 调试级别：0=警告, 1=仅网络, 2=完整调试, 3=跟踪
        try:
            dbg = int(float(debug))
        except Exception:
            dbg = 2 if bool(debug) else 0
        self.debug_level = max(0, min(int(dbg), 3))
        try:
            self._progress_log_interval_seconds = max(0.0, float(progress_log_interval_seconds))
        except Exception:
            self._progress_log_interval_seconds = 0.0
        self._progress_last_log: Dict[Tuple[str, str, str], float] = {}
        self._warning_last_log: Dict[str, float] = {}  # 节流重复警告
        self._warning_throttle_seconds: float = 300.0  # 重复警告间隔 5 分钟
        self._persist_batch_observer: Optional[
            Callable[[str, str, np.ndarray], None]
        ] = None
        # 严格缺口警告的摘要跟踪（每 15 分钟汇总一次，而非逐事件记录）
        self._strict_gaps_summary: Dict[str, int] = {}  # symbol -> 缺失数量
        self._strict_gaps_summary_last_log: float = 0.0
        self._strict_gaps_summary_interval: float = 900.0  # 15 分钟
        self._remote_fetch_callback = remote_fetch_callback
        # 每个交易所+交易对+时间周期的旧版分片路径缓存
        self._legacy_shard_paths_cache: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        # 旧版日质量决策缓存：(symbol, tf, date_key) -> legacy_is_complete
        self._legacy_day_quality_cache: Dict[Tuple[str, str, str], bool] = {}
        # 每个交易对+时间周期的主分片路径缓存 - 避免冗余 glob 扫描
        self._shard_paths_cache: Dict[Tuple[str, str], Dict[str, str]] = {}

        self._cache: Dict[str, np.ndarray] = {}
        self._index: Dict[str, dict] = {}
        self._index_mtime: Dict[str, Optional[float]] = {}
        # EMA 计算缓存：每个交易对 -> {(metric, span, tf): (value, end_ts, computed_at_ms)}
        self._ema_cache: Dict[str, Dict[Tuple[str, int, str], Tuple[float, int, int]]] = {}
        # 每个交易对的当前（进行中）分钟收盘价缓存：symbol -> (price, updated_ms)
        self._current_close_cache: Dict[str, Tuple[float, int]] = {}
        # 获取的高级时间周期窗口缓存，避免重复远程调用（每个交易对 LRU）
        # 每个交易对 -> OrderedDict[(tf_str, start_ts, end_ts) -> (array, fetched_at_ms)]
        self._tf_range_cache: Dict[str, OrderedDict[Tuple[str, int, int], Tuple[np.ndarray, int]]] = (
            {}
        )
        self._tf_range_cache_cap = 8
        self._step_warning_keys: set[Tuple[str, str, str]] = set()
        # 零成交 K 线合成警告去重 - 每个唯一缺口仅警告一次
        # 键: (symbol, first_ts) 通过起始点标识缺口
        # 结束时间戳随时间推移而变化，但起始时间戳标识缺口的来源
        self._synth_gap_warned: set[Tuple[str, int]] = set()
        # 启动批次模式：启用时收集警告并稍后汇总记录
        self._synth_candle_batch_mode: bool = False
        # 批次期间的 symbol -> {"count": int, "min_ts": int, "max_ts": int}
        self._synth_candle_batch: Dict[str, Dict[str, int]] = {}
        # K 线替换日志的批次模式：收集替换信息并在 INFO 级别汇总记录
        self._candle_replace_batch_mode: bool = False
        self._candle_replace_batch: Dict[str, int] = {}  # symbol -> 批次期间替换数量
        # 跟踪哪些时间戳是合成的（每个交易对），用于 EMA 重计算检测
        # 当真实数据到达之前合成的时间戳时，EMA 应被重计算
        self._synthetic_timestamps: Dict[str, set[int]] = {}  # symbol -> 合成时间戳集合 (ms)
        # 跨进程获取锁的超时参数
        self._lock_timeout_seconds = float(_LOCK_TIMEOUT_SECONDS)
        if lock_timeout_seconds is not None:
            try:
                candidate = float(lock_timeout_seconds)
                if candidate > 0.0 and math.isfinite(candidate):
                    self._lock_timeout_seconds = candidate
            except Exception:
                pass
        self._lock_stale_seconds = float(_LOCK_STALE_SECONDS)
        self._lock_backoff_initial = float(_LOCK_BACKOFF_INITIAL)
        self._lock_backoff_max = float(_LOCK_BACKOFF_MAX)
        # portalocker 获取锁的可重入记录：key -> _LockRecord
        self._held_fetch_locks: Dict[Tuple[str, str], _LockRecord] = {}
        self._shutdown_guard = threading.Lock()
        self._closed = False
        atexit.register(self._cleanup_on_exit)

        # 标准化缓存目录名称（如 binanceusdm -> binance），
        # 将所有旧版数据从 historical_data/ 迁移到 caches/ohlcv/，
        # 并合并不一致命名产生的重复交易对目录
        ohlcv_cache_base = os.path.join(self.cache_dir, "ohlcv")
        os.makedirs(ohlcv_cache_base, exist_ok=True)
        historical_data_path = os.path.join(
            os.path.dirname(os.path.abspath(self.cache_dir)),
            "historical_data",
        )
        GATEIO_CACHE_CUTOFF_DATE = "2026-02-07"
        if self.exchange_name == "gateio" and GATEIO_CACHE_CUTOFF_DATE:
            _quarantine_gateio_cache_if_stale(
                ohlcv_cache_base,
                GATEIO_CACHE_CUTOFF_DATE,
            )
        migration_lock = os.path.join(ohlcv_cache_base, ".migration.lock")
        migration_done = os.path.join(ohlcv_cache_base, ".migration_done")
        try:
            with portalocker.Lock(migration_lock, timeout=0.1, fail_when_locked=True):
                try:
                    _quarantine_root_level_timeframe_debris(ohlcv_cache_base)
                except Exception as exc:
                    logging.exception(
                        "Root-level OHLCV cache cleanup failed (non-fatal). Continuing: %s",
                        exc,
                    )
                if not os.path.exists(migration_done):
                    try:
                        standardize_cache_directories(ohlcv_cache_base)
                        migrate_legacy_data_all_on_init(
                            cache_base=ohlcv_cache_base,
                            historical_data_path=historical_data_path,
                        )
                        merge_duplicate_symbol_directories(ohlcv_cache_base)
                        try:
                            with open(migration_done, "w", encoding="utf-8") as handle:
                                handle.write(str(int(time.time())))
                        except Exception:
                            pass
                    except Exception as exc:
                        logging.exception(
                            "Cache migration failed (non-fatal). Continuing without migration: %s",
                            exc,
                        )
        except portalocker.exceptions.LockException:
            # 另一个进程正在处理迁移；跳过。
            pass

        self._setup_logging()
        self._cleanup_stale_locks()

        # 初始化远程调用的可选全局信号量
        try:
            mcr = None if max_concurrent_requests in (None, 0) else int(max_concurrent_requests)
            self._net_sem = asyncio.Semaphore(mcr) if (mcr and mcr > 0) else None
        except Exception:
            self._net_sem = None

        # 全局速率限制协调：当触发速率限制时，所有并发请求暂停到该时间戳（防止惊群重试）
        self._rate_limit_until: float = 0.0
        self._rate_limit_lock = asyncio.Lock()
        self._rate_limit_count: int = 0

        # 归档获取的持久 HTTP 会话（惰性创建）
        self._http_session: Optional["aiohttp.ClientSession"] = None
        self._http_session_lock = asyncio.Lock()

        # 获取控制参数
        # 存储/获取的基础时间周期始终为 1m；更高级别的时间周期按调用指定
        self._ccxt_timeframe = "1m"
        # 确定交易所 ID 并根据交易所特性调整默认值
        self._ex_id = getattr(self.exchange, "id", self.exchange_name) or self.exchange_name
        self._ccxt_limit_default = 1000
        self._ccxt_page_overlap_candles = 0
        self._record_payload_gaps_as_known = False
        self._ccxt_since_exclusive = False
        self._ccxt_limit_probe_done = False
        self._gateio_recent_window_clip_warned: set[str] = set()
        if isinstance(self._ex_id, str) and "bitget" in self._ex_id.lower():
            # Bitget 的 1m K 线通常每页限制 200 条
            self._ccxt_limit_default = 200
            # 页边界重叠以避免遗漏边界 K 线
            self._ccxt_page_overlap_candles = 1
            # Bitget 的 since 参数对于 1m OHLCV 表现为排他
            self._ccxt_since_exclusive = True
            # 运行时探测 Bitget 是否已支持每页 >200 条数据
            self._ccxt_limit_probe_done = False
        if isinstance(self._ex_id, str) and "kucoin" in self._ex_id.lower():
            # KuCoin 期货每次 OHLCV 调用最多返回 200 行，且可能稀疏（仅有交易的分钟）。
            self._ccxt_limit_default = 200
            # 页边界重叠以验证获取间的缺口。
            self._ccxt_page_overlap_candles = 1
            # 单次负载内的缺口被视为已验证的无交易缺口。
            self._record_payload_gaps_as_known = True
            # KuCoin 的 since 参数对于 1m OHLCV 表现为排他。
            self._ccxt_since_exclusive = True

        # 选定交易对的可选每页范围日志（调试分页）
        self._page_debug_all = False
        self._page_debug_symbols: set[str] = set()
        if page_debug_symbols:
            try:
                for sym in page_debug_symbols:
                    if sym is None:
                        continue
                    sym_str = str(sym).strip()
                    if not sym_str:
                        continue
                    if sym_str == "*":
                        self._page_debug_all = True
                    else:
                        self._page_debug_symbols.add(sym_str)
            except Exception:
                self._page_debug_symbols = set()

    # ----- 日志 -----

    def _setup_logging(self) -> None:
        trace_level = getattr(logging, "TRACE", None)
        if not isinstance(trace_level, int):
            trace_level = 5
            logging.addLevelName(trace_level, "TRACE")
            setattr(logging, "TRACE", trace_level)
        level_map = {
            0: logging.WARNING,
            1: logging.INFO,
            2: logging.DEBUG,
            3: trace_level,
        }
        desired_level = level_map.get(self.debug_level, logging.INFO)
        self.log = logging.getLogger("passivbot.candlestick_manager")
        self.log.setLevel(desired_level)

    def start_synth_candle_batch(self) -> None:
        """开始批次收集零成交 K 线合成警告，以便后续汇总记录。"""
        self._synth_candle_batch_mode = True
        self._synth_candle_batch.clear()

    def flush_synth_candle_batch(self) -> None:
        """记录汇总的零成交 K 线合成摘要并退出批次模式。"""
        self._synth_candle_batch_mode = False
        if not self._synth_candle_batch:
            return
        total_symbols = len(self._synth_candle_batch)
        total_candles = sum(v.get("count", 0) for v in self._synth_candle_batch.values())
        # 仅在合成 K 线数量较大（>1000）时使用 WARNING，否则使用 INFO
        # 在流动性不足的交易对预热期间，这是预期行为
        log_fn = self.log.warning if total_candles > 1000 else self.log.info

        def _fmt_range(min_ts: Optional[int], max_ts: Optional[int]) -> str:
            try:
                from datetime import datetime, timezone

                if min_ts is None or max_ts is None:
                    return "-"
                start = datetime.fromtimestamp(int(min_ts) / 1000, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M"
                )
                end = datetime.fromtimestamp(int(max_ts) / 1000, tz=timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M"
                )
                return f"{start} to {end}" if start != end else start
            except Exception:
                return "-"

        if total_symbols == 1:
            symbol, meta = next(iter(self._synth_candle_batch.items()))
            count = int(meta.get("count", 0))
            rng = _fmt_range(meta.get("min_ts"), meta.get("max_ts"))
            log_fn(
                "[candle] synthesized %d zero-candle%s for %s at %s (no data for requested minutes)",
                count,
                "s" if count > 1 else "",
                symbol,
                rng,
            )
        else:
            # 按合成数量记录前 N 个交易对（限制以保持日志简洁）
            top_n = 5
            sorted_syms = sorted(
                self._synth_candle_batch.items(),
                key=lambda kv: int(kv[1].get("count", 0)),
                reverse=True,
            )
            top_parts = []
            for sym, meta in sorted_syms[:top_n]:
                count = int(meta.get("count", 0))
                rng = _fmt_range(meta.get("min_ts"), meta.get("max_ts"))
                top_parts.append(f"{sym}:{count}@{rng}")
            extra = total_symbols - min(top_n, total_symbols)
            top_str = ", ".join(top_parts)
            if extra > 0:
                top_str = f"{top_str} (+{extra} more)"
            log_fn(
                "[candle] synthesized %d zero-candle%s across %d symbols (no data for requested minutes) top=%s",
                total_candles,
                "s" if total_candles > 1 else "",
                total_symbols,
                top_str,
            )
        self._synth_candle_batch.clear()

    def start_candle_replace_batch(self) -> None:
        """开始批次收集 K 线替换日志，以便后续汇总记录。"""
        self._candle_replace_batch_mode = True
        self._candle_replace_batch.clear()

    def flush_candle_replace_batch(self) -> None:
        """在 INFO 级别记录汇总的 K 线替换摘要并退出批次模式。"""
        self._candle_replace_batch_mode = False
        if not self._candle_replace_batch:
            return
        total_symbols = len(self._candle_replace_batch)
        total_candles = sum(self._candle_replace_batch.values())
        if total_symbols == 1:
            symbol, count = next(iter(self._candle_replace_batch.items()))
            self.log.info(
                "[candle] %s: real data replaced %d synthetic candle%s, EMA cache invalidated",
                symbol,
                count,
                "s" if count > 1 else "",
            )
        else:
            self.log.info(
                "[candle] real data replaced %d synthetic candle%s across %d symbols, EMA caches invalidated",
                total_candles,
                "s" if total_candles > 1 else "",
                total_symbols,
            )
        self._candle_replace_batch.clear()

    # ----- 保留策略辅助方法 -----

    def _cleanup_stale_locks(self) -> None:
        """移除明显过期的残留锁文件。"""
        try:
            base = Path(self.cache_dir) / self.exchange_name
        except Exception:
            return
        if not base.exists():
            return
        now = time.time()
        threshold = self._lock_stale_seconds
        for lock_path in base.glob("*/locks/*.lock"):
            try:
                stat = lock_path.stat()
            except FileNotFoundError:
                continue
            except Exception as exc:
                self.log.warning("failed to stat lock %s during cleanup: %s", lock_path, exc)
                continue
            age = now - stat.st_mtime
            if age > threshold:
                try:
                    lock_path.unlink()
                    self.log.info("removed stale candle lock %s (age %.1fs)", lock_path, age)
                except FileNotFoundError:
                    continue
                except Exception as exc:
                    self.log.error("failed to remove stale lock %s: %s", lock_path, exc)

    def _cleanup_on_exit(self) -> None:
        with self._shutdown_guard:
            if self._closed:
                return
            self._closed = True
        records = list(self._held_fetch_locks.values())
        self._held_fetch_locks.clear()
        for record in records:
            self._release_lock_sync(record)

    def _release_lock_sync(self, record: _LockRecord) -> None:
        try:
            record.lock.release()
        except Exception:
            pass
        self._remove_lockfile(record.path)

    def _remove_lockfile(self, path: str) -> None:
        try:
            os.remove(path)
        except FileNotFoundError:
            return
        except Exception:
            return

    async def _release_lock(
        self, lock: portalocker.Lock, path: str, symbol: str, timeframe: str
    ) -> None:
        """Release a portalocker lock safely and refresh its metadata."""
        try:
            await asyncio.to_thread(lock.release)
        except portalocker.exceptions.LockException as exc:
            self._log(
                "warning",
                "fetch_lock_release_failed",
                symbol=symbol,
                timeframe=timeframe,
                error=str(exc),
            )
        except Exception as exc:
            self._log(
                "warning",
                "fetch_lock_release_error",
                symbol=symbol,
                timeframe=timeframe,
                error=str(exc),
            )
        finally:
            self._remove_lockfile(path)

    def _touch_lockfile(self, path: str) -> None:
        try:
            os.utime(path, None)
        except FileNotFoundError:
            return
        except Exception:
            return

    def _lockfile_age(self, path: str) -> Optional[float]:
        try:
            mtime = os.path.getmtime(path)
        except FileNotFoundError:
            return None
        except Exception:
            return None
        return time.time() - mtime

    def _enforce_memory_retention(self, symbol: str) -> None:
        try:
            arr = self._cache.get(symbol)
            if arr is None or arr.size == 0:
                return
            nmax = self.max_memory_candles_per_symbol
            if nmax > 0 and arr.shape[0] > nmax:
                # 保留最后 nmax 条按 ts 排序的数据
                arr = np.sort(arr, order="ts")
                self._cache[symbol] = arr[-nmax:]
        except Exception:
            return

    def _enforce_disk_retention(
        self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None
    ) -> None:
        try:
            tf_norm = self._normalize_timeframe_arg(timeframe, tf)
            idx = self._ensure_symbol_index(symbol, tf=tf_norm)
            shards = idx.get("shards", {})
            if not shards:
                return
            # 累加数量；如果超出限制，删除最旧的分片文件直到在限制内
            total = 0
            items = []
            for k, v in shards.items():
                try:
                    count = int(v.get("count", 0))
                except Exception:
                    count = 0
                total += count
                items.append((k, v))
            limit = self.max_disk_candles_per_symbol_per_tf
            if limit <= 0 or total <= limit:
                return
            # 按日期键升序排列分片（最旧的在前）
            items.sort(key=lambda x: x[0])
            # 移除最旧的分片直到在限制内
            for date_key, meta in items:
                path = meta.get("path")
                try:
                    if path and os.path.exists(path):
                        os.remove(path)
                except Exception:
                    pass
                # 更新索引
                try:
                    cnt = int(meta.get("count", 0))
                except Exception:
                    cnt = 0
                total -= cnt
                shards.pop(date_key, None)
                if total <= limit:
                    break
            # 持久化更新后的索引
            idx["shards"] = shards
            key = f"{symbol}::{tf_norm}"
            self._index[key] = idx
            self._save_index(symbol, tf=tf_norm)
        except Exception:
            return

    # ----- 日志辅助方法 -----

    @staticmethod
    def _fmt_ts(ms: Optional[int]) -> str:
        try:
            return (
                time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(ms) / 1000.0))
                if ms is not None
                else "-"
            )
        except Exception:
            return str(ms)

    def _log(self, level: str, event: str, **fields) -> None:
        try:
            ex = getattr(self, "_ex_id", self.exchange_name)
        except Exception:
            ex = self.exchange_name
        base = [f"[candle] event={event}"]
        # 调试模式下包含调用者信息以便追溯
        if self.debug_level >= 1:
            try:
                caller = get_caller_name()
                base.append(f"called_by={caller}")
            except Exception:
                pass
        base.append(f"exchange={ex}")
        parts = []
        for k, v in fields.items():
            if k.endswith("_ts") and isinstance(v, (int, np.integer)):
                parts.append(f"{k}={self._fmt_ts(int(v))}")
            else:
                parts.append(f"{k}={v}")
        msg = " ".join(base + parts)
        if level == "debug":
            # 应用调试过滤：级别 0 -> 丢弃；级别 1 -> 仅 ccxt_* 事件；级别 2 -> 全部
            if self.debug_level <= 0:
                return
            is_network = isinstance(event, str) and (
                event.startswith("ccxt_") or event.startswith("archive_")
            )
            if self.debug_level == 1 and not is_network:
                return
            self.log.debug(msg)
        elif level == "info":
            self.log.info(msg)
        elif level == "warning":
            self.log.warning(msg)
        else:
            self.log.error(msg)

    def _progress_log(self, key: Tuple[str, str, str], event: str, **fields) -> None:
        """启用时发出节流的 DEBUG 级别进度日志。"""
        if self._progress_log_interval_seconds <= 0.0:
            return
        now = time.monotonic()
        last = self._progress_last_log.get(key, 0.0)
        if (now - last) < self._progress_log_interval_seconds:
            return
        self._progress_last_log[key] = now
        self._log("debug", event, **fields)

    def _log_persistent_gap_summary(self) -> None:
        """如果有累积的持久缺口摘要则记录，节流为每 30 分钟一次。"""
        if not hasattr(self, "_persistent_gap_summary") or not self._persistent_gap_summary:
            return
        now = time.monotonic()
        last = getattr(self, "_persistent_gap_summary_last_log", 0.0)
        if (now - last) < 1800.0:  # 每 30 分钟仅记录一次摘要
            return
        self._persistent_gap_summary_last_log = now
        summary = self._persistent_gap_summary
        total = sum(summary.values())
        symbols = ", ".join(f"{s}:{c}" for s, c in sorted(summary.items())[:5])
        if len(summary) > 5:
            symbols += f", +{len(summary) - 5} more"
        self.log.info(
            "[candle] persistent gaps: %d across %d symbols (%s). Use --force-refetch-gaps to retry.",
            total,
            len(summary),
            symbols,
        )
        self._persistent_gap_summary.clear()

    def _throttled_warning(self, throttle_key: str, event: str, **fields) -> None:
        """在节流窗口（默认 5 分钟）内最多发出一次警告。

        用于可能频繁重复但只需告知用户一次的警告。节流窗口过期后，
        如果条件持续存在，将再次发出警告。
        """
        now = time.monotonic()
        last = self._warning_last_log.get(throttle_key, 0.0)
        if (now - last) < self._warning_throttle_seconds:
            return
        self._warning_last_log[throttle_key] = now
        self._log("warning", event, **fields)

    def _record_strict_gap(self, symbol: str, missing_count: int) -> None:
        """累积严格缺口计数，用于摘要记录。"""
        self._strict_gaps_summary[symbol] = self._strict_gaps_summary.get(symbol, 0) + missing_count

    def _log_strict_gaps_summary(self) -> None:
        """如果有累积的严格缺口摘要则记录，节流为每 15 分钟一次。"""
        if not self._strict_gaps_summary:
            return
        now = time.monotonic()
        if (now - self._strict_gaps_summary_last_log) < self._strict_gaps_summary_interval:
            return
        self._strict_gaps_summary_last_log = now
        summary = self._strict_gaps_summary
        total = sum(summary.values())
        symbols = ", ".join(f"{s}:{c}" for s, c in sorted(summary.items(), key=lambda x: -x[1])[:5])
        if len(summary) > 5:
            symbols += f", +{len(summary) - 5} more"
        self.log.debug(
            "[candle] strict mode gaps: %d missing candles across %d symbols (%s)",
            total,
            len(summary),
            symbols,
        )
        self._strict_gaps_summary.clear()

    def _emit_remote_fetch(self, payload: Dict[str, Any]) -> None:
        cb = getattr(self, "_remote_fetch_callback", None)
        if cb is None:
            return
        try:
            cb(payload)
        except Exception:
            # 观测钩子绝不能中断获取路径或交易。
            return

    def set_persist_batch_observer(
        self,
        observer: Optional[Callable[[str, str, np.ndarray], None]],
    ) -> None:
        self._persist_batch_observer = observer

    # ----- 路径和索引 -----

    def _symbol_dir(
        self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None
    ) -> str:
        sym = _sanitize_symbol(symbol)
        tf_dir = self._normalize_timeframe_arg(timeframe, tf)
        return str(Path(self.cache_dir) / "ohlcv" / self.exchange_name / tf_dir / sym)

    def _index_path(
        self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None
    ) -> str:
        return str(Path(self._symbol_dir(symbol, timeframe=timeframe, tf=tf)) / "index.json")

    def _shard_path(
        self,
        symbol: str,
        date_key: str,
        timeframe: Optional[str] = None,
        *,
        tf: Optional[str] = None,
    ) -> str:
        return str(Path(self._symbol_dir(symbol, timeframe=timeframe, tf=tf)) / f"{date_key}.npy")

    def _prune_missing_shards_from_index(self, idx: dict) -> int:
        """移除文件缺失的分片条目；刷新派生的元数据字段。"""
        try:
            shards = idx.get("shards", {})
            if not isinstance(shards, dict) or not shards:
                return 0
            removed = 0
            for day_key, shard_meta in list(shards.items()):
                if not isinstance(shard_meta, dict):
                    continue
                path = shard_meta.get("path")
                if not path:
                    continue
                if not os.path.exists(str(path)):
                    shards.pop(day_key, None)
                    removed += 1
            if not removed:
                return 0
            idx["shards"] = shards
            meta = idx.setdefault("meta", {})
            try:
                last_ts = 0
                observed_start_ts: Optional[int] = None
                for shard_meta in shards.values():
                    if not isinstance(shard_meta, dict):
                        continue
                    mt = shard_meta.get("max_ts")
                    if mt is not None:
                        last_ts = max(last_ts, int(mt))
                    mi = shard_meta.get("min_ts")
                    if mi is not None:
                        observed_start_ts = (
                            int(mi)
                            if observed_start_ts is None
                            else min(observed_start_ts, int(mi))
                        )
                meta["last_final_ts"] = int(last_ts)
                meta["observed_start_ts"] = observed_start_ts
                meta["inception_ts"] = observed_start_ts
            except Exception:
                meta["last_final_ts"] = 0
                meta["observed_start_ts"] = None
                meta["inception_ts"] = None
            return int(removed)
        except Exception:
            return 0

    def _ensure_symbol_index(
        self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None
    ) -> dict:
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        key = f"{symbol}::{tf_norm}"
        idx_path = self._index_path(symbol, timeframe=timeframe, tf=tf_norm)
        existing = self._index.get(key)
        cached_mtime = self._index_mtime.get(key)
        try:
            current_mtime = os.path.getmtime(idx_path)
        except FileNotFoundError:
            current_mtime = None
        except Exception:
            current_mtime = None

        if existing is None or cached_mtime != current_mtime:
            idx = {"shards": {}, "meta": {}}
            # 尝试从磁盘加载
            if current_mtime is not None:
                try:
                    with open(idx_path, "r", encoding="utf-8") as f:
                        idx = json.load(f)
                except FileNotFoundError:
                    pass
                except Exception as e:  # pragma: no cover
                    self._log(
                        "warning",
                        "index_load_failed",
                        symbol=symbol,
                        timeframe=tf_norm,
                        error=str(e),
                    )
            if not isinstance(idx, dict):
                idx = {"shards": {}, "meta": {}}
            idx.setdefault("shards", {})
            meta = idx.setdefault("meta", {})
            legacy_history_bounds = (
                "observed_start_ts" not in meta and "authoritative_start_ts" not in meta
            )
            meta.setdefault("known_gaps", [])  # list of [start_ts, end_ts]
            meta.setdefault("last_refresh_ms", 0)
            meta.setdefault("last_final_ts", 0)
            observed_start_ts = meta.get("observed_start_ts", meta.get("inception_ts"))
            meta["observed_start_ts"] = int(observed_start_ts) if observed_start_ts is not None else None
            meta["inception_ts"] = meta["observed_start_ts"]  # 旧版别名，表示最早观测到的 K 线
            meta.setdefault("authoritative_start_ts", None)
            meta.setdefault("authoritative_start_source", None)
            meta.setdefault("inception_ts_probe_ms", 0)
            meta.setdefault("inception_ts_probe_end_ts", 0)
            migrated_pre_inception = False
            if legacy_history_bounds and meta.get("authoritative_start_ts") is None:
                legacy_authoritative_start = self._infer_legacy_authoritative_start_ts(meta)
                if legacy_authoritative_start is not None:
                    meta["authoritative_start_ts"] = int(legacy_authoritative_start)
                    meta["authoritative_start_source"] = "legacy_pre_inception_gap"
                else:
                    original_gaps = list(meta.get("known_gaps", []))
                    retained_gaps = []
                    for gap in original_gaps:
                        if isinstance(gap, dict) and str(gap.get("reason", "")) == "pre_inception":
                            migrated_pre_inception = True
                            continue
                        retained_gaps.append(gap)
                    if migrated_pre_inception:
                        meta["known_gaps"] = retained_gaps

            # 如果分片文件被删除，保持索引一致。
            removed = self._prune_missing_shards_from_index(idx)
            if removed:
                self._log(
                    "warning",
                    "index_pruned_missing_shards",
                    symbol=symbol,
                    timeframe=tf_norm,
                    removed=removed,
                )
            self._index[key] = idx
            self._index_mtime[key] = current_mtime
            if migrated_pre_inception:
                self._save_index(symbol, tf=tf_norm)
            self._log(
                "debug",
                "index_reload",
                symbol=symbol,
                timeframe=tf_norm,
                mtime=current_mtime,
                cache_hit=existing is not None,
            )
            return idx

        idx = existing
        # 即使对于缓存条目也确保元数据键存在（以防早期版本缺少它们）
        idx.setdefault("shards", {})
        meta = idx.setdefault("meta", {})
        legacy_history_bounds = (
            "observed_start_ts" not in meta and "authoritative_start_ts" not in meta
        )
        meta.setdefault("known_gaps", [])
        meta.setdefault("last_refresh_ms", 0)
        meta.setdefault("last_final_ts", 0)
        observed_start_ts = meta.get("observed_start_ts", meta.get("inception_ts"))
        migrated_pre_inception = False
        meta["observed_start_ts"] = int(observed_start_ts) if observed_start_ts is not None else None
        meta["inception_ts"] = meta["observed_start_ts"]
        meta.setdefault("authoritative_start_ts", None)
        meta.setdefault("authoritative_start_source", None)
        if legacy_history_bounds and meta.get("authoritative_start_ts") is None:
            legacy_authoritative_start = self._infer_legacy_authoritative_start_ts(meta)
            if legacy_authoritative_start is not None:
                meta["authoritative_start_ts"] = int(legacy_authoritative_start)
                meta["authoritative_start_source"] = "legacy_pre_inception_gap"
            else:
                original_gaps = list(meta.get("known_gaps", []))
                retained_gaps = []
                for gap in original_gaps:
                    if isinstance(gap, dict) and str(gap.get("reason", "")) == "pre_inception":
                        migrated_pre_inception = True
                        continue
                    retained_gaps.append(gap)
                if migrated_pre_inception:
                    meta["known_gaps"] = retained_gaps

        # 如果分片文件在运行期间被删除，保持缓存索引一致。
        removed = self._prune_missing_shards_from_index(idx)
        if removed:
            self._log(
                "warning",
                "index_pruned_missing_shards",
                symbol=symbol,
                timeframe=tf_norm,
                removed=removed,
            )
        self._index[key] = idx
        self._index_mtime[key] = current_mtime
        if migrated_pre_inception:
            self._save_index(symbol, tf=tf_norm)
        if current_mtime is not None:
            self._log("debug", "index_cached", symbol=symbol, timeframe=tf_norm, mtime=current_mtime)
        return idx

    def _atomic_write_bytes(self, path: str, data: bytes) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)

    def _save_index(
        self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None
    ) -> None:
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        key = f"{symbol}::{tf_norm}"
        idx_path = self._index_path(symbol, timeframe=timeframe, tf=tf_norm)
        payload = json.dumps(self._index[key], sort_keys=True).encode("utf-8")
        # 锁定最终目标 index.json 以序列化写入者
        os.makedirs(os.path.dirname(idx_path), exist_ok=True)
        # 使用 portalocker 基于文件名而非文件句柄，以便在文件缺失时创建
        lock_path = idx_path + ".lock"
        with portalocker.Lock(lock_path, timeout=5):
            self._atomic_write_bytes(idx_path, payload)
        try:
            self._index_mtime[key] = os.path.getmtime(idx_path)
        except Exception:
            self._index_mtime[key] = None

    def _fetch_lock_path(self, symbol: str, timeframe: str) -> str:
        safe_symbol = _sanitize_symbol(symbol)
        lock_dir = os.path.join(
            self.cache_dir,
            self.exchange_name,
            safe_symbol,
            "locks",
        )
        os.makedirs(lock_dir, exist_ok=True)
        return os.path.join(lock_dir, f"{timeframe}.lock")

    @asynccontextmanager
    async def _acquire_fetch_lock(self, symbol: str, timeframe: Optional[str]) -> AsyncIterator[None]:
        """获取跨进程获取锁，支持可重入和过期锁清理。"""
        tf_norm = self._normalize_timeframe_arg(timeframe, None)

        lock_path = self._fetch_lock_path(symbol, tf_norm)
        key = (symbol, tf_norm)
        held = self._held_fetch_locks.get(key)
        if held is not None:
            self._held_fetch_locks[key] = _LockRecord(
                lock=held.lock,
                path=held.path,
                count=held.count + 1,
                acquired_at=held.acquired_at,
            )
            self._log(
                "debug",
                "fetch_lock_reentrant",
                symbol=symbol,
                timeframe=tf_norm,
                depth=held.count + 1,
            )
            try:
                yield
            finally:
                record = self._held_fetch_locks.get(key)
                if record is None:
                    return
                if record.count <= 1:
                    self._held_fetch_locks.pop(key, None)
                    await self._release_lock(record.lock, record.path, symbol, tf_norm)
                else:
                    self._held_fetch_locks[key] = _LockRecord(
                        lock=record.lock,
                        path=record.path,
                        count=record.count - 1,
                        acquired_at=record.acquired_at,
                    )
            return

        backoff = self._lock_backoff_initial
        deadline = time.monotonic() + self._lock_timeout_seconds
        attempt = 0

        while True:
            attempt += 1
            lock_obj = portalocker.Lock(lock_path, timeout=0, fail_when_locked=True)
            try:
                await asyncio.to_thread(lock_obj.acquire)
                acquired_at = time.time()
                self._touch_lockfile(lock_path)
                self._held_fetch_locks[key] = _LockRecord(
                    lock=lock_obj,
                    path=lock_path,
                    count=1,
                    acquired_at=acquired_at,
                )
                self._log(
                    "debug",
                    "fetch_lock_acquired",
                    symbol=symbol,
                    timeframe=tf_norm,
                    attempt=attempt,
                )
                try:
                    yield
                finally:
                    record = self._held_fetch_locks.pop(key, None)
                    if record is not None:
                        await self._release_lock(record.lock, record.path, symbol, tf_norm)
                return
            except portalocker.exceptions.LockException as exc:
                age = self._lockfile_age(lock_path)
                if age is not None and age > self._lock_stale_seconds:
                    self._log(
                        "warning",
                        "fetch_lock_stale",
                        symbol=symbol,
                        timeframe=tf_norm,
                        age=f"{age:.2f}",
                        lock_path=lock_path,
                    )
                    try:
                        os.remove(lock_path)
                    except FileNotFoundError:
                        pass
                    except Exception as rm_exc:
                        self._log(
                            "error",
                            "fetch_lock_stale_remove_failed",
                            symbol=symbol,
                            timeframe=tf_norm,
                            error=str(rm_exc),
                        )
                    continue

                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Timed out acquiring candle lock for {symbol} ({tf_norm}) after "
                        f"{self._lock_timeout_seconds:.1f}s"
                    ) from exc

                self._log(
                    "debug",
                    "fetch_lock_wait",
                    symbol=symbol,
                    timeframe=tf_norm,
                    attempt=attempt,
                    error=str(exc),
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, self._lock_backoff_max)

    @staticmethod
    def _normalize_timeframe_arg(
        timeframe: Optional[str], tf: Optional[str], default: str = "1m"
    ) -> str:
        """将别名组合解析为规范的小写时间周期字符串。"""
        value = tf if tf is not None else timeframe
        if not value:
            return default
        try:
            return str(value).strip().lower() or default
        except Exception:
            return default

    def _ensure_symbol_cache(self, symbol: str) -> np.ndarray:
        arr = self._cache.get(symbol)
        if arr is None:
            arr = np.empty((0,), dtype=CANDLE_DTYPE)
            self._cache[symbol] = arr
        return arr

    # ----- 分片加载辅助方法 -----

    def _iter_shard_paths(
        self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None
    ) -> Dict[str, str]:
        """返回磁盘上可用分片文件的 date_key -> path 映射。

        结果按 (symbol, tf) 缓存以避免冗余 glob 扫描。
        保存新分片后调用 _invalidate_shard_paths_cache(symbol, tf)。
        """
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        cache_key = (symbol, tf_norm)
        if cache_key in self._shard_paths_cache:
            return self._shard_paths_cache[cache_key]

        sd = Path(self._symbol_dir(symbol, timeframe=timeframe, tf=tf))
        if not sd.exists():
            # 缓存空结果以避免重复目录检查
            self._shard_paths_cache[cache_key] = {}
            return {}
        out: Dict[str, str] = {}
        for p in sd.glob("*.npy"):
            name = p.stem  # YYYY-MM-DD
            if len(name) == 10 and name[4] == "-" and name[7] == "-":
                out[name] = str(p)
        self._shard_paths_cache[cache_key] = out
        return out

    def _invalidate_shard_paths_cache(
        self, symbol: str, timeframe: Optional[str] = None, *, tf: Optional[str] = None
    ) -> None:
        """保存新分片后，使交易对/tf 的缓存分片路径失效。"""
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        cache_key = (symbol, tf_norm)
        self._shard_paths_cache.pop(cache_key, None)

    def _date_range_of_key(self, date_key: str) -> Tuple[int, int]:
        """返回日期键 YYYY-MM-DD 在 UTC 下的 [start_ms, end_ms] 包含范围。"""
        # 解析简单日期而不导入 datetime 以保持最小依赖
        y, m, d = map(int, date_key.split("-"))
        # 使用 time.gmtime 计算该日期的 UTC 午夜时间
        tm = time.struct_time((y, m, d, 0, 0, 0, 0, 0, 0))
        start = int(calendar.timegm(tm)) * 1000
        end = start + 24 * 60 * 60 * 1000 - ONE_MIN_MS
        return start, end

    def _date_key(self, ts_ms: int) -> str:
        """返回 UTC 毫秒时间戳对应的 YYYY-MM-DD。"""
        return time.strftime("%Y-%m-%d", time.gmtime(int(ts_ms) / 1000.0))

    def _date_keys_between(self, start_ts: int, end_ts: int) -> Dict[str, Tuple[int, int]]:
        """返回覆盖 [start, end] 范围的 date_key -> (day_start_ms, day_end_ms) 映射。"""
        # 对齐到起始日的 UTC 00:00
        first_key = self._date_key(start_ts)
        y, m, d = map(int, first_key.split("-"))
        tm = time.struct_time((y, m, d, 0, 0, 0, 0, 0, 0))
        day_start = int(calendar.timegm(tm)) * 1000
        res: Dict[str, Tuple[int, int]] = {}
        t = day_start
        while t <= end_ts:
            key = self._date_key(t)
            ds, de = self._date_range_of_key(key)
            res[key] = (ds, de)
            t = de + ONE_MIN_MS
        return res

    def _legacy_coin_from_symbol(self, symbol: str) -> str:
        """返回旧版下载器缓存使用的币种键。"""
        symbol = str(symbol or "")
        if not symbol:
            return ""
        if "/" in symbol:
            base = symbol.split("/", 1)[0]
        elif ":" in symbol:
            base = symbol.split(":", 1)[0]
        else:
            base = symbol
        base = base.strip()
        # 某些交易所编码交易对如 "HYPE_USDT:USDT"。
        # 旧版下载器缓存通常仅使用基础币种（"HYPE"）。
        if "_" in base:
            left, right = base.rsplit("_", 1)
            if right in {"USDT", "USDC", "USD", "BUSD"}:
                base = left
        return base

    def _legacy_symbol_code_from_symbol(self, symbol: str) -> str:
        """返回某些 historical_data 子目录中使用的旧版交易对代码。"""
        try:
            return self._archive_symbol_code(symbol)
        except Exception:
            return ""

    def _legacy_shard_candidates(self, symbol: str, date_key: str, tf: str) -> List[str]:
        if tf != "1m":
            return []
        ex = str(self.exchange_name or "").lower()
        coin = self._legacy_coin_from_symbol(symbol)
        sym_code = self._legacy_symbol_code_from_symbol(symbol)
        out: List[str] = []

        if coin:
            out.append(os.path.join("historical_data", f"ohlcvs_{ex}", coin, f"{date_key}.npy"))
        if ex == "binanceusdm" and sym_code:
            out.append(os.path.join("historical_data", "ohlcvs_futures", sym_code, f"{date_key}.npy"))
        if ex == "bybit" and sym_code:
            out.append(os.path.join("historical_data", "ohlcvs_bybit", sym_code, f"{date_key}.npy"))
        return out

    def _legacy_shard_dirs(self, symbol: str, tf: str) -> List[str]:
        if tf != "1m":
            return []
        ex = str(self.exchange_name or "").lower()
        coin = self._legacy_coin_from_symbol(symbol)
        sym_code = self._legacy_symbol_code_from_symbol(symbol)
        out: List[str] = []
        if coin:
            out.append(os.path.join("historical_data", f"ohlcvs_{ex}", coin))
        if ex == "binanceusdm" and sym_code:
            out.append(os.path.join("historical_data", "ohlcvs_futures", sym_code))
        if ex == "bybit" and sym_code:
            out.append(os.path.join("historical_data", "ohlcvs_bybit", sym_code))
        return out

    def _get_legacy_shard_paths(self, symbol: str, tf: str) -> Dict[str, str]:
        """返回交易对+时间周期的 date_key -> 旧版分片路径映射（已缓存）。"""
        ex = str(self.exchange_name or "").lower()
        key = (ex, str(symbol), str(tf))
        cached = self._legacy_shard_paths_cache.get(key)
        if cached is not None:
            return cached
        mapping: Dict[str, str] = {}
        scanned_dirs: List[str] = []
        for d in self._legacy_shard_dirs(symbol, tf):
            try:
                dp = Path(d)
                if not dp.exists():
                    continue
                scanned_dirs.append(str(dp))
                for p in dp.glob("*.npy"):
                    name = p.stem
                    if len(name) == 10 and name[4] == "-" and name[7] == "-":
                        # 如果存在重复，优先使用列表中较早的目录。
                        mapping.setdefault(name, str(p))
            except Exception:
                continue
        self._legacy_shard_paths_cache[key] = mapping
        if mapping:
            self._log(
                "debug",
                "legacy_index_built",
                symbol=symbol,
                timeframe=tf,
                legacy_days=len(mapping),
                legacy_dirs=";".join(scanned_dirs[:3]) + (";..." if len(scanned_dirs) > 3 else ""),
            )
        return mapping

    def _load_shard(self, path: str) -> np.ndarray:
        if not os.path.exists(path):
            # 文件缺失对于预启动日期是正常的 - 以调试级别记录
            self.log.debug(f"Shard not found (expected for pre-inception): {path}")
            return np.empty((0,), dtype=CANDLE_DTYPE)
        try:
            with open(path, "rb") as f:
                arr = np.load(f, allow_pickle=False)
            if isinstance(arr, np.ndarray) and arr.dtype == CANDLE_DTYPE:
                return arr
            # 旧版下载器分片通常存储为二维浮点数组：
            # [timestamp, open, high, low, close, volume]
            if isinstance(arr, np.ndarray) and arr.ndim == 2 and arr.shape[1] >= 6:
                raw = np.asarray(arr[:, :6], dtype=np.float64)
                out = np.empty((raw.shape[0],), dtype=CANDLE_DTYPE)
                out["ts"] = raw[:, 0].astype(np.int64)
                out["o"] = raw[:, 1].astype(np.float32)
                out["h"] = raw[:, 2].astype(np.float32)
                out["l"] = raw[:, 3].astype(np.float32)
                out["c"] = raw[:, 4].astype(np.float32)
                out["bv"] = raw[:, 5].astype(np.float32)
                return out
            return _ensure_dtype(arr)
        except Exception as e:  # pragma: no cover - 尽力而为
            self.log.warning(f"Failed loading shard {path}: {e}")
            return np.empty((0,), dtype=CANDLE_DTYPE)

    def _legacy_day_is_complete(self, symbol: str, tf: str, date_key: str) -> bool:
        """如果旧版数据有该日期的连续分片则返回 True。

        "完整"定义为完整的 UTC 日 1m K 线：
        - 恰好 1440 分钟
        - 覆盖给定 date_key 的 [00:00, 23:59] UTC
        - 严格 1m 连续且无重复

        这是有意设计为严格的，因为此标志控制是否跳过写入主分片覆盖层。
        如果错误地将不完整的旧版分片视为完整，每次运行都会重新下载缺失的分钟
        但永远不会持久化它们。
        """
        cache_key = (str(symbol), str(tf), str(date_key))
        cached = self._legacy_day_quality_cache.get(cache_key)
        if cached is not None:
            return bool(cached)
        ok = False
        try:
            legacy_paths = self._get_legacy_shard_paths(symbol, tf)
            legacy_path = legacy_paths.get(date_key)
            if not legacy_path or not os.path.exists(str(legacy_path)):
                ok = False
            else:
                arr = self._load_shard(str(legacy_path))
                if arr.size == 0:
                    ok = False
                else:
                    day_start, day_end = self._date_range_of_key(str(date_key))
                    expected_len = int((day_end - day_start) // ONE_MIN_MS) + 1  # 1440
                    if int(arr.shape[0]) != int(expected_len):
                        ok = False
                    else:
                        ts = np.sort(arr["ts"].astype(np.int64, copy=False))
                        if int(ts[0]) != int(day_start) or int(ts[-1]) != int(day_end):
                            ok = False
                        else:
                            diffs = np.diff(ts)
                            ok = bool(
                                diffs.size
                                and int(diffs.min()) == ONE_MIN_MS
                                and int(diffs.max()) == ONE_MIN_MS
                            )
        except Exception:
            ok = False
        self._legacy_day_quality_cache[cache_key] = bool(ok)
        return bool(ok)

    def _load_from_disk(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> Optional[np.ndarray]:
        """加载所有与 [start_ts, end_ts] 相交的分片并合并到缓存。

        主缓存：`{cache_dir}/ohlcv/{exchange}/{tf}/{symbol}/YYYY-MM-DD.npy`

        注意：`historical_data/` 中的旧版数据会在 CandlestickManager 初始化时
        自动迁移到主缓存。下面的旧版回退作为安全网，处理未被迁移的数据。
        """
        try:
            tf_norm = self._normalize_timeframe_arg(timeframe, tf)
            shard_paths = self._iter_shard_paths(symbol, tf=tf_norm)
            legacy_paths = self._get_legacy_shard_paths(symbol, tf_norm)
            days = self._date_keys_between(start_ts, end_ts)
            load_keys: List[Tuple[str, str]] = []
            day_ctx: Dict[str, Dict[str, Any]] = {}
            legacy_hits = 0
            primary_hits = 0
            merged_hits = 0
            for key, (day_start, day_end) in days.items():
                if day_end < start_ts or day_start > end_ts:
                    continue
                primary_path = shard_paths.get(key)
                legacy_path = legacy_paths.get(key)
                if primary_path is None and legacy_path is None:
                    continue

                chosen_path: Optional[str] = None
                chosen_source: str = ""

                # 对于 1m，将旧版下载器分片视为规范的，仅在旧版缺失/不完整时
                # 使用主分片作为覆盖层。
                if tf_norm == "1m" and legacy_path is not None:
                    legacy_complete = False
                    try:
                        legacy_complete = self._legacy_day_is_complete(symbol, tf_norm, key)
                    except Exception:
                        legacy_complete = False

                    if legacy_complete:
                        chosen_path = legacy_path
                        chosen_source = "legacy"
                        legacy_hits += 1
                    else:
                        if primary_path is not None:
                            # 加载两者并合并以最大化覆盖（减少慢速重获路径）。
                            chosen_path = legacy_path
                            chosen_source = "merge"
                            merged_hits += 1
                        else:
                            chosen_path = legacy_path
                            chosen_source = "legacy"
                            legacy_hits += 1
                else:
                    if primary_path is not None:
                        chosen_path = primary_path
                        chosen_source = "primary"
                        primary_hits += 1
                    else:
                        chosen_path = legacy_path
                        chosen_source = "legacy"
                        legacy_hits += 1

                if chosen_path is not None:
                    load_keys.append((key, chosen_path))
                    day_ctx[key] = {
                        "day_start": int(day_start),
                        "day_end": int(day_end),
                        "source": chosen_source,
                        "primary_path": primary_path,
                        "legacy_path": legacy_path,
                    }
            if not load_keys:
                return
            self._log(
                "debug",
                "disk_load_plan",
                symbol=symbol,
                timeframe=tf_norm,
                days_total=len(days),
                primary_days=primary_hits,
                legacy_days=legacy_hits,
                merged_days=merged_hits,
            )
            # 加载并合并，带粗粒度进度更新以显示大范围操作的活跃状态。
            arrays: List[np.ndarray] = []
            t0 = time.monotonic()
            last_progress_log = t0
            for i, (day_key, path) in enumerate(sorted(load_keys), start=1):
                ctx = day_ctx.get(day_key, {})
                src = str(ctx.get("source") or "")
                if tf_norm == "1m" and src == "merge":
                    legacy_arr = self._load_shard(path)
                    primary_arr = np.empty((0,), dtype=CANDLE_DTYPE)
                    try:
                        pp = ctx.get("primary_path")
                        if pp:
                            primary_arr = self._load_shard(str(pp))
                    except Exception:
                        primary_arr = np.empty((0,), dtype=CANDLE_DTYPE)
                    # 保留旧版为规范数据：主分片仅用于填充旧版缺口。
                    a = self._merge_overwrite(primary_arr, legacy_arr)
                else:
                    a = self._load_shard(path)

                # 注意：我们有意不将旧版数据写入主分片。
                # 主分片仅用于填充旧版缺失/不完整的缺口。
                arrays.append(a)
                now = time.monotonic()
                if now - last_progress_log >= 5.0 or i == len(load_keys):
                    last_progress_log = now
                    self._log(
                        "debug",
                        "disk_load_progress",
                        symbol=symbol,
                        timeframe=tf_norm,
                        loaded=i,
                        total=len(load_keys),
                        current_day=day_key,
                        elapsed_s=f"{(now - t0):.1f}",
                    )
            arrays = [a for a in arrays if a.size]
            if not arrays:
                return
            merged_disk = np.sort(np.concatenate(arrays), order="ts")

            # 如果旧版数据揭示了比存储的 inception_ts 更早的 K 线，
            # 现在更新 inception_ts 以免归档预取逻辑跳过。
            if tf_norm == "1m":
                try:
                    self._maybe_update_inception_ts(symbol, merged_disk, save=True)
                except Exception as exc:
                    self._log(
                        "warning",
                        "maybe_update_inception_ts_failed",
                        symbol=symbol,
                        error=str(exc),
                    )
            self._log(
                "debug",
                "disk_load_done",
                symbol=symbol,
                timeframe=tf_norm,
                rows=int(merged_disk.shape[0]),
                elapsed_s=f"{(time.monotonic() - t0):.1f}",
            )
            self._log(
                "debug",
                "load_from_disk",
                symbol=symbol,
                timeframe=tf_norm,
                days=len(load_keys),
                primary_days=primary_hits,
                legacy_days=legacy_hits,
                rows=int(merged_disk.shape[0]),
                start_ts=start_ts,
                end_ts=end_ts,
            )
            if tf_norm == "1m":
                existing = self._ensure_symbol_cache(symbol)
                merged = self._merge_overwrite(existing, merged_disk)
                self._cache[symbol] = merged
                return merged
            else:
                # 不触碰高级别时间周期的 1m 缓存；让调用者处理
                return merged_disk
        except Exception as e:  # pragma: no cover - 非关键操作
            self._log("warning", "disk_load_error", symbol=symbol, timeframe=tf_norm, error=str(e))
            return None

    def _save_range(
        self,
        symbol: str,
        arr: np.ndarray,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> None:
        """按 date_key 将获取的 K 线持久化为日分片。"""
        if arr.size == 0:
            return
        arr = np.sort(_ensure_dtype(arr), order="ts")
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        current_key: Optional[str] = None
        bucket = []
        total = 0
        for row in arr:
            key = self._date_key(int(row["ts"]))
            if current_key is None:
                current_key = key
            if key != current_key:
                if bucket:
                    self._save_shard(
                        symbol,
                        current_key,
                        np.array(bucket, dtype=CANDLE_DTYPE),
                        tf=tf_norm,
                    )
                    total += len(bucket)
                bucket = []
                current_key = key
            bucket.append(tuple(row.tolist()))
        if bucket and current_key is not None:
            self._save_shard(symbol, current_key, np.array(bucket, dtype=CANDLE_DTYPE), tf=tf_norm)
            total += len(bucket)
            self._log("debug", "saved_range", symbol=symbol, rows=total)

    def _save_range_incremental(
        self,
        symbol: str,
        arr: np.ndarray,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
        defer_index: bool = False,
    ) -> None:
        """通过与磁盘上现有分片合并来持久化 K 线。

        参数:
            defer_index: 如果为 True，延迟 index.json 写入直到调用 flush_deferred_index。
        """
        if arr.size == 0:
            return
        arr = np.sort(_ensure_dtype(arr), order="ts")
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        shard_paths = self._iter_shard_paths(symbol, tf=tf_norm)
        shards_saved = []

        def flush_bucket(key: Optional[str], bucket: List[Tuple], is_last: bool = False) -> None:
            if key is None or not bucket:
                return
            chunk = np.array(bucket, dtype=CANDLE_DTYPE)
            existing = np.empty((0,), dtype=CANDLE_DTYPE)
            path = shard_paths.get(key)
            if path and os.path.exists(path):
                existing = self._load_shard(path)
            merged = self._merge_overwrite(existing, chunk)
            # 除最后一个分片外延迟索引写入（如果 defer_index=True 则全部延迟）
            should_defer = defer_index or not is_last
            self._save_shard(symbol, key, merged, tf=tf_norm, defer_index=should_defer)
            shard_paths[key] = self._shard_path(symbol, key, tf=tf_norm)
            shards_saved.append(key)

        current_key: Optional[str] = None
        bucket: List[Tuple] = []
        keys_to_process = []

        # 第一遍：收集所有键
        for row in arr:
            key = self._date_key(int(row["ts"]))
            if current_key is None:
                current_key = key
            if key != current_key:
                keys_to_process.append((current_key, bucket))
                bucket = []
                current_key = key
            bucket.append(tuple(row.tolist()))
        if current_key is not None:
            keys_to_process.append((current_key, bucket))

        # 第二遍：带 is_last 标志刷新
        for i, (key, bucket_data) in enumerate(keys_to_process):
            is_last = i == len(keys_to_process) - 1
            flush_bucket(key, bucket_data, is_last=is_last)

        # 使分片路径缓存失效，以便后续查找能看到新保存的文件
        if shards_saved:
            self._invalidate_shard_paths_cache(symbol, tf=tf_norm)

    def _persist_batch(
        self,
        symbol: str,
        batch: np.ndarray,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
        merge_cache: bool = False,
        last_refresh_ms: Optional[int] = None,
        defer_index: bool = False,
        skip_memory_retention: bool = False,
    ) -> None:
        """将 `batch` 合并到内存（可选）并增量持久化到磁盘。

        参数:
            defer_index: 如果为 True，延迟 index.json 写入直到调用 flush_deferred_index。
            skip_memory_retention: 如果为 True，跳过内存保留策略执行以在缓存中
                保留完整历史数据（适用于回测数据准备）。
        """
        if batch.size == 0:
            return
        arr = np.sort(_ensure_dtype(batch), order="ts")
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)

        # 如果这是 1m 的新的最早数据，则更新 inception_ts（延迟保存到末尾）
        if tf_norm == "1m":
            self._maybe_update_inception_ts(symbol, arr, save=not defer_index)

        if merge_cache or tf_norm == "1m":
            merged_cache = self._merge_overwrite(self._ensure_symbol_cache(symbol), arr)
            self._cache[symbol] = merged_cache
            if not skip_memory_retention:
                try:
                    self._enforce_memory_retention(symbol)
                except Exception:
                    pass
            if last_refresh_ms is not None and merged_cache.size:
                self._set_last_refresh_meta(
                    symbol,
                    last_refresh_ms=last_refresh_ms,
                    last_final_ts=int(merged_cache[-1]["ts"]),
                )
            # 检查真实数据是否替换了之前的合成时间戳
            # 如果是，标记该交易对的 EMA 需要重计算
            self._check_synthetic_replacement(symbol, arr)

        self._save_range_incremental(symbol, arr, timeframe=tf_norm, defer_index=defer_index)
        observer = self._persist_batch_observer
        if observer is not None:
            try:
                observer(symbol, tf_norm, arr)
            except Exception:
                # Observability hooks must never break persistence or trading.
                return

    def _check_synthetic_replacement(self, symbol: str, real_data: np.ndarray) -> None:
        """检查真实数据是否替换了之前合成的时间戳，如果是则使 EMA 缓存失效。"""
        if symbol not in self._synthetic_timestamps or not self._synthetic_timestamps[symbol]:
            return
        if real_data.size == 0:
            return

        real_ts_set = set(real_data["ts"].astype(np.int64).tolist())
        replaced = self._synthetic_timestamps[symbol] & real_ts_set
        if replaced:
            # 真实数据到达了之前合成的时间戳 - 使 EMA 缓存失效
            self._synthetic_timestamps[symbol] -= replaced
            self._invalidate_ema_cache(symbol)
            count = len(replaced)
            if self._candle_replace_batch_mode:
                # 批次模式：收集以供后续汇总
                self._candle_replace_batch[symbol] = self._candle_replace_batch.get(symbol, 0) + count
            else:
                # 正常操作：以 DEBUG 级别记录（单条消息较嘈杂）
                self.log.debug(
                    "[candle] %s: real data replaced %d synthetic candle%s, EMA cache invalidated",
                    symbol,
                    count,
                    "s" if count > 1 else "",
                )

    def _track_synthetic_timestamps(self, symbol: str, timestamps: List[int]) -> None:
        """跟踪运行时合成时间戳，用于替换检测。"""
        if not symbol or not timestamps:
            return
        ts_set = {int(ts) for ts in timestamps if int(ts) > 0}
        if not ts_set:
            return
        if symbol not in self._synthetic_timestamps:
            self._synthetic_timestamps[symbol] = set()
        self._synthetic_timestamps[symbol].update(ts_set)
        # 仅保留最近一周的数据以限制内存使用。
        cutoff = _utc_now_ms() - 7 * 24 * 60 * ONE_MIN_MS
        self._synthetic_timestamps[symbol] = {
            ts for ts in self._synthetic_timestamps[symbol] if ts > cutoff
        }

    def _materialize_runtime_synthetic_gap(self, symbol: str, through_ts: int) -> int:
        """仅在内存中填充已结束分钟的缺口（不持久化到磁盘）。

        返回添加到内存缓存的合成 K 线数量。
        """
        through_ts = _floor_minute(int(through_ts))
        if through_ts <= 0:
            return 0

        arr = _ensure_dtype(self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE)))
        if arr.size == 0:
            # 尝试加载更广泛的历史切片，以便从最后已知的收盘价开始填充。
            try:
                seed_start = max(0, through_ts - 30 * 24 * 60 * ONE_MIN_MS)
                loaded = self._load_from_disk(symbol, seed_start, through_ts, timeframe="1m")
                if isinstance(loaded, np.ndarray) and loaded.size:
                    arr = _ensure_dtype(loaded)
            except Exception as exc:
                self._log(
                    "debug",
                    "runtime_synthetic_seed_load_failed",
                    symbol=symbol,
                    error=str(exc),
                )
        if arr.size == 0:
            return 0

        arr = np.sort(arr, order="ts")
        ts_arr = arr["ts"].astype(np.int64, copy=False)
        idx = int(np.searchsorted(ts_arr, through_ts, side="right")) - 1
        if idx < 0:
            return 0

        last_ts = int(ts_arr[idx])
        if last_ts >= through_ts:
            return 0

        # 限制合成突发量，避免交易对长期不活跃时构建巨大的内存序列。
        max_synth = max(1, min(self.max_memory_candles_per_symbol, 24 * 60))
        first_synth_ts = max(last_ts + ONE_MIN_MS, through_ts - (max_synth - 1) * ONE_MIN_MS)
        if first_synth_ts > through_ts:
            return 0

        prev_close = float(arr[idx]["c"])
        if not math.isfinite(prev_close):
            return 0

        synth_ts = np.arange(first_synth_ts, through_ts + ONE_MIN_MS, ONE_MIN_MS, dtype=np.int64)
        if synth_ts.size == 0:
            return 0

        synth = np.empty((synth_ts.shape[0],), dtype=CANDLE_DTYPE)
        synth["ts"] = synth_ts
        synth["o"] = prev_close
        synth["h"] = prev_close
        synth["l"] = prev_close
        synth["c"] = prev_close
        synth["bv"] = 0.0

        merged = self._merge_overwrite(arr, synth)
        self._cache[symbol] = merged
        try:
            self._enforce_memory_retention(symbol)
        except Exception as exc:
            self._log(
                "debug",
                "runtime_synthetic_retention_enforcement_failed",
                symbol=symbol,
                error=str(exc),
            )
        self._track_synthetic_timestamps(symbol, synth_ts.tolist())

        self._log(
            "debug",
            "runtime_synthetic_gap_materialized",
            symbol=symbol,
            synthesized=int(synth_ts.shape[0]),
            first_ts=int(synth_ts[0]),
            last_ts=int(synth_ts[-1]),
            seed_last_real_ts=last_ts,
        )
        return int(synth_ts.shape[0])

    def _invalidate_ema_cache(self, symbol: str) -> None:
        """使交易对的所有缓存 EMA 值失效，强制重计算。"""
        if symbol in self._ema_cache:
            del self._ema_cache[symbol]

    def needs_ema_recompute(self, symbol: str) -> bool:
        """检查交易对的 EMA 是否应因合成数据替换而重计算。

        机器人可以调用此方法检查自上次 EMA 计算以来，真实数据是否替换了合成数据，
        表明 EMA 应被重计算。

        在以下情况返回 True：
        - EMA 缓存因合成替换而失效
        - 交易对没有缓存的 EMA（将重新计算）

        在以下情况返回 False：
        - 交易对有基于真实数据的有效缓存 EMA
        """
        # 如果该交易对没有 EMA 缓存，将重新计算
        if symbol not in self._ema_cache or not self._ema_cache[symbol]:
            return True
        # 如果缓存存在，则有效（失效操作会清除它）
        return False

    def clear_synthetic_tracking(self, symbol: Optional[str] = None) -> None:
        """清除交易对或所有交易对的合成时间戳跟踪。

        适用于预热完成或机器人确认所有真实数据已获取后。
        """
        if symbol is None:
            self._synthetic_timestamps.clear()
        elif symbol in self._synthetic_timestamps:
            del self._synthetic_timestamps[symbol]

    def _merge_overwrite(self, existing: np.ndarray, new: np.ndarray) -> np.ndarray:
        """按 ts 合并两个 K 线数组，冲突时优先使用 `new` 的值。"""
        if existing.size == 0:
            return np.sort(_ensure_dtype(new), order="ts")
        if new.size == 0:
            return np.sort(_ensure_dtype(existing), order="ts")
        a = _ensure_dtype(existing)
        b = _ensure_dtype(new)
        # 现有数据在前，新数据在后；然后保留每个时间戳的最后一行以优先使用新数据
        combo = np.concatenate([a, b])
        # 稳定排序确保相同时间戳时，`new` 的行在 `existing` 之后。
        combo = np.sort(combo, order="ts", kind="stable")
        ts = combo["ts"].astype(np.int64, copy=False)
        if combo.size <= 1:
            return combo
        # 去重：保留每个时间戳的最后一次出现（向量化操作）。
        keep = np.empty(combo.size, dtype=bool)
        keep[:-1] = ts[:-1] != ts[1:]
        keep[-1] = True
        merged = combo[keep]
        # 执行内存保留策略：每个交易对仅保留最新的 N 根 K 线（由调用者在赋值后应用）
        return merged

    # ----- 已知缺口辅助方法 -----

    def _get_known_gaps_enhanced(self, symbol: str) -> List[GapEntry]:
        """返回已知缺口为增强的 GapEntry 对象，包含完整元数据。"""
        idx = self._ensure_symbol_index(symbol)
        gaps = idx.get("meta", {}).get("known_gaps", [])
        out: List[GapEntry] = []
        now_ms = int(time.time() * 1000)
        for it in gaps:
            try:
                # 支持旧格式 [[start, end], ...] 和新格式 [GapEntry, ...]
                if isinstance(it, dict):
                    # 新版增强格式
                    entry: GapEntry = {
                        "start_ts": int(it.get("start_ts", 0)),
                        "end_ts": int(it.get("end_ts", 0)),
                        "retry_count": int(it.get("retry_count", 0)),
                        "reason": str(it.get("reason", GAP_REASON_AUTO)),
                        "added_at": int(it.get("added_at", now_ms)),
                    }
                    if entry["start_ts"] <= entry["end_ts"]:
                        out.append(entry)
                elif isinstance(it, (list, tuple)) and len(it) >= 2:
                    # 旧版格式：自动升级为增强格式
                    a, b = int(it[0]), int(it[1])
                    if a <= b:
                        out.append(
                            {
                                "start_ts": a,
                                "end_ts": b,
                                "retry_count": _GAP_MAX_RETRIES,  # Assume old gaps are persistent
                                "reason": GAP_REASON_AUTO,
                                "added_at": now_ms,
                            }
                        )
            except Exception:
                continue
        return out

    def _get_known_gaps(self, symbol: str) -> List[Tuple[int, int]]:
        """返回已知缺口为简单的 (start_ts, end_ts) 元组，用于向后兼容。"""
        enhanced = self._get_known_gaps_enhanced(symbol)
        return [(g["start_ts"], g["end_ts"]) for g in enhanced]

    def _save_known_gaps_enhanced(self, symbol: str, gaps: List[GapEntry]) -> None:
        """以增强格式保存缺口，合并重叠范围。"""
        # 按 start_ts 排序
        gaps = sorted(gaps, key=lambda g: g["start_ts"])
        merged: List[GapEntry] = []
        for gap in gaps:
            if not merged or gap["start_ts"] > merged[-1]["end_ts"] + ONE_MIN_MS:
                merged.append(gap)
            else:
                # 合并重叠缺口，保留最大重试次数和最早的添加时间
                prev = merged[-1]
                merged[-1] = {
                    "start_ts": prev["start_ts"],
                    "end_ts": max(prev["end_ts"], gap["end_ts"]),
                    "retry_count": max(prev.get("retry_count", 0), gap.get("retry_count", 0)),
                    "reason": prev.get("reason", GAP_REASON_AUTO),  # 保留原始原因
                    "added_at": min(prev.get("added_at", 0), gap.get("added_at", 0)),
                }
        idx = self._ensure_symbol_index(symbol)
        idx["meta"]["known_gaps"] = [
            {
                "start_ts": int(g["start_ts"]),
                "end_ts": int(g["end_ts"]),
                "retry_count": int(g.get("retry_count", 0)),
                "reason": str(g.get("reason", GAP_REASON_AUTO)),
                "added_at": int(g.get("added_at", 0)),
            }
            for g in merged
        ]
        self._index[symbol] = idx
        self._save_index(symbol)

    def _save_known_gaps(self, symbol: str, gaps: List[Tuple[int, int]]) -> None:
        """从简单元组保存缺口（向后兼容包装器）。"""
        now_ms = int(time.time() * 1000)
        enhanced = [
            {
                "start_ts": int(s),
                "end_ts": int(e),
                "retry_count": _GAP_MAX_RETRIES,  # Assume caller-provided gaps are persistent
                "reason": GAP_REASON_AUTO,
                "added_at": now_ms,
            }
            for s, e in gaps
        ]
        self._save_known_gaps_enhanced(symbol, enhanced)

    def _add_known_gap(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        *,
        reason: str = GAP_REASON_AUTO,
        increment_retry: bool = True,
        retry_count: Optional[int] = None,
    ) -> None:
        """添加或更新带增强元数据的已知缺口。

        如果与 [start_ts, end_ts] 重叠的缺口已存在：
        - 扩展缺口以覆盖完整范围
        - 如果 increment_retry 为 True 则递增 retry_count（除非指定了 retry_count）
        - 如果提供了 reason 则更新

        如果 retry_count 达到 _GAP_MAX_RETRIES，缺口被视为持久缺口，
        除非使用 force_refetch_gaps 否则不会重新获取。

        参数:
            retry_count: 如果指定，直接设置 retry_count 而非递增。
                         适用于应立即标记为持久的预启动缺口。
        """
        now_ms = int(time.time() * 1000)
        gaps = self._get_known_gaps_enhanced(symbol)

        # 检查是否有重叠的缺口需要更新
        updated = False
        previous_retry_count = 0
        for gap in gaps:
            if gap["start_ts"] <= end_ts + ONE_MIN_MS and gap["end_ts"] >= start_ts - ONE_MIN_MS:
                # 重叠 - 扩展并可选递增重试次数
                gap["start_ts"] = min(gap["start_ts"], int(start_ts))
                gap["end_ts"] = max(gap["end_ts"], int(end_ts))
                previous_retry_count = gap.get("retry_count", 0)
                if retry_count is not None:
                    gap["retry_count"] = retry_count
                elif increment_retry:
                    # 将 retry_count 上限设为 _GAP_MAX_RETRIES 以防止无限增长
                    # 并避免对持久缺口的冗余磁盘写入
                    new_retry_count = previous_retry_count + 1
                    gap["retry_count"] = min(new_retry_count, _GAP_MAX_RETRIES)
                if reason != GAP_REASON_AUTO:
                    gap["reason"] = reason
                updated = True
                break

        if not updated:
            initial_retry = retry_count if retry_count is not None else (1 if increment_retry else 0)
            new_gap: GapEntry = {
                "start_ts": int(start_ts),
                "end_ts": int(end_ts),
                "retry_count": initial_retry,
                "reason": reason,
                "added_at": now_ms,
            }
            gaps.append(new_gap)
            self._log(
                "debug",
                "gap_added",
                symbol=symbol,
                start_ts=start_ts,
                end_ts=end_ts,
                reason=reason,
                retry_count=new_gap["retry_count"],
            )
        else:
            # 仅当缺口从可重试转为持久时记录警告
            # （retry_count 从 <max 变为 >=max）。使用节流防止在相同缺口被多次处理的边缘情况下产生刷屏。
            updated_gap = next(
                (
                    g
                    for g in gaps
                    if g["start_ts"] <= end_ts + ONE_MIN_MS and g["end_ts"] >= start_ts - ONE_MIN_MS
                ),
                None,
            )
            if updated_gap:
                current_retry_count = updated_gap.get("retry_count", 0)
                gap_reason = updated_gap.get("reason", GAP_REASON_AUTO)
                # 仅在转为持久状态时警告（跳过 pre_inception - 预期行为）
                if (
                    current_retry_count >= _GAP_MAX_RETRIES
                    and previous_retry_count < _GAP_MAX_RETRIES
                    and gap_reason != "pre_inception"
                ):
                    gap_minutes = (updated_gap["end_ts"] - updated_gap["start_ts"]) // ONE_MIN_MS + 1
                    # 跟踪持久缺口用于摘要记录
                    if not hasattr(self, "_persistent_gap_summary"):
                        self._persistent_gap_summary: Dict[str, int] = {}
                    self._persistent_gap_summary[symbol] = (
                        self._persistent_gap_summary.get(symbol, 0) + 1
                    )

        self._save_known_gaps_enhanced(symbol, gaps)

    def _record_verified_gap(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        *,
        reason: str = GAP_REASON_NO_TRADES,
    ) -> None:
        """将缺口记录为已验证（交易所无数据），不再重试。"""
        if start_ts > end_ts:
            return
        self._add_known_gap(
            symbol,
            int(start_ts),
            int(end_ts),
            reason=reason,
            increment_retry=False,
            retry_count=_GAP_MAX_RETRIES,
        )

    def _should_retry_gap(self, gap: GapEntry) -> bool:
        """检查缺口是否应重试（retry_count < max）。"""
        return gap.get("retry_count", 0) < _GAP_MAX_RETRIES

    def clear_known_gaps(
        self,
        symbol: str,
        *,
        date_range: Optional[Tuple[int, int]] = None,
    ) -> int:
        """清除交易对的已知缺口，可选择按日期范围过滤。

        参数:
            symbol: 要清除缺口的交易对
            date_range: 可选的 (start_ts, end_ts)，仅清除此范围内的缺口

        返回:
            清除的缺口数量
        """
        gaps = self._get_known_gaps_enhanced(symbol)
        if not gaps:
            return 0

        if date_range is None:
            # 清除所有缺口
            cleared = len(gaps)
            idx = self._ensure_symbol_index(symbol)
            idx["meta"]["known_gaps"] = []
            self._index[symbol] = idx
            self._save_index(symbol)
            self._log(
                "info",
                "gaps_cleared",
                symbol=symbol,
                cleared_count=cleared,
            )
            return cleared

        # 仅清除与 date_range 重叠的缺口
        range_start, range_end = date_range
        remaining = []
        cleared = 0
        for gap in gaps:
            if gap["end_ts"] < range_start or gap["start_ts"] > range_end:
                # 在范围外 - 保留
                remaining.append(gap)
            else:
                cleared += 1

        if cleared > 0:
            self._save_known_gaps_enhanced(symbol, remaining)
            self._log(
                "info",
                "gaps_cleared",
                symbol=symbol,
                cleared_count=cleared,
                date_range_start=range_start,
                date_range_end=range_end,
            )
        return cleared

    def get_gap_summary(self, symbol: str) -> Dict[str, Any]:
        """获取交易对的已知缺口摘要。

        返回:
            包含以下键的字典：
            - total_gaps: 缺口条目数
            - total_minutes: 缺口总分钟数
            - persistent_gaps: retry_count >= max 的持久缺口
            - retryable_gaps: retry_count < max 的可重试缺口
            - by_reason: 原因 -> 数量的字典
            - gaps: 缺口详情列表
        """
        gaps = self._get_known_gaps_enhanced(symbol)
        if not gaps:
            return {
                "total_gaps": 0,
                "total_minutes": 0,
                "persistent_gaps": 0,
                "retryable_gaps": 0,
                "by_reason": {},
                "gaps": [],
            }

        total_minutes = sum((g["end_ts"] - g["start_ts"]) // ONE_MIN_MS + 1 for g in gaps)
        persistent = sum(1 for g in gaps if g.get("retry_count", 0) >= _GAP_MAX_RETRIES)
        retryable = len(gaps) - persistent

        by_reason: Dict[str, int] = {}
        for g in gaps:
            reason = g.get("reason", GAP_REASON_AUTO)
            by_reason[reason] = by_reason.get(reason, 0) + 1

        return {
            "total_gaps": len(gaps),
            "total_minutes": total_minutes,
            "persistent_gaps": persistent,
            "retryable_gaps": retryable,
            "by_reason": by_reason,
            "gaps": [
                {
                    "start_ts": g["start_ts"],
                    "end_ts": g["end_ts"],
                    "minutes": (g["end_ts"] - g["start_ts"]) // ONE_MIN_MS + 1,
                    "retry_count": g.get("retry_count", 0),
                    "reason": g.get("reason", GAP_REASON_AUTO),
                    "persistent": g.get("retry_count", 0) >= _GAP_MAX_RETRIES,
                }
                for g in gaps
            ],
        }

    def _missing_spans(self, arr: np.ndarray, start_ts: int, end_ts: int) -> List[Tuple[int, int]]:
        """返回 arr 中缺失的分钟对齐的包含区间 [gap_start, gap_end] 列表。"""
        spans: List[Tuple[int, int]] = []
        if start_ts > end_ts:
            return spans
        if arr.size == 0:
            return [(start_ts, end_ts)]
        ts = np.asarray(arr["ts"], dtype=np.int64)
        ts = ts[(ts >= start_ts) & (ts <= end_ts)]
        if ts.size == 0:
            return [(start_ts, end_ts)]
        # 头部缺口
        if ts[0] > start_ts:
            spans.append((start_ts, int(ts[0] - ONE_MIN_MS)))
        # 中间缺口
        for i in range(len(ts) - 1):
            if ts[i + 1] - ts[i] > ONE_MIN_MS:
                spans.append((int(ts[i] + ONE_MIN_MS), int(ts[i + 1] - ONE_MIN_MS)))
        # 尾部缺口
        if ts[-1] < end_ts:
            spans.append((int(ts[-1] + ONE_MIN_MS), end_ts))
        return spans

    @staticmethod
    def _missing_spans_step(
        arr: np.ndarray, start_ts: int, end_ts: int, step_ms: int
    ) -> List[Tuple[int, int]]:
        """返回 arr 中按 step_ms 步长缺失的包含区间 [gap_start, gap_end] 列表。"""
        spans: List[Tuple[int, int]] = []
        if start_ts > end_ts or step_ms <= 0:
            return spans
        if arr.size == 0:
            return [(start_ts, end_ts)]
        ts = np.asarray(arr["ts"], dtype=np.int64)
        ts = ts[(ts >= start_ts) & (ts <= end_ts)]
        if ts.size == 0:
            return [(start_ts, end_ts)]
        ts = np.sort(ts)
        # 头部缺口
        if ts[0] > start_ts:
            spans.append((int(start_ts), int(ts[0] - step_ms)))
        # 中间缺口
        for i in range(len(ts) - 1):
            if ts[i + 1] - ts[i] > step_ms:
                spans.append((int(ts[i] + step_ms), int(ts[i + 1] - step_ms)))
        # 尾部缺口
        if ts[-1] < end_ts:
            spans.append((int(ts[-1] + step_ms), int(end_ts)))
        return spans

    def check_disk_coverage(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
        log_level: str = "info",
        max_span_log: int = 3,
    ) -> Dict[str, Any]:
        """检查磁盘缓存是否完全覆盖交易对的 [start_ts, end_ts] 范围。

        返回包含以下键的字典：
            ok, missing_spans, missing_candles, loaded_rows, timeframe。
        """
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        step_ms = _tf_to_ms(tf_norm)
        if step_ms <= 0:
            step_ms = ONE_MIN_MS
        s_ts = (int(start_ts) // step_ms) * step_ms
        e_ts = (int(end_ts) // step_ms) * step_ms
        if s_ts > e_ts:
            return {
                "ok": True,
                "missing_spans": [],
                "missing_candles": 0,
                "loaded_rows": 0,
                "timeframe": tf_norm,
            }

        arr = self._load_from_disk(symbol, s_ts, e_ts, timeframe=tf_norm)
        if arr is None or arr.size == 0:
            missing = [(s_ts, e_ts)]
            loaded_rows = 0
        else:
            sub = self._slice_ts_range(arr, s_ts, e_ts)
            missing = (
                self._missing_spans(sub, s_ts, e_ts)
                if step_ms == ONE_MIN_MS
                else self._missing_spans_step(sub, s_ts, e_ts, step_ms)
            )
            loaded_rows = int(sub.shape[0]) if sub is not None else 0

        missing_candles = 0
        if missing:
            missing_candles = int(sum((e - s) // step_ms + 1 for s, e in missing))
            top_parts = []
            for s, e in missing[: max(1, int(max_span_log))]:
                top_parts.append(f"{self._fmt_ts(int(s))} to {self._fmt_ts(int(e))}")
            top_str = ", ".join(top_parts)
            if len(missing) > max_span_log:
                top_str = f"{top_str} (+{len(missing) - max_span_log} more)"
            self._log(
                log_level,
                "disk_coverage_missing",
                symbol=symbol,
                timeframe=tf_norm,
                start_ts=s_ts,
                end_ts=e_ts,
                missing_spans=len(missing),
                missing_candles=missing_candles,
                top=top_str,
            )
        return {
            "ok": len(missing) == 0,
            "missing_spans": missing,
            "missing_candles": missing_candles,
            "loaded_rows": loaded_rows,
            "timeframe": tf_norm,
        }

    def rebuild_index_for_range(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
        log_level: str = "info",
    ) -> Dict[str, Any]:
        """重建与 [start_ts, end_ts] 相交的分片的 index.json 元数据。"""
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        step_ms = _tf_to_ms(tf_norm)
        if step_ms <= 0:
            step_ms = ONE_MIN_MS
        s_ts = (int(start_ts) // step_ms) * step_ms
        e_ts = (int(end_ts) // step_ms) * step_ms
        if s_ts > e_ts:
            return {
                "updated": 0,
                "removed": 0,
                "scanned": 0,
                "timeframe": tf_norm,
                "start_ts": s_ts,
                "end_ts": e_ts,
            }

        # 确保该交易对/tf 的分片路径缓存是最新的。
        self._invalidate_shard_paths_cache(symbol, tf=tf_norm)
        shard_paths = self._iter_shard_paths(symbol, tf=tf_norm)

        idx = self._ensure_symbol_index(symbol, tf=tf_norm)
        shards = idx.setdefault("shards", {})

        updated = 0
        removed = 0
        scanned = 0

        for day_key, (day_start, day_end) in self._date_keys_between(s_ts, e_ts).items():
            if day_end < s_ts or day_start > e_ts:
                continue
            path = shard_paths.get(day_key)
            if path is None or not os.path.exists(path):
                if day_key in shards:
                    shards.pop(day_key, None)
                    removed += 1
                continue
            try:
                arr = _ensure_dtype(self._load_shard(path))
            except Exception:
                arr = np.empty((0,), dtype=CANDLE_DTYPE)
            if arr.size == 0:
                if day_key in shards:
                    shards.pop(day_key, None)
                    removed += 1
                continue
            arr = np.sort(arr, order="ts")
            crc = int(zlib.crc32(arr.tobytes()) & 0xFFFFFFFF)
            shards[day_key] = {
                "path": path,
                "min_ts": int(arr[0]["ts"]),
                "max_ts": int(arr[-1]["ts"]),
                "count": int(arr.shape[0]),
                "crc32": crc,
            }
            updated += 1
            scanned += 1

        idx["shards"] = shards
        pruned = 0
        try:
            pruned = int(self._prune_missing_shards_from_index(idx) or 0)
        except Exception:
            pruned = 0
        if pruned:
            removed += pruned

        # 防止损坏的刷新时间戳阻止更新。
        meta = idx.setdefault("meta", {})
        now = _utc_now_ms()
        try:
            last_refresh = int(meta.get("last_refresh_ms", 0) or 0)
        except Exception:
            last_refresh = 0
        meta_changed = False
        if last_refresh > (now + ONE_MIN_MS):
            meta["last_refresh_ms"] = 0
            meta_changed = True
            self._log(
                "warning",
                "index_last_refresh_in_future",
                symbol=symbol,
                timeframe=tf_norm,
                last_refresh_ms=last_refresh,
                now=now,
            )

        if updated or removed or meta_changed:
            self._save_index(symbol, tf=tf_norm)

        self._log(
            log_level,
            "index_rebuild_range",
            symbol=symbol,
            timeframe=tf_norm,
            start_ts=s_ts,
            end_ts=e_ts,
            scanned=scanned,
            updated=updated,
            removed=removed,
        )

        return {
            "updated": updated,
            "removed": removed,
            "scanned": scanned,
            "timeframe": tf_norm,
            "start_ts": s_ts,
            "end_ts": e_ts,
        }

    # ----- 刷新元数据辅助方法 -----

    def _get_last_refresh_ms(self, symbol: str) -> int:
        idx = self._ensure_symbol_index(symbol)
        try:
            return int(idx.get("meta", {}).get("last_refresh_ms", 0))
        except Exception:
            return 0

    def get_last_refresh_ms(self, symbol: str) -> int:
        """公共辅助方法：从索引元数据读取最后刷新时间戳 (ms)。"""
        return self._get_last_refresh_ms(symbol)

    def get_last_final_ts(self, symbol: str) -> int:
        """返回该交易对观测到的最后已结束 K 线时间戳 (ms)，如果未知则返回 0。"""
        idx = self._ensure_symbol_index(symbol)
        try:
            return int(idx.get("meta", {}).get("last_final_ts", 0))
        except Exception:
            return 0

    def _set_last_refresh_meta(
        self, symbol: str, last_refresh_ms: int, last_final_ts: Optional[int] = None
    ) -> None:
        idx = self._ensure_symbol_index(symbol)
        meta = idx.setdefault("meta", {})
        meta["last_refresh_ms"] = int(last_refresh_ms)
        if last_final_ts is not None:
            meta["last_final_ts"] = int(last_final_ts)
        self._index[symbol] = idx
        self._save_index(symbol)

    # ----- 覆盖范围/历史边界跟踪 -----

    def _infer_legacy_authoritative_start_ts(self, meta: Dict[str, Any]) -> Optional[int]:
        """从旧版 inception/pre_inception 元数据推断权威下界。

        旧版缓存将 `inception_ts` 同时用作最早观测 K 线和隐式下界
        （当紧邻其前的持久 `pre_inception` 缺口存在时）。
        在迁移期间保留该学习到的边界而非丢弃。
        """
        try:
            observed_start = meta.get("observed_start_ts", meta.get("inception_ts"))
            if observed_start is None:
                return None
            observed_start = int(observed_start)
            cutoff_end = observed_start - ONE_MIN_MS
            for gap in meta.get("known_gaps", []):
                if not isinstance(gap, dict):
                    continue
                if str(gap.get("reason", "")) != "pre_inception":
                    continue
                try:
                    gap_end = int(gap.get("end_ts"))
                    gap_start = int(gap.get("start_ts"))
                    retry_count = int(gap.get("retry_count", 0))
                except Exception:
                    continue
                if retry_count < _GAP_MAX_RETRIES:
                    continue
                if gap_start < observed_start and gap_end >= cutoff_end:
                    return observed_start
        except Exception:
            return None
        return None

    def _get_inception_ts(self, symbol: str) -> Optional[int]:
        """返回该交易对最早观测到的 K 线时间戳，如果未知则返回 None。

        历史上此字段也被用作交易所历史数据的权威下界。
        现在它仅跟踪本地观测覆盖，权威裁剪使用
        ``authoritative_start_ts``。
        """
        idx = self._ensure_symbol_index(symbol)
        try:
            meta = idx.get("meta", {})
            val = meta.get("observed_start_ts", meta.get("inception_ts"))
            return int(val) if val is not None else None
        except Exception:
            return None

    def _set_inception_ts(self, symbol: str, ts: int, *, save: bool = True) -> None:
        """设置该交易对最早观测到的 K 线时间戳。"""
        idx = self._ensure_symbol_index(symbol)
        meta = idx.setdefault("meta", {})
        current = meta.get("observed_start_ts", meta.get("inception_ts"))
        # 仅在未设置或新时间戳更早时更新
        if current is None or int(ts) < int(current):
            observed_ts = int(ts)
            meta["observed_start_ts"] = observed_ts
            meta["inception_ts"] = observed_ts  # legacy alias
            auth_current = meta.get("authoritative_start_ts")
            auth_updated = False
            if auth_current is not None and observed_ts < int(auth_current):
                meta["authoritative_start_ts"] = observed_ts
                meta["authoritative_start_source"] = "observed_data"
                auth_updated = True
            self._index[f"{symbol}::1m"] = idx
            if save:
                self._save_index(symbol)
            if auth_updated:
                # 如果之前将范围标记为预启动但后来观测到更早的
                # 真实数据，则该权威下界已过时。
                try:
                    self._prune_pre_inception_gaps(symbol, observed_ts, save=save)
                except Exception as exc:
                    self._log(
                        "warning",
                        "prune_pre_inception_gaps_failed",
                        symbol=symbol,
                        error=str(exc),
                    )
            self._log(
                "debug",
                "inception_ts_updated",
                symbol=symbol,
                old_ts=current,
                new_ts=observed_ts,
            )

    def _first_ohlcv_cache_path(self) -> Path:
        return Path(self.cache_dir) / "first_ohlcv_timestamps_unified_exchange_specific.json"

    def _first_ohlcv_cache_exchange_name(self) -> str:
        exchange_name = str(self.exchange_name or self._ex_id or "").lower()
        return _FIRST_OHLCV_EXCHANGE_CACHE_ALIASES.get(exchange_name, exchange_name)

    def _lookup_cached_authoritative_start_ts(self, symbol: str) -> Optional[int]:
        cache_path = self._first_ohlcv_cache_path()
        if not cache_path.exists():
            return None
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return None
            coin = symbol.split("/")[0].strip()
            exchange_name = self._first_ohlcv_cache_exchange_name()
            value = data.get(coin, {}).get(exchange_name)
            return int(value) if value is not None and float(value) > 0.0 else None
        except Exception:
            return None

    def _get_authoritative_start_ts(self, symbol: str) -> Optional[int]:
        """返回交易所历史数据的权威下界（如果已知）。"""
        idx = self._ensure_symbol_index(symbol)
        meta = idx.setdefault("meta", {})
        try:
            value = meta.get("authoritative_start_ts")
            if value is not None:
                return int(value)
        except Exception:
            pass

        cached = self._lookup_cached_authoritative_start_ts(symbol)
        if cached is not None:
            self._set_authoritative_start_ts(
                symbol,
                cached,
                source="exchange_specific_cache",
                save=True,
            )
            idx = self._ensure_symbol_index(symbol)
            value = idx.get("meta", {}).get("authoritative_start_ts")
            return int(value) if value is not None else None
        return None

    def _set_authoritative_start_ts(
        self, symbol: str, ts: int, *, source: str, save: bool = True
    ) -> None:
        """持久化该交易对的交易所历史数据权威下界。"""
        idx = self._ensure_symbol_index(symbol)
        meta = idx.setdefault("meta", {})
        observed_start = self._get_inception_ts(symbol)
        authoritative_ts = int(ts)
        authoritative_source = str(source)
        if observed_start is not None and observed_start < authoritative_ts:
            authoritative_ts = int(observed_start)
            authoritative_source = "observed_data"
        current = meta.get("authoritative_start_ts")
        if current is None or authoritative_ts < int(current):
            meta["authoritative_start_ts"] = authoritative_ts
            meta["authoritative_start_source"] = authoritative_source
            self._index[f"{symbol}::1m"] = idx
            if save:
                self._save_index(symbol)
            try:
                self._prune_pre_inception_gaps(symbol, authoritative_ts, save=save)
            except Exception as exc:
                self._log(
                    "warning",
                    "prune_pre_inception_gaps_failed",
                    symbol=symbol,
                    error=str(exc),
                )

    def _prune_pre_inception_gaps(self, symbol: str, inception_ts: int, *, save: bool = True) -> None:
        """修剪/移除现在已被真实数据覆盖的 reason='pre_inception' 已知缺口。"""
        gaps = self._get_known_gaps_enhanced(symbol)
        if not gaps:
            return
        cutoff_end = int(inception_ts) - ONE_MIN_MS
        changed = False
        new_gaps: List[GapEntry] = []

        for g in gaps:
            try:
                if str(g.get("reason", "")) != "pre_inception":
                    new_gaps.append(g)
                    continue
                s = int(g.get("start_ts", 0))
                e = int(g.get("end_ts", 0))
                if e <= cutoff_end:
                    new_gaps.append(g)
                    continue
                if s <= cutoff_end:
                    # 重叠：裁剪至启动前结尾
                    trimmed: GapEntry = {
                        "start_ts": s,
                        "end_ts": cutoff_end,
                        "retry_count": int(g.get("retry_count", 0)),
                        "reason": "pre_inception",
                        "added_at": int(g.get("added_at", 0)),
                    }
                    if trimmed["start_ts"] <= trimmed["end_ts"]:
                        new_gaps.append(trimmed)
                    changed = True
                    continue
                # 完全在启动之后：移除
                changed = True
            except Exception:
                new_gaps.append(g)

        if changed and save:
            self._save_known_gaps_enhanced(symbol, new_gaps)

    def _get_min_shard_ts(self, symbol: str) -> Optional[int]:
        """返回索引或磁盘上最早的分片时间戳 (ms)，如果可用。"""
        try:
            idx = self._ensure_symbol_index(symbol, tf="1m")
            shards = idx.get("shards") or {}
            if isinstance(shards, dict):
                min_ts: Optional[int] = None
                for shard_meta in shards.values():
                    if not isinstance(shard_meta, dict):
                        continue
                    mi = shard_meta.get("min_ts")
                    if mi is None:
                        continue
                    ts = int(mi)
                    min_ts = ts if min_ts is None else min(min_ts, ts)
                if min_ts is not None:
                    return min_ts
        except Exception:
            pass

        # 回退：从磁盘上的文件名推断最早分片。
        try:
            shard_dir = self._symbol_dir(symbol, tf="1m")
            if not os.path.isdir(shard_dir):
                return None
            day_keys = [f[:-4] for f in os.listdir(shard_dir) if f.endswith(".npy")]
            if not day_keys:
                return None
            day_keys.sort()
            start_ts, _ = self._date_range_of_key(day_keys[0])
            return int(start_ts)
        except Exception:
            return None

    def _get_inception_probe_meta(self, symbol: str) -> Tuple[int, int]:
        """返回启动探测的 (last_probe_ms, last_probe_end_ts)。"""
        idx = self._ensure_symbol_index(symbol)
        meta = idx.get("meta", {})
        try:
            last_probe_ms = int(meta.get("inception_ts_probe_ms", 0) or 0)
            last_probe_end_ts = int(meta.get("inception_ts_probe_end_ts", 0) or 0)
            return last_probe_ms, last_probe_end_ts
        except Exception:
            return 0, 0

    def _set_inception_probe_meta(
        self, symbol: str, probe_ms: int, probe_end_ts: int, *, save: bool = True
    ) -> None:
        """持久化启动探测元数据以避免重复探测。"""
        idx = self._ensure_symbol_index(symbol)
        meta = idx.setdefault("meta", {})
        meta["inception_ts_probe_ms"] = int(probe_ms)
        meta["inception_ts_probe_end_ts"] = int(probe_end_ts)
        self._index[f"{symbol}::1m"] = idx
        if save:
            self._save_index(symbol)

    def _maybe_update_inception_ts(self, symbol: str, arr: np.ndarray, *, save: bool = True) -> None:
        """如果 arr 包含比已知更早的时间戳则更新 inception_ts。"""
        if arr.size == 0:
            return
        first_ts = int(arr[0]["ts"]) if arr.ndim else int(arr["ts"])
        current = self._get_inception_ts(symbol)
        if current is None or first_ts < current:
            self._set_inception_ts(symbol, first_ts, save=save)

    # ----- CCXT 获取 -----

    async def _apply_rate_limit_backoff(self) -> None:
        """如果在全局速率限制退避期内则等待。

        当触发速率限制时，所有并发请求应暂停以避免惊群问题
        （所有请求同时重试）。
        """
        now = time.time()
        if now < self._rate_limit_until:
            wait_time = self._rate_limit_until - now
            if wait_time > 0:
                self._log("debug", "rate_limit_global_wait", wait_seconds=round(wait_time, 2))
                await asyncio.sleep(wait_time)

    async def _set_global_rate_limit(self, backoff_seconds: float = 5.0) -> None:
        """设置影响所有并发请求的全局速率限制退避。"""
        async with self._rate_limit_lock:
            new_until = time.time() + backoff_seconds
            # 仅在新退避比现有退避更长时扩展
            if new_until > self._rate_limit_until:
                self._rate_limit_until = new_until
                self._rate_limit_count += 1
                self._log(
                    "debug",
                    "rate_limit_global_set",
                    backoff_seconds=backoff_seconds,
                    total_count=self._rate_limit_count,
                )

    async def _ccxt_fetch_ohlcv_once(
        self,
        symbol: str,
        since_ms: int,
        limit: int,
        end_exclusive_ms: Optional[int] = None,
        timeframe: Optional[str] = None,
        *,
        tf: Optional[str] = None,
    ) -> list:
        """从 ccxt 获取单个 OHLCV 页面，带基本重试/退避。"""
        if self.exchange is None:
            return []
        # 确定要调用的方法（交易所实例或模块）
        ex = self.exchange
        if not hasattr(ex, "fetch_ohlcv"):
            return []

        exid = (self._ex_id or "").lower() if isinstance(self._ex_id, str) else ""
        is_bybit = "bybit" in exid
        is_hyperliquid = "hyperliquid" in exid
        max_attempts = 9 if is_bybit else 5
        backoff = 1.0 if is_bybit else 0.5
        backoff_cap = 20.0 if is_bybit else 8.0
        for attempt in range(max_attempts):
            # 如果有全局速率限制退避则等待
            await self._apply_rate_limit_backoff()
            try:
                params: Dict[str, Any] = {}
                # 为支持端点边界的交易所提供结束约束。
                # 注意：避免向 Bitget 传递 'until'，因为非 1m 时间周期会导致 API 验证错误。
                if end_exclusive_ms is not None:
                    exid = (self._ex_id or "").lower() if isinstance(self._ex_id, str) else ""
                    # 避免对产生尾锚定或不一致页面的交易所使用 'until'，
                    # 这会导致首次运行时前向分页不完整。
                    if (
                        "bitget" not in exid
                        and "okx" not in exid
                        and "bybit" not in exid
                        and "kucoin" not in exid
                        and "gateio" not in exid
                    ):
                        params["until"] = int(end_exclusive_ms) - 1

                # Bybit v5 对某些市场数据路由需要 category。CCXT 通常从市场信息推断，
                # 但显式指定可避免间歇性错误分类。
                if "bybit" in exid:
                    params.setdefault("category", "linear")

                tf_norm = self._normalize_timeframe_arg(timeframe, tf, default=self._ccxt_timeframe)
                t0 = time.monotonic()
                self._emit_remote_fetch(
                    {
                        "kind": "ccxt_fetch_ohlcv",
                        "stage": "start",
                        "exchange": str(self._ex_id),
                        "symbol": symbol,
                        "tf": tf_norm,
                        "since_ts": int(since_ms),
                        "limit": int(limit),
                        "attempt": int(attempt + 1),
                        "params": dict(params),
                    }
                )
                self._log(
                    "debug",
                    "ccxt_fetch_ohlcv",
                    symbol=symbol,
                    tf=tf_norm,
                    since_ts=int(since_ms),
                    limit=limit,
                    attempt=attempt + 1,
                    params=params,
                )
                if getattr(self, "_net_sem", None) is not None:
                    async with self._net_sem:  # type: ignore[attr-defined]
                        # 获取信号量后重新检查速率限制。
                        # 任务可能在 429 设置全局退避之前已排队；
                        # 现在遵守它而非在信号量解除阻塞后立即执行。
                        await self._apply_rate_limit_backoff()
                        res = await ex.fetch_ohlcv(
                            symbol,
                            timeframe=tf_norm,
                            since=since_ms,
                            limit=limit,
                            params=params,
                        )
                else:
                    res = await ex.fetch_ohlcv(
                        symbol,
                        timeframe=tf_norm,
                        since=since_ms,
                        limit=limit,
                        params=params,
                    )
                first_ts = None
                last_ts = None
                if res:
                    try:
                        first_ts = int(res[0][0])
                    except Exception:
                        first_ts = None
                    try:
                        last_ts = int(res[-1][0])
                    except Exception:
                        last_ts = None
                elapsed_ms = int((time.monotonic() - t0) * 1000)
                self._emit_remote_fetch(
                    {
                        "kind": "ccxt_fetch_ohlcv",
                        "stage": "ok",
                        "exchange": str(self._ex_id),
                        "symbol": symbol,
                        "tf": tf_norm,
                        "since_ts": int(since_ms),
                        "rows": int(len(res) if res else 0),
                        "first_ts": first_ts,
                        "last_ts": last_ts,
                        "elapsed_ms": elapsed_ms,
                    }
                )
                self._log(
                    "debug",
                    "ccxt_fetch_ohlcv_ok",
                    symbol=symbol,
                    tf=tf_norm,
                    since_ts=int(since_ms),
                    rows=(len(res) if res else 0),
                    first_ts=first_ts,
                    last_ts=last_ts,
                )
                return res or []
            except Exception as e:  # pragma: no cover - 测试中不使用网络
                err_type = type(e).__name__
                err_repr = repr(e)
                elapsed_ms = int((time.monotonic() - t0) * 1000) if "t0" in locals() else None
                self._emit_remote_fetch(
                    {
                        "kind": "ccxt_fetch_ohlcv",
                        "stage": "error",
                        "exchange": str(self._ex_id),
                        "symbol": symbol,
                        "tf": str(tf) if tf is not None else None,
                        "since_ts": int(since_ms),
                        "attempt": int(attempt + 1),
                        "elapsed_ms": elapsed_ms,
                        "params": dict(params) if "params" in locals() else None,
                        "error_type": err_type,
                        "error": str(e),
                        "error_repr": err_repr,
                    }
                )
                self._log(
                    "warning",
                    "ccxt_fetch_ohlcv_failed",
                    symbol=symbol,
                    tf=str(tf) if tf is not None else None,
                    attempt=attempt + 1,
                    params=params if "params" in locals() else None,
                    error_type=err_type,
                    error=str(e),
                    error_repr=err_repr,
                )
                sleep_s = backoff
                msg = str(e) or ""
                msg_l = msg.lower()
                # 启发式：对速率限制类响应加大退避力度。
                is_rate_limit = any(x in msg_l for x in ("rate limit", "too many", "429", "10006"))
                if is_rate_limit:
                    # 设置全局退避以协调所有并发请求
                    # Hyperliquid 因更严格的限制需要更长的退避
                    global_backoff = 10.0 if is_hyperliquid else 5.0
                    await self._set_global_rate_limit(global_backoff)
                    sleep_s = max(sleep_s, global_backoff)
                # Bybit：对瞬态网络类错误更加重试。
                if is_bybit and (
                    err_type
                    in {"RequestTimeout", "NetworkError", "ExchangeNotAvailable", "DDoSProtection"}
                    or any(
                        x in msg_l
                        for x in (
                            "timed out",
                            "timeout",
                            "etimedout",
                            "econnreset",
                            "502",
                            "503",
                            "504",
                        )
                    )
                ):
                    sleep_s = max(sleep_s, 2.0)
                await asyncio.sleep(sleep_s)
                backoff = min(backoff * 2.0, backoff_cap)
        return []

    # ----- 数组切片辅助方法 -----

    def _slice_ts_range(
        self, arr: np.ndarray, start_ts: int, end_ts: int, *, assume_sorted: bool = False
    ) -> np.ndarray:
        """按 'ts' 将 arr 切片到 [start_ts, end_ts] 包含范围。

        假设 arr 是结构化 dtype CANDLE_DTYPE。

        参数
        ----------
        assume_sorted : bool
            如果为 True，跳过排序（调用者保证 arr 已按 ts 排序）。
            当 arr 来自已排序的 get_candles/standardize_gaps 时使用。
        """
        if arr.size == 0:
            return arr
        arr = _ensure_dtype(arr)
        if not assume_sorted:
            # 仅在需要时排序 - 检查是否已排序以跳过 O(n log n) 排序
            ts_arr = arr["ts"]
            if ts_arr.size > 1 and not np.all(ts_arr[:-1] <= ts_arr[1:]):
                arr = np.sort(arr, order="ts")
        ts_arr = _ts_index(arr)
        i0 = int(np.searchsorted(ts_arr, start_ts, side="left"))
        i1 = int(np.searchsorted(ts_arr, end_ts, side="right"))
        return arr[i0:i1]

    def _normalize_ccxt_ohlcv(self, rows: list) -> np.ndarray:
        """将 ccxt 行 [ms,o,h,l,c,vol] 转换为 CANDLE_DTYPE 并过滤对齐。"""
        if not rows:
            return np.empty((0,), dtype=CANDLE_DTYPE)
        out = []
        for r in rows:
            try:
                ts = int(r[0])
                # 仅保留完全分钟对齐的 K 线
                if ts % ONE_MIN_MS != 0:
                    ts = _floor_minute(ts)
                o, h, l, c = map(float, (r[1], r[2], r[3], r[4]))
                bv = float(r[5]) if len(r) > 5 else 0.0
                bv = normalize_ccxt_volume_to_base(self._ex_id or "", c, bv)
                out.append((ts, o, h, l, c, bv))
            except Exception:
                continue
        if not out:
            return np.empty((0,), dtype=CANDLE_DTYPE)
        arr = np.array(out, dtype=CANDLE_DTYPE)
        arr = np.sort(arr, order="ts")
        # 去除重复 ts，保留最后一个
        ts = arr["ts"].astype(np.int64)
        keep = np.ones(len(arr), dtype=bool)
        last = None
        for i in range(len(arr)):
            if last is not None and ts[i] == last:
                keep[i - 1] = False
            last = ts[i]
        return arr[keep]

    async def _fetch_ohlcv_paginated(
        self,
        symbol: str,
        since_ms: int,
        end_exclusive_ms: int,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
        on_batch: Optional[Callable[[np.ndarray], None]] = None,
    ) -> np.ndarray:
        """从 `since_ms` 获取 OHLCV 直到但不包含 `end_exclusive_ms`。

        使用 ccxt 的 since+limit 分页。返回 CANDLE_DTYPE 数组。
        """
        if self.exchange is None:
            return np.empty((0,), dtype=CANDLE_DTYPE)
        since_start = int(since_ms)
        since = int(since_ms)
        end_excl = int(end_exclusive_ms)
        limit = self._ccxt_limit_default
        tf_norm = self._normalize_timeframe_arg(timeframe, tf, default=self._ccxt_timeframe)
        # 从时间周期推导分页步长
        period_ms = _tf_to_ms(tf_norm)
        # 某些交易所将 `since` 视为排他。退回重叠量以避免遗漏第一根 K 线。
        if self._ccxt_since_exclusive and self._ccxt_page_overlap_candles > 0 and since > 0:
            overlap_ms = period_ms * int(self._ccxt_page_overlap_candles)
            since = max(0, since - overlap_ms)
        all_rows = []
        pages = 0
        prev_last_ts: Optional[int] = None
        total_span = max(1, end_excl - since_start)
        while since < end_excl:
            # Bitget 自动探测：尝试一次更大的 limit 看看 API 是否支持。
            probe_limit = None
            if (
                not self._ccxt_limit_probe_done
                and isinstance(self._ex_id, str)
                and "bitget" in self._ex_id.lower()
                and tf_norm == "1m"
            ):
                probe_limit = 1000
            use_limit = probe_limit or limit
            page = await self._ccxt_fetch_ohlcv_once(
                symbol, since, use_limit, end_exclusive_ms=end_excl, tf=tf_norm
            )
            if not page:
                break
            arr = self._normalize_ccxt_ohlcv(page)
            if arr.size == 0:
                break
            if probe_limit is not None and not self._ccxt_limit_probe_done:
                # 如果 Bitget 返回 >200 行，可以安全地使用 1000。
                if arr.shape[0] > 200:
                    self._ccxt_limit_default = 1000
                    limit = 1000
                    self._log(
                        "debug",
                        "bitget_ohlcv_limit_probe",
                        symbol=symbol,
                        tf=tf_norm,
                        supported_limit=1000,
                        rows=int(arr.shape[0]),
                    )
                else:
                    self._ccxt_limit_default = 200
                    limit = 200
                    self._log(
                        "debug",
                        "bitget_ohlcv_limit_probe",
                        symbol=symbol,
                        tf=tf_norm,
                        supported_limit=200,
                        rows=int(arr.shape[0]),
                    )
                self._ccxt_limit_probe_done = True
            # 排除任何 >= end_exclusive 的 K 线
            arr = arr[arr["ts"] < end_excl]
            if arr.size == 0:
                break
            # 诊断：页时间戳范围和步长
            try:
                first_ts = int(arr[0]["ts"])  # type: ignore[index]
                last_ts = int(arr[-1]["ts"])  # type: ignore[index]
                if arr.shape[0] > 1:
                    diffs = np.diff(arr["ts"].astype(np.int64))
                    max_step = int(diffs.max())
                    min_step = int(diffs.min())
                    # 期望步长与请求的时间周期匹配
                    # 以 DEBUG 级别记录 - 非预期步长在流动性不足的交易所上很常见，无需操作
                    if max_step != period_ms or min_step != period_ms:
                        warn_key = (self._ex_id, symbol, tf_norm)
                        if warn_key not in self._step_warning_keys:
                            self._step_warning_keys.add(warn_key)
                            self.log.debug(
                                f"[candle] unexpected step for tf exchange={self._ex_id} symbol={symbol} tf={tf_norm} expected={period_ms} min_step={min_step} max_step={max_step}"
                            )
                else:
                    max_step = ONE_MIN_MS
            except Exception:
                first_ts = last_ts = 0
            # 将负载内和页面间的缺口记录为已验证的无交易缺口（交易所提供）。
            if self._record_payload_gaps_as_known and tf_norm == "1m":
                try:
                    ts_arr = arr["ts"].astype(np.int64)
                    if ts_arr.size > 1:
                        diffs = np.diff(ts_arr)
                        gap_idxs = np.where(diffs > period_ms)[0]
                        for i in gap_idxs:
                            gap_start = int(ts_arr[i] + period_ms)
                            gap_end = int(ts_arr[i + 1] - period_ms)
                            self._record_verified_gap(symbol, gap_start, gap_end)
                    if prev_last_ts is not None and first_ts > prev_last_ts + period_ms:
                        gap_start = int(prev_last_ts + period_ms)
                        gap_end = int(first_ts - period_ms)
                        self._record_verified_gap(symbol, gap_start, gap_end)
                except Exception:
                    pass

            all_rows.append(arr)
            pages += 1
            if self._page_debug_all or symbol in self._page_debug_symbols:
                self._log(
                    "info",
                    "ccxt_page_range",
                    symbol=symbol,
                    tf=tf_norm,
                    page=pages,
                    rows=int(arr.shape[0]),
                    first_ts=first_ts,
                    last_ts=last_ts,
                    since_ts=int(since),
                    end_exclusive_ts=int(end_excl),
                )
            if on_batch is not None:
                try:
                    on_batch(arr)
                except Exception as on_batch_err:
                    self.log.error(
                        "on_batch callback failed; stopping pagination",
                        extra={
                            "symbol": symbol,
                            "timeframe": tf_norm,
                            "error": str(on_batch_err),
                        },
                    )
                    break
            last_ts = int(arr[-1]["ts"])  # inclusive last
            # 长时间分页获取的节流进度日志（INFO）
            try:
                progressed = max(
                    0, min(100.0, 100.0 * float(last_ts - since_start) / float(total_span))
                )
            except Exception:
                progressed = 0.0
            self._progress_log(
                (symbol, tf_norm, "ccxt"),
                "ccxt_fetch_progress",
                symbol=symbol,
                tf=tf_norm,
                pages=pages,
                rows=sum(int(a.shape[0]) for a in all_rows) if all_rows else 0,
                since_ts=since_start,
                end_exclusive_ts=end_excl,
                last_ts=last_ts,
                progress_pct=f"{progressed:.1f}",
            )
            new_since = last_ts + period_ms
            if self._ccxt_page_overlap_candles > 0:
                overlap_ms = period_ms * int(self._ccxt_page_overlap_candles)
                new_since = max(last_ts - overlap_ms, since + period_ms)
            # 安全保护：避免交易所在返回重叠数据时无限循环
            if new_since <= since:
                self.log.debug(
                    f"pagination stop (no progress) exchange={self._ex_id} symbol={symbol} since={since} last_ts={last_ts}"
                )
                break
            since = new_since
            prev_last_ts = last_ts
        self.log.debug(
            f"paginated fetch done exchange={self._ex_id} symbol={symbol} tf={tf_norm} rows={sum(a.shape[0] for a in all_rows) if all_rows else 0}"
        )
        if not all_rows:
            return np.empty((0,), dtype=CANDLE_DTYPE)
        return np.sort(np.concatenate(all_rows), order="ts")

    # ----- 测试所需的公共辅助方法 -----

    def standardize_gaps(
        self,
        candles: np.ndarray,
        *,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        strict: bool = False,
        fill_leading_gaps: bool = False,
        assume_sorted: bool = False,
        symbol: Optional[str] = None,
    ) -> np.ndarray:
        """返回为缺失分钟合成零成交 K 线后的新数组。

        参数
        ----------
        candles : np.ndarray
            dtype 为 CANDLE_DTYPE 的结构化数组。必须按 `ts` 排序。
        start_ts : int, 可选
            包含的开始时间戳 (ms)。如果为 None，从第一根 K 线推断。
        end_ts : int, 可选
            包含的结束时间戳 (ms)。如果为 None，从最后一根 K 线推断。
        strict : bool
            如果为 True，当缺口存在且没有之前的 K 线可用于生成零成交 K 线时抛出异常。
        fill_leading_gaps : bool
            如果为 False（默认），不在第一个真实数据点之前合成 K 线。
            这防止在 start_ts 处数据不存在时生成虚假平坦数据。
            如果为 True，从第一个可用 K 线前向填充头部缺口。
        assume_sorted : bool
            如果为 True，跳过排序（调用者保证数组已按 ts 排序）。
        """
        a = _ensure_dtype(candles)
        if a.size == 0:
            # 无需标准化；由调用者决定如何处理空范围
            return a

        if not assume_sorted:
            # 检查是否已排序以跳过 O(n log n) 排序
            ts_check = a["ts"]
            if ts_check.size > 1 and not np.all(ts_check[:-1] <= ts_check[1:]):
                a = np.sort(a, order="ts")
        ts_arr = _ts_index(a)

        # 确定有效边界
        first_real_ts = int(ts_arr[0])
        last_real_ts = int(ts_arr[-1])

        lo = start_ts if start_ts is not None else first_real_ts
        hi = end_ts if end_ts is not None else last_real_ts
        lo = _floor_minute(lo)
        hi = _floor_minute(hi)

        # 如果不填充头部缺口，不从实际数据之前开始
        effective_lo = lo
        if not fill_leading_gaps and first_real_ts > lo:
            leading_gap_minutes = (first_real_ts - lo) // ONE_MIN_MS
            if leading_gap_minutes > 0:
                self._log(
                    "debug",
                    "standardize_gaps_skipping_leading",
                    requested_start_ts=lo,
                    actual_start_ts=first_real_ts,
                    skipped_minutes=int(leading_gap_minutes),
                )
            effective_lo = _floor_minute(first_real_ts)

        expected = np.arange(effective_lo, hi + ONE_MIN_MS, ONE_MIN_MS, dtype=np.int64)
        # 从 ts 到 a 中行索引的映射
        pos = {int(t): i for i, t in enumerate(ts_arr)}

        if strict:
            # 严格模式：不合成零成交 K 线。
            # 如果存在缺口，记录警告并返回范围内的全部真实 K 线。
            i0 = int(np.searchsorted(ts_arr, effective_lo, side="left"))
            i1 = int(np.searchsorted(ts_arr, hi, side="right"))
            missing_count = 0
            try:
                expected_len = int((hi - effective_lo) // ONE_MIN_MS) + 1
                slice_ts = ts_arr[i0:i1].astype(np.int64, copy=False)
                if slice_ts.size:
                    # 头部 + 尾部 + 内部缺失（如果不填充则不算头部缺口）
                    if fill_leading_gaps:
                        missing_count += int((int(slice_ts[0]) - effective_lo) // ONE_MIN_MS)
                    missing_count += int((hi - int(slice_ts[-1])) // ONE_MIN_MS)
                    if slice_ts.size > 1:
                        diffs = np.diff(slice_ts)
                        gaps = diffs[diffs > ONE_MIN_MS]
                        if gaps.size:
                            missing_count += int(np.sum((gaps // ONE_MIN_MS) - 1))
                    # 如果存在重复，也将其视为缺失覆盖
                    missing_count += int(
                        max(0, expected_len - int(np.unique(slice_ts).size) - missing_count)
                    )
                else:
                    missing_count = expected_len
            except Exception:
                # 回退：保持行为安全（宁可不警告也不要抛出异常）
                missing_count = 0
            if missing_count:
                # 累积用于摘要记录而非逐事件警告
                sym_key = symbol or "unknown"
                self._record_strict_gap(sym_key, int(missing_count))
                self._log_strict_gaps_summary()
            return a[i0:i1]

        out_rows = []
        prev_close: Optional[float] = None

        # 初始化 prev_close 来源：
        # 1) 恰好在 effective_lo 处的 K 线，否则
        # 2) effective_lo 之前最后一根 K 线（从更早数据前向填充），否则
        # 3) 如果 fill_leading_gaps=True，使用第一个可用 K 线（对头部缺口回填）
        if effective_lo in pos:
            prev_close = float(a[pos[effective_lo]]["c"])
        else:
            idx = int(np.searchsorted(ts_arr, effective_lo))
            if idx > 0:
                # effective_lo 之前有 K 线 - 用它做前向填充
                prev_close = float(a[idx - 1]["c"])
            elif fill_leading_gaps and a.size > 0:
                # effective_lo 之前没有 K 线，但 fill_leading_gaps=True
                # 使用第一根 K 线的收盘价回填头部缺口
                prev_close = float(a[0]["c"])
            # 如果之前没有 K 线，prev_close 保持 None 直到遇到真实数据

        synthesized_count = 0
        synthesized_timestamps: List[int] = []
        for t in expected:
            if t in pos:
                row = a[pos[t]]
                out_rows.append(tuple(row.tolist()))
                prev_close = float(row["c"])  # update seed
            else:
                if prev_close is None:
                    # 没有之前的数据可用于前向填充 - 跳过该时间戳
                    continue
                # 使用前一根收盘价合成零成交 K 线（仅内部缺口）
                out_rows.append((int(t), prev_close, prev_close, prev_close, prev_close, 0.0))
                synthesized_timestamps.append(int(t))
                synthesized_count += 1

        # 跟踪合成时间戳用于 EMA 重计算检测
        if symbol and synthesized_timestamps:
            self._track_synthetic_timestamps(symbol, synthesized_timestamps)

        # 当合成了零成交 K 线时记录日志（节流或批处理）
        if synthesized_count > 0 and symbol:
            # 批次模式下，收集以供后续汇总记录
            if self._synth_candle_batch_mode:
                try:
                    first_ts = min(synthesized_timestamps)
                    last_ts = max(synthesized_timestamps)
                except Exception:
                    first_ts = None
                    last_ts = None
                meta = self._synth_candle_batch.get(symbol)
                if not isinstance(meta, dict):
                    meta = {"count": 0, "min_ts": None, "max_ts": None}
                meta["count"] = int(meta.get("count", 0)) + int(synthesized_count)
                if first_ts is not None:
                    try:
                        meta["min_ts"] = (
                            int(first_ts)
                            if meta.get("min_ts") is None
                            else min(int(meta["min_ts"]), int(first_ts))
                        )
                    except Exception:
                        meta["min_ts"] = int(first_ts)
                if last_ts is not None:
                    try:
                        meta["max_ts"] = (
                            int(last_ts)
                            if meta.get("max_ts") is None
                            else max(int(meta["max_ts"]), int(last_ts))
                        )
                    except Exception:
                        meta["max_ts"] = int(last_ts)
                self._synth_candle_batch[symbol] = meta
            else:
                # 正常模式：按缺口起始去重（每个唯一缺口来源仅警告一次）
                # 将 first_ts 取整到最近的小时，减少在不同获取窗口以略微不同的
                # 边界检测到同一底层缺口时的重复警告
                first_ts = min(synthesized_timestamps)
                last_ts = max(synthesized_timestamps)
                hour_ms = 3600_000
                first_ts_hour = (first_ts // hour_ms) * hour_ms  # 向下取整到小时边界
                gap_key = (symbol, first_ts_hour)

                # 如果已对该小时窗口的缺口警告过则跳过
                if gap_key in self._synth_gap_warned:
                    pass  # Already warned, skip
                else:
                    self._synth_gap_warned.add(gap_key)
                    # 格式化时间戳范围以便人类阅读
                    from datetime import datetime, timezone

                    first_dt = datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M"
                    )
                    if synthesized_count == 1:
                        ts_info = first_dt
                    else:
                        last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).strftime(
                            "%Y-%m-%dT%H:%M"
                        )
                        ts_info = f"{first_dt} to {last_dt}"
                    # 对单个缺口警告使用 DEBUG - 启动时的批次摘要以 WARNING 级别已足够
                    # 仅在真正异常情况下使用 WARNING（实盘运行时缺口 > 1000 根 K 线）
                    log_fn = self.log.warning if synthesized_count > 1000 else self.log.debug
                    log_fn(
                        "[candle] %s: synthesized %d zero-candle%s at %s (no data for requested minutes) using prev_close=%.6f",
                        symbol,
                        synthesized_count,
                        "s" if synthesized_count > 1 else "",
                        ts_info,
                        prev_close if prev_close is not None else 0.0,
                    )

        if not out_rows:
            return np.empty((0,), dtype=CANDLE_DTYPE)
        return np.array(out_rows, dtype=CANDLE_DTYPE)

    # ----- 外部归档（历史数据） -----

    def _archive_supported(self) -> bool:
        """检查该交易所是否支持并启用了归档获取。"""
        if not self.archive_enabled:
            return False
        try:
            exid = (self._ex_id or "").lower() if isinstance(self._ex_id, str) else ""
        except Exception:
            exid = ""
        # 注意：排除 Bybit - CCXT 更快且消耗更少带宽。
        # Bybit 的归档端点提供原始交易（而非分桶的 OHLCV），因此获取和
        # 分桶交易对 BTC 来说数据量大约多 700 倍。保留下面的归档获取逻辑
        # 作为参考/可选使用，但默认避免使用。
        return exid in {"binanceusdm", "bitget", "kucoinfutures", "hyperliquid"}

    @staticmethod
    def _archive_symbol_code(symbol: str) -> str:
        """返回 ccxt 风格交易对的归档交易对代码（通常为 BASEQUOTE）。"""
        symbol = str(symbol or "")
        if not symbol:
            return ""
        base = symbol
        quote = ""
        if "/" in symbol:
            base, rest = symbol.split("/", 1)
            quote = rest.split(":", 1)[0] if ":" in rest else rest
        elif ":" in symbol:
            base, quote = symbol.split(":", 1)
        # 尽力而为的回退
        base = (base or "").replace("/", "").replace(":", "")
        quote = (quote or "").replace("/", "").replace(":", "")
        return f"{base}{quote}" if quote else base

    async def _archive_fetch_day(self, symbol: str, day_key: str) -> Optional[np.ndarray]:
        """从外部归档获取完整一天的 (1440x1m) K 线数组。

        返回包含 UTC 全天时间戳的 CANDLE_DTYPE，如果不可用则返回 None。
        """
        try:
            exid = (self._ex_id or "").lower() if isinstance(self._ex_id, str) else ""
        except Exception:
            exid = ""
        if exid not in {"binanceusdm", "bybit", "bitget", "kucoinfutures", "hyperliquid"}:
            return None

        symbol_code = self._archive_symbol_code(symbol)
        if not symbol_code:
            return None

        if exid == "kucoinfutures":
            symbol_code = f"{symbol_code}M"

        if exid == "binanceusdm":
            url = (
                "https://data.binance.vision/data/futures/um/"
                f"daily/klines/{symbol_code}/1m/{symbol_code}-1m-{day_key}.zip"
            )
            return await self._archive_fetch_binance_zip(url, day_key)

        if exid == "bybit":
            # 注意：Bybit 归档提供原始交易而非分桶的 OHLCV。
            # 由于带宽消耗，_archive_supported() 默认禁用。
            url = f"https://public.bybit.com/trading/{symbol_code}/{symbol_code}{day_key}.csv.gz"
            return await self._archive_fetch_bybit_trades(url, day_key)

        if exid == "bitget":
            # Bitget 归档布局因日期而异；遵循现有逻辑。
            day_comp = day_key
            day_yymmdd = day_key.replace("-", "")
            if day_comp <= "2024-04-18":
                url = (
                    "https://img.bitgetimg.com/online/kline/"
                    f"{symbol_code}/{symbol_code}_UMCBL_1min_{day_yymmdd}.zip"
                )
            else:
                url = f"https://img.bitgetimg.com/online/kline/{symbol_code}/UMCBL/{day_yymmdd}.zip"
            return await self._archive_fetch_bitget_zip(url, day_key)

        if exid == "kucoinfutures":
            url = (
                "https://historical-data.kucoin.com/data/futures/daily/klines/"
                f"{symbol_code}/1m/{symbol_code}-1m-{day_key}.zip"
            )
            return await self._archive_fetch_kucoin_zip(url, day_key)

        if exid == "hyperliquid":
            return await self._archive_fetch_hyperliquid(symbol, day_key)

        return None

    async def _get_http_session(self) -> "aiohttp.ClientSession":
        """获取或创建归档获取的持久 HTTP 会话。"""
        import aiohttp

        async with self._http_session_lock:
            if self._http_session is None or self._http_session.closed:
                # 归档主机可能很慢，归档文件可能很大；使用宽松的超时设置。
                # 保持连接超时有界，但允许更多读取时间。
                timeout = aiohttp.ClientTimeout(total=120, connect=20, sock_read=60)
                connector = aiohttp.TCPConnector(
                    # 保持并发适度以避免超时。
                    limit=20,
                    limit_per_host=6,
                    ttl_dns_cache=300,  # DNS 缓存 TTL（秒）
                    enable_cleanup_closed=True,
                )
                self._http_session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                )
            return self._http_session

    async def _close_http_session(self) -> None:
        """如果 HTTP 会话已打开则关闭。"""
        async with self._http_session_lock:
            if self._http_session is not None and not self._http_session.closed:
                await self._http_session.close()
                self._http_session = None

    async def _archive_fetch_bytes(self, url: str) -> Optional[bytes]:
        t0 = time.monotonic()
        self._emit_remote_fetch(
            {
                "kind": "archive_http_get",
                "stage": "start",
                "exchange": str(self._ex_id),
                "url": str(url),
            }
        )
        self._log("debug", "archive_http_get", url=url)

        session = await self._get_http_session()
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    self._emit_remote_fetch(
                        {
                            "kind": "archive_http_get",
                            "stage": "not_found",
                            "exchange": str(self._ex_id),
                            "url": str(url),
                            "status": 404,
                            "elapsed_ms": int((time.monotonic() - t0) * 1000),
                        }
                    )
                    self._log(
                        "debug",
                        "archive_http_404",
                        url=url,
                        elapsed_ms=int((time.monotonic() - t0) * 1000),
                    )
                    return None
                resp.raise_for_status()
                data = await resp.read()
        except Exception as e:
            err_type = type(e).__name__
            err_repr = repr(e)
            self._emit_remote_fetch(
                {
                    "kind": "archive_http_get",
                    "stage": "error",
                    "exchange": str(self._ex_id),
                    "url": str(url),
                    "error_type": err_type,
                    "error": str(e),
                    "error_repr": err_repr,
                    "elapsed_ms": int((time.monotonic() - t0) * 1000),
                }
            )
            self._log(
                "debug",
                "archive_http_error",
                url=url,
                error_type=err_type,
                error=str(e),
                error_repr=err_repr,
            )
            raise

        self._emit_remote_fetch(
            {
                "kind": "archive_http_get",
                "stage": "ok",
                "exchange": str(self._ex_id),
                "url": str(url),
                "bytes": int(len(data)),
                "elapsed_ms": int((time.monotonic() - t0) * 1000),
            }
        )
        self._log(
            "debug",
            "archive_http_ok",
            url=url,
            bytes=len(data),
            elapsed_ms=int((time.monotonic() - t0) * 1000),
        )
        return data

    async def _archive_fetch_binance_zip(self, url: str, day_key: str) -> Optional[np.ndarray]:
        raw = await self._archive_fetch_bytes(url)
        if raw is None:
            return None
        import zipfile
        from io import BytesIO
        import pandas as pd

        col_names = ["timestamp", "open", "high", "low", "close", "volume"]
        with zipfile.ZipFile(BytesIO(raw), "r") as z:
            dfs = []
            for name in z.namelist():
                with z.open(name) as f:
                    df = pd.read_csv(f, header=None)
                df.columns = col_names + [
                    f"extra_{i}" for i in range(len(df.columns) - len(col_names))
                ]
                dfs.append(df[col_names])
        if not dfs:
            return None
        dfc = pd.concat(dfs).sort_values("timestamp").reset_index(drop=True)
        dfc = dfc[dfc.timestamp != "open_time"]
        for c in col_names:
            dfc[c] = pd.to_numeric(dfc[c], errors="coerce")
        dfc = dfc.dropna(subset=["timestamp"]).reset_index(drop=True)
        start_ts, end_ts = self._date_range_of_key(day_key)
        # Binance 时间戳应已是毫秒。
        dfc = dfc[(dfc["timestamp"] >= start_ts) & (dfc["timestamp"] <= end_ts)]
        if dfc.empty:
            return None
        return self._ohlcv_df_to_day_arr(dfc, day_key)

    async def _archive_fetch_bitget_zip(self, url: str, day_key: str) -> Optional[np.ndarray]:
        raw = await self._archive_fetch_bytes(url)
        if raw is None:
            return None
        import zipfile
        from io import BytesIO
        import pandas as pd

        col_names = ["timestamp", "open", "high", "low", "close", "volume"]
        with zipfile.ZipFile(BytesIO(raw), "r") as z:
            dfs = []
            for name in z.namelist():
                with z.open(name) as f:
                    # Bitget 提供 xlsx 风格的工作表；pandas 可以从字节流读取 excel。
                    df = pd.read_excel(f)
                df.columns = col_names + [
                    f"extra_{i}" for i in range(len(df.columns) - len(col_names))
                ]
                dfs.append(df[col_names])
        if not dfs:
            return None
        dfc = pd.concat(dfs).sort_values("timestamp").reset_index(drop=True)
        for c in col_names:
            dfc[c] = pd.to_numeric(dfc[c], errors="coerce")
        dfc = dfc.dropna(subset=["timestamp"]).reset_index(drop=True)
        start_ts, end_ts = self._date_range_of_key(day_key)
        # Bitget 时间戳有时以秒为单位。
        ts = dfc["timestamp"].astype("float64").values
        if np.isfinite(ts).any() and float(np.nanmax(np.abs(ts))) < 1e11:
            dfc["timestamp"] = dfc["timestamp"] * 1000.0
        dfc = dfc[(dfc["timestamp"] >= start_ts) & (dfc["timestamp"] <= end_ts)]
        if dfc.empty:
            return None
        return self._ohlcv_df_to_day_arr(dfc, day_key)

    async def _archive_fetch_kucoin_zip(self, url: str, day_key: str) -> Optional[np.ndarray]:
        raw = await self._archive_fetch_bytes(url)
        if raw is None:
            return None
        import zipfile
        from io import BytesIO
        import pandas as pd

        required = ["timestamp", "open", "high", "low", "close", "volume"]
        with zipfile.ZipFile(BytesIO(raw), "r") as z:
            dfs = []
            for name in z.namelist():
                with z.open(name) as f:
                    df = pd.read_csv(f)
                df.columns = [str(c).strip().lower() for c in df.columns]
                if "time" in df.columns and "timestamp" not in df.columns:
                    df = df.rename(columns={"time": "timestamp"})
                missing = [c for c in required if c not in df.columns]
                if missing:
                    raise ValueError(f"kucoin archive missing columns {missing} in {url}")
                dfs.append(df[required])
        if not dfs:
            return None
        dfc = pd.concat(dfs, ignore_index=True)
        for c in required:
            dfc[c] = pd.to_numeric(dfc[c], errors="coerce")
        dfc = dfc.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        # KuCoin 时间戳通常以秒为单位。
        ts = dfc["timestamp"].astype("float64").values
        if np.isfinite(ts).any() and float(np.nanmax(np.abs(ts))) < 1e11:
            dfc["timestamp"] = dfc["timestamp"] * 1000.0
        start_ts, end_ts = self._date_range_of_key(day_key)
        dfc = dfc[(dfc["timestamp"] >= start_ts) & (dfc["timestamp"] <= end_ts)]
        if dfc.empty:
            return None
        return self._ohlcv_df_to_day_arr(dfc, day_key)

    async def _archive_fetch_bybit_trades(self, url: str, day_key: str) -> Optional[np.ndarray]:
        raw = await self._archive_fetch_bytes(url)
        if raw is None:
            return None
        import gzip
        from io import BytesIO
        import pandas as pd

        with gzip.open(BytesIO(raw)) as f:
            trades = pd.read_csv(f)
        if "timestamp" not in trades.columns or "price" not in trades.columns:
            return None
        # Bybit 归档时间戳以秒为单位（交易时间）。
        ts_sec = pd.to_numeric(trades["timestamp"], errors="coerce").astype("float64")
        price = pd.to_numeric(trades["price"], errors="coerce").astype("float64")
        size = pd.to_numeric(trades.get("size", 0.0), errors="coerce").astype("float64")
        trades = pd.DataFrame({"timestamp": ts_sec, "price": price, "size": size}).dropna(
            subset=["timestamp", "price"]
        )
        if trades.empty:
            return None
        interval = 60_000
        minute_ts = (trades["timestamp"] * 1000.0) // interval * interval
        groups = trades.groupby(minute_ts)
        ohlcvs = pd.DataFrame(
            {
                "open": groups["price"].first(),
                "high": groups["price"].max(),
                "low": groups["price"].min(),
                "close": groups["price"].last(),
                "volume": groups["size"].sum(),
            }
        )
        ohlcvs["timestamp"] = ohlcvs.index.astype("int64")
        ohlcvs = ohlcvs.reset_index(drop=True)
        start_ts, end_ts = self._date_range_of_key(day_key)
        ohlcvs = ohlcvs[(ohlcvs["timestamp"] >= start_ts) & (ohlcvs["timestamp"] <= end_ts)]
        if ohlcvs.empty:
            return None
        return self._ohlcv_df_to_day_arr(ohlcvs, day_key)

    async def _archive_fetch_hyperliquid(self, symbol: str, day_key: str) -> Optional[np.ndarray]:
        """获取 Hyperliquid 归档数据用于回测。

        按顺序尝试的数据源：
        1. 本地预处理缓存 (caches/ohlcv/hyperliquid/{coin}/{day_key}.parquet)
        2. 对于股票永续合约：TradFi API（如果 api-keys.json 中有凭据）

        对于 Hyperliquid 的 S3 原始交易数据，用户可使用以下命令预处理：
            python -m src.tools.hyperliquid_s3_fetcher --start YYYY-MM-DD --end YYYY-MM-DD
            python -m src.tools.trades_to_ohlcv --input caches/hyperliquid_trades --output caches/ohlcv/hyperliquid

        参数:
            symbol: CCXT 风格交易对（如 "BTC/USDC:USDC" 或 "xyz:TSLA/USDC:USDC"）
            day_key: 日期字符串 (YYYY-MM-DD)

        返回:
            包含 1440 根 K 线的 CANDLE_DTYPE 数组，如果不可用则返回 None
        """
        import pandas as pd
        from pathlib import Path

        # 从交易对推导币种名用于缓存路径
        base = symbol.split("/")[0] if "/" in symbol else symbol
        # 处理路径中的 xyz: 前缀（将 : 替换为 _ 以兼容文件系统）
        safe_coin = base.replace(":", "_")

        # 1. 首先检查本地预处理缓存
        cache_path = Path("caches/ohlcv/hyperliquid") / safe_coin / f"{day_key}.parquet"

        if cache_path.exists():
            try:
                import pyarrow.parquet as pq

                table = pq.read_table(cache_path)
                df = table.to_pandas()

                # 重命名列以匹配预期格式
                col_map = {
                    "ts": "timestamp",
                    "o": "open",
                    "h": "high",
                    "l": "low",
                    "c": "close",
                    "bv": "volume",
                }
                df = df.rename(columns=col_map)

                self._log(
                    "debug",
                    "hyperliquid_archive_hit",
                    symbol=symbol,
                    day_key=day_key,
                    path=str(cache_path),
                )
                return self._ohlcv_df_to_day_arr(df, day_key)
            except Exception as e:
                self._log(
                    "debug", "hyperliquid_archive_error", symbol=symbol, day_key=day_key, error=str(e)
                )

        # 2. 对于股票永续合约，尝试 TradFi 数据获取
        try:
            from tradfi_data import is_stock_ticker, hip3_to_tradfi_symbol
        except ImportError:
            self._log("debug", "hyperliquid_archive_miss", symbol=symbol, day_key=day_key)
            return None

        if is_stock_ticker(base):
            arr = await self._fetch_tradfi_day(base, day_key, cache_path)
            if arr is not None and arr.size > 0:
                return arr

        self._log("debug", "hyperliquid_archive_miss", symbol=symbol, day_key=day_key)
        return None

    async def _fetch_tradfi_day(
        self, coin: str, day_key: str, cache_path: "Path"
    ) -> Optional[np.ndarray]:
        """从 TradFi API 获取股票数据并缓存。

        参数:
            coin: 股票代码（如 "TSLA"、"xyz:TSLA"）
            day_key: 日期字符串 (YYYY-MM-DD)
            cache_path: 缓存数据保存路径

        返回:
            CANDLE_DTYPE 数组或 None
        """
        from pathlib import Path

        try:
            from tradfi_data import (
                get_provider,
                hip3_to_tradfi_symbol,
                TradFiDataFetcher,
            )
        except ImportError:
            return None

        # 从 api-keys.json 加载 TradFi 凭据
        # 默认使用 yfinance（免费，无需 API 密钥）
        tradfi_config = self._load_tradfi_config()
        if tradfi_config:
            provider_name = tradfi_config.get("provider", "yfinance")
            api_key = tradfi_config.get("api_key")
            api_secret = tradfi_config.get("api_secret")  # 用于 Alpaca
        else:
            # 使用 yfinance 作为免费默认
            provider_name = "yfinance"
            api_key = None
            api_secret = None

        try:
            provider = get_provider(provider_name, api_key=api_key, api_secret=api_secret)
            ticker = hip3_to_tradfi_symbol(coin)

            self._log("info", "tradfi_fetch", ticker=ticker, day_key=day_key, provider=provider_name)

            async with TradFiDataFetcher(provider) as fetcher:
                # 为获取器构建 HIP-3 交易对
                hip3_symbol = f"xyz:{ticker}/USDC:USDC" if not coin.startswith("xyz:") else coin
                arr = await fetcher.fetch_day(hip3_symbol, day_key)

            if arr is not None and arr.size > 0:
                # 缓存结果以供后续使用
                self._save_tradfi_cache(arr, cache_path)
                # 转换为日数组格式
                import pandas as pd

                df = pd.DataFrame(
                    {
                        "timestamp": arr["ts"],
                        "open": arr["o"],
                        "high": arr["h"],
                        "low": arr["l"],
                        "close": arr["c"],
                        "volume": arr["bv"],
                    }
                )
                return self._ohlcv_df_to_day_arr(df, day_key)

        except Exception as e:
            self._log("debug", "tradfi_fetch_error", coin=coin, day_key=day_key, error=str(e))

        return None

    def _load_tradfi_config(self) -> Optional[dict]:
        """从 api-keys.json 加载 TradFi API 配置。"""
        from pathlib import Path
        import json

        api_keys_path = Path("api-keys.json")
        if not api_keys_path.exists():
            return None

        try:
            with open(api_keys_path) as f:
                api_keys = json.load(f)
            return api_keys.get("tradfi")
        except Exception:
            return None

    def _save_tradfi_cache(self, arr: np.ndarray, cache_path: "Path") -> None:
        """将 TradFi 数据保存到本地缓存。"""
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            cache_path.parent.mkdir(parents=True, exist_ok=True)

            table = pa.table(
                {
                    "ts": pa.array(arr["ts"].astype("int64")),
                    "o": pa.array(arr["o"].astype("float32")),
                    "h": pa.array(arr["h"].astype("float32")),
                    "l": pa.array(arr["l"].astype("float32")),
                    "c": pa.array(arr["c"].astype("float32")),
                    "bv": pa.array(arr["bv"].astype("float32")),
                }
            )
            pq.write_table(table, cache_path, compression="zstd")
            self._log("debug", "tradfi_cache_saved", path=str(cache_path))
        except Exception as e:
            self._log("debug", "tradfi_cache_save_error", error=str(e))

    def _ohlcv_df_to_day_arr(self, df, day_key: str) -> np.ndarray:
        """将包含 timestamp/open/high/low/close/volume 的 DataFrame 转换为 1m 日数组。"""
        start_ts, end_ts = self._date_range_of_key(day_key)
        cols = ["timestamp", "open", "high", "low", "close", "volume"]
        for c in cols:
            df[c] = df[c].astype("float64")
        df = (
            df.dropna(subset=["timestamp", "close"])
            .sort_values("timestamp")
            .drop_duplicates(subset=["timestamp"], keep="last")
        )
        # 转换为 CANDLE_DTYPE 然后标准化为全天网格。
        arr = np.empty((df.shape[0],), dtype=CANDLE_DTYPE)
        arr["ts"] = df["timestamp"].astype("int64").values
        arr["o"] = df["open"].values
        arr["h"] = df["high"].values
        arr["l"] = df["low"].values
        arr["c"] = df["close"].values
        arr["bv"] = df["volume"].values
        arr = arr[(arr["ts"] >= start_ts) & (arr["ts"] <= end_ts)]
        if arr.size == 0:
            return np.empty((0,), dtype=CANDLE_DTYPE)
        # 对于归档日数据，期望全天覆盖 - 使用 fill_leading_gaps=True
        out = self.standardize_gaps(
            arr, start_ts=start_ts, end_ts=end_ts, strict=False, fill_leading_gaps=True
        )
        # 验证全天覆盖（尽力而为；调用者可回退到 ccxt）。
        if out.size != 1440 or int(out[0]["ts"]) != start_ts or int(out[-1]["ts"]) != end_ts:
            return np.empty((0,), dtype=CANDLE_DTYPE)
        return out

    async def _prefetch_archives_for_range(
        self, symbol: str, start_ts: int, end_ts: int, *, parallel_days: int = 5
    ) -> None:
        """尝试使用外部归档物化缺失的完整日分片。

        参数:
            symbol: 要获取归档的交易对
            start_ts: 开始时间戳 (ms)
            end_ts: 结束时间戳 (ms)
            parallel_days: 并行获取的天数（默认 5）
        """
        if not self._archive_supported():
            return

        # 对于配置了 TradFi 的股票永续合约，允许在交易所启动前获取
        # 以便从 TradFi 提供商（alpaca/polygon 等）回填历史数据。
        allow_pre_inception_for_stock_perp = False
        try:
            from tradfi_data import is_stock_ticker

            base = symbol.split("/")[0].strip()
            tradfi_cfg = self._load_tradfi_config()
            allow_pre_inception_for_stock_perp = bool(tradfi_cfg) and is_stock_ticker(base)
        except Exception:
            allow_pre_inception_for_stock_perp = False

        # 仅当我们知道交易所可用历史的权威下界时才裁剪/跳过。
        # 最早的缓存分片是观测到的本地覆盖，不是交易所启动的证明，
        # 因此它不能阻止更早的回填。
        authoritative_start_ts = self._get_authoritative_start_ts(symbol)
        if (
            authoritative_start_ts is not None
            and start_ts < authoritative_start_ts
            and not allow_pre_inception_for_stock_perp
        ):
            if authoritative_start_ts > end_ts:
                pre_inception_end = min(authoritative_start_ts - ONE_MIN_MS, end_ts)
                if start_ts <= pre_inception_end:
                    self._add_known_gap(
                        symbol,
                        start_ts,
                        pre_inception_end,
                        reason="pre_inception",
                        retry_count=_GAP_MAX_RETRIES,
                    )
                    self._log(
                        "warning",
                        "skip_pre_inception_fetch",
                        symbol=symbol,
                        original_start=start_ts,
                        original_end=end_ts,
                        authoritative_start_ts=authoritative_start_ts,
                        uncovered_start=start_ts,
                        uncovered_end=pre_inception_end,
                    )
                return

            pre_inception_end = min(authoritative_start_ts - ONE_MIN_MS, end_ts)
            if start_ts <= pre_inception_end:
                self._add_known_gap(
                    symbol,
                    start_ts,
                    pre_inception_end,
                    reason="pre_inception",
                    retry_count=_GAP_MAX_RETRIES,
                )
                self._log(
                    "warning",
                    "skip_pre_inception_fetch",
                    symbol=symbol,
                    original_start=start_ts,
                    original_end=end_ts,
                    authoritative_start_ts=authoritative_start_ts,
                    uncovered_start=start_ts,
                    uncovered_end=pre_inception_end,
                )
            start_ts = authoritative_start_ts
            if start_ts > end_ts:
                return  # 没有剩余需要获取的内容

        day_map = self._date_keys_between(start_ts, end_ts)
        shard_paths = self._iter_shard_paths(symbol, tf="1m")
        legacy_paths = self._get_legacy_shard_paths(symbol, "1m")

        # 通过 index.json 确定主分片完整性（廉价；避免加载 npy 文件）。
        idx_shards: Dict[str, Dict[str, Any]] = {}
        try:
            idx = self._ensure_symbol_index(symbol, tf="1m")
            idx_shards = idx.get("shards") or {}
            if not isinstance(idx_shards, dict):
                idx_shards = {}
        except Exception:
            idx_shards = {}

        # 不要尝试获取最近几天的归档 - 它们还不存在
        # 交易所通常需要 48-72 小时才能发布归档数据
        archive_freshness_hours = 72
        archive_cutoff_ms = _utc_now_ms() - (archive_freshness_hours * 3600 * 1000)

        # 第一遍：计算需要获取的天数
        days_to_fetch = []
        skipped_reasons = {
            "partial_day_request": 0,
            "too_recent": 0,
            "legacy_present": 0,
            "primary_complete": 0,
            "verified_from_disk": 0,  # Files verified by loading (no index metadata)
        }
        for day_key, (day_start, day_end) in day_map.items():
            if start_ts > day_start or end_ts < day_end:
                skipped_reasons["partial_day_request"] += 1
                continue  # 不是该天的完整请求
            if day_end > archive_cutoff_ms:
                skipped_reasons["too_recent"] += 1
                continue  # 太近 - 归档尚不可用，使用 CCXT
            if day_key in legacy_paths:
                skipped_reasons["legacy_present"] += 1
                continue  # 旧版缓存已覆盖该天

            # 仅为主分片中缺失或不完整的天数获取归档。
            # 注意：之前我们跳过任何已有主分片路径的天。
            # 这可能阻止归档修复（如果先前 CCXT 运行写了部分/不完整的天数据）。
            if day_key in shard_paths:
                meta = idx_shards.get(day_key) if isinstance(idx_shards, dict) else None
                try:
                    if isinstance(meta, dict):
                        # 完整 UTC 日覆盖（包含两端点）
                        if (
                            int(meta.get("count") or -1) == 1440
                            and int(meta.get("min_ts") or 0) == int(day_start)
                            and int(meta.get("max_ts") or 0) == int(day_end)
                        ):
                            skipped_reasons["primary_complete"] += 1
                            continue
                    else:
                        # 无索引元数据但文件存在 - 通过加载分片验证
                        # 以避免冗余重新下载已完整的文件。
                        try:
                            arr = self._load_shard(shard_paths[day_key])
                            if (
                                len(arr) == 1440
                                and len(arr) > 0
                                and int(arr["ts"][0]) == int(day_start)
                                and int(arr["ts"][-1]) == int(day_end)
                            ):
                                # 文件完整 - 更新索引元数据并跳过
                                crc = int(zlib.crc32(arr.tobytes()) & 0xFFFFFFFF)
                                idx_shards[day_key] = {
                                    "path": shard_paths[day_key],
                                    "min_ts": int(arr["ts"][0]),
                                    "max_ts": int(arr["ts"][-1]),
                                    "count": int(len(arr)),
                                    "crc32": crc,
                                }
                                skipped_reasons["verified_from_disk"] += 1
                                continue
                        except Exception:
                            # 加载失败 - 继续重新下载
                            pass
                except Exception:
                    # 如果元数据缺失/损坏，视为不完整并允许归档获取。
                    pass

            days_to_fetch.append((day_key, day_start, day_end))

        # 如果从磁盘验证了任何文件，持久化索引更新
        if skipped_reasons["verified_from_disk"] > 0:
            try:
                idx["shards"] = idx_shards
                self._index[f"{symbol}::1m"] = idx
                self._save_index(symbol, tf="1m")
                self._log(
                    "debug",
                    "index_updated_from_disk_verification",
                    symbol=symbol,
                    shards_verified=skipped_reasons["verified_from_disk"],
                )
            except Exception:
                pass

        if not days_to_fetch:
            # 说明为何归档预取未运行（当存在大缺口但不符合
            # 全天归档物化条件时有参考价值）。
            try:
                self._emit_remote_fetch(
                    {
                        "kind": "archive_prefetch",
                        "stage": "skip",
                        "exchange": str(self._ex_id),
                        "symbol": symbol,
                        "reasons": dict(skipped_reasons),
                    }
                )
            except Exception:
                pass
            return

        total_days = len(days_to_fetch)
        completed = 0
        skipped = 0
        start_time = time.monotonic()

        # Log start of archive prefetch
        self._log(
            "info",
            "archive_prefetch_start",
            symbol=symbol,
            days_to_fetch=total_days,
            parallel=parallel_days,
            date_range=f"{days_to_fetch[0][0]}..{days_to_fetch[-1][0]}",
        )
        self._emit_remote_fetch(
            {
                "kind": "archive_prefetch",
                "stage": "start",
                "exchange": str(self._ex_id),
                "symbol": symbol,
                "days_to_fetch": int(total_days),
                "parallel": int(parallel_days),
                "date_range": f"{days_to_fetch[0][0]}..{days_to_fetch[-1][0]}",
            }
        )

        last_progress_emit = 0.0

        # 限制并发获取的信号量
        sem = asyncio.Semaphore(max(1, parallel_days))

        def _format_archive_exc(exc: BaseException) -> Tuple[str, str]:
            """返回用于日志记录的 (error_type, error_repr)。"""
            try:
                return (type(exc).__name__, repr(exc))
            except Exception:
                return (type(exc).__name__, "<unrepresentable exception>")

        async def fetch_single_day(
            day_info: Tuple[str, int, int],
        ) -> Tuple[str, Optional[np.ndarray], Optional[Tuple[str, str]]]:
            """获取单天的归档数据。返回 (day_key, array 或 None, (err_type, err_repr) 或 None)。"""
            day_key, day_start, day_end = day_info
            async with sem:
                try:
                    self._log("debug", "archive_day_attempt", symbol=symbol, day=day_key)
                    arr = await self._archive_fetch_day(symbol, day_key)
                    return (day_key, arr, None)
                except Exception as e:
                    return (day_key, None, _format_archive_exc(e))

        try:
            # 按信号量限制批量处理以避免任务排队
            batch_size = max(1, parallel_days)  # Match semaphore for optimal throughput

            for batch_start in range(0, total_days, batch_size):
                batch = days_to_fetch[batch_start : batch_start + batch_size]
                batch_start_time = time.monotonic()

                # 节流进度日志（约每 10 秒）
                self._progress_log(
                    (symbol, "1m", "archive"),
                    "archive_prefetch_progress",
                    symbol=symbol,
                    progress=f"{completed}/{total_days}",
                    pct=int(100 * completed / total_days) if total_days > 0 else 0,
                    batch=f"{batch[0][0]}..{batch[-1][0]}",
                    elapsed_s=round(time.monotonic() - start_time, 1),
                )
                try:
                    now = time.monotonic()
                    if (now - last_progress_emit) >= float(
                        self._progress_log_interval_seconds or 0.0
                    ):
                        last_progress_emit = now
                        self._emit_remote_fetch(
                            {
                                "kind": "archive_prefetch",
                                "stage": "progress",
                                "exchange": str(self._ex_id),
                                "symbol": symbol,
                                "completed": int(completed),
                                "total": int(total_days),
                                "pct": int(100 * completed / total_days) if total_days > 0 else 0,
                                "batch": f"{batch[0][0]}..{batch[-1][0]}",
                                "elapsed_s": round(time.monotonic() - start_time, 1),
                            }
                        )
                except Exception:
                    pass

                # 并行获取批次
                tasks = [fetch_single_day(d) for d in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # 处理结果并持久化（带延迟索引写入）
                batch_had_saves = False
                for i, result in enumerate(results):
                    day_key = batch[i][0]
                    if isinstance(result, Exception):
                        err_type, err_repr = _format_archive_exc(result)
                        self._log(
                            "warning",
                            "archive_day_failed",
                            symbol=symbol,
                            day=day_key,
                            error=err_repr,
                            error_type=err_type,
                        )
                        skipped += 1
                    elif result[2] is not None:  # (error_type, error_repr)
                        err_type, err_repr = result[2]
                        self._log(
                            "warning",
                            "archive_day_failed",
                            symbol=symbol,
                            day=day_key,
                            error=err_repr,
                            error_type=err_type,
                        )
                        skipped += 1
                    elif result[1] is None or result[1].size == 0:
                        self._log("debug", "archive_day_unavailable", symbol=symbol, day=day_key)
                        skipped += 1
                    else:
                        arr = result[1]
                        # 延迟索引写入 - 在批次结束时一次性刷新
                        # 跳过内存保留以保留完整的历史数据用于回测
                        self._persist_batch(
                            symbol,
                            arr,
                            timeframe="1m",
                            merge_cache=True,
                            last_refresh_ms=_utc_now_ms(),
                            defer_index=True,
                            skip_memory_retention=True,
                        )
                        shard_paths[day_key] = self._shard_path(symbol, day_key, tf="1m")
                        self._log(
                            "debug",  # Changed from info to debug to reduce log noise
                            "archive_day_saved",
                            symbol=symbol,
                            day=day_key,
                            rows=int(arr.size),
                        )
                        completed += 1
                        batch_had_saves = True

                batch_elapsed = round(time.monotonic() - batch_start_time, 2)
                if len(batch) > 1:
                    self._log(
                        "debug",
                        "archive_batch_complete",
                        symbol=symbol,
                        batch_size=len(batch),
                        elapsed_s=batch_elapsed,
                    )
        except Exception:
            # 重新抛出，但确保下面仍然记录完成日志
            raise

        # 所有批次完成后一次性刷新延迟的索引写入
        if completed > 0:
            self.flush_deferred_index(symbol, tf="1m")

        # 记录完成摘要
        total_elapsed = round(time.monotonic() - start_time, 1)
        self._log(
            "info",
            "archive_prefetch_complete",
            symbol=symbol,
            fetched=completed,
            skipped=skipped,
            total=total_days,
            elapsed_s=total_elapsed,
        )
        self._emit_remote_fetch(
            {
                "kind": "archive_prefetch",
                "stage": "done",
                "exchange": str(self._ex_id),
                "symbol": symbol,
                "fetched": int(completed),
                "skipped": int(skipped),
                "total": int(total_days),
                "elapsed_s": float(total_elapsed),
            }
        )

    async def get_candles(
        self,
        symbol: str,
        *,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        max_age_ms: Optional[int] = None,
        strict: bool = False,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
        force_refetch_gaps: bool = False,
        fill_leading_gaps: bool = False,
        skip_historical_gap_fill: bool = False,
        max_lookback_candles: Optional[int] = None,
    ) -> np.ndarray:
        """返回包含范围 [start_ts, end_ts] 内的 K 线。

        - 如果 `end_ts` 为 None：floor(now/1m)*1m + 1m
        - 如果 `start_ts` 为 None：最后 `default_window_candles` 分钟
        - 如果提供了 `end_ts` 但 `start_ts` 为 None：end_ts - window
        - 如果 `max_age_ms` == 0：强制刷新（exchange 为 None 时无操作）
        - 负数的 `max_age_ms` 抛出 ValueError
        - 应用缺口标准化（仅 1m）
        - 如果 `force_refetch_gaps` 为 True：在获取前清除请求范围内的已知缺口，
          强制重试所有缺口，无论重试次数
        - 如果 `fill_leading_gaps` 为 True：在第一个真实数据点之前也合成零成交 K 线
          （适用于 EMA 计算）
        - 如果 `skip_historical_gap_fill` 为 True：不尝试获取/填充超过 1 天的历史缺口。
          适用于实盘机器人预热，仅需要近期数据，填充旧缺口浪费时间。
        - 如果设置了 `max_lookback_candles`：裁剪 start_ts 使请求跨越的 K 线数
          不超过该值（按时间周期计算）。
        """
        if max_age_ms is not None and max_age_ms < 0:
            raise ValueError("max_age_ms cannot be negative")

        # 强制重新获取：清除请求范围内的已知缺口
        if force_refetch_gaps:
            # Compute actual range first
            now = _utc_now_ms()
            eff_end = end_ts if end_ts is not None else _floor_minute(now)
            eff_start = (
                start_ts
                if start_ts is not None
                else (int(eff_end) - self.default_window_candles * ONE_MIN_MS)
            )
            cleared = self.clear_known_gaps(symbol, date_range=(eff_start, eff_end))
            if cleared > 0:
                self._log(
                    "info",
                    "force_refetch_gaps",
                    symbol=symbol,
                    start_ts=eff_start,
                    end_ts=eff_end,
                    gaps_cleared=cleared,
                )

        # 当请求高级别时间周期时，直接从交易所获取
        # 并绕过 1m 缓存/标准化逻辑。
        out_tf = timeframe or tf
        if out_tf is not None:
            # 解析时间周期为毫秒（桶大小）
            period_ms = _tf_to_ms(out_tf)
            if period_ms > ONE_MIN_MS and self.exchange is not None:
                now = _utc_now_ms()
                finalized_end = (int(now) // period_ms) * period_ms - period_ms
                if end_ts is None:
                    end_ts = finalized_end
                else:
                    end_ts = min((int(end_ts) // period_ms) * period_ms, finalized_end)

                if start_ts is None:
                    # 默认窗口以所请求时间周期的桶数表示
                    start_ts = int(end_ts) - self.default_window_candles * period_ms
                start_ts = (int(start_ts) // period_ms) * period_ms

                if max_lookback_candles is not None:
                    try:
                        lookback = max(1, int(max_lookback_candles))
                        lookback_start = int(end_ts) - period_ms * (lookback - 1)
                        if int(start_ts) < int(lookback_start):
                            start_ts = int(lookback_start)
                    except Exception:
                        pass

                if start_ts > end_ts:
                    return np.empty((0,), dtype=CANDLE_DTYPE)

                # Hyperliquid 特殊情况：任何时间周期从当前时间起最多 5000 根 K 线
                try:
                    exid = (self._ex_id or "").lower() if isinstance(self._ex_id, str) else ""
                except Exception:
                    exid = ""
                if "hyperliquid" in exid:
                    earliest = int(finalized_end - period_ms * (5000 - 1))
                    if start_ts < earliest:
                        # 将较旧部分标记为已知缺口以避免重复获取尝试
                        gap_end = min(end_ts, earliest - period_ms)
                        if start_ts <= gap_end:
                            self._add_known_gap(symbol, int(start_ts), int(gap_end))
                        start_ts = max(start_ts, earliest)

                # 在求助于网络之前，先从磁盘分片加载该时间周期的数据
                try:
                    disk_arr = self._load_from_disk(symbol, start_ts, end_ts, timeframe=out_tf)
                except Exception:
                    disk_arr = None

                # 首先检查内存中的时间周期范围缓存（LRU）
                cache_key = (str(out_tf), int(start_ts), int(end_ts))
                sym_cache = self._tf_range_cache.setdefault(symbol, OrderedDict())
                if cache_key in sym_cache:
                    arr_cached, fetched_at = sym_cache[cache_key]
                    try:
                        sym_cache.move_to_end(cache_key)
                    except Exception:
                        pass
                    if (
                        max_age_ms is None
                        or max_age_ms == 0
                        or (now - int(fetched_at)) <= int(max_age_ms)
                    ):
                        return arr_cached

                # 如果磁盘对该时间周期窗口有完整覆盖，不通过网络直接返回
                if isinstance(disk_arr, np.ndarray) and disk_arr.size:
                    out_disk = self._slice_ts_range(disk_arr, start_ts, end_ts)
                    if out_disk.size:
                        # verify full coverage with proper step
                        tsd = _ts_index(out_disk)
                        expected_len = int((end_ts - start_ts) // period_ms) + 1
                        if (
                            out_disk.shape[0] == expected_len
                            and int(tsd[0]) == int(start_ts)
                            and int(tsd[-1]) == int(end_ts)
                            and (
                                expected_len == 1
                                or (
                                    int(np.diff(tsd).min(initial=period_ms)) == period_ms
                                    and int(np.diff(tsd).max(initial=period_ms)) == period_ms
                                )
                            )
                        ):
                            sym_cache[cache_key] = (out_disk, int(now))
                            try:
                                sym_cache.move_to_end(cache_key)
                            except Exception:
                                pass
                            while len(sym_cache) > self._tf_range_cache_cap:
                                sym_cache.popitem(last=False)
                            self._tf_range_cache[symbol] = sym_cache
                            return out_disk

                end_excl = int(end_ts) + period_ms

                async with self._acquire_fetch_lock(symbol, out_tf):
                    try:
                        disk_arr = self._load_from_disk(symbol, start_ts, end_ts, timeframe=out_tf)
                    except Exception:
                        disk_arr = None

                    if isinstance(disk_arr, np.ndarray) and disk_arr.size:
                        out_disk = self._slice_ts_range(disk_arr, start_ts, end_ts)
                        if out_disk.size:
                            tsd = _ts_index(out_disk)
                            expected_len = int((end_ts - start_ts) // period_ms) + 1
                            if (
                                out_disk.shape[0] == expected_len
                                and int(tsd[0]) == int(start_ts)
                                and int(tsd[-1]) == int(end_ts)
                                and (
                                    expected_len == 1
                                    or (
                                        int(np.diff(tsd).min(initial=period_ms)) == period_ms
                                        and int(np.diff(tsd).max(initial=period_ms)) == period_ms
                                    )
                                )
                            ):
                                sym_cache[cache_key] = (out_disk, int(now))
                                try:
                                    sym_cache.move_to_end(cache_key)
                                except Exception:
                                    pass
                                while len(sym_cache) > self._tf_range_cache_cap:
                                    sym_cache.popitem(last=False)
                                self._tf_range_cache[symbol] = sym_cache
                                return out_disk

                    persisted_batches = False

                    def _persist_tf_batch(batch: np.ndarray) -> None:
                        nonlocal persisted_batches
                        persisted_batches = True
                        self._persist_batch(symbol, batch, timeframe=out_tf)

                    try:
                        fetched = await self._fetch_ohlcv_paginated(
                            symbol,
                            int(start_ts),
                            int(end_excl),
                            timeframe=out_tf,
                            on_batch=_persist_tf_batch,
                        )
                    except TypeError:
                        fetched = await self._fetch_ohlcv_paginated(
                            symbol,
                            int(start_ts),
                            int(end_excl),
                            timeframe=out_tf,
                        )
                    if fetched.size == 0:
                        if isinstance(disk_arr, np.ndarray) and disk_arr.size:
                            out = self._slice_ts_range(disk_arr, start_ts, end_ts)
                            sym_cache[cache_key] = (out, int(now))
                            try:
                                sym_cache.move_to_end(cache_key)
                            except Exception:
                                pass
                            while len(sym_cache) > self._tf_range_cache_cap:
                                sym_cache.popitem(last=False)
                            self._tf_range_cache[symbol] = sym_cache
                            return out
                        return fetched
                    out = self._slice_ts_range(fetched, start_ts, end_ts)
                    if out.size and not persisted_batches:
                        self._persist_batch(symbol, out, timeframe=out_tf)
                    sym_cache[cache_key] = (out, int(now))
                    try:
                        sym_cache.move_to_end(cache_key)
                    except Exception:
                        pass
                    while len(sym_cache) > self._tf_range_cache_cap:
                        sym_cache.popitem(last=False)
                    self._tf_range_cache[symbol] = sym_cache
                    return out

        now = _utc_now_ms()
        if end_ts is None:
            # 使用最后已结束的分钟作为包含端（排除当前进行中的分钟）
            end_ts = _floor_minute(now) - ONE_MIN_MS
        else:
            # 裁剪到最后已结束的分钟
            end_ts = min(_floor_minute(int(end_ts)), _floor_minute(now) - ONE_MIN_MS)

        if start_ts is None:
            start_ts = int(end_ts) - ONE_MIN_MS * self.default_window_candles
        else:
            start_ts = _floor_minute(int(start_ts))

        if max_lookback_candles is not None:
            try:
                lookback = max(1, int(max_lookback_candles))
                lookback_start = int(end_ts) - ONE_MIN_MS * (lookback - 1)
                if int(start_ts) < int(lookback_start):
                    start_ts = int(lookback_start)
            except Exception:
                pass

        if start_ts > end_ts:
            return np.empty((0,), dtype=CANDLE_DTYPE)

        # 可选：如果范围触及最新已结束分钟则刷新
        allow_fetch_present = True
        skip_present_fetch_due_to_ttl = False
        latest_finalized = _floor_minute(now) - ONE_MIN_MS
        if end_ts >= latest_finalized and self.exchange is not None:
            if max_age_ms == 0:
                self._log(
                    "debug",
                    "get_candles_force_refresh",
                    symbol=symbol,
                    end_ts=end_ts,
                )
                await self.refresh(symbol, through_ts=end_ts)
            elif max_age_ms is not None and max_age_ms > 0:
                last_ref = self._get_last_refresh_ms(symbol)
                last_final = 0
                try:
                    idx = self._ensure_symbol_index(symbol, tf="1m")
                    last_final = int(idx.get("meta", {}).get("last_final_ts", 0) or 0)
                except Exception:
                    last_final = 0
                self._log(
                    "debug",
                    "get_candles_check_refresh",
                    symbol=symbol,
                    end_ts=end_ts,
                    last_refresh_ms=last_ref,
                    last_final_ts=last_final,
                    max_age_ms=max_age_ms,
                    now=now,
                )
                need_refresh = last_ref == 0 or (now - last_ref) > int(max_age_ms)
                if not need_refresh:
                    # 仅当缓存数据落后超过 1 根 K 线周期时才强制刷新。
                    # 恰好落后 1 分钟是正常的（如刚完成预热后的分钟边界跨越）。
                    # 在这种情况下由 TTL 单独控制刷新时机 — 避免所有交易对
                    # 在分钟边界同时刷新产生的惊群效应。
                    if last_final and (int(end_ts) - int(last_final)) > ONE_MIN_MS:
                        need_refresh = True
                if need_refresh:
                    await self.refresh(symbol, through_ts=end_ts)
                else:
                    allow_fetch_present = False
                    skip_present_fetch_due_to_ttl = True

        # 在切分内存之前，尝试从磁盘分片加载该范围
        try:
            self._load_from_disk(symbol, start_ts, end_ts, timeframe="1m")
        except Exception:  # pragma: no cover - best effort
            pass

        # 获取交易对的内存缓存 K 线并切分到请求范围
        arr = _ensure_dtype(self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE)))
        sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr

        # 确定请求的历史窗口是否在内存中完全覆盖
        def _is_fully_covered(a: np.ndarray, s_ts: int, e_ts: int) -> bool:
            if a.size == 0:
                return False
            expected_len = int((e_ts - s_ts) // ONE_MIN_MS) + 1
            if a.shape[0] != expected_len:
                return False
            if int(a[0]["ts"]) != s_ts or int(a[-1]["ts"]) != e_ts:
                return False
            if expected_len > 1:
                diffs = np.diff(a["ts"].astype(np.int64))
                if int(diffs.max()) != ONE_MIN_MS or int(diffs.min()) != ONE_MIN_MS:
                    return False
            return True

        fully_covered = _is_fully_covered(sub, start_ts, end_ts)
        if skip_present_fetch_due_to_ttl and not fully_covered:
            # TTL 表示数据新鲜，但请求范围的覆盖不完整。
            # 允许即时获取/缺口填充尝试修复缺失区间。
            allow_fetch_present = True
            try:
                missing_now = self._missing_spans(sub, start_ts, end_ts)
                self._log(
                    "debug",
                    "ttl_bypass_missing_coverage",
                    symbol=symbol,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    max_age_ms=max_age_ms,
                    last_refresh_ms=self._get_last_refresh_ms(symbol),
                    missing_spans=len(missing_now),
                )
            except Exception:
                pass

        # 对于历史范围，如果尚无所有天的分片，则精确获取该范围
        # 并持久化分片以供后续调用。
        end_finalized = latest_finalized

        # 大范围预取：如果请求跨越超过 2 天且未完全覆盖，
        # 即使 end_ts 接近当前时间也触发历史部分的归档预取。
        # 这修复了跨 31 天的预热请求之前因 end_ts == latest_finalized
        # 而跳过归档获取的问题。
        span_minutes = (end_ts - start_ts) // ONE_MIN_MS
        large_span_threshold = 2 * 24 * 60  # 2 days in minutes
        archive_supported = self._archive_supported()
        if not fully_covered and span_minutes > large_span_threshold:
            self._log(
                "debug",
                "large_span_check",
                symbol=symbol,
                exchange_present=self.exchange is not None,
                span_minutes=int(span_minutes),
                fully_covered=fully_covered,
                archive_supported=archive_supported,
                sub_size=sub.size if hasattr(sub, "size") else 0,
            )
        if (
            self.exchange is not None
            and span_minutes > large_span_threshold
            and not fully_covered
            and archive_supported
        ):
            # 预取历史部分的归档（最多到 2 天前，因为归档通常滞后 1-2 天）。
            # 同时尊重用户请求的 end_ts 以避免获取超出请求日期范围的数据。
            archive_end_ts = min(end_finalized - 2 * 24 * 60 * ONE_MIN_MS, end_ts)
            if start_ts < archive_end_ts:
                self._log(
                    "info",
                    "large_span_archive_prefetch",
                    symbol=symbol,
                    span_minutes=int(span_minutes),
                    start_ts=start_ts,
                    archive_end_ts=archive_end_ts,
                )
                await self._prefetch_archives_for_range(symbol, start_ts, archive_end_ts)
                # 从磁盘重新加载归档获取后的数据
                try:
                    self._load_from_disk(symbol, start_ts, end_ts, timeframe="1m")
                except Exception:
                    pass
                arr = _ensure_dtype(self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE)))
                sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr
                fully_covered = _is_fully_covered(sub, start_ts, end_ts)

        # 将恰好结束于最新已结束分钟的范围视为触及当前时间，
        # 除非跨度大（>2 天）且未完全覆盖 - 此时视为历史范围
        # 以触发通过 CCXT 填充缺口。这修复了跨 31 天的预热请求
        # 之前因 end_ts == latest_finalized 而跳过缺口检测的问题。
        historical = end_ts < end_finalized
        if (
            not historical
            and span_minutes > large_span_threshold
            and not fully_covered
            and self.exchange is not None
        ):
            self._log(
                "debug",
                "large_span_needs_gap_fill",
                symbol=symbol,
                span_minutes=int(span_minutes),
                fully_covered=fully_covered,
            )
            historical = True
        if self.exchange is not None and historical and not skip_historical_gap_fill:
            # 如果请求的历史窗口在内存中未完全覆盖，
            # 尝试获取未知的缺失区间，无论分片是否存在。
            # 如果设置了 skip_historical_gap_fill 则跳过（例如实盘预热仅需要
            # 近期数据，旧缺口无关紧要）。
            if not fully_covered:
                # Hyperliquid 特殊情况：限制回看长度为最后 5000 分钟
                try:
                    exid = (self._ex_id or "").lower() if isinstance(self._ex_id, str) else ""
                except Exception:
                    exid = ""
                adj_start_ts = start_ts
                if "hyperliquid" in exid:
                    earliest = int(end_finalized - ONE_MIN_MS * (5000 - 1))
                    if adj_start_ts < earliest:
                        gap_end = min(end_ts, earliest - ONE_MIN_MS)
                        if adj_start_ts <= gap_end:
                            self._add_known_gap(symbol, int(adj_start_ts), int(gap_end))
                        adj_start_ts = max(adj_start_ts, earliest)
                if "gateio" in exid:
                    earliest = int(
                        end_finalized - ONE_MIN_MS * (_GATEIO_RECENT_1M_LIMIT_CANDLES - 1)
                    )
                    if adj_start_ts < earliest:
                        gap_end = min(end_ts, earliest - ONE_MIN_MS)
                        if adj_start_ts <= gap_end:
                            self._record_verified_gap(
                                symbol,
                                int(adj_start_ts),
                                int(gap_end),
                                reason=GAP_REASON_NO_ARCHIVE,
                            )
                        if symbol not in self._gateio_recent_window_clip_warned:
                            self._log(
                                "warning",
                                "gateio_ohlcv_recent_window_clipped",
                                symbol=symbol,
                                requested_start_ts=int(start_ts),
                                requested_end_ts=int(end_ts),
                                earliest_fetchable_ts=int(earliest),
                                reason="gateio_public_1m_ohlcv_recent_window",
                            )
                            self._gateio_recent_window_clip_warned.add(symbol)
                        adj_start_ts = max(adj_start_ts, earliest)

                # 如果所有缺失区间都已是已知的持久缺口则跳过获取
                missing_before = self._missing_spans(sub, start_ts, end_ts)

                def span_in_persistent_gap(s: int, e: int) -> bool:
                    """检查区间是否完全包含在一个持久（已达最大重试次数）缺口中。

                    注意：每次调用重新加载缺口以避免在同一函数上下文中
                    调用 _add_known_gap() 时的过期闭包。
                    """
                    known_enhanced = self._get_known_gaps_enhanced(symbol)
                    for gap in known_enhanced:
                        if s >= gap["start_ts"] and e <= gap["end_ts"]:
                            # Only consider it "known" if it's persistent (max retries reached)
                            if not self._should_retry_gap(gap):
                                return True  # 仅当达到最大重试次数时视为"已知"
                    return False

                unknown_missing = [
                    (s, e) for (s, e) in missing_before if not span_in_persistent_gap(s, e)
                ]

                if unknown_missing:
                    end_excl = min(end_ts + ONE_MIN_MS, end_finalized + ONE_MIN_MS)
                    if adj_start_ts < end_excl:
                        async with self._acquire_fetch_lock(symbol, "1m"):
                            try:
                                self._load_from_disk(symbol, start_ts, end_ts, timeframe="1m")
                            except Exception:
                                pass
                            arr = _ensure_dtype(
                                self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE))
                            )
                            sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr
                            missing_after = self._missing_spans(sub, start_ts, end_ts)
                            unknown_after = [
                                (s, e) for (s, e) in missing_after if not span_in_persistent_gap(s, e)
                            ]
                            if unknown_after:
                                # 仅对真正缺失的完整天尝试归档预取。
                                await self._prefetch_archives_for_range(symbol, adj_start_ts, end_ts)
                                try:
                                    self._load_from_disk(symbol, start_ts, end_ts, timeframe="1m")
                                except Exception:
                                    pass
                                arr = _ensure_dtype(
                                    self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE))
                                )
                                sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr
                                missing_after = self._missing_spans(sub, start_ts, end_ts)
                                unknown_after = [
                                    (s, e)
                                    for (s, e) in missing_after
                                    if not span_in_persistent_gap(s, e)
                                ]
                            if unknown_after:
                                persisted_batches = False
                                deferred_index_any = False
                                flush_failed_once = False

                                def _persist_hist_batch(batch: np.ndarray) -> None:
                                    nonlocal persisted_batches, deferred_index_any
                                    persisted_batches = True
                                    deferred_index_any = True
                                    # 跳过内存保留以保留完整的历史数据
                                    self._persist_batch(
                                        symbol,
                                        batch,
                                        timeframe="1m",
                                        merge_cache=True,
                                        last_refresh_ms=now,
                                        defer_index=True,
                                        skip_memory_retention=True,
                                    )

                                self._log(
                                    "debug",
                                    "historical_missing_spans",
                                    symbol=symbol,
                                    spans=len(unknown_after),
                                    first_start_ts=int(unknown_after[0][0]),
                                    last_end_ts=int(unknown_after[-1][1]),
                                )

                                # 将许多小缺失区间合并为按天的获取窗口。
                                # 这避免了缺口碎片化时产生数千个微小的 CCXT 请求。
                                spans_to_fetch: List[Tuple[int, int]] = list(unknown_after)
                                try:
                                    day_windows: Dict[str, Tuple[int, int]] = {}
                                    for s0, e0 in spans_to_fetch:
                                        s = int(s0)
                                        e = int(e0)
                                        if e < s:
                                            continue
                                        while s <= e:
                                            dk = self._date_key(s)
                                            ds, de = self._date_range_of_key(dk)
                                            w_start = max(int(ds), int(adj_start_ts))
                                            w_end = min(int(de), int(end_ts))
                                            if w_end >= w_start:
                                                prev = day_windows.get(dk)
                                                if prev is None:
                                                    day_windows[dk] = (w_start, w_end)
                                                else:
                                                    day_windows[dk] = (
                                                        min(int(prev[0]), w_start),
                                                        max(int(prev[1]), w_end),
                                                    )
                                            s = int(de) + ONE_MIN_MS
                                    spans_to_fetch = [
                                        day_windows[k] for k in sorted(day_windows.keys())
                                    ]
                                    if len(spans_to_fetch) != len(unknown_after):
                                        self._log(
                                            "debug",
                                            "historical_missing_spans_coalesced",
                                            symbol=symbol,
                                            spans_before=len(unknown_after),
                                            spans_after=len(spans_to_fetch),
                                        )
                                except Exception:
                                    spans_to_fetch = list(unknown_after)

                                # 仅获取缺失区间（而非整个历史范围）。
                                for s, e in spans_to_fetch:
                                    s2 = max(int(s), int(adj_start_ts))
                                    e2 = int(e)
                                    if e2 < s2:
                                        continue
                                    span_end_excl = min(e2 + ONE_MIN_MS, end_excl)
                                    if s2 >= span_end_excl:
                                        continue
                                    try:
                                        fetched = await self._fetch_ohlcv_paginated(
                                            symbol,
                                            s2,
                                            span_end_excl,
                                            on_batch=_persist_hist_batch,
                                        )
                                    except TypeError:
                                        fetched = await self._fetch_ohlcv_paginated(
                                            symbol,
                                            s2,
                                            span_end_excl,
                                        )
                                    if deferred_index_any:
                                        try:
                                            self.flush_deferred_index(symbol, tf="1m")
                                        except (
                                            Exception
                                        ) as exc:  # 尽力而为；即使索引更新失败也继续获取
                                            if not flush_failed_once:
                                                try:
                                                    err_type = type(exc).__name__
                                                    err_repr = repr(exc)
                                                except Exception:
                                                    err_type = "Exception"
                                                    err_repr = "<unrepresentable exception>"
                                                self._log(
                                                    "warning",
                                                    "flush_deferred_index_failed",
                                                    symbol=symbol,
                                                    timeframe="1m",
                                                    error_type=err_type,
                                                    error=err_repr,
                                                )
                                                flush_failed_once = True
                                        deferred_index_any = False
                                    if fetched.size and not persisted_batches:
                                        # 跳过内存保留以保留完整的历史数据
                                        self._persist_batch(
                                            symbol,
                                            fetched,
                                            timeframe="1m",
                                            merge_cache=True,
                                            last_refresh_ms=now,
                                            defer_index=True,
                                            skip_memory_retention=True,
                                        )
                                        try:
                                            self.flush_deferred_index(symbol, tf="1m")
                                        except (
                                            Exception
                                        ) as exc:  # 尽力而为；即使索引更新失败也继续获取
                                            if not flush_failed_once:
                                                try:
                                                    err_type = type(exc).__name__
                                                    err_repr = repr(exc)
                                                except Exception:
                                                    err_type = "Exception"
                                                    err_repr = "<unrepresentable exception>"
                                                self._log(
                                                    "warning",
                                                    "flush_deferred_index_failed",
                                                    symbol=symbol,
                                                    timeframe="1m",
                                                    error_type=err_type,
                                                    error=err_repr,
                                                )
                                                flush_failed_once = True
                            arr = (
                                np.sort(self._cache[symbol], order="ts")
                                if symbol in self._cache
                                else np.empty((0,), dtype=CANDLE_DTYPE)
                            )
                            sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr
                            still_missing = self._missing_spans(sub, start_ts, end_ts)
                            # Re-fetch authoritative lower bound after archive prefetch.
                            authoritative_start_ts = self._get_authoritative_start_ts(symbol)
                            for s, e in still_missing:
                                if not span_in_persistent_gap(s, e):
                                    # Only mark pre_inception when we know a real authoritative
                                    # lower bound for exchange-available history.
                                    if (
                                        authoritative_start_ts is not None
                                        and e < authoritative_start_ts
                                    ):
                                        self._add_known_gap(
                                            symbol,
                                            s,
                                            e,
                                            reason="pre_inception",
                                            retry_count=_GAP_MAX_RETRIES,  # Persistent immediately
                                        )
                                    else:
                                        # Normal gap - will retry and eventually warn
                                        self._add_known_gap(
                                            symbol,
                                            s,
                                            e,
                                            reason=GAP_REASON_FETCH_FAILED,
                                            increment_retry=True,
                                        )
        elif self.exchange is not None and allow_fetch_present:
            # 范围触及当前时间（end 在或超过当前分钟）；获取到当前分钟（包含）
            end_current = _floor_minute(now)
            end_excl = min(end_ts + ONE_MIN_MS, end_current + ONE_MIN_MS)
            if start_ts < end_excl:
                need_fetch = False
                fetch_start = start_ts
                if sub.size == 0:
                    need_fetch = True
                else:
                    last_have = int(sub[-1]["ts"]) if sub.size else start_ts - ONE_MIN_MS
                    if last_have < end_excl - ONE_MIN_MS:
                        need_fetch = True
                        fetch_start = max(start_ts, last_have + ONE_MIN_MS)
                self._log(
                    "debug",
                    "get_candles_present_decision",
                    symbol=symbol,
                    need_fetch=need_fetch,
                    fetch_start=fetch_start,
                    last_have=int(sub[-1]["ts"]) if sub.size else None,
                    end_excl=end_excl,
                    sub_size=int(sub.shape[0]) if sub.size else 0,
                )
                if need_fetch:
                    async with self._acquire_fetch_lock(symbol, "1m"):
                        try:
                            self._load_from_disk(symbol, start_ts, end_ts, timeframe="1m")
                        except Exception:
                            pass
                        arr = _ensure_dtype(
                            self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE))
                        )
                        sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr
                        last_have = int(sub[-1]["ts"]) if sub.size else start_ts - ONE_MIN_MS
                        need_fetch_inner = sub.size == 0 or last_have < end_excl - ONE_MIN_MS
                        self._log(
                            "debug",
                            "get_candles_present_inner",
                            symbol=symbol,
                            need_fetch=need_fetch_inner,
                            fetch_start=fetch_start,
                            last_have=last_have if sub.size else None,
                            end_excl=end_excl,
                            sub_size=int(sub.shape[0]) if sub.size else 0,
                        )
                        if need_fetch_inner:
                            persisted_batches = False

                            def _persist_present_batch(batch: np.ndarray) -> None:
                                nonlocal persisted_batches
                                persisted_batches = True
                                self._persist_batch(
                                    symbol,
                                    batch,
                                    timeframe="1m",
                                    merge_cache=True,
                                    last_refresh_ms=now,
                                )

                            try:
                                fetched = await self._fetch_ohlcv_paginated(
                                    symbol,
                                    fetch_start,
                                    end_excl,
                                    on_batch=_persist_present_batch,
                                )
                            except TypeError:
                                fetched = await self._fetch_ohlcv_paginated(
                                    symbol,
                                    fetch_start,
                                    end_excl,
                                )
                            if fetched.size and not persisted_batches:
                                self._persist_batch(
                                    symbol,
                                    fetched,
                                    timeframe="1m",
                                    merge_cache=True,
                                    last_refresh_ms=now,
                                )
                        arr = (
                            np.sort(self._cache[symbol], order="ts")
                            if symbol in self._cache
                            else np.empty((0,), dtype=CANDLE_DTYPE)
                        )
                        sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr

        # 尽力尾部补全（仅当前时间）：如果仍缺失请求窗口内的尾部分钟，
        # 从最后一个可用 ts 尝试一次额外获取。跳过历史范围以避免交易所有
        # 永久性空洞时的冗余调用。
        if self.exchange is not None and allow_fetch_present and not historical:
            end_current = _floor_minute(now)
            end_excl_range = (
                end_ts + ONE_MIN_MS
                if historical
                else min(end_ts + ONE_MIN_MS, end_current + ONE_MIN_MS)
            )
            for _ in range(2):
                if sub.size == 0:
                    break
                last_have = int(sub[-1]["ts"]) if sub.size else start_ts - ONE_MIN_MS
                if last_have >= end_excl_range - ONE_MIN_MS:
                    break
                fetch_start = last_have + ONE_MIN_MS
                if fetch_start >= end_excl_range:
                    break
                async with self._acquire_fetch_lock(symbol, "1m"):
                    try:
                        self._load_from_disk(symbol, start_ts, end_ts, timeframe="1m")
                    except Exception:
                        pass
                    arr = _ensure_dtype(self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE)))
                    sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr
                    if sub.size == 0:
                        break
                    last_have = int(sub[-1]["ts"]) if sub.size else start_ts - ONE_MIN_MS
                    if last_have >= end_excl_range - ONE_MIN_MS:
                        break
                    fetch_start = last_have + ONE_MIN_MS
                    if fetch_start >= end_excl_range:
                        break
                    persisted_batches = False

                    def _persist_tail_batch(batch: np.ndarray) -> None:
                        nonlocal persisted_batches
                        persisted_batches = True
                        self._persist_batch(
                            symbol,
                            batch,
                            timeframe="1m",
                            merge_cache=True,
                            last_refresh_ms=now,
                        )

                    try:
                        fetched = await self._fetch_ohlcv_paginated(
                            symbol,
                            fetch_start,
                            end_excl_range,
                            on_batch=_persist_tail_batch,
                        )
                    except TypeError:
                        fetched = await self._fetch_ohlcv_paginated(
                            symbol,
                            fetch_start,
                            end_excl_range,
                        )
                    if fetched.size == 0:
                        break
                    if not persisted_batches:
                        self._persist_batch(
                            symbol,
                            fetched,
                            timeframe="1m",
                            merge_cache=True,
                            last_refresh_ms=now,
                        )
                    arr = np.sort(self._cache[symbol], order="ts")
                    sub = self._slice_ts_range(arr, start_ts, end_ts)

        # 缺口导向的获取和标记（仅当前时间）：尝试填充内部缺口一次；
        # 将剩余的标记为已知缺口。跳过纯历史窗口；这些在上方已通过
        # 已知缺口标记处理。
        if self.exchange is not None and allow_fetch_present and not historical:
            end_current = _floor_minute(now)
            inclusive_end = end_ts if historical else min(end_ts, end_current)
            missing = self._missing_spans(sub, start_ts, inclusive_end)
            if missing:
                # 辅助函数：测试区间是否完全在某个持久已知缺口内
                def span_in_persistent_gap_present(s: int, e: int) -> bool:
                    """检查区间是否在持久缺口内。重新加载缺口以避免过期数据。"""
                    known_enhanced_present = self._get_known_gaps_enhanced(symbol)
                    for gap in known_enhanced_present:
                        if s >= gap["start_ts"] and e <= gap["end_ts"]:
                            if not self._should_retry_gap(gap):
                                return True
                    return False

                # 对未知区间尝试有限的目标获取
                attempts = 0
                max_attempts = 10 if self._ccxt_since_exclusive else 3
                attempted: List[Tuple[int, int]] = []
                noresult: List[Tuple[int, int]] = []
                for s, e in missing:
                    if attempts >= max_attempts:
                        break
                    if span_in_persistent_gap_present(s, e):
                        continue
                    end_excl_gap = e + ONE_MIN_MS
                    async with self._acquire_fetch_lock(symbol, "1m"):
                        try:
                            self._load_from_disk(symbol, start_ts, end_ts, timeframe="1m")
                        except Exception:
                            pass
                        arr = _ensure_dtype(
                            self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE))
                        )
                        sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr
                        missing_now = self._missing_spans(sub, start_ts, inclusive_end)
                        if not any(ms == s and me == e for ms, me in missing_now):
                            continue
                        persisted_batches = False

                        def _persist_gap_batch(batch: np.ndarray) -> None:
                            nonlocal persisted_batches
                            persisted_batches = True
                            self._persist_batch(
                                symbol,
                                batch,
                                timeframe="1m",
                                merge_cache=True,
                                last_refresh_ms=now,
                            )

                        try:
                            fetched = await self._fetch_ohlcv_paginated(
                                symbol,
                                s,
                                end_excl_gap,
                                on_batch=_persist_gap_batch,
                            )
                        except TypeError:
                            fetched = await self._fetch_ohlcv_paginated(
                                symbol,
                                s,
                                end_excl_gap,
                            )
                        attempts += 1
                        attempted.append((s, e))
                        if fetched.size:
                            if not persisted_batches:
                                self._persist_batch(
                                    symbol,
                                    fetched,
                                    timeframe="1m",
                                    merge_cache=True,
                                    last_refresh_ms=now,
                                )
                            arr = np.sort(self._cache[symbol], order="ts")
                            sub = self._slice_ts_range(arr, start_ts, end_ts, assume_sorted=True)
                        else:
                            noresult.append((s, e))
                # 尝试后，重新计算缺失并将剩余的标记为已知缺口
                still_missing = self._missing_spans(sub, start_ts, inclusive_end)
                # 仅将仍缺失的已尝试区间标记为已知
                for s, e in noresult:
                    # 找到与仍缺失区间的重叠部分
                    for ms, me in still_missing:
                        if not (e < ms or s > me):
                            self._add_known_gap(symbol, max(s, ms), min(e, me))

        # 触及当前时间的运行时路径：如果已结束分钟内没有交易，
        # 在内存中物化零成交量 K 线（不持久化）。
        if self.exchange is not None and not strict and end_ts >= latest_finalized:
            synth_through = min(int(end_ts), int(latest_finalized))
            if synth_through >= int(start_ts):
                synthesized = self._materialize_runtime_synthetic_gap(symbol, synth_through)
                if synthesized > 0:
                    arr = _ensure_dtype(self._cache.get(symbol, np.empty((0,), dtype=CANDLE_DTYPE)))
                    sub = self._slice_ts_range(arr, start_ts, end_ts) if arr.size else arr

        # 标准化缺口：为缺失的分钟合成零成交 K 线。
        # 为帮助前向填充种子计算，如果可用则包含 start_ts 之前的一根 K 线。
        # 这确保标准缺口操作有一个 prev_close，即使 sub 在 start_ts 之后开始。
        data_for_gaps = sub
        if sub.size == 0 or (sub.size > 0 and int(sub[0]["ts"]) > start_ts):
            full_arr = self._cache.get(symbol)
            if full_arr is not None and full_arr.size > 0:
                full_arr = _ensure_dtype(full_arr)
                ts_idx = full_arr["ts"].astype(np.int64)
                idx = int(np.searchsorted(ts_idx, start_ts, side="left"))
                if idx > 0:
                    seed_candle = full_arr[idx - 1 : idx]
                    if sub.size > 0:
                        data_for_gaps = np.concatenate([seed_candle, sub])
                    else:
                        data_for_gaps = seed_candle

        result = self.standardize_gaps(
            data_for_gaps,
            start_ts=start_ts,
            end_ts=end_ts,
            strict=strict,
            fill_leading_gaps=fill_leading_gaps,
            assume_sorted=True,
            symbol=symbol,
        )

        # 记录累积的缺口摘要（节流）
        self._log_persistent_gap_summary()
        self._log_strict_gaps_summary()

        return result

    async def get_current_close(self, symbol: str, max_age_ms: Optional[int] = None) -> float:
        """返回交易对当前进行中分钟的最新收盘价。

        优先使用 K 线而非行情：
        - TTL 内的缓存当前收盘价
        - 内存中新鲜的当前分钟 K 线
        - 当前分钟的 get_candles
        - 最后手段：fetch_ticker
        - 兜底：最后已缓存的已结束收盘价
        """
        if max_age_ms is not None and max_age_ms < 0:
            raise ValueError("max_age_ms cannot be negative")
        now = _utc_now_ms()
        end_current = _floor_minute(now)

        # 1) TTL 缓存
        if max_age_ms is not None and max_age_ms > 0:
            prev = self._current_close_cache.get(symbol)
            if prev is not None:
                price, updated = prev
                if (now - int(updated)) <= int(max_age_ms):
                    self._log("debug", "get_current_close_cache_hit", symbol=symbol)
                    return float(price)

        price: Optional[float] = None

        # 2) 内存中足够新鲜的当前分钟 K 线
        try:
            arr = self._cache.get(symbol)
            if arr is not None and arr.size:
                arr_sorted = np.sort(_ensure_dtype(arr), order="ts")
                last_ts = int(arr_sorted[-1]["ts"])
                if last_ts == end_current:
                    fresh_enough = True
                    if max_age_ms is not None and max_age_ms > 0:
                        last_refresh = self._get_last_refresh_ms(symbol)
                        fresh_enough = (now - int(last_refresh)) <= int(max_age_ms)
                    if fresh_enough:
                        price = float(arr_sorted[-1]["c"])
                        self._current_close_cache[symbol] = (price, now)
                        self._log("debug", "get_current_close_mem_candle", symbol=symbol)
                        return price
        except Exception:
            pass

        # 3) 使用 K 线 API 获取当前分钟
        got = None
        try:
            self._log(
                "debug",
                "get_current_close_via_candles",
                symbol=symbol,
                start_ts=end_current,
                end_ts=end_current,
            )
            got = await self.get_candles(
                symbol,
                start_ts=end_current,
                end_ts=end_current,
                max_age_ms=max_age_ms,
                timeframe=None,
                strict=False,
            )
            if got is not None and got.size:
                got_sorted = np.sort(_ensure_dtype(got), order="ts")
                price = float(got_sorted[-1]["c"])
                self._current_close_cache[symbol] = (price, now)
                self._log("debug", "get_current_close_from_candles", symbol=symbol)
                return price
        except Exception:
            pass

        if got is None or got.size == 0:
            try:
                last_ref = self._get_last_refresh_ms(symbol)
            except Exception:
                last_ref = 0
            # 如果有近期刷新（或未强制 TTL），回退到最后已结束的 K 线
            # 以避免冗余尾部获取。将 max_age_ms=None 视为无 TTL 限制。
            ttl_ok = True
            if max_age_ms is not None and max_age_ms > 0:
                ttl_ok = (now - int(last_ref)) <= int(max_age_ms)
            if last_ref and ttl_ok:
                last_final = int(end_current - ONE_MIN_MS)
                if last_final >= 0:
                    try:
                        got_prev = await self.get_candles(
                            symbol,
                            start_ts=last_final,
                            end_ts=last_final,
                            max_age_ms=max_age_ms,
                            timeframe=None,
                            strict=False,
                        )
                        if got_prev is not None and got_prev.size:
                            got_prev_sorted = np.sort(_ensure_dtype(got_prev), order="ts")
                            price = float(got_prev_sorted[-1]["c"])
                            self._current_close_cache[symbol] = (price, now)
                            self._log(
                                "debug",
                                "get_current_close_from_candles_finalized",
                                symbol=symbol,
                                ts=int(got_prev_sorted[-1]["ts"]),
                            )
                            return price
                    except Exception:
                        pass

        # 3b) 直接通过 OHLCV 获取小尾部窗口（带跨进程锁）并合并到缓存
        if self.exchange is not None:
            try:
                async with self._acquire_fetch_lock(symbol, "1m"):
                    now_locked = _utc_now_ms()
                    end_current_locked = _floor_minute(now_locked)
                    last_final_locked = end_current_locked - ONE_MIN_MS

                    # 在决定获取前从磁盘刷新缓存
                    try:
                        self._load_from_disk(
                            symbol, last_final_locked, end_current_locked, timeframe="1m"
                        )
                    except Exception:
                        pass

                    arr_cache = self._cache.get(symbol)
                    if arr_cache is not None and arr_cache.size:
                        arr_sorted = np.sort(_ensure_dtype(arr_cache), order="ts")
                        last_ts = int(arr_sorted[-1]["ts"])
                        if last_ts >= end_current_locked:
                            price = float(arr_sorted[-1]["c"])
                            self._current_close_cache[symbol] = (price, now_locked)
                            self._set_last_refresh_meta(symbol, last_refresh_ms=now_locked)
                            self._log(
                                "debug",
                                "get_current_close_mem_candle_locked",
                                symbol=symbol,
                            )
                            return price
                        if last_ts >= last_final_locked:
                            price = float(arr_sorted[-1]["c"])
                            self._current_close_cache[symbol] = (price, now_locked)
                            self._log(
                                "debug",
                                "get_current_close_from_candles_finalized_locked",
                                symbol=symbol,
                                ts=last_ts,
                            )
                            return price

                    n = int(self.overlap_candles) if getattr(self, "overlap_candles", 0) else 1
                    if n <= 0:
                        n = 1
                    try:
                        n = int(min(max(1, n), int(self._ccxt_limit_default)))
                    except Exception:
                        n = max(1, n)
                    since_tail = max(0, int(end_current_locked) - ONE_MIN_MS * (n - 1))
                    self._log(
                        "debug",
                        "ccxt_fetch_ohlcv_tail_for_current_close",
                        symbol=symbol,
                        tf="1m",
                        since_ts=since_tail,
                        limit=n,
                    )
                    rows = await self._ccxt_fetch_ohlcv_once(
                        symbol,
                        since_ms=since_tail,
                        limit=n,
                        end_exclusive_ms=None,
                        timeframe="1m",
                    )
                    arr = self._normalize_ccxt_ohlcv(rows)
                    if arr.size:
                        price = float(arr[-1]["c"])
                        merged = self._merge_overwrite(self._ensure_symbol_cache(symbol), arr)
                        self._cache[symbol] = merged
                        try:
                            self._enforce_memory_retention(symbol)
                            self._save_range(symbol, arr, timeframe="1m")
                        except Exception:
                            pass
                        self._set_last_refresh_meta(symbol, last_refresh_ms=now_locked)
                        self._current_close_cache[symbol] = (price, now_locked)
                        self._log(
                            "debug",
                            "get_current_close_from_direct_ohlcv",
                            symbol=symbol,
                            rows=arr.shape[0],
                        )
                        return price
            except Exception:
                pass

        # 4) 最后手段：行情
        if self.exchange is not None:
            try:
                if hasattr(self.exchange, "fetch_ticker"):
                    self._log("debug", "ccxt_fetch_ticker", symbol=symbol)
                    if getattr(self, "_net_sem", None) is not None:
                        async with self._net_sem:  # type: ignore[attr-defined]
                            t = await self.exchange.fetch_ticker(symbol)
                    else:
                        t = await self.exchange.fetch_ticker(symbol)
                    self._log(
                        "debug",
                        "ccxt_fetch_ticker_ok",
                        symbol=symbol,
                        last=(t.get("last") if isinstance(t, dict) else None),
                        close=(t.get("close") if isinstance(t, dict) else None),
                    )
                    price = float(t.get("last") or t.get("bid") or t.get("ask")) if t else None
                    if price is not None:
                        self._current_close_cache[symbol] = (price, now)
                        return price
            except Exception:
                pass

        # 5) 兜底：最后缓存的已结束 K 线
        if price is None:
            arr2 = self._cache.get(symbol)
            if arr2 is not None and arr2.size:
                arr2 = np.sort(_ensure_dtype(arr2), order="ts")
                price = float(arr2[-1]["c"])
                self._log("debug", "get_current_close_from_cache_finalized", symbol=symbol)

        if price is None:
            return float("nan")

        self._current_close_cache[symbol] = (float(price), int(now))
        return float(price)

    def set_current_close(self, symbol: str, price: float, timestamp_ms: int) -> None:
        """将价格注入当前收盘价缓存（例如来自批量 API 调用）。"""
        self._current_close_cache[symbol] = (float(price), int(timestamp_ms))

    def is_rate_limited(self) -> bool:
        """如果全局速率限制退避处于活动状态则返回 True。"""
        return self._rate_limit_until > time.time()

    # ----- EMA 辅助方法 -----

    def _ema(self, values: np.ndarray, span: float) -> float:
        return float(self._ema_series(values, span)[-1])

    def _ema_series(self, values: np.ndarray, span: float) -> np.ndarray:
        """返回 `values` 的偏差修正 EMA（pandas ewm adjust=True）。"""

        n = int(values.shape[0])
        if n == 0:
            return np.empty((0,), dtype=np.float64)
        span = float(span)
        alpha = 2.0 / (span + 1.0)
        one_minus = 1.0 - alpha
        out = np.empty((n,), dtype=np.float64)
        num = float(values[0])
        den = 1.0
        out[0] = num / den
        for i in range(1, n):
            v = float(values[i])
            if not np.isfinite(v):
                out[i] = out[i - 1]
                continue
            num = alpha * v + one_minus * num
            den = alpha + one_minus * den
            if den <= np.finfo(np.float64).tiny:
                num = alpha * v
                den = alpha
            out[i] = num / den
        return out

    async def _latest_finalized_range(
        self, span: float, *, period_ms: int = ONE_MIN_MS
    ) -> Tuple[int, int]:
        span_candles = max(1, int(math.ceil(float(span))))
        now = _utc_now_ms()
        # 对齐到时间周期桶并排除当前进行中的桶
        end_floor = (int(now) // int(period_ms)) * int(period_ms)
        end_ts = int(end_floor - period_ms)
        start_ts = int(end_ts - period_ms * (span_candles - 1))
        return start_ts, end_ts

    async def get_latest_ema_close(
        self,
        symbol: str,
        span: float,
        max_age_ms: Optional[int] = None,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> float:
        """返回最后 `span` 根已结束 K 线的收盘价 EMA。

        支持通过 `tf`/`timeframe` 指定高级别时间周期。
        """
        out_tf = timeframe if timeframe is not None else tf
        period_ms = _tf_to_ms(out_tf)
        start_ts, end_ts = await self._latest_finalized_range(span, period_ms=period_ms)
        # EMA 结果缓存：如果 end_ts 不变且在 TTL 内则复用
        now = _utc_now_ms()
        tf_key = str(period_ms)
        key = ("close", float(span), tf_key)
        cache = self._ema_cache.setdefault(symbol, {})
        if max_age_ms is not None and max_age_ms > 0 and key in cache:
            val, cached_end_ts, computed_at = cache[key]
            if int(cached_end_ts) == int(end_ts) and (now - int(computed_at)) <= int(max_age_ms):
                return float(val)
        arr = await self.get_candles(
            symbol, start_ts=start_ts, end_ts=end_ts, max_age_ms=max_age_ms, timeframe=out_tf
        )
        if arr.size == 0:
            return float("nan")
        closes = np.asarray(arr["c"], dtype=np.float64)
        res = float(self._ema(closes, span))
        # 存入缓存
        cache[key] = (res, int(end_ts), int(now))
        return res

    async def get_ema_bounds(
        self,
        symbol: str,
        span_0: float,
        span_1: float,
        max_age_ms: Optional[int] = None,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> Tuple[float, float]:
        """返回从 span {span_0, span_1, span_2} 的 EMA 计算的 (lower, upper) 边界。

        span_2 = sqrt(span_0 * span_1)。span 视为浮点数（不舍入），
        匹配规范 EMA-alpha 公式 `2/(span+1)`。
        将时间周期和 TTL 传递给 get_latest_ema_close 并并发计算三个 EMA。
        """
        from math import isfinite

        s2 = (float(span_0) * float(span_1)) ** 0.5
        e0, e1, e2 = await asyncio.gather(
            self.get_latest_ema_close(
                symbol, span_0, max_age_ms=max_age_ms, timeframe=timeframe, tf=tf
            ),
            self.get_latest_ema_close(
                symbol, span_1, max_age_ms=max_age_ms, timeframe=timeframe, tf=tf
            ),
            self.get_latest_ema_close(symbol, s2, max_age_ms=max_age_ms, timeframe=timeframe, tf=tf),
        )
        vals = [e for e in (e0, e1, e2) if isinstance(e, (int, float)) and isfinite(float(e))]
        if not vals:
            nan = float("nan")
            return nan, nan
        return float(min(vals)), float(max(vals))

    async def get_last_prices(self, symbols: List[str], max_age_ms: int = 10_000) -> Dict[str, float]:
        """返回每个交易对当前分钟的最新收盘价。

        先使用廉价缓存遍历，然后在安全时使用一次批量行情快照，
        最后仅对剩余交易对回退到逐个 get_current_close。
        失败时返回 0.0。
        """
        out: Dict[str, float] = {}
        if not symbols:
            return out

        ordered_symbols = list(dict.fromkeys(symbols))
        now = _utc_now_ms()
        end_current = _floor_minute(now)

        for symbol in ordered_symbols:
            cached = self._get_last_price_cached_fast(
                symbol,
                now_ms=now,
                end_current_ms=end_current,
                max_age_ms=max_age_ms,
            )
            if cached is not None:
                out[symbol] = cached

        remaining = [s for s in ordered_symbols if s not in out]
        if remaining:
            bulk_prices = await self._get_last_prices_via_bulk_tickers(
                remaining,
                now_ms=now,
            )
            for symbol, price in bulk_prices.items():
                if isinstance(price, (int, float)) and np.isfinite(float(price)) and float(price) > 0.0:
                    out[symbol] = float(price)

        async def one(sym: str) -> float:
            try:
                val = await self.get_current_close(sym, max_age_ms=max_age_ms)
                return float(val) if isinstance(val, (int, float)) else 0.0
            except Exception:
                return 0.0

        remaining = [s for s in ordered_symbols if s not in out]
        tasks = {s: asyncio.create_task(one(s)) for s in remaining}
        for s, t in tasks.items():
            out[s] = await t
        return out

    def _get_last_price_cached_fast(
        self,
        symbol: str,
        *,
        now_ms: int,
        end_current_ms: int,
        max_age_ms: Optional[int],
    ) -> Optional[float]:
        """廉价的非获取式最新价格探测，用于批量 get_last_prices()。"""
        try:
            if max_age_ms is not None and max_age_ms > 0:
                prev = self._current_close_cache.get(symbol)
                if prev is not None:
                    price, updated = prev
                    if (int(now_ms) - int(updated)) <= int(max_age_ms):
                        self._log("debug", "get_last_prices_cache_hit", symbol=symbol)
                        return float(price)
        except Exception:
            pass

        try:
            arr = self._cache.get(symbol)
            if arr is None or not arr.size:
                return None
            arr_sorted = np.sort(_ensure_dtype(arr), order="ts")
            last_ts = int(arr_sorted[-1]["ts"])
            if last_ts == int(end_current_ms):
                fresh_enough = True
                if max_age_ms is not None and max_age_ms > 0:
                    last_refresh = self._get_last_refresh_ms(symbol)
                    fresh_enough = (int(now_ms) - int(last_refresh)) <= int(max_age_ms)
                if fresh_enough:
                    price = float(arr_sorted[-1]["c"])
                    self._current_close_cache[symbol] = (price, int(now_ms))
                    self._log("debug", "get_last_prices_mem_candle", symbol=symbol)
                    return price
        except Exception:
            return None
        return None

    def _bulk_last_price_tickers_allowed(self) -> bool:
        """当批量行情快照是安全的最新价格回退时返回 True。"""
        ex = str(getattr(self, "exchange_name", "") or getattr(self, "_ex_id", "") or "").lower()
        return ex not in {"hyperliquid"}

    async def _get_last_prices_via_bulk_tickers(
        self,
        symbols: List[str],
        *,
        now_ms: int,
    ) -> Dict[str, float]:
        """在安全时通过一次批量行情快照获取多个交易对的最新价格。"""
        out: Dict[str, float] = {}
        if (
            len(symbols) <= 1
            or self.exchange is None
            or not hasattr(self.exchange, "fetch_tickers")
            or not self._bulk_last_price_tickers_allowed()
        ):
            return out
        self._log("debug", "get_last_prices_bulk_tickers_try", symbols=len(symbols))
        fetched = None
        try:
            if getattr(self, "_net_sem", None) is not None:
                async with self._net_sem:  # type: ignore[attr-defined]
                    try:
                        fetched = await self.exchange.fetch_tickers(symbols)
                    except TypeError:
                        fetched = await self.exchange.fetch_tickers()
            else:
                try:
                    fetched = await self.exchange.fetch_tickers(symbols)
                except TypeError:
                    fetched = await self.exchange.fetch_tickers()
        except Exception as exc:
            self._log(
                "debug",
                "get_last_prices_bulk_tickers_failed",
                symbols=len(symbols),
                error_type=type(exc).__name__,
                error=str(exc),
            )
            return out
        if not isinstance(fetched, dict):
            return out
        hits = 0
        for symbol in symbols:
            tick = fetched.get(symbol)
            if not isinstance(tick, dict):
                continue
            raw = tick.get("last")
            if raw is None:
                raw = tick.get("close")
            if raw is None:
                raw = tick.get("bid") or tick.get("ask")
            try:
                price = float(raw)
            except (TypeError, ValueError):
                continue
            if not np.isfinite(price) or price <= 0.0:
                continue
            out[symbol] = price
            self._current_close_cache[symbol] = (price, int(now_ms))
            hits += 1
        self._log(
            "debug",
            "get_last_prices_bulk_tickers_ok",
            symbols=len(symbols),
            hits=hits,
            misses=max(0, len(symbols) - hits),
        )
        return out

    async def get_ema_bounds_many(
        self,
        items: List[Tuple[str, float, float]],
        *,
        max_age_ms: Optional[int] = 60_000,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> Dict[str, Tuple[float, float]]:
        """返回多个交易对的 EMA 边界，输入为 (symbol, span_0, span_1) 列表。

        返回映射 symbol -> (lower, upper)，使用每个交易对的 get_ema_bounds。
        """
        out: Dict[str, Tuple[float, float]] = {}
        if not items:
            return out

        async def one(sym: str, s0: float, s1: float) -> Tuple[float, float]:
            try:
                lo, hi = await self.get_ema_bounds(
                    sym, s0, s1, max_age_ms=max_age_ms, timeframe=timeframe, tf=tf
                )
                lo = float(lo) if isinstance(lo, (int, float)) else float("nan")
                hi = float(hi) if isinstance(hi, (int, float)) else float("nan")
                if not (np.isfinite(lo) and np.isfinite(hi)):
                    return (0.0, 0.0)
                return (lo, hi)
            except Exception:
                return (0.0, 0.0)

        tasks = {sym: asyncio.create_task(one(sym, s0, s1)) for (sym, s0, s1) in items}
        for sym, t in tasks.items():
            out[sym] = await t
        return out

    async def get_latest_ema_log_range_many(
        self,
        items: List[Tuple[str, float]],
        *,
        max_age_ms: Optional[int] = 600_000,
        timeframe: Optional[str] = None,
        tf: Optional[str] = "1h",
    ) -> Dict[str, float]:
        """返回每个 (symbol, span) 对的最新对数区间 EMA。

        每个 span 以所提供的时间周期（`tf` 默认为 1h）的 K 线数为单位。
        失败或非有限结果时返回 0.0。
        """
        out: Dict[str, float] = {}
        if not items:
            return out

        async def one(sym: str, span: float) -> float:
            try:
                val = await self.get_latest_ema_log_range(
                    sym,
                    span,
                    max_age_ms=max_age_ms,
                    timeframe=timeframe,
                    tf=tf,
                )
                return float(val) if np.isfinite(val) else 0.0
            except Exception:
                return 0.0

        tasks = {sym: asyncio.create_task(one(sym, span)) for (sym, span) in items}
        for sym, t in tasks.items():
            out[sym] = await t
        return out

    async def get_latest_ema_volume(
        self,
        symbol: str,
        span: float,
        max_age_ms: Optional[int] = None,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> float:
        return await self._get_latest_ema_generic(
            symbol,
            span,
            max_age_ms,
            timeframe,
            tf=tf,
            metric_key="volume",
            series_fn=lambda a: np.asarray(a["bv"], dtype=np.float64),
        )

    async def get_latest_ema_quote_volume(
        self,
        symbol: str,
        span: float,
        max_age_ms: Optional[int] = None,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> float:
        """返回最后 `span` 根已结束 K 线的报价量 EMA。

        每根 K 线的报价量近似为 base_volume * typical_price，
        其中 typical_price = (high + low + close) / 3。这是当逐笔
        VWAP 不可用时的常用近似方法。
        """
        return await self._get_latest_ema_generic(
            symbol,
            span,
            max_age_ms,
            timeframe,
            tf=tf,
            metric_key="qv",
            series_fn=lambda a: (
                np.asarray(a["bv"], dtype=np.float64)
                * (
                    np.asarray(a["h"], dtype=np.float64)
                    + np.asarray(a["l"], dtype=np.float64)
                    + np.asarray(a["c"], dtype=np.float64)
                )
                / 3.0
            ),
        )

    async def _get_latest_ema_generic(
        self,
        symbol: str,
        span: float,
        max_age_ms: Optional[int],
        timeframe: Optional[str],
        *,
        tf: Optional[str] = None,
        metric_key: str,
        series_fn,
    ) -> float:
        """EMA 辅助方法的共享实现，基于派生序列计算。

        series_fn: 接受 K 线 ndarray 并返回一维 float64 序列的可调用对象。
        metric_key: 用于 EMA 缓存中区分指标的短键（如 'volume'、'qv'）。
        """
        out_tf = timeframe if timeframe is not None else tf
        period_ms = _tf_to_ms(out_tf)
        start_ts, end_ts = await self._latest_finalized_range(span, period_ms=period_ms)
        now = _utc_now_ms()
        tf_key = str(period_ms)
        key = (metric_key, float(span), tf_key)
        cache = self._ema_cache.setdefault(symbol, {})
        if max_age_ms is not None and max_age_ms > 0 and key in cache:
            val, cached_end_ts, computed_at = cache[key]
            if int(cached_end_ts) == int(end_ts) and (now - int(computed_at)) <= int(max_age_ms):
                return float(val)
        arr = await self.get_candles(
            symbol, start_ts=start_ts, end_ts=end_ts, max_age_ms=max_age_ms, timeframe=out_tf
        )
        if arr.size == 0:
            return float("nan")
        series = series_fn(arr)
        res = float(self._ema(series, span))
        cache[key] = (res, int(end_ts), int(now))
        return res

    async def get_latest_ema_metrics(
        self,
        symbol: str,
        spans_by_metric: Dict[str, float],
        max_age_ms: Optional[int] = None,
        *,
        window_candles: Optional[int] = None,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> Dict[str, float]:
        """通过单次 K 线获取计算多个最新 EMA 指标。

        这是 get_candles() + EMA 计算的优化包装。它保留了
        get_latest_ema_* 辅助方法的逐指标行为：
        - 使用单次 `get_candles()` 调用获取最大请求 span 的超集窗口。
        - 对于 1m K 线：按指标窗口应用缺口标准化（与 get_candles() 相同）。
        - 将结果缓存在 `self._ema_cache` 中，按 (metric, span, timeframe) 键。
        """
        out: Dict[str, float] = {}
        if not spans_by_metric:
            return out

        out_tf = timeframe if timeframe is not None else tf
        period_ms = _tf_to_ms(out_tf)
        # 使用最大 span 获取超集窗口。
        max_span = max(float(s) for s in spans_by_metric.values())
        max_candles = max(1, int(math.ceil(max_span)))
        start_ts, end_ts = await self._latest_finalized_range(max_span, period_ms=period_ms)
        if window_candles is not None:
            try:
                lookback = max(1, int(window_candles))
                start_ts = int(end_ts - period_ms * (lookback - 1))
            except Exception:
                pass
        now = _utc_now_ms()
        tf_key = str(period_ms)

        cache = self._ema_cache.setdefault(symbol, {})
        missing: List[str] = []
        for metric_key, span in spans_by_metric.items():
            key = (str(metric_key), float(span), tf_key)
            if max_age_ms is not None and max_age_ms > 0 and key in cache:
                val, cached_end_ts, computed_at = cache[key]
                if int(cached_end_ts) == int(end_ts) and (now - int(computed_at)) <= int(max_age_ms):
                    out[str(metric_key)] = float(val)
                    continue
            missing.append(str(metric_key))

        if not missing:
            return out

        # 一次获取超集范围的原始 K 线。
        # 对于 1m，按指标窗口重新应用 standardize_gaps 以匹配逐调用行为。
        raw = await self.get_candles(
            symbol,
            start_ts=start_ts,
            end_ts=end_ts,
            max_age_ms=max_age_ms,
            strict=True if period_ms == ONE_MIN_MS else False,
            timeframe=out_tf,
            max_lookback_candles=window_candles,
        )
        if raw.size == 0:
            for metric_key in missing:
                out[metric_key] = float("nan")
            return out

        def series_for(metric_key: str, arr: np.ndarray) -> np.ndarray:
            if metric_key == "volume":
                return np.asarray(arr["bv"], dtype=np.float64)
            if metric_key == "qv":
                return (
                    np.asarray(arr["bv"], dtype=np.float64)
                    * (
                        np.asarray(arr["h"], dtype=np.float64)
                        + np.asarray(arr["l"], dtype=np.float64)
                        + np.asarray(arr["c"], dtype=np.float64)
                    )
                    / 3.0
                )
            if metric_key == "log_range":
                return np.log(
                    np.maximum(np.asarray(arr["h"], dtype=np.float64), 1e-12)
                    / np.maximum(np.asarray(arr["l"], dtype=np.float64), 1e-12)
                )
            if metric_key == "close":
                return np.asarray(arr["c"], dtype=np.float64)
            raise KeyError(f"Unknown EMA metric_key {metric_key!r}")

        for metric_key in missing:
            span = float(spans_by_metric[metric_key])
            span_candles = max(1, int(math.ceil(span)))
            # 获取以 end_ts 结尾的窗口。优先按尾部长度切分；如果数据短则用全部。
            tail = raw[-span_candles:] if raw.size > span_candles else raw
            if period_ms == ONE_MIN_MS:
                # 在请求的指标窗口重新应用缺口标准化。
                # 这匹配同一 [start,end] 窗口的 get_candles(strict=False) 行为。
                # tail 是已排序的 get_candles 输出的切片，因此 assume_sorted=True
                metric_start_ts = int(end_ts - period_ms * (span_candles - 1))
                tail = self.standardize_gaps(
                    tail, start_ts=metric_start_ts, end_ts=end_ts, strict=False, assume_sorted=True
                )
            if tail.size == 0:
                out[metric_key] = float("nan")
                continue
            series = series_for(metric_key, tail)
            res = float(self._ema(series, span))
            out[metric_key] = res
            cache[(metric_key, span, tf_key)] = (res, int(end_ts), int(now))

        return out

    async def get_latest_ema_log_range(
        self,
        symbol: str,
        span: float,
        max_age_ms: Optional[int] = None,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> float:
        return await self._get_latest_ema_generic(
            symbol,
            span,
            max_age_ms,
            timeframe,
            tf=tf,
            metric_key="log_range",
            series_fn=lambda a: np.log(
                np.maximum(np.asarray(a["h"], dtype=np.float64), 1e-12)
                / np.maximum(np.asarray(a["l"], dtype=np.float64), 1e-12)
            ),
        )

    # ----- EMA 序列辅助方法 -----

    async def get_ema_close_series(
        self,
        symbol: str,
        span: float,
        max_age_ms: Optional[int] = None,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> np.ndarray:
        out_tf = timeframe if timeframe is not None else tf
        period_ms = _tf_to_ms(out_tf)
        start_ts, end_ts = await self._latest_finalized_range(span, period_ms=period_ms)
        arr = await self.get_candles(
            symbol, start_ts=start_ts, end_ts=end_ts, max_age_ms=max_age_ms, timeframe=out_tf
        )
        if arr.size == 0:
            return np.empty((0,), dtype=EMA_SERIES_DTYPE)
        values = np.asarray(arr["c"], dtype=np.float64)
        ema_vals = self._ema_series(values, span)
        n = ema_vals.shape[0]
        out = np.empty((n,), dtype=EMA_SERIES_DTYPE)
        out["ts"] = np.asarray(arr["ts"], dtype=np.int64)
        out["ema"] = ema_vals.astype(np.float32, copy=False)
        return out

    async def get_ema_volume_series(
        self,
        symbol: str,
        span: float,
        max_age_ms: Optional[int] = None,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> np.ndarray:
        out_tf = timeframe if timeframe is not None else tf
        period_ms = _tf_to_ms(out_tf)
        start_ts, end_ts = await self._latest_finalized_range(span, period_ms=period_ms)
        arr = await self.get_candles(
            symbol, start_ts=start_ts, end_ts=end_ts, max_age_ms=max_age_ms, timeframe=out_tf
        )
        if arr.size == 0:
            return np.empty((0,), dtype=EMA_SERIES_DTYPE)
        values = np.asarray(arr["bv"], dtype=np.float64)
        ema_vals = self._ema_series(values, span)
        n = ema_vals.shape[0]
        out = np.empty((n,), dtype=EMA_SERIES_DTYPE)
        out["ts"] = np.asarray(arr["ts"], dtype=np.int64)
        out["ema"] = ema_vals.astype(np.float32, copy=False)
        return out

    async def get_ema_log_range_series(
        self,
        symbol: str,
        span: float,
        max_age_ms: Optional[int] = None,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> np.ndarray:
        out_tf = timeframe if timeframe is not None else tf
        period_ms = _tf_to_ms(out_tf)
        start_ts, end_ts = await self._latest_finalized_range(span, period_ms=period_ms)
        arr = await self.get_candles(
            symbol, start_ts=start_ts, end_ts=end_ts, max_age_ms=max_age_ms, timeframe=out_tf
        )
        if arr.size == 0:
            return np.empty((0,), dtype=EMA_SERIES_DTYPE)
        highs = np.asarray(arr["h"], dtype=np.float64)
        lows = np.asarray(arr["l"], dtype=np.float64)
        log_ranges = np.log(np.maximum(highs, 1e-12) / np.maximum(lows, 1e-12))
        ema_vals = self._ema_series(log_ranges, span)
        n = ema_vals.shape[0]
        out = np.empty((n,), dtype=EMA_SERIES_DTYPE)
        out["ts"] = np.asarray(arr["ts"], dtype=np.int64)
        out["ema"] = ema_vals.astype(np.float32, copy=False)
        return out

    # ----- 预热和刷新 -----

    async def warmup_since(self, symbols, since_ts: int) -> None:
        """从指定时间戳起回填/预热交易对（测试中网络操作为空操作）。"""
        tasks = [self.refresh(sym, through_ts=None) for sym in symbols]
        # 按顺序执行以匹配测试 monkeypatch 预期
        for t in tasks:
            await t

    async def refresh(self, symbol: str, through_ts: Optional[int] = None) -> None:
        """获取新 K 线并合并到缓存。

        - 按 `overlap_candles` 重叠
        - 排除当前进行中的分钟
        - 如果 `self.exchange` 为 None 则无操作
        """
        if self.exchange is None:
            return None

        now = _utc_now_ms()
        end_exclusive = _floor_minute(now)
        if through_ts is not None:
            end_exclusive = min(end_exclusive, _floor_minute(int(through_ts)) + ONE_MIN_MS)

        # 刷新仅需要协调最近的磁盘 K 线以避免不必要的
        # 全历史加载/排序。历史范围通过 get_candles() 按需处理。
        lookback_candles = max(int(self.default_window_candles), int(self.overlap_candles)) + 10
        disk_since = max(0, int(end_exclusive) - int(lookback_candles) * ONE_MIN_MS)

        try:
            self._load_from_disk(symbol, disk_since, end_exclusive, timeframe="1m")
        except Exception:
            pass

        existing = self._ensure_symbol_cache(symbol)
        existing_last_ts = (
            int(np.asarray(existing["ts"], dtype=np.int64).max()) if existing.size else None
        )
        if existing.size == 0:
            proposed_since = end_exclusive - self.default_window_candles * ONE_MIN_MS
        else:
            last_ts = existing_last_ts if existing_last_ts is not None else 0
            if last_ts >= end_exclusive - ONE_MIN_MS:
                self._log(
                    "debug",
                    "refresh_skip_fresh",
                    symbol=symbol,
                    end_exclusive=end_exclusive,
                    last_ts=last_ts,
                )
                return None
            proposed_since = max(0, last_ts - self.overlap_candles * ONE_MIN_MS)

        if proposed_since >= end_exclusive:
            self._log(
                "debug",
                "refresh_skip_since",
                symbol=symbol,
                since=proposed_since,
                end_exclusive=end_exclusive,
            )
            return None

        async with self._acquire_fetch_lock(symbol, "1m"):
            # 加锁后重新评估，以防另一进程已经获取。
            try:
                self._load_from_disk(symbol, disk_since, end_exclusive, timeframe="1m")
            except Exception:
                pass

            existing = self._ensure_symbol_cache(symbol)
            existing_last_ts = (
                int(np.asarray(existing["ts"], dtype=np.int64).max()) if existing.size else None
            )
            if existing.size == 0:
                since = end_exclusive - self.default_window_candles * ONE_MIN_MS
            else:
                last_ts = existing_last_ts if existing_last_ts is not None else 0
                if last_ts >= end_exclusive - ONE_MIN_MS:
                    self._log(
                        "debug",
                        "refresh_skip_fresh",
                        symbol=symbol,
                        end_exclusive=end_exclusive,
                        last_ts=last_ts,
                    )
                    return None
                since = max(0, last_ts - self.overlap_candles * ONE_MIN_MS)

            if since >= end_exclusive:
                self._log(
                    "debug",
                    "refresh_skip_since",
                    symbol=symbol,
                    since=since,
                    end_exclusive=end_exclusive,
                )
                return None

            persisted_batches = False
            now_fetch = _utc_now_ms()
            self._log(
                "debug",
                "refresh_fetch",
                symbol=symbol,
                since=since,
                end_exclusive=end_exclusive,
                existing_last_ts=existing_last_ts,
            )

            def _persist_refresh_batch(batch: np.ndarray) -> None:
                nonlocal persisted_batches
                persisted_batches = True
                self._persist_batch(
                    symbol,
                    batch,
                    timeframe="1m",
                    merge_cache=True,
                    last_refresh_ms=now_fetch,
                )

            try:
                new_arr = await self._fetch_ohlcv_paginated(
                    symbol,
                    since,
                    end_exclusive,
                    on_batch=_persist_refresh_batch,
                )
            except TypeError:
                new_arr = await self._fetch_ohlcv_paginated(symbol, since, end_exclusive)
            if new_arr.size == 0:
                # 即使没有填充也保持已结束的运行时 K 线连续。
                self._materialize_runtime_synthetic_gap(symbol, end_exclusive - ONE_MIN_MS)
                return None
            if not persisted_batches:
                self._persist_batch(
                    symbol,
                    new_arr,
                    timeframe="1m",
                    merge_cache=True,
                    last_refresh_ms=now_fetch,
                )
            return None

    # ----- 持久化 -----

    def _save_shard(
        self,
        symbol: str,
        date_key: str,
        array: np.ndarray,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
        defer_index: bool = False,
    ) -> None:
        """将分片保存为 .npy 并原子性更新 index.json。

        参数
        ----------
        symbol : str
            交易对。
        date_key : str
            用作分片文件名的 YYYY-MM-DD 字符串。
        array : np.ndarray
            要写入的 dtype 为 CANDLE_DTYPE 的结构化数组。
        defer_index : bool
            如果为 True，跳过写入 index.json（调用者必须稍后调用 flush_deferred_index）。
        """
        arr = _ensure_dtype(array)
        if arr.size == 0:
            return

        arr = np.sort(arr, order="ts")
        data_bytes = arr.tobytes()
        crc = int(zlib.crc32(data_bytes) & 0xFFFFFFFF)

        tf_norm = self._normalize_timeframe_arg(timeframe, tf)

        # 如果旧版已有连续的 1m 日分片，跳过写入此主分片。
        # 主分片仅用于填充旧版缺口。
        if tf_norm == "1m":
            try:
                if self._legacy_day_is_complete(symbol, tf_norm, date_key):
                    return
            except (
                Exception
            ) as exc:  # 尽力而为；旧版缓存可能不可读，回退到主分片写入
                try:
                    err_type = type(exc).__name__
                    err_repr = repr(exc)
                except Exception:
                    err_type = "Exception"
                    err_repr = "<unrepresentable exception>"
                self._log(
                    "warning",
                    "legacy_day_quality_check_failed",
                    symbol=symbol,
                    timeframe=tf_norm,
                    day=date_key,
                    error_type=err_type,
                    error=err_repr,
                )
        shard_path = self._shard_path(symbol, date_key, tf=tf_norm)
        os.makedirs(os.path.dirname(shard_path), exist_ok=True)
        # 原子性写入 .npy 内容
        # 使用 numpy.save 确保 .npy 格式，先写入临时路径再替换
        tmp_path = f"{shard_path}.tmp"
        with open(tmp_path, "wb") as f:
            np.save(f, arr)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, shard_path)

        # 直接更新分片路径缓存而非失效（避免重新扫描）
        cache_key = (symbol, tf_norm)
        if cache_key in self._shard_paths_cache:
            self._shard_paths_cache[cache_key][date_key] = shard_path

        # 更新内存索引
        idx = self._ensure_symbol_index(symbol, tf=tf_norm)
        shards = idx.setdefault("shards", {})
        shards[date_key] = {
            "path": shard_path,
            "min_ts": int(arr[0]["ts"]),
            "max_ts": int(arr[-1]["ts"]),
            "count": int(arr.shape[0]),
            "crc32": crc,
        }
        key = f"{symbol}::{tf_norm}"
        self._index[key] = idx

        # 除非延迟否则将索引写入磁盘
        if not defer_index:
            self._save_index(symbol, tf=tf_norm)
            # 写入此分片后按时间周期执行磁盘保留策略
            try:
                self._enforce_disk_retention(symbol, tf=tf_norm)
            except Exception:
                pass

    def flush_deferred_index(
        self,
        symbol: str,
        *,
        timeframe: Optional[str] = None,
        tf: Optional[str] = None,
    ) -> None:
        """将交易对的任何延迟索引更新刷新到磁盘。"""
        tf_norm = self._normalize_timeframe_arg(timeframe, tf)
        self._save_index(symbol, tf=tf_norm)
        try:
            self._enforce_disk_retention(symbol, tf=tf_norm)
        except Exception:
            pass

    # ----- 上下文管理器和关闭 -----

    async def aclose(self) -> None:
        """异步关闭：刷新并关闭资源，包括 HTTP 会话。"""
        await self._close_http_session()

    def close(self) -> None:
        """同步关闭：如果事件循环正在运行则尝试关闭 HTTP 会话。"""
        # 尝试同步关闭 HTTP 会话
        if self._http_session is not None and not self._http_session.closed:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    # 安排清理但不等待
                    asyncio.create_task(self._close_http_session())
                else:
                    loop.run_until_complete(self._close_http_session())
            except Exception:
                pass  # 尽力清理

    def __enter__(self):  # pragma: no cover - not exercised by tests
        return self

    def __exit__(self, exc_type, exc, tb):  # pragma: no cover - not exercised by tests
        self.close()
        return False

    async def __aenter__(self):  # pragma: no cover
        return self

    async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover
        await self.aclose()
        return False


__all__ = [
    "CandlestickManager",
    "CANDLE_DTYPE",
    "ONE_MIN_MS",
    "_floor_minute",
]
