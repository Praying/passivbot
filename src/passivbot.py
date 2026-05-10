from __future__ import annotations
import os
import time

# 修复 Windows 上的崩溃问题
from tools.event_loop_policy import set_windows_event_loop_policy

set_windows_event_loop_policy()

from ccxt.base import errors as ccxt_errors
import random
import traceback
import argparse
import asyncio
import json
import sys
import signal
import hjson
import bisect
import pprint
import numpy as np
import inspect
import passivbot_rust as pbr
import logging
import math
from pathlib import Path
from cli_utils import (
    add_help_all_argument,
    build_command_parser,
    expand_help_all_argv,
    get_cli_prog,
    help_all_requested,
)
from candlestick_manager import CandlestickManager, CANDLE_DTYPE, synthesize_1m_from_higher_tf
from fill_events_manager import (
    FillEventsManager,
    _build_fetcher_for_bot,
    _extract_symbol_pool,
    compute_psize_pprice,
)
from monitor_publisher import MonitorPublisher
from passivbot_exceptions import RestartBotException, FatalBotException
import passivbot_hsl as pb_hsl
import passivbot_monitor as pb_monitor
from typing import Dict, Iterable, Tuple, List, Optional, Any, Callable
from config import get_template_config, load_input_config, prepare_config
from config.access import (
    get_optional_config_value,
    get_optional_live_value,
    require_config_value,
    require_live_value,
)
from config.coerce import (
    normalize_hsl_cooldown_position_policy,
    normalize_hsl_signal_mode,
)
from config.pnl_lookback import parse_pnls_max_lookback_days
from config.overrides import parse_overrides
from logging_setup import (
    configure_logging,
    get_last_log_activity_monotonic,
    resolve_live_log_file_settings,
    resolve_log_level,
)
from utils import (
    load_markets,
    coin_to_symbol,
    symbol_to_coin,
    utc_ms,
    ts_to_date,
    make_get_filepath,
    format_approved_ignored_coins,
    filter_markets,
    to_ccxt_exchange_id,
    coin_symbol_warning_counts,
    _coins_source_side_is_all,
    normalize_coins_source,
)
from prettytable import PrettyTable
from uuid import uuid4
from copy import deepcopy
from dataclasses import dataclass
from collections import defaultdict, Counter
from sortedcontainers import SortedDict

try:
    import psutil  # type: ignore
except Exception:
    psutil = None

try:
    import resource  # type: ignore
except Exception:
    resource = None
from config_utils import (
    add_config_arguments,
    update_config_with_args,
    expand_PB_mode,
    merge_negative_cli_values,
)
from procedures import (
    load_broker_code,
    load_user_info,
    get_first_timestamps_unified,
    print_async_exception,
)
from utils import get_file_mod_ms
from warmup_utils import compute_per_coin_warmup_minutes
import re

NetworkError = ccxt_errors.NetworkError
RateLimitExceeded = ccxt_errors.RateLimitExceeded
# 某些隔离测试在没有 RequestTimeout 的情况下 stub ccxt.base.errors；
# 当专用符号不存在时，将其视为 NetworkError 类的瞬态启动错误。
RequestTimeout = getattr(ccxt_errors, "RequestTimeout", NetworkError)

# 仅编排器模式：理想订单通过 Rust 编排器（JSON API）计算。
# 传统 Python 订单计算路径已在本分支中移除。

FOREIGN_PASSIVBOT_LOOKBACK_MS = 24 * 60 * 60 * 1000
FOREIGN_PASSIVBOT_GRACE_MS = 15_000
FOREIGN_PASSIVBOT_WINDOW_MS = 60 * 60 * 1000
FOREIGN_PASSIVBOT_MAX_UNIQUE_PER_WINDOW = 3
FOREIGN_PASSIVBOT_FINGERPRINT_MATCH_MS = 5 * 60 * 1000

from custom_endpoint_overrides import (
    apply_rest_overrides_to_ccxt,
    configure_custom_endpoint_loader,
    get_custom_endpoint_source,
    load_custom_endpoint_config,
    resolve_custom_endpoint_override,
)

calc_min_entry_qty = pbr.calc_min_entry_qty_py
round_ = pbr.round_
round_up = pbr.round_up
round_dn = pbr.round_dn
round_dynamic = pbr.round_dynamic
calc_order_price_diff = pbr.calc_order_price_diff

DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL = 20_000
PARTIAL_FILL_MERGE_MAX_DELAY_MS = 60_000
FILL_EVENT_FETCH_OVERLAP_COUNT = 20
FILL_EVENT_FETCH_OVERLAP_MAX_MS = 86_400_000  # 24 小时
FILL_EVENT_FETCH_LIMIT_DEFAULT = 20


# 匹配任意位置的 "...0xABCD..."（不区分大小写）
_TYPE_MARKER_RE = re.compile(r"0x([0-9a-fA-F]{4})", re.IGNORECASE)
# 前导纯十六进制回退：可选 0x 后跟 4 位十六进制，位于字符串起始处
_LEADING_HEX4_RE = re.compile(r"^(?:0x)?([0-9a-fA-F]{4})", re.IGNORECASE)


def _get_process_rss_bytes() -> Optional[int]:
    """返回当前进程的 RSS 字节数，若不可用则返回 None。"""
    try:
        if psutil is not None:
            return int(psutil.Process(os.getpid()).memory_info().rss)
    except Exception:
        pass
    if resource is not None:
        try:
            usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if sys.platform.startswith("linux"):
                usage = int(usage) * 1024
            else:
                usage = int(usage)
            return int(usage)
        except Exception:
            pass
    return None


def clip_by_timestamp(xs, start_ts, end_ts):
    # 假设 xs 已按时间戳排序
    timestamps = [x["timestamp"] for x in xs]
    i0 = bisect.bisect_left(timestamps, start_ts) if start_ts else 0
    i1 = bisect.bisect_right(timestamps, end_ts) if end_ts else len(xs)
    return xs[i0:i1]


def custom_id_to_snake(custom_id) -> str:
    """将经纪商自定义 ID 转换为 snake_case 订单类型名称。"""
    try:
        return snake_of(try_decode_type_id_from_custom_id(custom_id))
    except Exception as e:
        logging.error(f"failed to convert custom_id {custom_id} to str order_type")
        return "unknown"


def try_decode_type_id_from_custom_id(custom_id: str) -> int | None:
    """从自定义订单 ID 字符串中提取编码的 16 位订单类型 ID。"""
    # 1) 首选：在任意位置查找 "...0x<4-hex>..."
    m = _TYPE_MARKER_RE.search(custom_id)
    if m:
        return int(m.group(1), 16)

    # 2) 回退：若字符串为纯十六进制风格（无经纪商代码），解析前导 4 位
    m = _LEADING_HEX4_RE.match(custom_id)
    if m:
        return int(m.group(1), 16)

    return None


def custom_id_has_explicit_passivbot_marker(custom_id) -> bool:
    """仅当自定义 ID 包含显式 0xABCD Passivbot 标记时返回 True。"""
    try:
        return bool(_TYPE_MARKER_RE.search(str(custom_id)))
    except Exception:
        return False


def order_type_id_to_hex4(type_id: int) -> str:
    """返回订单类型 ID 的四位十六进制表示。"""
    return f"{type_id:04x}"


def type_token(type_id: int, with_marker: bool = True) -> str:
    """返回可打印的订单类型标记，可选添加 `0x` 前缀。"""
    h4 = order_type_id_to_hex4(type_id)
    return ("0x" + h4) if with_marker else h4


def snake_of(type_id: int) -> str:
    """将订单类型 ID 映射为其 snake_case 字符串表示。"""
    try:
        return pbr.order_type_id_to_snake(type_id)
    except Exception:
        return "unknown"


# 传统 EMA 辅助函数已移除；CandlestickManager 提供 EMA 工具


def _trailing_bundle_tuple_to_dict(bundle_tuple: tuple[float, float, float, float]) -> dict:
    min_since_open, max_since_min, max_since_open, min_since_max = bundle_tuple
    return {
        "min_since_open": float(min_since_open),
        "max_since_min": float(max_since_min),
        "max_since_open": float(max_since_open),
        "min_since_max": float(min_since_max),
    }


def _trailing_bundle_default_dict() -> dict:
    return _trailing_bundle_tuple_to_dict(pbr.trailing_bundle_default_py())


def _trailing_bundle_from_arrays(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray) -> dict:
    if highs.size == 0:
        return _trailing_bundle_default_dict()
    bundle_tuple = pbr.update_trailing_bundle_py(
        np.asarray(highs, dtype=np.float64),
        np.asarray(lows, dtype=np.float64),
        np.asarray(closes, dtype=np.float64),
        bundle=None,
    )
    return _trailing_bundle_tuple_to_dict(bundle_tuple)


def calc_pnl(position_side, entry_price, close_price, qty, inverse, c_mult):
    """通过调用相应的 Rust 辅助函数计算交易 PnL。"""
    try:
        if isinstance(position_side, str):
            if position_side == "long":
                return pbr.calc_pnl_long(entry_price, close_price, qty, c_mult)
            else:
                return pbr.calc_pnl_short(entry_price, close_price, qty, c_mult)
        else:
            # 回退：假设为多头
            return pbr.calc_pnl_long(entry_price, close_price, qty, c_mult)
    except Exception:
        # 重新抛出以保持原有行为
        raise


def order_market_diff(side: str, order_price: float, market_price: float) -> float:
    """返回订单与市价之间的方向感知相对价格差。"""
    return float(calc_order_price_diff(side, float(order_price), float(market_price)))


from pure_funcs import (
    numpyize,
    denumpyize,
    filter_orders,
    multi_replace,
    shorten_custom_id,
    determine_side_from_order_tuple,
    str2bool,
    flatten,
    log_dict_changes,
    ensure_millis,
)

ONE_MIN_MS = 60_000


def signal_handler(sig, frame):
    """处理 SIGINT 信号，指示正在运行的机器人优雅停止。"""
    print("\nReceived shutdown signal. Stopping bot...")
    bot = globals().get("bot")
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = None

    if bot is not None:
        bot.stop_signal_received = True
        if loop is not None:
            shutdown_task = getattr(bot, "_shutdown_task", None)
            if shutdown_task is None or shutdown_task.done():
                bot._shutdown_task = loop.create_task(bot.shutdown_gracefully())
            loop.call_soon_threadsafe(lambda: None)
    elif loop is not None:
        loop.call_soon_threadsafe(loop.stop)


signal.signal(signal.SIGINT, signal_handler)


def get_function_name():
    """返回当前作用域上一层的调用者函数名。"""
    return inspect.currentframe().f_back.f_code.co_name


def get_caller_name():
    """返回当前作用域上两层的调用者名称。"""
    return inspect.currentframe().f_back.f_back.f_code.co_name


def or_default(f, *args, default=None, **kwargs):
    """安全执行 `f`，若抛出异常则返回 `default`。"""
    try:
        return f(*args, **kwargs)
    except:
        return default


def orders_matching(o0, o1, tolerance_qty=0.01, tolerance_price=0.002):
    """若两个订单在指定容差范围内等价则返回 True。"""
    for k in ["symbol", "side", "position_side"]:
        if o0[k] != o1[k]:
            return False
    if tolerance_price:
        if abs(o0["price"] - o1["price"]) / o0["price"] > tolerance_price:
            return False
    else:
        if o0["price"] != o1["price"]:
            return False
    if tolerance_qty:
        if abs(o0["qty"] - o1["qty"]) / o0["qty"] > tolerance_qty:
            return False
    else:
        if o0["qty"] != o1["qty"]:
            return False
    return True


def order_has_match(order, orders, tolerance_qty=0.01, tolerance_price=0.002):
    """返回 `orders` 中第一个匹配的订单，若无匹配则返回 False。"""
    for elm in orders:
        if orders_matching(order, elm, tolerance_qty, tolerance_price):
            return elm
    return False


def compute_live_warmup_windows(
    symbols_by_side: Dict[str, set],
    bp_lookup: Callable[[str, str, str], float],
    *,
    forager_enabled: Optional[Dict[str, bool]] = None,
    window_candles: Optional[int] = None,
    warmup_ratio: float = 0.0,
    max_warmup_minutes: Optional[int] = None,
    span_buffer: Optional[float] = None,
    large_span_threshold: int = 2 * 24 * 60,
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, bool]]:
    """返回每个交易对的 1m/1h K线预热窗口。"""
    symbols: set = set()
    for symset in symbols_by_side.values():
        symbols.update(symset or set())

    per_symbol_win: Dict[str, int] = {}
    per_symbol_h1_hours: Dict[str, int] = {}
    per_symbol_skip_historical: Dict[str, bool] = {}

    if not symbols:
        return per_symbol_win, per_symbol_h1_hours, per_symbol_skip_historical

    if forager_enabled is None:
        forager_enabled = {}
    is_forager_long = bool(forager_enabled.get("long"))
    is_forager_short = bool(forager_enabled.get("short"))

    if span_buffer is None:
        try:
            ratio = float(warmup_ratio)
        except Exception:
            ratio = 0.0
        span_buffer = 1.0 + max(0.0, ratio)

    cap_minutes = None
    try:
        cap_minutes = int(max_warmup_minutes) if max_warmup_minutes is not None else None
    except Exception:
        cap_minutes = None
    if cap_minutes is not None and cap_minutes <= 0:
        cap_minutes = None
    cap_hours = None
    if cap_minutes is not None:
        cap_hours = max(1, int(math.ceil(cap_minutes / 60.0)))

    def _to_float(val) -> float:
        try:
            return float(val)
        except Exception:
            return 0.0

    def _get_bp(pside: str, key: str, sym: str) -> float:
        try:
            return _to_float(bp_lookup(pside, key, sym))
        except Exception:
            return 0.0

    if window_candles is not None:
        win = max(1, int(window_candles))
        if cap_minutes is not None:
            win = min(win, cap_minutes)
        h1_hours = max(1, int(math.ceil(win / 60.0)))
        if cap_hours is not None:
            h1_hours = min(h1_hours, cap_hours)
        skip_historical = win <= large_span_threshold
        for sym in sorted(symbols):
            per_symbol_win[sym] = win
            per_symbol_h1_hours[sym] = h1_hours
            per_symbol_skip_historical[sym] = skip_historical
        return per_symbol_win, per_symbol_h1_hours, per_symbol_skip_historical

    for sym in sorted(symbols):
        max_1m_span = 0.0
        max_h1_span = 0.0
        for pside in ("long", "short"):
            if sym not in symbols_by_side.get(pside, set()):
                continue
            max_1m_span = max(
                max_1m_span,
                _get_bp(pside, "ema_span_0", sym),
                _get_bp(pside, "ema_span_1", sym),
            )
            if (pside == "long" and is_forager_long) or (pside == "short" and is_forager_short):
                max_1m_span = max(
                    max_1m_span,
                    _get_bp(pside, "forager_volume_ema_span", sym),
                    _get_bp(pside, "forager_volatility_ema_span", sym),
                )
            max_h1_span = max(max_h1_span, _get_bp(pside, "entry_volatility_ema_span_hours", sym))

        if max_1m_span > 0.0:
            win = int(math.ceil(max_1m_span * span_buffer))
        else:
            win = 1
        win = max(1, win)
        if cap_minutes is not None:
            win = min(win, cap_minutes)
        per_symbol_win[sym] = win
        per_symbol_skip_historical[sym] = win <= large_span_threshold

        if max_h1_span > 0.0:
            h1_hours = max(1, int(math.ceil(max_h1_span * span_buffer)))
            if cap_hours is not None:
                h1_hours = min(h1_hours, cap_hours)
            per_symbol_h1_hours[sym] = h1_hours
        else:
            per_symbol_h1_hours[sym] = 0

    return per_symbol_win, per_symbol_h1_hours, per_symbol_skip_historical


class Passivbot:
    """Passivbot 交易机器人核心类，负责配置加载、状态管理、订单编排与执行。"""

    def __init__(self, config: dict):
        """初始化机器人：加载配置、用户上下文和运行时缓存。"""
        self.config = config
        try:
            lvl_raw = get_optional_config_value(config, "logging.level", 1)
            lvl = int(float(lvl_raw)) if lvl_raw is not None else 1
        except Exception:
            lvl = 1
        self.logging_level = max(0, min(int(lvl), 3))
        self.user = require_live_value(config, "user")
        self.user_info = load_user_info(self.user)
        self.exchange = self.user_info["exchange"]
        self.broker_code = load_broker_code(self.user_info["exchange"])
        self.exchange_ccxt_id = to_ccxt_exchange_id(self.exchange)
        self.endpoint_override = resolve_custom_endpoint_override(self.exchange_ccxt_id)
        self.ws_enabled = True
        if self.endpoint_override:
            self.ws_enabled = not self.endpoint_override.disable_ws
            source_path = get_custom_endpoint_source()
            logging.info(
                "Custom endpoint override active for %s (disable_ws=%s, source=%s)",
                self.exchange_ccxt_id,
                self.endpoint_override.disable_ws,
                source_path if source_path else "auto-discovered",
            )
        self.custom_id_max_length = 36
        self.sym_padding = 17
        self.action_str_max_len = max(
            len(a)
            for a in [
                "posting order",
                "cancelling order",
                "removed order",
                "added order",
            ]
        )
        self.order_details_str_len = 34
        self.order_type_str_len = 32
        self.stop_websocket = False
        raw_balance_override = get_optional_live_value(self.config, "balance_override", None)
        self.balance_override = (
            None if raw_balance_override in (None, "") else float(raw_balance_override)
        )
        self._balance_override_logged = False
        self.balance = 1e-12
        self.balance_raw = 1e-12
        self.previous_hysteresis_balance = None
        self.balance_hysteresis_snap_pct = float(
            get_optional_live_value(self.config, "balance_hysteresis_snap_pct", 0.02)
        )
        # hedge_mode 控制同一币种是否允许同时做多做空。
        # 此为配置层设置；交易所特定的子类可将 self.hedge_mode 覆盖为 False，
        # 若交易所不支持双向模式。
        # 有效 hedge_mode = 配置设置 AND 交易所能力。
        self._config_hedge_mode = bool(get_optional_live_value(self.config, "hedge_mode", True))
        self.hedge_mode = True  # 交易所能力，子类可覆盖
        self.inverse = False
        self.active_symbols = []
        self.fetched_positions = []
        self.fetched_open_orders = []
        self.open_orders = {}
        self.positions = {}
        self.symbol_ids = {}
        self.min_costs = {}
        self.min_qtys = {}
        self.qty_steps = {}
        self.price_steps = {}
        self.c_mults = {}
        self.max_leverage = {}
        self.pside_int_map = {"long": 0, "short": 1}
        self.PB_modes = {"long": {}, "short": {}}
        # 传统 pnls_cache_filepath 已移除；FillEventsManager 处理缓存
        self.quote = "USDT"

        self.minimum_market_age_millis = (
            float(require_live_value(config, "minimum_coin_age_days")) * 24 * 60 * 60 * 1000
        )
        # 传统 EMA 缓存已移除；使用 CandlestickManager EMA 辅助函数
        # 传统 ohlcvs_1m 字段已移除，改用 CandlestickManager
        self.stop_signal_received = False
        self.cca = None
        self.ccp = None
        self.create_ccxt_sessions()
        self.debug_mode = False
        self.balance_threshold = 1.0  # 余额低于阈值时不创建订单
        self.hyst_pct = 0.02
        self.state_change_detected_by_symbol = set()
        self.recent_order_executions = []
        self.recent_order_cancellations = []
        self._disabled_psides_logged = set()
        self._last_coin_symbol_warning_counts = {
            "symbol_to_coin_fallbacks": 0,
            "coin_to_symbol_fallbacks": 0,
        }
        self._last_plan_detail: dict[str, tuple[int, int, int]] = {}
        self._last_action_summary: dict[tuple[str, str], str] = {}
        self.start_time_ms = utc_ms()
        self.bot_start_exchange_ts = int(self.get_exchange_time())
        self.orders_emitted_to_exchange: list[dict] = []
        self.foreign_passivbot_seen: dict[str, int] = {}
        self._foreign_passivbot_stop_requested = False
        self._bot_ready = False
        self._monitor_last_equity = float(self.balance_raw)
        self._monitor_stop_emitted = False
        self.monitor_publisher: Optional[MonitorPublisher] = None
        self.monitor_enabled = bool(get_optional_config_value(config, "monitor.enabled", False))
        if self.monitor_enabled:
            try:
                self.monitor_publisher = MonitorPublisher.from_config(
                    exchange=self.exchange,
                    user=self.user,
                    config=require_config_value(config, "monitor"),
                )
            except Exception as exc:
                logging.error("[monitor] failed to initialize monitor publisher: %s", exc)
                self.monitor_publisher = None
        # CandlestickManager settings from config.live
        # Use denormalized exchange name for cache paths (e.g., "binance" not "binanceusdm")
        cm_kwargs = {
            "exchange": self.cca,
            "exchange_name": self.exchange,
            "debug": self.logging_level,
        }
        mem_cap_raw = require_live_value(config, "max_memory_candles_per_symbol")
        mem_cap_effective = DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL
        try:
            if mem_cap_raw is not None:
                mem_cap_effective = int(float(mem_cap_raw))
        except Exception:
            logging.warning(
                "Unable to parse live.max_memory_candles_per_symbol=%r, using default %d",
                mem_cap_raw,
                DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL,
            )
            mem_cap_effective = DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL
        if mem_cap_effective <= 0:
            logging.warning(
                "live.max_memory_candles_per_symbol=%r is non-positive; using default %d",
                mem_cap_raw,
                DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL,
            )
            mem_cap_effective = DEFAULT_MAX_MEMORY_CANDLES_PER_SYMBOL
        cm_kwargs["max_memory_candles_per_symbol"] = mem_cap_effective
        disk_cap = require_live_value(config, "max_disk_candles_per_symbol_per_tf")
        if disk_cap is not None:
            cm_kwargs["max_disk_candles_per_symbol_per_tf"] = int(disk_cap)
        lock_timeout = get_optional_live_value(config, "candle_lock_timeout_seconds", None)
        if lock_timeout not in (None, ""):
            try:
                cm_kwargs["lock_timeout_seconds"] = float(lock_timeout)
            except Exception:
                logging.warning(
                    "Unable to parse live.candle_lock_timeout_seconds=%r; using default",
                    lock_timeout,
                )
        max_concurrent = get_optional_live_value(config, "max_concurrent_api_requests", None)
        if max_concurrent not in (None, "", 0):
            try:
                cm_kwargs["max_concurrent_requests"] = int(max_concurrent)
            except Exception:
                logging.warning(
                    "Unable to parse live.max_concurrent_api_requests=%r; ignoring",
                    max_concurrent,
                )
        raw_page_debug = get_optional_config_value(config, "logging.candle_page_debug_symbols", None)
        page_debug_symbols = []
        if raw_page_debug not in (None, "", []):
            if isinstance(raw_page_debug, str):
                raw = raw_page_debug.strip()
                if raw:
                    if raw == "*":
                        page_debug_symbols = ["*"]
                    else:
                        raw = raw.replace(",", " ").replace(";", " ")
                        page_debug_symbols = [s for s in raw.split() if s]
            elif isinstance(raw_page_debug, (list, tuple, set)):
                page_debug_symbols = [str(s) for s in raw_page_debug if s]
            if page_debug_symbols:
                cm_kwargs["page_debug_symbols"] = page_debug_symbols
        # Archive fetching: disabled by default for live bots (avoids timeout issues)
        # Set live.enable_archive_candle_fetch=true to enable if needed
        archive_enabled = get_optional_live_value(config, "enable_archive_candle_fetch", False)
        cm_kwargs["archive_enabled"] = bool(archive_enabled)
        self.cm = CandlestickManager(**cm_kwargs)
        if self.monitor_publisher is not None:
            self.cm.set_persist_batch_observer(self._monitor_handle_candlestick_persist)
        # TTL (minutes) for EMA candles on non-traded symbols
        ttl_min = require_live_value(config, "inactive_coin_candle_ttl_minutes")
        self.inactive_coin_candle_ttl_ms = int(float(ttl_min) * 60_000)
        raw_mem_interval = get_optional_config_value(
            config, "logging.memory_snapshot_interval_minutes", 30.0
        )
        try:
            interval_minutes = float(raw_mem_interval)
        except Exception:
            logging.warning(
                "Unable to parse logging.memory_snapshot_interval_minutes=%r; using fallback 30",
                raw_mem_interval,
            )
            interval_minutes = 30.0
        if interval_minutes <= 0.0:
            logging.warning(
                "logging.memory_snapshot_interval_minutes=%r is non-positive; using fallback 30",
                raw_mem_interval,
            )
            interval_minutes = 30.0
        self.memory_snapshot_interval_ms = max(60_000, int(interval_minutes * 60_000))
        raw_volume_threshold = get_optional_config_value(
            config, "logging.volume_refresh_info_threshold_seconds", 30.0
        )
        try:
            volume_threshold = float(raw_volume_threshold)
        except Exception:
            logging.warning(
                "Unable to parse logging.volume_refresh_info_threshold_seconds=%r; using fallback 30",
                raw_volume_threshold,
            )
            volume_threshold = 30.0
        if volume_threshold < 0:
            logging.warning(
                "logging.volume_refresh_info_threshold_seconds=%r is negative; using 0",
                raw_volume_threshold,
            )
            volume_threshold = 0.0
        self.volume_refresh_info_threshold_seconds = float(volume_threshold)
        raw_candle_check_interval = get_optional_config_value(
            config, "logging.candle_disk_check_interval_minutes", 60.0
        )
        try:
            candle_check_minutes = float(raw_candle_check_interval)
        except Exception:
            logging.warning(
                "Unable to parse logging.candle_disk_check_interval_minutes=%r; using fallback 60",
                raw_candle_check_interval,
            )
            candle_check_minutes = 60.0
        if candle_check_minutes < 0:
            logging.warning(
                "logging.candle_disk_check_interval_minutes=%r is negative; disabling",
                raw_candle_check_interval,
            )
            candle_check_minutes = 0.0
        self.candle_disk_check_interval_ms = int(candle_check_minutes * 60_000)
        raw_tail_slack_min = get_optional_config_value(
            config, "logging.candle_disk_check_tail_slack_minutes", 1.0
        )
        try:
            tail_slack_min = float(raw_tail_slack_min)
        except Exception:
            logging.warning(
                "Unable to parse logging.candle_disk_check_tail_slack_minutes=%r; using 1",
                raw_tail_slack_min,
            )
            tail_slack_min = 1.0
        if tail_slack_min < 0:
            tail_slack_min = 0.0
        self.candle_disk_check_tail_slack_ms = int(tail_slack_min * 60_000)
        raw_tail_slack_hours = get_optional_config_value(
            config, "logging.candle_disk_check_tail_slack_hours", 1.0
        )
        try:
            tail_slack_hours = float(raw_tail_slack_hours)
        except Exception:
            logging.warning(
                "Unable to parse logging.candle_disk_check_tail_slack_hours=%r; using 1",
                raw_tail_slack_hours,
            )
            tail_slack_hours = 1.0
        if tail_slack_hours < 0:
            tail_slack_hours = 0.0
        self.candle_disk_check_tail_slack_hour_ms = int(tail_slack_hours * 60 * 60_000)
        self._candle_disk_check_last_ms = 0
        auto_gs = bool(self.live_value("auto_gs"))
        self.PB_mode_stop = {
            "long": "graceful_stop" if auto_gs else "manual",
            "short": "graceful_stop" if auto_gs else "manual",
        }

        # FillEventsManager 用于 PnL 跟踪（替代传统 self.pnls 列表）
        self._pnls_manager: Optional[FillEventsManager] = None
        self._pnls_initialized = False

        # Health tracking for periodic summary
        self._health_start_ms = utc_ms()
        self._health_orders_placed = 0
        self._health_orders_cancelled = 0
        self._health_fills = 0
        self._health_pnl = 0.0  # sum of realized PnL from fills
        self._health_errors = 0
        self._health_ws_reconnects = 0
        self._health_rate_limits = 0
        self._health_last_summary_ms = 0
        self._health_summary_interval_ms = 15 * 60 * 1000  # 15 分钟
        self._last_loop_duration_ms = 0

        raw_silence_watchdog = get_optional_config_value(
            config, "logging.silence_watchdog_seconds", 60.0
        )
        try:
            silence_watchdog_seconds = float(raw_silence_watchdog)
        except Exception:
            logging.warning(
                "Unable to parse logging.silence_watchdog_seconds=%r; using fallback 60",
                raw_silence_watchdog,
            )
            silence_watchdog_seconds = 60.0
        if silence_watchdog_seconds < 0:
            logging.warning(
                "logging.silence_watchdog_seconds=%r is negative; disabling",
                raw_silence_watchdog,
            )
            silence_watchdog_seconds = 0.0
        self._log_silence_watchdog_seconds = float(silence_watchdog_seconds)
        self._log_silence_watchdog_phase = "boot"
        self._log_silence_watchdog_stage = "idle"
        self._log_silence_watchdog_task: Optional[asyncio.Task] = None
        self._bot_ready = False

        # Unstuck 日志节流
        self._unstuck_last_log_ms = 0
        self._unstuck_log_interval_ms = 5 * 60 * 1000  # 5 分钟

        # 已实现亏损门限日志节流
        self._loss_gate_last_log_ms = {}
        self._loss_gate_log_interval_ms = 5 * 60 * 1000  # 5 分钟
        self._orchestrator_prev_close_ema = {}
        self._orchestrator_close_ema_fallback_counts = {}
        self.hsl = self._parse_hsl_config()
        self._runtime_forced_modes = {"long": {}, "short": {}}
        self._equity_hard_stop_supervisor_running = False
        self._equity_hard_stop_status_log_interval_ms = 15 * 60 * 1000
        self._equity_hard_stop_cooldown_log_interval_ms = 60 * 1000
        self._equity_hard_stop = {
            pside: {
                "runtime": pbr.EquityHardStopRuntime(),
                "strategy_pnl_peak": pbr.EquityHardStopRollingPeak(),
                "halted": False,
                "no_restart_latched": False,
                "last_metrics": None,
                "last_red_progress": None,
                "red_flat_confirmations": 0,
                "pending_red_since_ms": None,
                "cooldown_until_ms": None,
                "pending_stop_event": None,
                "last_stop_event": None,
                "last_status_log_ms": 0,
                "last_cooldown_log_ms": 0,
                "cooldown_intervention_active": False,
                "cooldown_repanic_reset_pending": False,
                "last_cooldown_intervention_log_ms": 0,
                "cooldown_unresolved_residue": False,
            }
            for pside in ("long", "short")
        }

    _monitor_record_event = pb_monitor._monitor_record_event
    _monitor_record_error = pb_monitor._monitor_record_error
    _monitor_emit_stop = pb_monitor._monitor_emit_stop
    _monitor_hsl_payload = pb_monitor._monitor_hsl_payload
    _monitor_order_payload = pb_monitor._monitor_order_payload
    _monitor_fill_payload = pb_monitor._monitor_fill_payload
    _monitor_record_fill_history = pb_monitor._monitor_record_fill_history
    _monitor_record_price_ticks = pb_monitor._monitor_record_price_ticks
    _monitor_handle_candlestick_persist = pb_monitor._monitor_handle_candlestick_persist
    _build_health_summary_payload = pb_monitor._build_health_summary_payload
    _monitor_recent_orders_payload = pb_monitor._monitor_recent_orders_payload
    _build_monitor_market_section = pb_monitor._build_monitor_market_section
    _build_monitor_trailing_section = pb_monitor._build_monitor_trailing_section
    _build_monitor_forager_section = pb_monitor._build_monitor_forager_section
    _build_monitor_unstuck_section = pb_monitor._build_monitor_unstuck_section
    _build_monitor_runtime_market_hints = pb_monitor._build_monitor_runtime_market_hints
    _build_monitor_runtime_unstuck_hints = pb_monitor._build_monitor_runtime_unstuck_hints
    _update_monitor_runtime_hints = pb_monitor._update_monitor_runtime_hints
    _build_monitor_recent_section = pb_monitor._build_monitor_recent_section
    _build_monitor_position_side_payload = pb_monitor._build_monitor_position_side_payload
    _build_monitor_positions_section = pb_monitor._build_monitor_positions_section
    _build_monitor_snapshot = pb_monitor._build_monitor_snapshot
    _monitor_flush_snapshot = pb_monitor._monitor_flush_snapshot

    def live_value(self, key: str):
        return require_live_value(self.config, key)

    def bot_value(self, pside: str, key: str):
        return require_config_value(self.config, f"bot.{pside}.{key}")

    def _set_log_silence_watchdog_context(
        self, *, phase: Optional[str] = None, stage: Optional[str] = None
    ) -> None:
        if phase is not None:
            self._log_silence_watchdog_phase = str(phase)
        if stage is not None:
            self._log_silence_watchdog_stage = str(stage)

    def _maybe_log_silence_watchdog(self, *, now_monotonic: Optional[float] = None) -> bool:
        """检测日志静默期，若超过阈值则发出警告。"""
        threshold = float(getattr(self, "_log_silence_watchdog_seconds", 0.0) or 0.0)
        if threshold <= 0.0:
            return False
        if now_monotonic is None:
            now_monotonic = time.monotonic()
        silent_for_s = max(0.0, now_monotonic - float(get_last_log_activity_monotonic()))
        if silent_for_s < threshold:
            return False
        phase = str(getattr(self, "_log_silence_watchdog_phase", "runtime") or "runtime")
        stage = str(getattr(self, "_log_silence_watchdog_stage", "unknown") or "unknown")
        uptime_ms = max(0, utc_ms() - int(getattr(self, "_health_start_ms", utc_ms())))
        loop_ms = int(getattr(self, "_last_loop_duration_ms", 0) or 0)
        loop_str = f"{loop_ms / 1000:.1f}s" if loop_ms > 0 else "n/a"
        logging.info(
            "[health] silence watchdog: no logs for %.0fs | phase=%s | stage=%s | uptime=%s | loop=%s",
            silent_for_s,
            phase,
            stage,
            self._format_duration(uptime_ms),
            loop_str,
        )
        return True

    async def _run_log_silence_watchdog(self) -> None:
        threshold = float(getattr(self, "_log_silence_watchdog_seconds", 0.0) or 0.0)
        if threshold <= 0.0:
            return
        poll_seconds = min(5.0, max(1.0, threshold / 4.0))
        while not self.stop_signal_received:
            await asyncio.sleep(poll_seconds)
            if self.stop_signal_received:
                break
            self._maybe_log_silence_watchdog()

    def _start_log_silence_watchdog(self) -> None:
        threshold = float(getattr(self, "_log_silence_watchdog_seconds", 0.0) or 0.0)
        if threshold <= 0.0:
            return
        task = getattr(self, "_log_silence_watchdog_task", None)
        if task is not None and not task.done():
            return
        self._log_silence_watchdog_task = asyncio.create_task(self._run_log_silence_watchdog())

    async def _stop_log_silence_watchdog(self) -> None:
        task = getattr(self, "_log_silence_watchdog_task", None)
        self._log_silence_watchdog_task = None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _equity_hard_stop_enabled(self) -> bool:
        return bool(self.equity_hard_stop_loss["enabled"])

    def _equity_hard_stop_latch_path(self) -> str:
        return make_get_filepath(f"caches/equity_hard_stop/{self.exchange}/{self.user}.json")

    def _equity_hard_stop_write_latch(self, metrics: dict) -> str:
        path = self._equity_hard_stop_latch_path()
        payload = dict(metrics)
        tmp_path = path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
        return path

    def _equity_hard_stop_remove_latch_file(self) -> None:
        path = self._equity_hard_stop_latch_path()
        if os.path.isfile(path):
            os.remove(path)

    def _equity_hard_stop_reset_state(self) -> None:
        self._equity_hard_stop_runtime.reset()
        self._equity_hard_stop_strategy_pnl_peak.reset()
        self._equity_hard_stop_last_metrics = None
        self._equity_hard_stop_last_red_progress = None
        self._equity_hard_stop_red_flat_confirmations = 0
        self._equity_hard_stop_pending_red_since_ms = None
        self._equity_hard_stop_halted = False
        self._equity_hard_stop_no_restart_latched = False
        self._equity_hard_stop_halted_until_ms = None
        self._equity_hard_stop_cooldown_intervention_active = False
        self._equity_hard_stop_cooldown_repanic_reset_pending = False
        self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
        self._equity_hard_stop_pending_stop_event = None
        self._equity_hard_stop_last_stop_event = None
        self._runtime_forced_modes = {"long": {}, "short": {}}

    def _equity_hard_stop_runtime_initialized(self) -> bool:
        return bool(self._equity_hard_stop_runtime.initialized())

    def _equity_hard_stop_runtime_red_latched(self) -> bool:
        return bool(self._equity_hard_stop_runtime.red_latched())

    def _equity_hard_stop_runtime_tier(self) -> str:
        return str(self._equity_hard_stop_runtime.tier())

    def _equity_hard_stop_cooldown_position_policy(self) -> str:
        return normalize_hsl_cooldown_position_policy(
            get_optional_live_value(self.config, "hsl_position_during_cooldown_policy", "panic")
        )

    async def _calc_upnl_sum_strict(self) -> float:
        """严格计算未实现盈亏总和，确保持仓数据已刷新。"""
        if not self.fetched_positions:
            return 0.0
        symbols = {x["symbol"] for x in self.fetched_positions}
        last_prices = await self.cm.get_last_prices(symbols, max_age_ms=60_000)
        upnl_sum = 0.0
        for elm in self.fetched_positions:
            symbol = elm["symbol"]
            if symbol not in last_prices:
                raise RuntimeError(f"missing last price for {symbol} while evaluating hard stop")
            upnl = calc_pnl(
                elm["position_side"],
                elm["price"],
                last_prices[symbol],
                elm["size"],
                self.inverse,
                self.c_mults[symbol],
            )
            if not math.isfinite(upnl):
                raise RuntimeError(
                    f"non-finite upnl for {symbol} {elm['position_side']} while evaluating hard stop"
                )
            upnl_sum += upnl
        return upnl_sum

    @staticmethod
    def _equity_hard_stop_fee_cost(fill: Any) -> float:
        """从成交记录中提取手续费成本。"""
        if fill is None:
            return 0.0
        if isinstance(fill, dict):
            fee_obj = fill.get("fee")
            if isinstance(fee_obj, dict):
                return float(fee_obj.get("cost", 0.0) or 0.0)
            if isinstance(fee_obj, (int, float, str)):
                return float(fee_obj or 0.0)
            fees_obj = fill.get("fees")
        else:
            fees_obj = getattr(fill, "fees", None)
        if isinstance(fees_obj, dict):
            return float(fees_obj.get("cost", 0.0) or 0.0)
        if isinstance(fees_obj, (list, tuple)):
            total = 0.0
            for item in fees_obj:
                if isinstance(item, dict):
                    total += float(item.get("cost", 0.0) or 0.0)
            return total
        return 0.0

    def _equity_hard_stop_realized_pnl_now(self) -> float:
        if self._pnls_manager is None:
            return 0.0
        realized = 0.0
        for event in self._pnls_manager.get_events():
            realized += float(getattr(event, "pnl", 0.0) or 0.0)
            realized += self._equity_hard_stop_fee_cost(event)
        return realized

    def _pnls_lookback_start_ms(self) -> Optional[int]:
        config = getattr(self, "config", None)
        if config is None:
            return None
        lookback = parse_pnls_max_lookback_days(
            require_live_value(config, "pnls_max_lookback_days"),
            field_name="live.pnls_max_lookback_days",
        )
        return lookback.event_history_start_ms(self.get_exchange_time())

    def _get_effective_pnl_events(self) -> list:
        if self._pnls_manager is None:
            return []
        start_ms = self._pnls_lookback_start_ms()
        if start_ms is None:
            return self._pnls_manager.get_events()
        return self._pnls_manager.get_events(start_ms=start_ms)

    def _equity_hard_stop_lookback_ms(self) -> Optional[int]:
        lookback = parse_pnls_max_lookback_days(
            require_live_value(self.config, "pnls_max_lookback_days"),
            field_name="live.pnls_max_lookback_days",
        )
        return lookback.hsl_window_ms()

    def _equity_hard_stop_apply_sample(
        self,
        timestamp_ms: int,
        balance: float,
        realized_pnl: float,
        unrealized_pnl: float,
    ) -> dict:
        """将新的权益快照应用到 HSL 状态机，计算当前风险等级。"""
        if not math.isfinite(balance) or balance <= 0.0:
            raise ValueError(f"balance must be finite and > 0, got {balance}")
        if not math.isfinite(realized_pnl):
            raise ValueError(f"realized_pnl must be finite, got {realized_pnl}")
        if not math.isfinite(unrealized_pnl):
            raise ValueError(f"unrealized_pnl must be finite, got {unrealized_pnl}")
        last_metrics = self._equity_hard_stop_last_metrics
        current_minute = int(timestamp_ms) // 60_000
        if last_metrics is not None and int(last_metrics["timestamp_ms"]) // 60_000 == current_minute:
            cached = dict(last_metrics)
            cached["changed"] = False
            cached["elapsed_minutes"] = 0
            self._equity_hard_stop_last_metrics = cached
            return cached
        lookback_ms = self._equity_hard_stop_lookback_ms()
        prev_tier = self._equity_hard_stop_runtime_tier()
        red_threshold = float(self.equity_hard_stop_loss["red_threshold"])
        ratio_yellow = float(self.equity_hard_stop_loss["tier_ratios"]["yellow"])
        ratio_orange = float(self.equity_hard_stop_loss["tier_ratios"]["orange"])
        ema_span_minutes = float(self.equity_hard_stop_loss["ema_span_minutes"])
        strategy_pnl = realized_pnl + unrealized_pnl
        peak_strategy_pnl = float(
            self._equity_hard_stop_strategy_pnl_peak.update(
                int(timestamp_ms),
                float(strategy_pnl),
                int(lookback_ms) if lookback_ms is not None else (2**64 - 1),
            )
        )
        baseline_balance = balance - realized_pnl
        equity = balance + unrealized_pnl
        peak_strategy_equity = max(float(equity), float(baseline_balance + peak_strategy_pnl))
        step = self._equity_hard_stop_runtime.apply_sample(
            timestamp_ms=int(timestamp_ms),
            equity=float(equity),
            peak_strategy_equity=float(peak_strategy_equity),
            red_threshold=red_threshold,
            ema_span_minutes=ema_span_minutes,
            tier_ratio_yellow=ratio_yellow,
            tier_ratio_orange=ratio_orange,
        )
        if not isinstance(step, dict):
            raise TypeError(
                "passivbot_rust.EquityHardStopRuntime.apply_sample() must return a dict, "
                f"got {type(step).__name__}"
            )

        metrics = {
            "timestamp_ms": int(timestamp_ms),
            "balance": float(balance),
            "realized_pnl": float(realized_pnl),
            "unrealized_pnl": float(unrealized_pnl),
            "strategy_pnl": float(strategy_pnl),
            "peak_strategy_pnl": float(peak_strategy_pnl),
            "baseline_balance": float(baseline_balance),
            "equity": float(equity),
            "peak_strategy_equity": float(step["peak_strategy_equity"]),
            "rolling_peak_strategy_equity": float(step["rolling_peak_strategy_equity"]),
            "drawdown_raw": float(step["drawdown_raw"]),
            "drawdown_ema": float(step["drawdown_ema"]),
            "drawdown_score": float(step["drawdown_score"]),
            "red_threshold": red_threshold,
            "tier": str(step["tier"]),
            "changed": bool(step["changed"]) or str(step["tier"]) != prev_tier,
            "alpha": float(step["alpha"]),
            "elapsed_minutes": int(step["elapsed_minutes"]),
        }
        self._equity_hard_stop_last_metrics = metrics
        return metrics

    def _equity_hard_stop_log_transition(self, metrics: dict, prev_tier: str) -> None:
        """记录 HSL 风险等级转换日志。"""
        logging.info(
            "[risk] equity hard stop tier transition %s -> %s | balance=%.6f equity=%.6f "
            "peak_strategy_equity=%.6f drawdown_raw=%.6f drawdown_ema=%.6f drawdown_score=%.6f "
            "strategy_pnl=%.6f peak_strategy_pnl=%.6f "
            "red_threshold=%.6f yellow=%.3f orange=%.3f",
            prev_tier,
            metrics["tier"],
            metrics["balance"],
            metrics["equity"],
            metrics["peak_strategy_equity"],
            metrics["drawdown_raw"],
            metrics["drawdown_ema"],
            metrics["drawdown_score"],
            metrics["strategy_pnl"],
            metrics["peak_strategy_pnl"],
            metrics["red_threshold"],
            float(self.equity_hard_stop_loss["tier_ratios"]["yellow"]),
            float(self.equity_hard_stop_loss["tier_ratios"]["orange"]),
        )

    def _equity_hard_stop_build_latch_payload(
        self,
        *,
        stop_event_timestamp_ms: int,
        balance: Optional[float] = None,
        realized_pnl: Optional[float] = None,
        unrealized_pnl: Optional[float] = None,
        strategy_pnl: Optional[float] = None,
        peak_strategy_pnl: Optional[float] = None,
        equity: float,
        peak_strategy_equity: float,
        trigger_peak_strategy_equity: float,
        drawdown_raw: float,
        drawdown_ema: float,
        drawdown_score: float,
        no_restart_latched: bool,
        cooldown_until_ms: Optional[int],
    ) -> dict:
        """构建 HSL 锁存事件的负载字典，用于持久化和日志记录。"""
        return {
            "triggered_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "exchange": str(self.exchange),
            "user": str(self.user),
            "tier": "red",
            "red_threshold": float(self.equity_hard_stop_loss["red_threshold"]),
            "ema_span_minutes": float(self.equity_hard_stop_loss["ema_span_minutes"]),
            "cooldown_minutes_after_red": float(
                self.equity_hard_stop_loss["cooldown_minutes_after_red"]
            ),
            "no_restart_drawdown_threshold": float(self.equity_hard_stop_loss["no_restart_drawdown_threshold"]),
            "tier_ratios": {
                "yellow": float(self.equity_hard_stop_loss["tier_ratios"]["yellow"]),
                "orange": float(self.equity_hard_stop_loss["tier_ratios"]["orange"]),
            },
            "orange_tier_mode": str(self.equity_hard_stop_loss["orange_tier_mode"]),
            "panic_close_order_type": str(self.equity_hard_stop_loss["panic_close_order_type"]),
            "stop_event_timestamp_ms": int(stop_event_timestamp_ms),
            "balance": None if balance is None else float(balance),
            "realized_pnl": None if realized_pnl is None else float(realized_pnl),
            "unrealized_pnl": None if unrealized_pnl is None else float(unrealized_pnl),
            "strategy_pnl": None if strategy_pnl is None else float(strategy_pnl),
            "peak_strategy_pnl": None if peak_strategy_pnl is None else float(peak_strategy_pnl),
            "equity": float(equity),
            "peak_strategy_equity": float(peak_strategy_equity),
            "trigger_peak_strategy_equity": float(trigger_peak_strategy_equity),
            "drawdown_raw": float(drawdown_raw),
            "drawdown_ema": float(drawdown_ema),
            "drawdown_score": float(drawdown_score),
            "no_restart_latched": bool(no_restart_latched),
            "auto_restart_eligible": bool(
                (not no_restart_latched)
                and float(self.equity_hard_stop_loss["cooldown_minutes_after_red"]) > 0.0
            ),
            "cooldown_until_ms": None if cooldown_until_ms is None else int(cooldown_until_ms),
        }

    async def _equity_hard_stop_compute_stop_event(self, stop_event_ts_ms: int) -> dict:
        """计算止损事件的综合指标并返回锁存负载。"""
        balance = float(self.get_raw_balance())
        unrealized_pnl = float(await self._calc_upnl_sum_strict())
        realized_pnl = float(self._equity_hard_stop_realized_pnl_now())
        strategy_pnl = realized_pnl + unrealized_pnl
        peak_strategy_pnl = float(
            max(
                strategy_pnl,
                (self._equity_hard_stop_last_metrics or {}).get("peak_strategy_pnl", strategy_pnl),
            )
        )
        equity = float(balance + unrealized_pnl)
        trigger_peak_strategy_equity = float(self._equity_hard_stop_runtime.peak_strategy_equity())
        peak_strategy_equity = float(max(equity, (balance - realized_pnl) + peak_strategy_pnl))
        if not math.isfinite(trigger_peak_strategy_equity) or trigger_peak_strategy_equity <= 0.0:
            raise RuntimeError(
                f"invalid hard-stop trigger_peak_strategy_equity at stop finalization: {trigger_peak_strategy_equity}"
            )
        if not math.isfinite(peak_strategy_equity) or peak_strategy_equity <= 0.0:
            raise RuntimeError(f"invalid hard-stop rolling peak_strategy_equity at stop finalization: {peak_strategy_equity}")
        drawdown_ema = float(self._equity_hard_stop_runtime.drawdown_ema())
        drawdown_raw = max(0.0, 1.0 - equity / max(peak_strategy_equity, 1e-12))
        return {
            "stop_event_timestamp_ms": int(stop_event_ts_ms),
            "balance": balance,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
            "strategy_pnl": strategy_pnl,
            "peak_strategy_pnl": peak_strategy_pnl,
            "equity": equity,
            "peak_strategy_equity": peak_strategy_equity,
            "trigger_peak_strategy_equity": trigger_peak_strategy_equity,
            "drawdown_raw": drawdown_raw,
            "drawdown_ema": drawdown_ema,
            "drawdown_score": min(drawdown_raw, drawdown_ema),
        }

    async def _equity_hard_stop_wait_for_cooldown(self, cooldown_until_ms: int) -> None:
        self._equity_hard_stop_halted_until_ms = int(cooldown_until_ms)
        while not self.stop_signal_received:
            now_ms = int(self.get_exchange_time())
            if now_ms >= cooldown_until_ms:
                return
            remaining_seconds = max(0.0, (cooldown_until_ms - now_ms) / 1000.0)
            logging.info("[risk] RED cooldown active | remaining_seconds=%.1f", remaining_seconds)
            await asyncio.sleep(min(float(self.live_value("execution_delay_seconds")), 5.0))

    def _equity_hard_stop_reset_after_restart(self) -> None:
        self._equity_hard_stop_runtime.reset()
        self._equity_hard_stop_strategy_pnl_peak.reset()
        self._equity_hard_stop_clear_runtime_forced_modes()
        self._equity_hard_stop_red_flat_confirmations = 0
        self._equity_hard_stop_last_red_progress = None
        self._equity_hard_stop_pending_red_since_ms = None
        self._equity_hard_stop_halted = False
        self._equity_hard_stop_no_restart_latched = False
        self._equity_hard_stop_halted_until_ms = None
        self._equity_hard_stop_cooldown_intervention_active = False
        self._equity_hard_stop_cooldown_repanic_reset_pending = False
        self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
        self._equity_hard_stop_pending_stop_event = None

    def _equity_hard_stop_position_symbols(self) -> list[str]:
        symbols = []
        for symbol, position in self.positions.items():
            if any(
                float(position.get(pside, {}).get("size", 0.0) or 0.0) != 0.0
                for pside in ("long", "short")
            ):
                symbols.append(symbol)
        return sorted(symbols)

    def _equity_hard_stop_halted_mode(self, pside: str, symbol: str | None) -> str:
        policy = self._equity_hard_stop_cooldown_position_policy()
        size = 0.0
        if symbol is not None:
            size = float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0) or 0.0)
        if size == 0.0:
            return "graceful_stop"
        if policy == "panic":
            return "panic"
        if policy == "manual":
            return "manual"
        if policy == "tp_only":
            return "tp_only"
        return "graceful_stop"

    def _equity_hard_stop_refresh_halted_runtime_forced_modes(self) -> None:
        if not self._equity_hard_stop_halted:
            self._equity_hard_stop_clear_runtime_forced_modes()
            return
        forced = {"long": {}, "short": {}}
        symbols = set(self.positions.keys()) | set(self.open_orders.keys()) | set(self.active_symbols)
        for symbol in symbols:
            for pside in ("long", "short"):
                forced[pside][symbol] = self._equity_hard_stop_halted_mode(pside, symbol)
        self._runtime_forced_modes = forced

    async def _equity_hard_stop_refresh_cooldown_after_repanic(self, now_ms: int) -> None:
        """在红色恐慌重启后刷新冷却计时器。"""
        cooldown_minutes = float(self.equity_hard_stop_loss["cooldown_minutes_after_red"])
        cooldown_ms = int(round(cooldown_minutes * 60_000.0)) if cooldown_minutes > 0.0 else 0
        cooldown_until_ms = now_ms + cooldown_ms if cooldown_ms > 0 else None
        stop_event = await self._equity_hard_stop_compute_stop_event(now_ms)
        payload = self._equity_hard_stop_build_latch_payload(
            stop_event_timestamp_ms=now_ms,
            balance=stop_event.get("balance"),
            realized_pnl=stop_event.get("realized_pnl"),
            unrealized_pnl=stop_event.get("unrealized_pnl"),
            strategy_pnl=stop_event.get("strategy_pnl"),
            peak_strategy_pnl=stop_event.get("peak_strategy_pnl"),
            equity=float(stop_event["equity"]),
            peak_strategy_equity=float(stop_event["peak_strategy_equity"]),
            trigger_peak_strategy_equity=float(stop_event["trigger_peak_strategy_equity"]),
            drawdown_raw=float(stop_event["drawdown_raw"]),
            drawdown_ema=float(stop_event["drawdown_ema"]),
            drawdown_score=float(stop_event["drawdown_score"]),
            no_restart_latched=False,
            cooldown_until_ms=cooldown_until_ms,
        )
        self._equity_hard_stop_last_stop_event = payload
        self._equity_hard_stop_halted_until_ms = cooldown_until_ms
        self._equity_hard_stop_cooldown_intervention_active = False
        self._equity_hard_stop_cooldown_repanic_reset_pending = False
        self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
        latch_path = self._equity_hard_stop_write_latch(payload)
        self._equity_hard_stop_refresh_halted_runtime_forced_modes()
        logging.critical(
            "[risk] cooldown violation repanic flattened; cooldown reset from flat_ts=%s to cooldown_until_ms=%s latch=%s",
            now_ms,
            cooldown_until_ms if cooldown_until_ms is not None else "none",
            latch_path,
        )

    async def _equity_hard_stop_handle_position_during_cooldown(self, now_ms: int) -> bool:
        """在红色冷却期间管理持仓，检测是否需要重新恐慌。"""
        if not self._equity_hard_stop_halted or self._equity_hard_stop_no_restart_latched:
            return False
        cooldown_until_ms = self._equity_hard_stop_halted_until_ms
        if cooldown_until_ms is None or now_ms >= cooldown_until_ms:
            return False

        symbols = self._equity_hard_stop_position_symbols()
        policy = self._equity_hard_stop_cooldown_position_policy()
        if not symbols:
            if self._equity_hard_stop_cooldown_repanic_reset_pending:
                await self._equity_hard_stop_refresh_cooldown_after_repanic(now_ms)
                return True
            if self._equity_hard_stop_cooldown_intervention_active:
                logging.info(
                    "[risk] cooldown intervention ended flat; policy=%s original_cooldown_until_ms=%s",
                    policy,
                    cooldown_until_ms,
                )
            self._equity_hard_stop_cooldown_intervention_active = False
            self._equity_hard_stop_cooldown_repanic_reset_pending = False
            self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
            self._equity_hard_stop_refresh_halted_runtime_forced_modes()
            return False

        should_log = (
            not self._equity_hard_stop_cooldown_intervention_active
            or self._equity_hard_stop_last_cooldown_intervention_log_ms == 0
            or now_ms - self._equity_hard_stop_last_cooldown_intervention_log_ms
            >= self._equity_hard_stop_cooldown_log_interval_ms
        )
        if should_log:
            logging.critical(
                "[risk] detected non-flat position during RED cooldown | policy=%s symbols=%s cooldown_until_ms=%s",
                policy,
                ",".join(symbols),
                cooldown_until_ms,
            )
            self._equity_hard_stop_last_cooldown_intervention_log_ms = now_ms
        self._equity_hard_stop_cooldown_intervention_active = True

        if policy == "normal":
            self._equity_hard_stop_reset_after_restart()
            self._equity_hard_stop_remove_latch_file()
            logging.critical(
                "[risk] operator override during RED cooldown: resumed normal operation and reset drawdown tracker"
            )
            return True

        self._equity_hard_stop_cooldown_repanic_reset_pending = policy == "panic"
        self._equity_hard_stop_refresh_halted_runtime_forced_modes()
        return False

    async def _equity_hard_stop_initialize_from_history(self) -> None:
        """从历史数据初始化 HSL 状态机，回放过去的权益快照以确定初始风险等级。"""
        if not self._equity_hard_stop_enabled():
            return
        self._equity_hard_stop_reset_state()
        history = await self.get_balance_equity_history(current_balance=self.get_raw_balance())
        if "timeline" not in history:
            raise ValueError("get_balance_equity_history() missing required key: timeline")
        timeline = history["timeline"]
        if not isinstance(timeline, list):
            raise TypeError(
                f"get_balance_equity_history()['timeline'] must be a list, got {type(timeline).__name__}"
            )

        cooldown_minutes = float(self.equity_hard_stop_loss["cooldown_minutes_after_red"])
        no_restart_drawdown_threshold = float(
            self.equity_hard_stop_loss["no_restart_drawdown_threshold"]
        )
        cooldown_ms = int(round(cooldown_minutes * 60_000.0)) if cooldown_minutes > 0.0 else 0
        cooldown_until_ms = None
        pending_red = False
        n_rows = 0
        latest_terminal_stop = None
        for row in timeline:
            if not isinstance(row, dict):
                continue
            required = ("timestamp", "balance", "realized_pnl", "unrealized_pnl")
            if any(key not in row for key in required):
                continue
            ts = int(row["timestamp"])
            balance = float(row["balance"])
            realized_pnl = float(row["realized_pnl"])
            unrealized_pnl = float(row["unrealized_pnl"])

            if cooldown_until_ms is not None:
                if ts < cooldown_until_ms:
                    continue
                self._equity_hard_stop_reset_after_restart()
                cooldown_until_ms = None
                pending_red = False

            current_metrics = self._equity_hard_stop_apply_sample(
                int(ts), balance, realized_pnl, unrealized_pnl
            )
            n_rows += 1

            if self._equity_hard_stop_runtime_tier() == "red":
                pending_red = True
                self._equity_hard_stop_pending_red_since_ms = int(ts)

            is_flat = bool(row["is_flat"]) if "is_flat" in row else False
            if pending_red and is_flat:
                peak_strategy_equity = float(current_metrics["peak_strategy_equity"])
                trigger_peak_strategy_equity = float(
                    self._equity_hard_stop_runtime.peak_strategy_equity()
                )
                if not math.isfinite(peak_strategy_equity) or peak_strategy_equity <= 0.0:
                    raise RuntimeError(
                        "invalid peak_strategy_equity during hard-stop replay at "
                        f"ts={ts}: {peak_strategy_equity}"
                    )
                if (
                    not math.isfinite(trigger_peak_strategy_equity)
                    or trigger_peak_strategy_equity <= 0.0
                ):
                    raise RuntimeError(
                        "invalid trigger_peak_strategy_equity during hard-stop replay at "
                        f"ts={ts}: {trigger_peak_strategy_equity}"
                    )
                stop_drawdown_raw = float(current_metrics["drawdown_raw"])
                if stop_drawdown_raw >= no_restart_drawdown_threshold or cooldown_ms <= 0:
                    payload = self._equity_hard_stop_build_latch_payload(
                        stop_event_timestamp_ms=ts,
                        balance=balance,
                        realized_pnl=realized_pnl,
                        unrealized_pnl=unrealized_pnl,
                        strategy_pnl=float(current_metrics["strategy_pnl"]),
                        peak_strategy_pnl=float(current_metrics["peak_strategy_pnl"]),
                        equity=float(current_metrics["equity"]),
                        peak_strategy_equity=peak_strategy_equity,
                        trigger_peak_strategy_equity=trigger_peak_strategy_equity,
                        drawdown_raw=float(current_metrics["drawdown_raw"]),
                        drawdown_ema=float(current_metrics["drawdown_ema"]),
                        drawdown_score=float(current_metrics["drawdown_score"]),
                        no_restart_latched=bool(
                            stop_drawdown_raw >= no_restart_drawdown_threshold
                        ),
                        cooldown_until_ms=None,
                    )
                    self._equity_hard_stop_last_stop_event = payload
                    latest_terminal_stop = payload
                    latch_path = self._equity_hard_stop_write_latch(payload)
                    logging.critical(
                        "[risk] hard-stop replay found terminal RED stop event in exchange-derived "
                        "history | stop_ts=%s drawdown_raw=%.6f "
                        "no_restart_drawdown_threshold=%.6f diagnostic=%s",
                        ts,
                        stop_drawdown_raw,
                        no_restart_drawdown_threshold,
                        latch_path,
                    )
                    break
                cooldown_until_ms = ts + cooldown_ms
                pending_red = False
                self._equity_hard_stop_pending_red_since_ms = None

        if latest_terminal_stop is not None:
            self.stop_signal_received = True
            return

        now_ms = int(self.get_exchange_time())
        if cooldown_until_ms is not None:
            if now_ms >= cooldown_until_ms:
                self._equity_hard_stop_reset_after_restart()
                cooldown_until_ms = None
                pending_red = False
            else:
                self._equity_hard_stop_halted = True
                self._equity_hard_stop_no_restart_latched = False
                self._equity_hard_stop_halted_until_ms = cooldown_until_ms
                self._equity_hard_stop_cooldown_intervention_active = False
                self._equity_hard_stop_cooldown_repanic_reset_pending = False
                self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
                self._equity_hard_stop_refresh_halted_runtime_forced_modes()
                logging.critical(
                    "[risk] reconstructed active RED cooldown from exchange-derived history | remaining_seconds=%.1f policy=%s",
                    (cooldown_until_ms - now_ms) / 1000.0,
                    self._equity_hard_stop_cooldown_position_policy(),
                )
                return

        current_balance = self.get_raw_balance()
        current_realized = self._equity_hard_stop_realized_pnl_now()
        current_upnl = await self._calc_upnl_sum_strict()
        current_metrics = self._equity_hard_stop_apply_sample(
            now_ms,
            float(current_balance),
            float(current_realized),
            float(current_upnl),
        )
        logging.info(
            "[risk] hard-stop initialized from equity history | rows=%d tier=%s equity=%.6f "
            "peak_strategy_equity=%.6f rolling_peak_strategy_equity=%.6f "
            "drawdown_raw=%.6f drawdown_ema=%.6f drawdown_score=%.6f",
            n_rows,
            current_metrics["tier"],
            current_metrics["equity"],
            current_metrics["peak_strategy_equity"],
            current_metrics["rolling_peak_strategy_equity"],
            current_metrics["drawdown_raw"],
            current_metrics["drawdown_ema"],
            current_metrics["drawdown_score"],
        )
        if current_metrics["tier"] == "red":
            self._equity_hard_stop_pending_red_since_ms = int(current_metrics["timestamp_ms"])

    async def _equity_hard_stop_check(self) -> Optional[dict]:
        """检查权益硬止损条件，返回止损事件负载或 None。"""
        if not self._equity_hard_stop_enabled():
            return None
        if not self._equity_hard_stop_runtime_initialized():
            await self._equity_hard_stop_initialize_from_history()
        now_ms = int(self.get_exchange_time())
        if self._equity_hard_stop_halted:
            if await self._equity_hard_stop_handle_position_during_cooldown(now_ms):
                if not self._equity_hard_stop_halted:
                    return None
            if self._equity_hard_stop_halted:
                cooldown_until_ms = self._equity_hard_stop_halted_until_ms
                if (
                    not self._equity_hard_stop_no_restart_latched
                    and cooldown_until_ms is not None
                    and now_ms >= cooldown_until_ms
                ):
                    self._equity_hard_stop_reset_after_restart()
                    self._equity_hard_stop_remove_latch_file()
                    logging.info("[risk] RED cooldown elapsed; trading resumed")
                else:
                    self._equity_hard_stop_refresh_halted_runtime_forced_modes()
                    return {
                        "halted": True,
                        "cooldown_until_ms": cooldown_until_ms,
                    }

        prev_latched = self._equity_hard_stop_runtime_red_latched()
        prev_tier = self._equity_hard_stop_runtime_tier()
        balance = self.get_raw_balance()
        realized_pnl = self._equity_hard_stop_realized_pnl_now()
        unrealized_pnl = await self._calc_upnl_sum_strict()
        metrics = self._equity_hard_stop_apply_sample(
            now_ms,
            float(balance),
            float(realized_pnl),
            float(unrealized_pnl),
        )
        if metrics["changed"]:
            self._equity_hard_stop_log_transition(metrics, prev_tier)
        if metrics["tier"] == "red" and not prev_latched:
            self._equity_hard_stop_pending_red_since_ms = int(metrics["timestamp_ms"])
            logging.critical(
                "[risk] equity hard stop RED triggered | equity=%.6f "
                "peak_strategy_equity=%.6f rolling_peak_strategy_equity=%.6f "
                "drawdown_score=%.6f "
                "red_threshold=%.6f",
                metrics["equity"],
                metrics["peak_strategy_equity"],
                metrics["rolling_peak_strategy_equity"],
                metrics["drawdown_score"],
                metrics["red_threshold"],
            )
        elif metrics["tier"] != "red":
            self._equity_hard_stop_pending_red_since_ms = None
        return metrics

    def _equity_hard_stop_set_red_runtime_forced_modes(self) -> None:
        forced = {"long": {}, "short": {}}
        symbols = set(self.positions.keys()) | set(self.open_orders.keys()) | set(self.active_symbols)
        for symbol in symbols:
            for pside in ("long", "short"):
                forced[pside][symbol] = "panic"
        self._runtime_forced_modes = forced

    def _equity_hard_stop_clear_runtime_forced_modes(self) -> None:
        self._runtime_forced_modes = {"long": {}, "short": {}}

    def _equity_hard_stop_count_open_positions(self) -> int:
        n_positions = 0
        for pos in self.positions.values():
            for pside in ("long", "short"):
                if float(pos.get(pside, {}).get("size", 0.0) or 0.0) != 0.0:
                    n_positions += 1
        return n_positions

    def _equity_hard_stop_count_blocking_open_orders(self) -> tuple[int, int]:
        entry_orders = 0
        nonpanic_close_orders = 0
        for orders in self.open_orders.values():
            for order in orders:
                reduce_only = bool(order.get("reduce_only") or order.get("reduceOnly"))
                if not reduce_only:
                    entry_orders += 1
                    continue
                pb_type = self._resolve_pb_order_type(order).lower()
                if "panic" not in pb_type:
                    nonpanic_close_orders += 1
        return entry_orders, nonpanic_close_orders

    def _equity_hard_stop_log_red_progress(
        self,
        n_positions: int,
        entry_orders: int,
        nonpanic_close_orders: int,
        flat_confirmations: int,
    ) -> None:
        """记录红色止损期间的持仓缩减进度。"""
        progress = (n_positions, entry_orders, nonpanic_close_orders, flat_confirmations)
        if progress == self._equity_hard_stop_last_red_progress:
            return
        self._equity_hard_stop_last_red_progress = progress
        logging.info(
            "[risk] RED supervisor progress | positions=%d entry_orders=%d "
            "nonpanic_close_orders=%d flat_confirmations=%d/2",
            n_positions,
            entry_orders,
            nonpanic_close_orders,
            flat_confirmations,
        )

    async def _equity_hard_stop_finalize_red_stop(self, stop_event: Optional[dict] = None) -> None:
        """执行红色止损：平掉所有持仓并进入冷却期。"""
        stop_ts_ms = int(self.get_exchange_time())
        if stop_event is None:
            stop_event = await self._equity_hard_stop_compute_stop_event(stop_ts_ms)
        else:
            stop_ts_ms = int(stop_event["stop_event_timestamp_ms"])
        cooldown_minutes = float(self.equity_hard_stop_loss["cooldown_minutes_after_red"])
        no_restart_drawdown_threshold = float(self.equity_hard_stop_loss["no_restart_drawdown_threshold"])
        no_restart_latched = bool(stop_event["drawdown_raw"] >= no_restart_drawdown_threshold)
        cooldown_ms = int(round(cooldown_minutes * 60_000.0)) if cooldown_minutes > 0.0 else 0
        cooldown_until_ms = (
            None if no_restart_latched or cooldown_ms <= 0 else int(stop_ts_ms + cooldown_ms)
        )
        payload = self._equity_hard_stop_build_latch_payload(
            stop_event_timestamp_ms=stop_ts_ms,
            balance=stop_event.get("balance"),
            realized_pnl=stop_event.get("realized_pnl"),
            unrealized_pnl=stop_event.get("unrealized_pnl"),
            strategy_pnl=stop_event.get("strategy_pnl"),
            peak_strategy_pnl=stop_event.get("peak_strategy_pnl"),
            equity=float(stop_event["equity"]),
            peak_strategy_equity=float(stop_event["peak_strategy_equity"]),
            trigger_peak_strategy_equity=float(stop_event["trigger_peak_strategy_equity"]),
            drawdown_raw=float(stop_event["drawdown_raw"]),
            drawdown_ema=float(stop_event["drawdown_ema"]),
            drawdown_score=float(stop_event["drawdown_score"]),
            no_restart_latched=no_restart_latched,
            cooldown_until_ms=cooldown_until_ms,
        )
        self._equity_hard_stop_last_stop_event = payload
        latch_path = self._equity_hard_stop_write_latch(payload)

        if no_restart_latched or cooldown_until_ms is None:
            logging.critical(
                "[risk] RED stop finalized (terminal) | stop_ts=%s equity=%.6f "
                "peak_strategy_equity=%.6f drawdown_raw=%.6f "
                "no_restart_drawdown_threshold=%.6f latch=%s",
                stop_ts_ms,
                stop_event["equity"],
                stop_event["peak_strategy_equity"],
                stop_event["drawdown_raw"],
                no_restart_drawdown_threshold,
                latch_path,
            )
            self._equity_hard_stop_clear_runtime_forced_modes()
            self._equity_hard_stop_pending_stop_event = None
            self.stop_signal_received = True
            return

        self._equity_hard_stop_halted = True
        self._equity_hard_stop_no_restart_latched = False
        self._equity_hard_stop_halted_until_ms = cooldown_until_ms
        self._equity_hard_stop_cooldown_intervention_active = False
        self._equity_hard_stop_cooldown_repanic_reset_pending = False
        self._equity_hard_stop_last_cooldown_intervention_log_ms = 0
        self._equity_hard_stop_pending_stop_event = None
        self._equity_hard_stop_refresh_halted_runtime_forced_modes()
        logging.critical(
            "[risk] RED stop finalized (cooldown active) | stop_ts=%s "
            "drawdown_raw=%.6f cooldown_until_ms=%s policy=%s latch=%s",
            stop_ts_ms,
            stop_event["drawdown_raw"],
            cooldown_until_ms,
            self._equity_hard_stop_cooldown_position_policy(),
            latch_path,
        )
        return

    async def _equity_hard_stop_run_red_supervisor(self) -> None:
        """红色止损期间的后台监控，等待全部持仓平仓后结束冻结状态。"""
        if self._equity_hard_stop_supervisor_running:
            return
        self._equity_hard_stop_supervisor_running = True
        self._equity_hard_stop_red_flat_confirmations = 0
        self._equity_hard_stop_last_red_progress = None
        self._equity_hard_stop_pending_stop_event = None
        try:
            logging.critical("[risk] entering RED supervisor loop (panic-close until confirmed flat)")
            while not self.stop_signal_received:
                if not await self.update_pos_oos_pnls_ohlcvs():
                    await asyncio.sleep(0.5)
                    continue

                n_positions = self._equity_hard_stop_count_open_positions()
                entry_orders, nonpanic_close_orders = self._equity_hard_stop_count_blocking_open_orders()
                if n_positions == 0 and entry_orders == 0 and nonpanic_close_orders == 0:
                    if self._equity_hard_stop_red_flat_confirmations == 0:
                        self._equity_hard_stop_pending_stop_event = (
                            await self._equity_hard_stop_compute_stop_event(
                                int(self.get_exchange_time())
                            )
                        )
                    self._equity_hard_stop_red_flat_confirmations += 1
                else:
                    self._equity_hard_stop_red_flat_confirmations = 0
                    self._equity_hard_stop_pending_stop_event = None
                self._equity_hard_stop_log_red_progress(
                    n_positions,
                    entry_orders,
                    nonpanic_close_orders,
                    self._equity_hard_stop_red_flat_confirmations,
                )
                if self._equity_hard_stop_red_flat_confirmations >= 2:
                    await self._equity_hard_stop_finalize_red_stop(
                        self._equity_hard_stop_pending_stop_event
                    )
                    return

                self._equity_hard_stop_set_red_runtime_forced_modes()
                try:
                    await self.execute_to_exchange()
                except RestartBotException as e:
                    logging.error("[risk] RED supervisor ignored restart request: %s", e)
                except FatalBotException:
                    raise
                except Exception as e:
                    logging.error("[risk] RED supervisor execute_to_exchange failed: %s", e)
                    traceback.print_exc()
                await asyncio.sleep(float(self.live_value("execution_delay_seconds")))
        finally:
            self._equity_hard_stop_supervisor_running = False

    def _apply_equity_hard_stop_orange_overlay(self) -> None:
        """在橙色风险等级下覆盖交易模式，限制开仓行为。"""
        if not self._equity_hard_stop_enabled():
            return
        if self._equity_hard_stop_runtime_red_latched() or self._equity_hard_stop_runtime_tier() != "orange":
            return
        orange_mode = str(self.equity_hard_stop_loss["orange_tier_mode"])
        symbols = (
            set(self.PB_modes["long"].keys())
            | set(self.PB_modes["short"].keys())
            | set(self.positions.keys())
            | set(self.open_orders.keys())
        )
        for symbol in symbols:
            for pside in ("long", "short"):
                if symbol not in self.PB_modes[pside]:
                    continue
                current_mode = self.PB_modes[pside][symbol]
                if orange_mode == "graceful_stop":
                    if current_mode == "normal":
                        self.PB_modes[pside][symbol] = "graceful_stop"
                else:
                    size = float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0) or 0.0)
                    if size == 0.0:
                        continue
                    if current_mode in ("normal", "graceful_stop"):
                        self.PB_modes[pside][symbol] = "tp_only_with_active_entry_cancellation"

    _hsl_psides = pb_hsl._hsl_psides
    _hsl_state = pb_hsl._hsl_state
    _parse_hsl_config = pb_hsl._parse_hsl_config
    _equity_hard_stop_enabled = pb_hsl._equity_hard_stop_enabled
    _equity_hard_stop_signal_mode = pb_hsl._equity_hard_stop_signal_mode
    _equity_hard_stop_cooldown_position_policy = pb_hsl._equity_hard_stop_cooldown_position_policy
    _equity_hard_stop_halted_mode = pb_hsl._equity_hard_stop_halted_mode
    _equity_hard_stop_panic_close_order_type = pb_hsl._equity_hard_stop_panic_close_order_type
    _equity_hard_stop_signal_values = pb_hsl._equity_hard_stop_signal_values
    _equity_hard_stop_latch_path = pb_hsl._equity_hard_stop_latch_path
    _equity_hard_stop_write_latch = pb_hsl._equity_hard_stop_write_latch
    _equity_hard_stop_remove_latch_file = pb_hsl._equity_hard_stop_remove_latch_file
    _equity_hard_stop_reset_state = pb_hsl._equity_hard_stop_reset_state
    _equity_hard_stop_runtime_initialized = pb_hsl._equity_hard_stop_runtime_initialized
    _equity_hard_stop_runtime_red_latched = pb_hsl._equity_hard_stop_runtime_red_latched
    _equity_hard_stop_runtime_tier = pb_hsl._equity_hard_stop_runtime_tier
    _equity_hard_stop_fill_pside = staticmethod(pb_hsl._equity_hard_stop_fill_pside)
    _calc_upnl_sum_strict = pb_hsl._calc_upnl_sum_strict
    _equity_hard_stop_fee_cost = staticmethod(pb_hsl._equity_hard_stop_fee_cost)
    _get_exchange_fee_rates = pb_hsl._get_exchange_fee_rates
    _orchestrator_exchange_params = pb_hsl._orchestrator_exchange_params
    _equity_hard_stop_realized_pnl_now = pb_hsl._equity_hard_stop_realized_pnl_now
    _equity_hard_stop_lookback_ms = pb_hsl._equity_hard_stop_lookback_ms
    _equity_hard_stop_apply_sample = pb_hsl._equity_hard_stop_apply_sample
    _equity_hard_stop_log_transition = pb_hsl._equity_hard_stop_log_transition
    _equity_hard_stop_format_remaining_time = staticmethod(
        pb_hsl._equity_hard_stop_format_remaining_time
    )
    _equity_hard_stop_build_latch_payload = pb_hsl._equity_hard_stop_build_latch_payload
    _equity_hard_stop_compute_stop_event = pb_hsl._equity_hard_stop_compute_stop_event
    _equity_hard_stop_infer_replay_contract = pb_hsl._equity_hard_stop_infer_replay_contract
    _equity_hard_stop_log_cooldown_status = pb_hsl._equity_hard_stop_log_cooldown_status
    _equity_hard_stop_position_symbols = pb_hsl._equity_hard_stop_position_symbols
    _equity_hard_stop_refresh_cooldown_after_repanic = (
        pb_hsl._equity_hard_stop_refresh_cooldown_after_repanic
    )
    _equity_hard_stop_handle_position_during_cooldown = (
        pb_hsl._equity_hard_stop_handle_position_during_cooldown
    )
    _equity_hard_stop_reset_after_restart = pb_hsl._equity_hard_stop_reset_after_restart
    _equity_hard_stop_replay_from_boundary = pb_hsl._equity_hard_stop_replay_from_boundary
    _equity_hard_stop_refresh_halted_runtime_forced_modes = (
        pb_hsl._equity_hard_stop_refresh_halted_runtime_forced_modes
    )
    _equity_hard_stop_initialize_from_history = pb_hsl._equity_hard_stop_initialize_from_history
    _equity_hard_stop_log_status = pb_hsl._equity_hard_stop_log_status
    _equity_hard_stop_check = pb_hsl._equity_hard_stop_check
    _equity_hard_stop_set_red_runtime_forced_modes = pb_hsl._equity_hard_stop_set_red_runtime_forced_modes
    _equity_hard_stop_clear_runtime_forced_modes = pb_hsl._equity_hard_stop_clear_runtime_forced_modes
    _equity_hard_stop_count_open_positions = pb_hsl._equity_hard_stop_count_open_positions
    _equity_hard_stop_count_blocking_open_orders = pb_hsl._equity_hard_stop_count_blocking_open_orders
    _equity_hard_stop_log_red_progress = pb_hsl._equity_hard_stop_log_red_progress
    _equity_hard_stop_finalize_red_stop = pb_hsl._equity_hard_stop_finalize_red_stop
    _equity_hard_stop_run_red_supervisor = pb_hsl._equity_hard_stop_run_red_supervisor
    _apply_equity_hard_stop_orange_overlay = pb_hsl._apply_equity_hard_stop_orange_overlay

    def _filter_approved_symbols(self, pside: str, symbols: set[str]) -> set[str]:
        """钩子：交易所特定的已批准交易对过滤，用于新开仓。"""
        return symbols

    def _assert_supported_live_state(self) -> None:
        """钩子：交易所特定的启动/运行时验证，用于检测不支持的实盘状态。"""
        return None

    def _build_ccxt_options(self, overrides: Optional[dict] = None) -> dict:
        options = {"adjustForTimeDifference": True}
        recv_window = get_optional_live_value(self.config, "recv_window_ms", None)
        if recv_window not in (None, ""):
            try:
                recv_int = int(float(recv_window))
                if recv_int > 0:
                    options["recvWindow"] = recv_int
            except (TypeError, ValueError):
                logging.warning("Unable to parse live.recv_window_ms=%r; ignoring", recv_window)
        if overrides:
            options.update(overrides)
        return options

    def _log_startup_banner(self) -> None:
        """记录包含关键配置信息的启动横幅。"""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        user = self.user
        exchange = self.exchange

        # 确定已启用的方向
        long_enabled = float(self.bot_value("long", "total_wallet_exposure_limit") or 0.0) > 0.0
        short_enabled = float(self.bot_value("short", "total_wallet_exposure_limit") or 0.0) > 0.0
        if long_enabled and short_enabled:
            mode = "LONG + SHORT"
        elif long_enabled:
            mode = "LONG only"
        elif short_enabled:
            mode = "SHORT only"
        else:
            mode = "DISABLED"

        n_pos_long = int(self.bot_value("long", "n_positions") or 0)
        n_pos_short = int(self.bot_value("short", "n_positions") or 0)
        n_pos = f"{n_pos_long}L" if long_enabled else ""
        if short_enabled:
            n_pos = f"{n_pos}/{n_pos_short}S" if n_pos else f"{n_pos_short}S"

        twel_long = float(self.bot_value("long", "total_wallet_exposure_limit") or 0.0)
        twel_short = float(self.bot_value("short", "total_wallet_exposure_limit") or 0.0)
        if long_enabled and short_enabled:
            twel_str = f"L:{twel_long:.0%} S:{twel_short:.0%}"
        elif long_enabled:
            twel_str = f"{twel_long:.0%}"
        elif short_enabled:
            twel_str = f"{twel_short:.0%}"
        else:
            twel_str = "0%"

        # 构建内容行并动态计算宽度
        line1 = f"  PASSIVBOT  │  {exchange}:{user}  │  {now}  "
        line2 = f"  Mode: {mode}  │  Positions: {n_pos}  │  TWEL: {twel_str}  "
        width = max(len(line1), len(line2), 50)  # 最小 50 字符
        border = "═" * width

        # 填充行以匹配宽度
        line1 = line1.ljust(width)
        line2 = line2.ljust(width)

        logging.info("╔%s╗", border)
        logging.info("║%s║", line1)
        logging.info("╠%s╣", border)
        logging.info("║%s║", line2)
        logging.info("╚%s╝", border)

    def _format_duration(self, ms: int) -> str:
        """将毫秒格式化为可读的持续时间（如 '2d5h15m'）。"""
        total_seconds = ms // 1000
        days, remainder = divmod(total_seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, seconds = divmod(remainder, 60)
        if days > 0:
            return f"{days}d{hours}h{minutes}m"
        if hours > 0:
            return f"{hours}h{minutes}m"
        if minutes > 0:
            return f"{minutes}m{seconds}s"
        return f"{seconds}s"

    def _maybe_log_health_summary(self) -> None:
        """若间隔已到期，记录周期性健康摘要。"""
        now_ms = utc_ms()
        if (now_ms - self._health_last_summary_ms) < self._health_summary_interval_ms:
            return
        self._health_last_summary_ms = now_ms
        self._log_health_summary()

    def _log_health_summary(self) -> None:
        """记录包含运行时间和计数器的健康摘要。"""
        now_ms = utc_ms()
        uptime_ms = now_ms - self._health_start_ms
        uptime_str = self._format_duration(uptime_ms)

        # 统计当前持仓
        n_long = 0
        n_short = 0
        for symbol, pos_data in self.positions.items():
            if pos_data.get("long", {}).get("size", 0.0) != 0.0:
                n_long += 1
            if pos_data.get("short", {}).get("size", 0.0) != 0.0:
                n_short += 1

        balance_raw = self.get_raw_balance()
        balance_snapped = self.get_hysteresis_snapped_balance()
        balance_str = f"{balance_raw:.2f} {self.quote}"
        if abs(balance_raw - balance_snapped) > 1e-9:
            balance_str += f" (snap {balance_snapped:.2f})"

        # 若有成交则构建包含 PnL 的成交字符串
        if self._health_fills > 0:
            pnl_sign = "+" if self._health_pnl >= 0 else ""
            fills_str = f"fills={self._health_fills} (pnl={pnl_sign}{self._health_pnl:.2f})"
        else:
            fills_str = "fills=0"

        # 循环计时
        loop_ms = getattr(self, "_last_loop_duration_ms", 0)
        loop_str = f"{loop_ms / 1000:.1f}s" if loop_ms > 0 else "n/a"

        # 错误预算：最近一小时的错误数 vs 上限
        error_counts = getattr(self, "error_counts", [])
        now = utc_ms()
        recent_errors = len([x for x in error_counts if x > now - 1000 * 60 * 60])
        max_errors = 10
        error_budget_str = f"{recent_errors}/{max_errors}"

        # 内存使用
        try:
            import resource

            rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024 / 1024
            mem_str = f"rss={rss_mb:.0f}MB"
        except Exception:
            mem_str = ""

        logging.info(
            "[health] uptime=%s | loop=%s | positions=%d long, %d short | balance=%s | "
            "orders=+%d/-%d | %s | errors=%s | ws_reconnects=%d | rate_limits=%d%s",
            uptime_str,
            loop_str,
            n_long,
            n_short,
            balance_str,
            self._health_orders_placed,
            self._health_orders_cancelled,
            fills_str,
            error_budget_str,
            self._health_ws_reconnects,
            self._health_rate_limits,
            f" | {mem_str}" if mem_str else "",
        )

    def _calc_unstuck_allowance_for_logging(self, pside: str) -> dict:
        """计算用于日志的原始解套额度值（包含负值）。"""
        twel = float(self.bot_value(pside, "total_wallet_exposure_limit") or 0.0)
        if twel <= 0.0:
            return {"status": "disabled"}

        pct = float(self.bot_value(pside, "unstuck_loss_allowance_pct") or 0.0)
        if pct <= 0.0:
            return {"status": "unstuck_disabled"}

        if self._pnls_manager is None:
            return {"status": "no_pnl_manager"}

        events = self._get_effective_pnl_events()
        if not events:
            return {"status": "no_history"}

        pnls_cumsum = np.array([ev.pnl for ev in events]).cumsum()
        pnls_cumsum_max, pnls_cumsum_last = float(pnls_cumsum.max()), float(pnls_cumsum[-1])

        balance_raw = self.get_raw_balance()
        balance_peak = balance_raw + (pnls_cumsum_max - pnls_cumsum_last)
        pct_from_peak = (balance_raw / balance_peak - 1.0) * 100.0
        # 不带 .max(0.0) 的原始额度 — 可能为负值
        allowance_raw = balance_peak * (pct * twel + pct_from_peak / 100.0)

        return {
            "status": "ok",
            "allowance": allowance_raw,
            "peak": balance_peak,
            "pct_from_peak": pct_from_peak,
        }

    def _log_unstuck_status(self) -> None:
        """记录双向解套额度预算。"""
        parts = []
        for pside in ["long", "short"]:
            info = self._calc_unstuck_allowance_for_logging(pside)
            status = info.get("status")
            if status == "disabled":
                parts.append(f"{pside}: disabled")
            elif status == "unstuck_disabled":
                parts.append(f"{pside}: unstuck disabled")
            elif status == "no_pnl_manager" or status == "no_history":
                parts.append(f"{pside}: no pnl history")
            else:
                allowance = info["allowance"]
                if allowance < 0:
                    parts.append(
                        "%s: allowance=%.2f (over budget) | peak=%.2f | pct_from_peak=%.1f%%"
                        % (pside, allowance, info["peak"], info["pct_from_peak"])
                    )
                else:
                    parts.append(
                        "%s: allowance=%.2f | peak=%.2f | pct_from_peak=%.1f%%"
                        % (pside, allowance, info["peak"], info["pct_from_peak"])
                    )
        logging.info("[unstuck] %s", " | ".join(parts))

    def _maybe_log_unstuck_status(self) -> None:
        """若间隔已到期，记录周期性解套状态。"""
        now_ms = utc_ms()
        if (now_ms - self._unstuck_last_log_ms) < self._unstuck_log_interval_ms:
            return
        self._unstuck_last_log_ms = now_ms
        self._log_unstuck_status()

    async def start_bot(self):
        """初始化状态，预热缓存数据，并启动后台循环。"""
        self._log_startup_banner()
        self._bot_ready = False
        logging.info("[boot] starting bot %s...", self.exchange)
        boot_stage = "start"
        try:
            self._monitor_record_event(
                "bot.start",
                ("bot", "lifecycle", "start"),
                {
                    "exchange": self.exchange,
                    "user": self.user,
                    "pid": os.getpid(),
                    "quote": self.quote,
                    "start_time_ms": int(self.start_time_ms),
                },
                ts=int(self.start_time_ms),
            )

            # 随机启动错峰以分散多个机器人同时启动时的 API 负载。
            # 在 init_markets() 之前应用，确保首批 API 调用也错峰。
            boot_stage = "boot_stagger"
            boot_stagger = get_optional_live_value(self.config, "boot_stagger_seconds", None)
            if boot_stagger is None:
                exchange_lower = (self.exchange or "").lower()
                if exchange_lower == "hyperliquid":
                    boot_stagger = 30.0
                else:
                    boot_stagger = 0.0
            try:
                boot_stagger = float(boot_stagger)
            except Exception:
                boot_stagger = 0.0
            if boot_stagger > 0:
                delay = random.uniform(0, boot_stagger)
                logging.info(
                    "[boot] stagger delay: waiting %.1fs before init (max=%.0fs)...",
                    delay,
                    boot_stagger,
                )
                await asyncio.sleep(delay)

            boot_stage = "format_approved_ignored_coins"
            await format_approved_ignored_coins(
                self.config, self.user_info["exchange"], quote=self.quote
            )
            boot_stage = "init_markets"
            await self.init_markets()
            await self._monitor_flush_snapshot(force=True, ts=utc_ms())
            # 已批准交易对的交错式 K线预热（大集合也能平稳处理）
            boot_stage = "warmup_candles_staggered"
            try:
                await self.warmup_candles_staggered()
            except Exception as e:
                logging.info("[boot] warmup skipped due to: %s", e)
            if self._equity_hard_stop_enabled():
                boot_stage = "equity_hard_stop_initialize_from_history"
                await self._equity_hard_stop_initialize_from_history()
                if self.stop_signal_received:
                    self._monitor_emit_stop(
                        "startup_aborted",
                        ts=utc_ms(),
                        payload={"stage": boot_stage, "stop_signal_received": True},
                    )
                    return
            boot_stage = "post_init_sleep"
            await asyncio.sleep(1)
            self._log_memory_snapshot()
            logging.info("[boot] starting data maintainers...")
            boot_stage = "start_data_maintainers"
            await self.start_data_maintainers()

            logging.info("[boot] starting execution loop...")
            logging.info("[boot] ══════════════════════════════════════════════════════════════════════")
            logging.info("[boot] READY - Bot initialization complete, entering main trading loop")
            logging.info("[boot] ══════════════════════════════════════════════════════════════════════")
            self._bot_ready = True
            ready_ts = utc_ms()
            self._monitor_record_event(
                "bot.ready",
                ("bot", "lifecycle", "ready"),
                {"debug_mode": bool(self.debug_mode)},
                ts=ready_ts,
            )
            await self._monitor_flush_snapshot(force=True, ts=ready_ts)
            if not self.debug_mode:
                await self.run_execution_loop()
        except Exception as exc:
            error_ts = utc_ms()
            self._monitor_record_error(
                "error.bot",
                exc,
                tags=("error", "bot", "startup"),
                payload={"source": "start_bot", "stage": boot_stage},
                ts=error_ts,
            )
            await self._monitor_flush_snapshot(force=True, ts=error_ts)
            self._monitor_emit_stop(
                "startup_error",
                ts=error_ts,
                payload={"stage": boot_stage, "error_type": type(exc).__name__},
            )
            raise

    async def init_markets(self, verbose=True):
        """加载交易所市场元数据并刷新审批列表。"""
        # 在机器人启动时和之后每小时调用一次
        self.init_markets_last_update_ms = utc_ms()
        # 对瞬态网络错误重试（新 aiohttp 会话的 TCP + TLS 握手可能超时；
        # 同时每小时调用，瞬态错误不应中断刷新周期）。
        for _attempt in range(1, 4):
            try:
                await self.update_exchange_config()  # 设置 hedge mode
                break
            except (RequestTimeout, NetworkError) as e:
                if _attempt == 3:
                    raise
                logging.warning(
                    "[init_markets] update_exchange_config error (attempt %d/3): %s – retrying in %ds",
                    _attempt,
                    e,
                    5 * _attempt,
                )
                await asyncio.sleep(5 * _attempt)
        # 复用已有的 ccxt 会话（确保共享选项如 fetchMarkets 类型）。
        cc_instance = getattr(self, "cca", None)
        self.markets_dict = await load_markets(
            self.exchange, 0, verbose=False, cc=cc_instance, quote=self.quote
        )
        if hasattr(self, "refresh_and_log_user_abstraction_state"):
            await self.refresh_and_log_user_abstraction_state()
        # 不合格的交易对无法开新仓
        eligible, _, reasons = filter_markets(
            self.markets_dict, self.exchange, quote=self.quote, verbose=verbose
        )
        self.eligible_symbols = set(eligible)
        self.ineligible_symbols = reasons
        self.set_market_specific_settings()
        # 用于更美观的打印
        self.max_len_symbol = max([len(s) for s in self.markets_dict])
        self.sym_padding = max(self.sym_padding, self.max_len_symbol + 1)
        # await self.init_flags()
        self.init_coin_overrides()
        # await self.update_tickers()
        self.refresh_approved_ignored_coins_lists()
        self._assert_supported_live_state()
        # self.set_live_configs()
        self.set_wallet_exposure_limits()
        await self.update_positions_and_balance()
        await self.update_open_orders()
        self._assert_supported_live_state()
        await self.update_effective_min_cost()
        # 传统：不再进行 1m OHLCV REST 维护；CandlestickManager 处理缓存
        if self.is_forager_mode():
            await self.update_first_timestamps()

    def log_once(self, msg: str):
        if not hasattr(self, "log_once_set"):
            self.log_once_set = set()
        if msg in self.log_once_set:
            return
        logging.info(msg)
        self.log_once_set.add(msg)

    def _log_ema_gating(
        self,
        ideal_orders: dict,
        m1_close_emas: dict,
        last_prices: dict,
        symbols: list,
    ) -> None:
        """记录因 EMA 距离门控导致的入场被阻断情况。

        对于处于正常模式且无持仓的交易对，若没有初始入场订单，
        检查价格是否超出 EMA 入场阈值并记录原因。
        """
        if not hasattr(self, "_ema_gating_last_log_ms"):
            self._ema_gating_last_log_ms = {}
        ema_gating_throttle_ms = 300_000  # 每个交易对/方向每 5 分钟记录一次
        now_ms = utc_ms()

        for symbol in symbols:
            for pside in ("long", "short"):
                # 检查模式是否为 normal（非 graceful_stop、manual 等）
                mode = self.PB_modes.get(symbol, {}).get(pside)
                if mode != "normal":
                    continue

                # 检查是否已有持仓
                pos = self.positions.get(symbol, {}).get(pside, {})
                pos_size = abs(pos.get("size", 0.0))
                if pos_size > 0:
                    continue

                # 检查该交易对/方向是否有初始入场订单
                symbol_orders = ideal_orders.get(symbol, [])
                has_initial_entry = any(
                    f"entry_initial" in (o[2] if len(o) > 2 else "")
                    and pside in (o[2] if len(o) > 2 else "")
                    for o in symbol_orders
                )
                if has_initial_entry:
                    continue

                # 无初始入场 — 检查是否因 EMA 门控
                try:
                    span0 = float(self.bp(pside, "ema_span_0", symbol))
                    span1 = float(self.bp(pside, "ema_span_1", symbol))
                    ema_dist = float(self.bp(pside, "entry_initial_ema_dist", symbol))

                    if span0 <= 0 or span1 <= 0:
                        continue

                    span2 = (span0 * span1) ** 0.5
                    emas = m1_close_emas.get(symbol, {})
                    ema0 = emas.get(span0, 0.0)
                    ema1 = emas.get(span1, 0.0)
                    ema2 = emas.get(span2, 0.0)

                    if ema0 <= 0 or ema1 <= 0 or ema2 <= 0:
                        continue

                    ema_lower = min(ema0, ema1, ema2)
                    ema_upper = max(ema0, ema1, ema2)
                    current_price = last_prices.get(symbol, 0.0)

                    if current_price <= 0:
                        continue

                    # 计算 EMA 入场阈值并检查是否被门控
                    if pside == "long":
                        ema_entry_price = ema_lower * (1.0 - ema_dist)
                        is_gated = current_price > ema_entry_price
                        dist_pct = (
                            (current_price / ema_entry_price - 1.0) * 100
                            if ema_entry_price > 0
                            else 0
                        )
                    else:  # 空头
                        ema_entry_price = ema_upper * (1.0 + ema_dist)
                        is_gated = current_price < ema_entry_price
                        dist_pct = (
                            (1.0 - current_price / ema_entry_price) * 100
                            if ema_entry_price > 0
                            else 0
                        )

                    if is_gated and abs(dist_pct) > 0.1:  # 仅在有意义的门控时记录
                        throttle_key = f"{symbol}:{pside}"
                        last_log_ms = self._ema_gating_last_log_ms.get(throttle_key, 0)
                        if (now_ms - last_log_ms) < ema_gating_throttle_ms:
                            continue
                        self._ema_gating_last_log_ms[throttle_key] = now_ms

                        coin = symbol.split("/")[0] if "/" in symbol else symbol
                        logging.info(
                            "[ema] %s %s entry gated | price=%.4g ema_thresh=%.4g (+%.1f%% away)",
                            coin,
                            pside,
                            current_price,
                            ema_entry_price,
                            dist_pct,
                        )
                except Exception:
                    pass  # 任何计算错误时静默跳过

    def debug_print(self, *args):
        """仅在实例处于调试模式时输出调试信息。"""
        if hasattr(self, "debug_mode") and self.debug_mode:
            print(*args)

    def _log_memory_snapshot(self, *, now_ms: Optional[int] = None) -> None:
        """记录进程 RSS 和关键缓存指标以供可观测性。"""
        if now_ms is None:
            now_ms = utc_ms()
        rss = _get_process_rss_bytes()
        if rss is None:
            return
        cache_bytes = None
        cache_candles = None
        cache_symbols = None
        cache_top = None
        tf_cache_bytes = None
        tf_cache_ranges = None
        tf_cache_top = None
        try:
            cache = getattr(self.cm, "_cache", {}) if hasattr(self, "cm") else {}
            cache_symbols = len(cache)
            stats = []
            for sym, arr in cache.items():
                if arr is None:
                    continue
                arr_bytes = int(getattr(arr, "nbytes", 0))
                arr_rows = int(arr.shape[0]) if hasattr(arr, "shape") else 0
                stats.append((sym, arr_bytes, arr_rows))
            cache_bytes = sum(val for _, val, _ in stats)
            cache_candles = sum(rows for _, _, rows in stats)
            if stats:
                top = sorted(stats, key=lambda item: item[1], reverse=True)[:3]
                cache_top = ", ".join(
                    f"{sym}:{bytes_ / (1024 * 1024):.1f}MiB/{rows}" for sym, bytes_, rows in top
                )
            tf_cache = getattr(self.cm, "_tf_range_cache", {}) if hasattr(self, "cm") else {}
            tf_stats = []
            for sym, entries in tf_cache.items():
                if not isinstance(entries, dict):
                    continue
                for key, val in entries.items():
                    try:
                        tf_label = key[0] if isinstance(key, tuple) and key else str(key)
                    except Exception:
                        tf_label = "unknown"
                    arr = val[0] if isinstance(val, tuple) and val else val
                    if not hasattr(arr, "nbytes"):
                        continue
                    arr_bytes = int(getattr(arr, "nbytes", 0))
                    arr_rows = int(arr.shape[0]) if hasattr(arr, "shape") else 0
                    tf_stats.append(((sym, tf_label), arr_bytes, arr_rows))
            if tf_stats:
                tf_cache_bytes = sum(size for _, size, _ in tf_stats)
                tf_cache_ranges = len(tf_stats)
                top_tf = sorted(tf_stats, key=lambda item: item[1], reverse=True)[:3]
                tf_cache_top = ", ".join(
                    f"{sym}:{tf}:{bytes_ / (1024 * 1024):.1f}MiB/{rows}"
                    for (sym, tf), bytes_, rows in top_tf
                )
        except Exception:
            cache_bytes = None
        prev = getattr(self, "_mem_log_prev", None)
        pct_change = None
        if prev and prev.get("rss"):
            prev_rss = prev["rss"]
            if prev_rss:
                pct_change = 100.0 * (rss - prev_rss) / prev_rss
        parts = [f"[memory] rss={rss / (1024 * 1024):.2f} MiB"]
        if pct_change is not None:
            parts.append(f"Δ={pct_change:+.2f}% vs previous snapshot")
        if cache_bytes is not None:
            cache_mib = cache_bytes / (1024 * 1024)
            cache_desc = f"cm_cache={cache_mib:.2f} MiB"
            if cache_candles is not None:
                detail = f"{cache_candles} candles"
                if cache_symbols is not None:
                    detail += f" across {cache_symbols} symbols"
                cache_desc += f" ({detail})"
            parts.append(cache_desc)
            if cache_top:
                parts.append(f"cm_top={cache_top}")
        if tf_cache_bytes is not None:
            tf_desc = f"cm_tf_cache={tf_cache_bytes / (1024 * 1024):.2f} MiB"
            if tf_cache_ranges is not None:
                tf_desc += f" ({tf_cache_ranges} ranges)"
            parts.append(tf_desc)
            if tf_cache_top:
                parts.append(f"cm_tf_top={tf_cache_top}")
        try:
            loop = asyncio.get_running_loop()
            tasks = asyncio.all_tasks(loop)
            total_tasks = len(tasks)
            pending = sum(1 for t in tasks if not t.done())
            task_counts: Dict[str, int] = {}
            for t in tasks:
                coro = getattr(t, "get_coro", None)
                name = None
                if callable(coro):
                    try:
                        coro_obj = coro()
                        name = getattr(coro_obj, "__qualname__", None)
                    except Exception:
                        name = None
                if not name:
                    name = getattr(t, "get_name", lambda: None)()
                if not name:
                    name = type(t).__name__
                task_counts[name] = task_counts.get(name, 0) + 1
            top_tasks = ", ".join(
                f"{name}:{count}"
                for name, count in sorted(task_counts.items(), key=lambda kv: kv[1], reverse=True)[:4]
            )
            parts.append(f"tasks={total_tasks} pending={pending}")
            if top_tasks:
                parts.append(f"task_top={top_tasks}")
        except Exception:
            pass
        logging.info("; ".join(parts))
        self._mem_log_prev = {"timestamp": now_ms, "rss": rss}
        if cache_bytes is not None:
            self._mem_log_prev["cm_cache_bytes"] = cache_bytes

    def init_coin_overrides(self):
        """填充以交易对为键的币种覆盖映射，便于快速查找。"""
        self.coin_overrides = {
            s: v
            for k, v in self.config.get("coin_overrides", {}).items()
            if (s := self.coin_to_symbol(k))
        }
        if self.coin_overrides:
            logging.debug(
                "Initialized coin overrides for %s",
                ", ".join(sorted(self.coin_overrides.keys())),
            )

    def config_get(self, path: [str], symbol=None):
        """
        获取配置值，当提供交易对时优先使用逐交易对覆盖。
        """
        log_key = None
        if symbol and symbol in self.coin_overrides:
            d = self.coin_overrides[symbol]
            for p in path:
                if isinstance(d, dict) and p in d:
                    d = d[p]
                else:
                    d = None
                    break
            if d is not None:
                log_key = (symbol, ".".join(path))
                if not hasattr(self, "_override_hits_logged"):
                    self._override_hits_logged = set()
                if log_key not in self._override_hits_logged:
                    logging.debug("Using override for %s: %s", symbol, ".".join(path))
                    self._override_hits_logged.add(log_key)
                return d

        # 回退到全局配置
        d = self.config
        for p in path:
            if isinstance(d, dict) and p in d:
                d = d[p]
            else:
                raise KeyError(f"Key path {'.'.join(path)} not found in config or coin overrides.")
        return d

    def bp(self, pside, key, symbol=None):
        """
        精简辅助函数（bp = bot param），封装 config_get(['bot', pside, key], symbol)
        """
        return self.config_get(["bot", pside, key], symbol)

    def maybe_log_ema_debug(
        self,
        ema_bounds_long: Dict[str, Tuple[float, float]],
        ema_bounds_short: Dict[str, Tuple[float, float]],
        entry_volatility_logrange_ema_1h: Dict[str, Dict[str, float]],
    ) -> None:
        """按条件输出 EMA 调试日志，显示各交易对的 EMA 边界和波动率指标。"""

        ema_debug_logging_enabled = False

        """以节流方式输出 EMA 输入日志，使切换可见性保持简单。"""
        if not ema_debug_logging_enabled:
            return
        self._ema_debug_log_interval_ms = 30_000
        self._last_ema_debug_log_ms = 0
        now = utc_ms()
        if now - getattr(self, "_last_ema_debug_log_ms", 0) < self._ema_debug_log_interval_ms:
            return
        self._last_ema_debug_log_ms = now

        def _safe_span(pside: str, key: str, symbol: str) -> Optional[int]:
            try:
                val = self.bp(pside, key, symbol)
                return int(val) if val is not None else None
            except Exception:
                return None

        logs: List[str] = []
        for pside, bounds in ("long", ema_bounds_long), ("short", ema_bounds_short):
            if not bounds:
                continue
            side_entries: List[str] = []
            for symbol, (lower, upper) in sorted(bounds.items()):
                span0 = _safe_span(pside, "ema_span_0", symbol)
                span1 = _safe_span(pside, "ema_span_1", symbol)
                grid_lr = (entry_volatility_logrange_ema_1h or {}).get(pside, {}).get(symbol)
                parts = [f"{symbol}"]
                if span0 is not None or span1 is not None:
                    parts.append(
                        f"spans=({span0 if span0 is not None else '?'}"
                        f", {span1 if span1 is not None else '?'})"
                    )
                parts.append(f"lower={lower:.6g}")
                parts.append(f"upper={upper:.6g}")
                if grid_lr is not None:
                    parts.append(f"log_range_ema={grid_lr:.6g}")
                side_entries.append(" ".join(parts))
            if side_entries:
                logs.append(f"{pside} -> " + "; ".join(side_entries))

        if logs:
            logging.info("EMA debug | " + " | ".join(logs))

    async def warmup_candles_staggered(
        self,
        *,
        concurrency: int | None = None,
        window_candles: int | None = None,
        ttl_ms: int = 300_000,
    ) -> None:
        """以交错方式预热所有已批准交易对的近期 K线。

        - concurrency: max in-flight symbols; if None, uses config or exchange-specific default
        - window_candles: number of 1m candles to warm; defaults to CandlestickManager.default_window_candles
        - ttl_ms: skip refresh if data newer than this TTL exists

        预热超过 20 个交易对时输出精简倒计时日志。
        """
        # 构建交易对集合：惰性预热。若有空位，预热该方向的合格交易对。
        # 若仓位已满，仅预热有持仓的交易对。
        if not hasattr(self, "approved_coins_minus_ignored_coins"):
            return
        symbols_by_side: Dict[str, set] = {}
        forager_needed = {"long": False, "short": False}
        slots_open_by_side: Dict[str, bool] = {}
        pos_counts: Dict[str, int] = {}
        max_counts: Dict[str, int] = {}
        for pside in ("long", "short"):
            try:
                max_n = int(self.get_max_n_positions(pside))
            except Exception:
                max_n = 0
            try:
                current_n = int(self.get_current_n_positions(pside))
            except Exception:
                current_n = len(self.get_symbols_with_pos(pside))
            max_counts[pside] = max_n
            pos_counts[pside] = current_n
            slots_open = max_n > current_n
            slots_open_by_side[pside] = bool(slots_open)
            forager_needed[pside] = bool(self.is_forager_mode(pside) and slots_open)
            if slots_open:
                symbols_by_side[pside] = set(self.get_symbols_approved_or_has_pos(pside))
            else:
                symbols_by_side[pside] = set(self.get_symbols_with_pos(pside))
        symbols = sorted(set().union(*symbols_by_side.values()))
        if not symbols:
            return

        # 确定并发数：显式参数 > 配置 > 交易所特定默认值
        if concurrency is None:
            cfg_concurrency = get_optional_live_value(self.config, "warmup_concurrency", 0)
            try:
                cfg_concurrency = int(cfg_concurrency) if cfg_concurrency else 0
            except Exception:
                cfg_concurrency = 0
            if cfg_concurrency > 0:
                concurrency = cfg_concurrency
            else:
                # 交易所特定默认值：Hyperliquid 具有更严格的速率限制
                exchange_lower = self.exchange.lower() if self.exchange else ""
                if exchange_lower == "hyperliquid":
                    concurrency = 1
                else:
                    concurrency = 8
        concurrency = max(1, int(concurrency))

        # 随机抖动延迟，防止多个机器人同时启动时引发 API 速率限制风暴
        max_jitter = get_optional_live_value(self.config, "warmup_jitter_seconds", 30.0)
        try:
            max_jitter = float(max_jitter)
        except Exception:
            max_jitter = 30.0
        if max_jitter > 0:
            jitter = random.uniform(0, max_jitter)
            if jitter > 5:
                logging.info(
                    "[boot] warmup jitter: waiting %.1fs before starting (max=%.0fs)...",
                    jitter,
                    max_jitter,
                )
                # 对于较长的等待，每 10 秒记录一次进度
                waited = 0.0
                while waited < jitter:
                    sleep_chunk = min(10.0, jitter - waited)
                    await asyncio.sleep(sleep_chunk)
                    waited += sleep_chunk
                    if waited < jitter:
                        logging.info("[boot] warmup jitter: %.0fs remaining...", jitter - waited)
            else:
                logging.info("[boot] warmup jitter: sleeping %.1fs (max=%.0fs)", jitter, max_jitter)
                await asyncio.sleep(jitter)

        n = len(symbols)
        now = utc_ms()
        end_final = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
        # 根据实际 EMA 需求确定每个交易对的窗口（惰性且节省）。
        # 获取 max-span * (1 + warmup_ratio)，为 EMA 提供足够的运行空间而不过度抓取。
        default_win = int(getattr(self.cm, "default_window_candles", 120))
        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0
        large_span_threshold = 2 * 24 * 60  # 分钟；匹配 CandlestickManager 大跨度逻辑

        per_symbol_win, per_symbol_h1_hours, per_symbol_skip_historical = compute_live_warmup_windows(
            symbols_by_side,
            lambda pside, key, sym: self.bp(pside, key, sym),
            forager_enabled=forager_needed,
            window_candles=window_candles,
            warmup_ratio=warmup_ratio,
            max_warmup_minutes=max_warmup_minutes,
            large_span_threshold=large_span_threshold,
        )
        end_final_hour = (now // (60 * ONE_MIN_MS)) * (60 * ONE_MIN_MS) - 60 * ONE_MIN_MS
        try:
            await self.rebuild_required_candle_indices(
                symbols,
                per_symbol_win,
                per_symbol_h1_hours,
                end_final,
                end_final_hour,
            )
        except Exception as e:
            logging.info("[boot] candle index rebuild skipped due to: %s", e)

        sem = asyncio.Semaphore(max(1, int(concurrency)))
        completed = 0
        started_ms = utc_ms()
        last_log_ms = started_ms

        # 信息性启动日志
        if n > 0:
            wmins = [per_symbol_win[s] for s in symbols]
            wmin, wmax = (min(wmins), max(wmins)) if wmins else (default_win, default_win)
            logging.info(
                f"[warmup] starting: {n} symbols, concurrency={concurrency}, ttl={int(ttl_ms/1000)}s, window=[{wmin},{wmax}]m"
            )
            try:
                longest_span = int(math.ceil(wmax / max(1.0, (1.0 + warmup_ratio))))
            except Exception:
                longest_span = wmax
            logging.info(
                "[warmup] target | longest_span=%dm warmup_ratio=%.3g max_warmup_minutes=%s",
                int(longest_span),
                float(warmup_ratio),
                "none" if not max_warmup_minutes else str(int(max_warmup_minutes)),
            )
            try:
                logging.info(
                    "[warmup] slot view | long: %d/%d open=%s forager=%s symbols=%d | short: %d/%d open=%s forager=%s symbols=%d",
                    pos_counts.get("long", 0),
                    max_counts.get("long", 0),
                    "yes" if slots_open_by_side.get("long") else "no",
                    "yes" if forager_needed.get("long") else "no",
                    len(symbols_by_side.get("long", set())),
                    pos_counts.get("short", 0),
                    max_counts.get("short", 0),
                    "yes" if slots_open_by_side.get("short") else "no",
                    "yes" if forager_needed.get("short") else "no",
                    len(symbols_by_side.get("short", set())),
                )
            except Exception:
                pass
            try:
                long_syms = symbols_by_side.get("long", set())
                short_syms = symbols_by_side.get("short", set())
                long_wins = [per_symbol_win[s] for s in long_syms if s in per_symbol_win]
                short_wins = [per_symbol_win[s] for s in short_syms if s in per_symbol_win]
                long_min = min(long_wins) if long_wins else 0
                long_max = max(long_wins) if long_wins else 0
                short_min = min(short_wins) if short_wins else 0
                short_max = max(short_wins) if short_wins else 0
                logging.info(
                    "[warmup] windows | long:[%d,%d]m short:[%d,%d]m",
                    long_min,
                    long_max,
                    short_min,
                    short_max,
                )
            except Exception:
                pass
                # 启用批量模式以减少预热期间的零 K线合成警告
            self.cm.start_synth_candle_batch()
                # 启用批量模式以减少预热期间的 K线替换日志
            self.cm.start_candle_replace_batch()

        fetch_delay_s = self._get_fetch_delay_seconds()

        async def one(sym: str):
            """获取单个交易对的 K 线预热数据。"""
            nonlocal completed, last_log_ms
            async with sem:
                try:
                    win = int(per_symbol_win.get(sym, default_win))
                    skip_hist = bool(per_symbol_skip_historical.get(sym, True))
                    start_ts = int(end_final - ONE_MIN_MS * max(1, win))
                    await self.cm.get_candles(
                        sym,
                        start_ts=start_ts,
                        end_ts=None,
                        max_age_ms=ttl_ms,
                        strict=False,
                        skip_historical_gap_fill=skip_hist,  # 允许对大跨度预热进行缺口填充
                        max_lookback_candles=win,
                    )
                except Exception:
                    pass
                finally:
                    if fetch_delay_s > 0:
                        await asyncio.sleep(fetch_delay_s)
                    completed += 1
                        # 基于时间的节流：每约 2 秒或完成时记录
                    if n > 20:
                        now_ms = utc_ms()
                        if (completed == n) or (now_ms - last_log_ms >= 2000) or completed == 1:
                            elapsed_s = max(0.001, (now_ms - started_ms) / 1000.0)
                            rate = completed / elapsed_s
                            remaining = max(0, n - completed)
                            eta_s = int(remaining / max(1e-6, rate))
                            pct = int(100 * completed / n)
                            logging.info(
                                f"[warmup] candles: {completed}/{n} {pct}% elapsed={int(elapsed_s)}s eta~{eta_s}s"
                            )
                            last_log_ms = now_ms

        await asyncio.gather(*(one(s) for s in symbols))

        # 预热 1h K线用于网格对数范围 EMA
        hour_sem = asyncio.Semaphore(max(1, int(concurrency)))

        async def warm_hour(sym: str):
            """获取单个交易对的小时级预热数据。"""
            async with hour_sem:
                warm_hours = int(per_symbol_h1_hours.get(sym, 0) or 0)
                if warm_hours <= 0:
                    return
                start_ts = int(end_final_hour - warm_hours * 60 * ONE_MIN_MS)
                try:
                    await self.cm.get_candles(
                        sym,
                        start_ts=start_ts,
                        end_ts=None,
                        max_age_ms=ttl_ms,
                        timeframe="1h",
                        strict=False,
                        skip_historical_gap_fill=True,  # 实盘预热：不浪费时间处理旧缺口
                        max_lookback_candles=warm_hours,
                    )
                except Exception:
                    pass
                finally:
                    if fetch_delay_s > 0:
                        await asyncio.sleep(fetch_delay_s)

        await asyncio.gather(*(warm_hour(s) for s in symbols))

        # 刷新批量零 K线合成警告
        self.cm.flush_synth_candle_batch()
        # 刷新批量 K线替换日志
        self.cm.flush_candle_replace_batch()

    async def rebuild_required_candle_indices(
        self,
        symbols: Iterable[str],
        per_symbol_win: Dict[str, int],
        per_symbol_h1_hours: Dict[str, int],
        end_final: int,
        end_final_hour: int,
    ) -> None:
        """为所需预热范围重建 K 线索引元数据。"""
        if not getattr(self, "cm", None):
            return

        symbols = list(symbols or [])
        if not symbols:
            return

        started = utc_ms()
        logging.info(
            "[boot] rebuilding candle index for %d symbols (recent ranges only)...", len(symbols)
        )

        def _rebuild_sync() -> Tuple[int, int]:
            """同步重建 K 线索引，返回更新和移除的计数。"""
            updated_total = 0
            removed_total = 0
            for sym in symbols:
                win = int(per_symbol_win.get(sym, 0) or 0)
                if win > 0 and end_final > 0:
                    start_ts = max(0, int(end_final - win * ONE_MIN_MS))
                    res = self.cm.rebuild_index_for_range(
                        sym,
                        start_ts,
                        int(end_final),
                        timeframe="1m",
                        log_level="debug",
                    )
                    updated_total += int(res.get("updated", 0) or 0)
                    removed_total += int(res.get("removed", 0) or 0)
                warm_hours = int(per_symbol_h1_hours.get(sym, 0) or 0)
                if warm_hours > 0 and end_final_hour > 0:
                    start_ts = max(0, int(end_final_hour - warm_hours * 60 * ONE_MIN_MS))
                    res = self.cm.rebuild_index_for_range(
                        sym,
                        start_ts,
                        int(end_final_hour),
                        timeframe="1h",
                        log_level="debug",
                    )
                    updated_total += int(res.get("updated", 0) or 0)
                    removed_total += int(res.get("removed", 0) or 0)
            return updated_total, removed_total

        updated_total, removed_total = await asyncio.to_thread(_rebuild_sync)
        elapsed_s = max(0.0, (utc_ms() - started) / 1000.0)
        logging.info(
            "[boot] candle index rebuild complete: updated=%d removed=%d elapsed=%.2fs",
            updated_total,
            removed_total,
            elapsed_s,
        )

    async def update_first_timestamps(self, symbols=[]):
        """获取并缓存指定交易对的首笔交易时间戳。"""
        if not hasattr(self, "first_timestamps"):
            self.first_timestamps = {}
        symbols = sorted(set(symbols + flatten(self.approved_coins_minus_ignored_coins.values())))
        if all([s in self.first_timestamps for s in symbols]):
            return
        first_timestamps = await get_first_timestamps_unified(symbols)
        self.first_timestamps.update(first_timestamps)
        for symbol in sorted(self.first_timestamps):
            symbolf = self.coin_to_symbol(symbol, verbose=False)
            if symbolf not in self.markets_dict:
                continue
            if symbolf not in self.first_timestamps:
                self.first_timestamps[symbolf] = self.first_timestamps[symbol]
        for symbol in symbols:
            if symbol not in self.first_timestamps:
                logging.info(f"warning: unable to get first timestamp for {symbol}. Setting to zero.")
                self.first_timestamps[symbol] = 0.0

    async def audit_required_candle_disk_coverage(
        self, symbols: Optional[Iterable[str]] = None
    ) -> None:
        """检查所需 K线范围的磁盘覆盖情况并记录缺失区间。"""
        try:
            if self.cm is None:
                return
        except Exception:
            return

        # 仅记录与实盘机器人活跃相关的交易对。
        def _should_log_symbol(sym: str) -> bool:
            try:
                if sym in getattr(self, "active_symbols", []):
                    return True
            except Exception:
                pass
            try:
                if sym in getattr(self, "open_orders", {}) and self.open_orders.get(sym):
                    return True
            except Exception:
                pass
            try:
                return bool(self.has_position(sym))
            except Exception:
                return False

        symbol_filter = set(symbols) if symbols is not None else None
        symbols_by_side: Dict[str, set] = {}
        forager_needed = {"long": False, "short": False}
        for pside in ("long", "short"):
            try:
                max_n = int(self.get_max_n_positions(pside))
            except Exception:
                max_n = 0
            try:
                current_n = int(self.get_current_n_positions(pside))
            except Exception:
                current_n = len(self.get_symbols_with_pos(pside))
            slots_open = max_n > current_n
            forager_needed[pside] = bool(self.is_forager_mode(pside) and slots_open)
            try:
                if slots_open:
                    syms = set(self.get_symbols_approved_or_has_pos(pside))
                else:
                    syms = set(self.get_symbols_with_pos(pside))
            except Exception:
                syms = set()
            if symbol_filter is not None:
                syms = syms & symbol_filter
            symbols_by_side[pside] = syms
        symbol_list = sorted(set().union(*symbols_by_side.values()))
        if not symbol_list:
            return

        forager_enabled = {
            "long": bool(forager_needed.get("long")),
            "short": bool(forager_needed.get("short")),
        }

        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0

        per_symbol_win, per_symbol_h1_hours, _ = compute_live_warmup_windows(
            symbols_by_side,
            lambda pside, key, sym: self.bp(pside, key, sym),
            forager_enabled=forager_enabled,
            warmup_ratio=warmup_ratio,
            max_warmup_minutes=max_warmup_minutes,
        )

        now = utc_ms()
        end_final = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
        end_final_hour = (now // (60 * ONE_MIN_MS)) * (60 * ONE_MIN_MS) - 60 * ONE_MIN_MS
        tail_slack_ms = int(getattr(self, "candle_disk_check_tail_slack_ms", 0) or 0)
        tail_slack_hour_ms = int(getattr(self, "candle_disk_check_tail_slack_hour_ms", 0) or 0)
        end_final = max(0, int(end_final) - tail_slack_ms)
        end_final_hour = max(0, int(end_final_hour) - tail_slack_hour_ms)

        for sym in symbol_list:
            win = int(per_symbol_win.get(sym, 0) or 0)
            if win > 0 and end_final > 0:
                start_ts = max(0, int(end_final - win * ONE_MIN_MS))
                log_level = "debug"
                self.cm.check_disk_coverage(
                    sym,
                    start_ts,
                    int(end_final),
                    timeframe="1m",
                    log_level=log_level,
                )
            warm_hours = int(per_symbol_h1_hours.get(sym, 0) or 0)
            if warm_hours > 0 and end_final_hour > 0:
                start_ts = max(0, int(end_final_hour - warm_hours * 60 * ONE_MIN_MS))
                log_level = "debug"
                self.cm.check_disk_coverage(
                    sym,
                    start_ts,
                    int(end_final_hour),
                    timeframe="1h",
                    log_level=log_level,
                )

    def get_first_timestamp(self, symbol):
        """返回 `symbol` 缓存的首个可交易时间戳，填充默认值。"""
        if symbol not in self.first_timestamps:
            logging.info(f"warning: {symbol} missing from first_timestamps. Setting to zero.")
            self.first_timestamps[symbol] = 0.0
        return self.first_timestamps[symbol]

    def coin_to_symbol(self, coin, verbose=True):
        """将币种标识映射到交易所特定的交易对。"""
        if coin == "":
            return ""
        if not hasattr(self, "coin_to_symbol_map"):
            self.coin_to_symbol_map = {}
        if coin in self.coin_to_symbol_map:
            return self.coin_to_symbol_map[coin]
        coinf = symbol_to_coin(coin, verbose=verbose)
        if coinf in self.coin_to_symbol_map:
            self.coin_to_symbol_map[coin] = self.coin_to_symbol_map[coinf]
            return self.coin_to_symbol_map[coinf]
        result = coin_to_symbol(coin, self.exchange, quote=self.quote, verbose=verbose)
        self.coin_to_symbol_map[coin] = result
        return result

    def order_to_order_tuple(self, order):
        """将订单字典转换为用于比较的标准化元组。"""
        return (
            order["symbol"],
            order["side"],
            order["position_side"],
            round(float(order["qty"]), 12),
            round(float(order["price"]), 12),
        )

    def has_open_unstuck_order(self) -> bool:
        """若交易所当前挂有解套订单则返回 True。"""
        for orders in getattr(self, "open_orders", {}).values():
            for order in orders or []:
                custom_id = order.get("custom_id") if isinstance(order, dict) else None
                if not custom_id:
                    continue
                type_id = try_decode_type_id_from_custom_id(custom_id)
                if type_id is None:
                    continue
                try:
                    order_type = snake_of(type_id)
                except Exception:
                    continue
                if order_type in {"close_unstuck_long", "close_unstuck_short"}:
                    return True
        return False

    async def run_execution_loop(self):
        """主执行循环，协调订单生成和交易所交互。"""
        failed_update_pos_oos_pnls_ohlcvs_count = 0
        max_n_fails = 10
        if self._equity_hard_stop_enabled() and not all(
            self._equity_hard_stop_runtime_initialized(pside)
            or not self._equity_hard_stop_enabled(pside)
            for pside in self._hsl_psides()
        ):
            await self._equity_hard_stop_initialize_from_history()
        while not self.stop_signal_received:
            try:
                loop_start_ms = utc_ms()
                self.execution_scheduled = False
                self.state_change_detected_by_symbol = set()
                self._set_log_silence_watchdog_context(
                    phase="runtime", stage="update_pos_oos_pnls_ohlcvs"
                )
                if not await self.update_pos_oos_pnls_ohlcvs():
                    await asyncio.sleep(0.5)
                    failed_update_pos_oos_pnls_ohlcvs_count += 1
                    if failed_update_pos_oos_pnls_ohlcvs_count > max_n_fails:
                        await self.restart_bot_on_too_many_errors()
                    continue
                failed_update_pos_oos_pnls_ohlcvs_count = 0
                if self.stop_signal_received:
                    break
                if self._equity_hard_stop_enabled():
                    await self._equity_hard_stop_check()
                    if any(
                        self._equity_hard_stop_runtime_red_latched(pside)
                        and not self._hsl_state(pside)["halted"]
                        for pside in self._hsl_psides()
                        if self._equity_hard_stop_enabled(pside)
                    ):
                        await self._equity_hard_stop_run_red_supervisor()
                        continue
                if self.stop_signal_received:
                    break
                self._set_log_silence_watchdog_context(phase="runtime", stage="execute_to_exchange")
                res = await self.execute_to_exchange()
                if self.debug_mode:
                    return res
                if self.stop_signal_received:
                    break
                # 跟踪循环耗时用于健康报告
                self._last_loop_duration_ms = utc_ms() - loop_start_ms
                # 周期性健康摘要
                self._maybe_log_health_summary()
                self._maybe_log_unstuck_status()
                self._set_log_silence_watchdog_context(phase="runtime", stage="flush_snapshot")
                await self._monitor_flush_snapshot()
                self._set_log_silence_watchdog_context(phase="runtime", stage="execution_delay")
                await asyncio.sleep(float(self.live_value("execution_delay_seconds")))
                sleep_duration = 30
                self._set_log_silence_watchdog_context(phase="runtime", stage="scheduled_wait")
                for i in range(sleep_duration * 10):
                    if self.execution_scheduled or self.stop_signal_received:
                        break
                    await asyncio.sleep(0.1)
            except RestartBotException:
                raise  # 传播重启请求，不增加错误计数
            except FatalBotException:
                raise
            except RateLimitExceeded as e:
                self._health_errors += 1
                self._health_rate_limits += 1
                self._monitor_record_error(
                    "error.exchange",
                    e,
                    tags=("error", "exchange", "rate_limit"),
                    payload={"source": "run_execution_loop"},
                )
                logging.warning("[rate] execution loop hit rate limit; backing off 5s...")
                await self.restart_bot_on_too_many_errors()
                await asyncio.sleep(5.0)
            except Exception as e:
                self._health_errors += 1
                self._monitor_record_error(
                    "error.bot",
                    e,
                    tags=("error", "bot"),
                    payload={"source": "run_execution_loop"},
                )
                logging.error(f"error with {get_function_name()} {e}")
                traceback.print_exc()
                await self.restart_bot_on_too_many_errors()
                await asyncio.sleep(1.0)

    async def shutdown_gracefully(self):
        """优雅关闭：停止维护器、关闭连接、清空持仓。"""
        if getattr(self, "_shutdown_in_progress", False):
            return
        self._shutdown_in_progress = True
        self.stop_signal_received = True
        stop_ts = utc_ms()
        self._monitor_emit_stop("shutdown_gracefully", ts=stop_ts)
        logging.info("[shutdown] shutdown requested; closing background tasks and sessions")
        maintainer_tasks = []
        try:
            self.stop_data_maintainers(verbose=False)
            for task_map_name in ("maintainers", "WS_ohlcvs_1m_tasks"):
                task_map = getattr(self, task_map_name, None)
                if not task_map:
                    continue
                for task in task_map.values():
                    if task is not None:
                        maintainer_tasks.append(task)
        except Exception as e:
            logging.error("[shutdown] error stopping maintainers: %s", e)
        if maintainer_tasks:
            try:
                await asyncio.gather(*maintainer_tasks, return_exceptions=True)
            except Exception as e:
                logging.error("[shutdown] error awaiting maintainer cancellation: %s", e)
        await asyncio.sleep(0)
        try:
            if getattr(self, "ccp", None) is not None:
                await self.ccp.close()
                self.ccp = None
        except Exception as e:
            logging.error("[shutdown] error closing private ccxt session: %s", e)
        try:
            if getattr(self, "cca", None) is not None:
                await self.cca.close()
                self.cca = None
        except Exception as e:
            logging.error("[shutdown] error closing public ccxt session: %s", e)
        await self._monitor_flush_snapshot(force=True, ts=utc_ms())
        publisher = getattr(self, "monitor_publisher", None)
        if publisher is not None:
            publisher.close()

    async def update_pos_oos_pnls_ohlcvs(self) -> bool:
        """刷新持仓、挂单、已实现 PnL 和 1m K线。"""
        if self.stop_signal_received:
            return False
        balance_ok, positions_ok = await self.update_positions_and_balance()
        if not positions_ok:
            return False
        if not balance_ok:
            return False

        open_orders_ok, pnls_ok = await asyncio.gather(
            self.update_open_orders(),
            self.update_pnls(),
        )

        if not open_orders_ok or not pnls_ok:
            return False
        if self.stop_signal_received:
            return False
        await self.update_ohlcvs_1m_for_actives()
        return True

    def add_to_recent_order_cancellations(self, order):
        """记录最近取消的订单以节流重复取消操作。"""
        self.recent_order_cancellations.append({**order, **{"execution_timestamp": utc_ms()}})

    def order_was_recently_cancelled(self, order, max_age_ms=15_000) -> float:
        """若订单在 `max_age_ms` 内被取消，返回剩余节流延迟。"""
        age_limit = utc_ms() - max_age_ms
        self.recent_order_cancellations = [
            x for x in self.recent_order_cancellations if x["execution_timestamp"] > age_limit
        ]
        if matching := order_has_match(
            order, self.recent_order_cancellations, tolerance_price=0.0, tolerance_qty=0.0
        ):
            return max(0.0, (matching["execution_timestamp"] + max_age_ms) - utc_ms())
        return 0.0

    def add_to_recent_order_executions(self, order):
        """跟踪新创建的订单以限制重复提交。"""
        self.recent_order_executions.append({**order, **{"execution_timestamp": utc_ms()}})

    def order_was_recently_updated(self, order, max_age_ms=15_000) -> float:
        """若订单在 `max_age_ms` 内被下单，返回节流延迟。"""
        age_limit = utc_ms() - max_age_ms
        self.recent_order_executions = [
            x for x in self.recent_order_executions if x["execution_timestamp"] > age_limit
        ]
        if matching := order_has_match(order, self.recent_order_executions):
            return max(0.0, (matching["execution_timestamp"] + max_age_ms) - utc_ms())
        return 0.0

    def _extract_order_custom_id(self, order: dict) -> str:
        """从统一或原始字段中返回第一个标准化的客户/自定义订单 ID。"""
        if not isinstance(order, dict):
            return ""
        candidates = (
            "custom_id",
            "customId",
            "client_order_id",
            "clientOrderId",
            "client_oid",
            "clientOid",
            "order_link_id",
            "orderLinkId",
            "clOrdId",
            "text",
        )
        for source in (order, order.get("info", {})):
            if not isinstance(source, dict):
                continue
            for key in candidates:
                value = source.get(key)
                if value not in (None, ""):
                    return str(value)
        return ""

    def _extract_order_exchange_id(self, order: dict) -> str:
        """从统一或原始字段中返回交易所分配的订单 ID。"""
        if not isinstance(order, dict):
            return ""
        candidates = ("id", "order_id", "orderId", "orderID", "ordId")
        for source in (order, order.get("info", {})):
            if not isinstance(source, dict):
                continue
            for key in candidates:
                value = source.get(key)
                if value not in (None, ""):
                    return str(value)
        return ""

    def _canonical_passivbot_custom_id(self, custom_id: str) -> str:
        """规范化 Passivbot 自定义 ID 的经纪商/交易所封装。"""
        if not custom_id:
            return ""
        custom_id = str(custom_id)
        marker = _TYPE_MARKER_RE.search(custom_id)
        if marker:
            return custom_id[marker.start() :]
        return custom_id

    def _extract_order_reduce_only(self, order: dict) -> Optional[bool]:
        if not isinstance(order, dict):
            return None
        for source in (order, order.get("info", {})):
            if not isinstance(source, dict):
                continue
            for key in ("reduce_only", "reduceOnly"):
                if key not in source:
                    continue
                value = source[key]
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.strip().lower() in {"true", "1", "yes", "y"}
                return bool(value)
        return None

    def _extract_order_float(self, order: dict, candidates: tuple[str, ...]) -> Optional[float]:
        if not isinstance(order, dict):
            return None
        for source in (order, order.get("info", {})):
            if not isinstance(source, dict):
                continue
            for key in candidates:
                value = source.get(key)
                if value in (None, ""):
                    continue
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
        return None

    def _order_identity_fingerprint(self, order: dict, pb_type: str) -> Optional[dict]:
        """生成订单的身份指纹字典，用于匹配和去重。"""
        if not isinstance(order, dict) or not pb_type or pb_type == "unknown":
            return None
        reduce_only = Passivbot._extract_order_reduce_only(self, order)
        qty = Passivbot._extract_order_float(self, order, ("qty", "amount", "size"))
        price = Passivbot._extract_order_float(self, order, ("price",))
        symbol = order.get("symbol")
        side = order.get("side")
        position_side = order.get("position_side") or order.get("positionSide")
        if any(x in (None, "") for x in (symbol, side, position_side, reduce_only, qty, price)):
            return None
        return {
            "symbol": str(symbol),
            "side": str(side).lower(),
            "position_side": str(position_side).lower(),
            "reduce_only": bool(reduce_only),
            "pb_type": str(pb_type),
            "qty": round(abs(float(qty)), 12),
            "price": round(float(price), 12),
        }

    def _build_emitted_order_record(self, order: dict, emitted_ts: int) -> Optional[dict]:
        custom_id = Passivbot._extract_order_custom_id(self, order)
        pb_type = custom_id_to_snake(custom_id) if custom_id else self._resolve_pb_order_type(order)
        if not pb_type or pb_type == "unknown":
            pb_type = self._resolve_pb_order_type(order)
        record = {
            "timestamp": int(emitted_ts),
            "exchange_id": Passivbot._extract_order_exchange_id(self, order),
            "custom_id": custom_id,
            "canonical_custom_id": Passivbot._canonical_passivbot_custom_id(self, custom_id),
            "pb_type": pb_type if pb_type and pb_type != "unknown" else "",
        }
        record["fingerprint"] = Passivbot._order_identity_fingerprint(self, order, record["pb_type"])
        if not (record["exchange_id"] or record["canonical_custom_id"] or record["fingerprint"]):
            return None
        return record

    def _emitted_order_records(self) -> list[dict]:
        """返回最近发出的订单记录，必要时升级传统自定义 ID 映射。"""
        records = getattr(self, "orders_emitted_to_exchange", [])
        if isinstance(records, dict):
            upgraded = []
            for custom_id, timestamp in records.items():
                custom_id = str(custom_id)
                upgraded.append(
                    {
                        "timestamp": int(timestamp),
                        "exchange_id": "",
                        "custom_id": custom_id,
                        "canonical_custom_id": Passivbot._canonical_passivbot_custom_id(
                            self, custom_id
                        ),
                        "pb_type": custom_id_to_snake(custom_id),
                        "fingerprint": None,
                    }
                )
            self.orders_emitted_to_exchange = upgraded
            return upgraded
        if not isinstance(records, list):
            self.orders_emitted_to_exchange = []
            return []
        return records

    def _prune_emitted_order_custom_ids(self, now_ts: int) -> None:
        """丢弃超出外部写入者回看窗口的已发出订单记录。"""
        cutoff_ts = int(now_ts) - FOREIGN_PASSIVBOT_LOOKBACK_MS
        self.orders_emitted_to_exchange = [
            record
            for record in Passivbot._emitted_order_records(self)
            if int(record.get("timestamp", 0)) >= cutoff_ts
        ]

    def _prune_foreign_passivbot_seen(self, now_ts: int) -> None:
        """丢弃超出滚动停止窗口的旧外部 Passivbot 检测记录。"""
        cutoff_ts = int(now_ts) - FOREIGN_PASSIVBOT_WINDOW_MS
        self.foreign_passivbot_seen = {
            cid: ts
            for cid, ts in getattr(self, "foreign_passivbot_seen", {}).items()
            if int(ts) >= cutoff_ts
        }

    def _record_emitted_order_custom_id(self, order: dict, emitted_ts: Optional[int] = None) -> None:
        """记录成功确认的创建订单，以便后续刷新可以识别。"""
        if emitted_ts is None:
            emitted_ts = (
                int(self.get_exchange_time()) if hasattr(self, "get_exchange_time") else utc_ms()
            )
        record = Passivbot._build_emitted_order_record(self, order, emitted_ts)
        if record is None:
            return
        if not hasattr(self, "orders_emitted_to_exchange"):
            self.orders_emitted_to_exchange = []
        Passivbot._emitted_order_records(self).append(record)

    def _foreign_passivbot_detection_key(self, order: dict, custom_id: str, pb_type: str) -> str:
        exchange_id = Passivbot._extract_order_exchange_id(self, order)
        if exchange_id:
            return f"id:{exchange_id}"
        canonical_custom_id = Passivbot._canonical_passivbot_custom_id(self, custom_id)
        if canonical_custom_id:
            return f"cid:{canonical_custom_id}"
        fingerprint = Passivbot._order_identity_fingerprint(self, order, pb_type)
        if fingerprint:
            return "fp:" + json.dumps(fingerprint, sort_keys=True, separators=(",", ":"))
        return f"unknown:{custom_id}"

    def _order_matches_recent_emitted_record(
        self,
        order: dict,
        custom_id: str,
        pb_type: str,
        order_ts: int,
        consumed_record_indices: set[int],
    ) -> bool:
        """检查订单是否与最近发出的订单记录匹配。"""
        exchange_id = Passivbot._extract_order_exchange_id(self, order)
        canonical_custom_id = Passivbot._canonical_passivbot_custom_id(self, custom_id)
        fingerprint = Passivbot._order_identity_fingerprint(self, order, pb_type)
        for idx, record in enumerate(Passivbot._emitted_order_records(self)):
            if idx in consumed_record_indices:
                continue
            record_exchange_id = record.get("exchange_id") or ""
            if exchange_id and record_exchange_id and exchange_id == record_exchange_id:
                consumed_record_indices.add(idx)
                return True
            record_custom_id = record.get("canonical_custom_id") or ""
            if canonical_custom_id and record_custom_id and canonical_custom_id == record_custom_id:
                consumed_record_indices.add(idx)
                return True
            if exchange_id and record_exchange_id:
                continue
            if canonical_custom_id and record_custom_id:
                continue
            record_fingerprint = record.get("fingerprint")
            record_ts = int(record.get("timestamp", 0))
            if (
                fingerprint
                and record_fingerprint
                and fingerprint == record_fingerprint
                and abs(int(order_ts) - record_ts) <= FOREIGN_PASSIVBOT_FINGERPRINT_MATCH_MS
            ):
                consumed_record_indices.add(idx)
                return True
        return False

    async def _stop_for_foreign_passivbot_orders(
        self, detections: list[tuple[dict, str, str, int]], unique_count: int
    ) -> None:
        """在反复发现竞争性 Passivbot 写入者后停止机器人。"""
        if getattr(self, "_foreign_passivbot_stop_requested", False):
            return
        self._foreign_passivbot_stop_requested = True
        orders_summary = ", ".join(
            f"{symbol_to_coin(order.get('symbol'), verbose=False) or order.get('symbol')}"
            f":{pb_type}:{shorten_custom_id(custom_id)}"
            for order, pb_type, custom_id, _ in detections
        )
        logging.critical(
            "[safety] detected %s unique foreign Passivbot orders in the last %.1f minutes; "
            "stopping bot to avoid competing writers | latest=%s",
            unique_count,
            FOREIGN_PASSIVBOT_WINDOW_MS / (60 * 1000),
            orders_summary,
        )
        self.stop_signal_received = True
        if hasattr(self, "stop_data_maintainers"):
            try:
                self.stop_data_maintainers(verbose=False)
            except Exception as exc:
                logging.error("[safety] failed to stop data maintainers: %s", exc)
        raise Exception("foreign Passivbot writer detected; stopping bot")

    async def _detect_foreign_passivbot_orders(self, open_orders: list[dict]) -> None:
        """检测非本机器人实例发出的较新 Passivbot 管理的挂单。"""
        now_ts = int(self.get_exchange_time())
        bot_start_ts = int(getattr(self, "bot_start_exchange_ts", now_ts))
        self._prune_emitted_order_custom_ids(now_ts)
        self._prune_foreign_passivbot_seen(now_ts)
        if not open_orders:
            return
        cutoff_ts = max(
            bot_start_ts + FOREIGN_PASSIVBOT_GRACE_MS,
            now_ts - FOREIGN_PASSIVBOT_LOOKBACK_MS,
        )
        new_detections: list[tuple[dict, str, str, int]] = []
        consumed_emitted_records: set[int] = set()
        for order in open_orders:
            ts_raw = order.get("timestamp")
            if ts_raw is None:
                continue
            try:
                order_ts = int(float(ts_raw))
            except Exception:
                continue
            if order_ts < cutoff_ts:
                continue
            custom_id = self._extract_order_custom_id(order)
            if not custom_id:
                continue
            if not custom_id_has_explicit_passivbot_marker(custom_id):
                continue
            pb_type = custom_id_to_snake(custom_id)
            if not pb_type or pb_type == "unknown":
                continue
            if self._order_matches_recent_emitted_record(
                order, custom_id, pb_type, order_ts, consumed_emitted_records
            ):
                continue
            detection_key = self._foreign_passivbot_detection_key(order, custom_id, pb_type)
            if detection_key in self.foreign_passivbot_seen:
                continue
            self.foreign_passivbot_seen[detection_key] = order_ts
            new_detections.append((order, pb_type, custom_id, order_ts))
        if not new_detections:
            return
        for order, pb_type, custom_id, order_ts in new_detections:
            logging.error(
                "[safety] detected foreign Passivbot order candidate | symbol=%s type=%s "
                "custom_id=%s ts=%s",
                order.get("symbol"),
                pb_type,
                shorten_custom_id(custom_id),
                ts_to_date(order_ts),
            )
        if len(self.foreign_passivbot_seen) >= FOREIGN_PASSIVBOT_MAX_UNIQUE_PER_WINDOW:
            await self._stop_for_foreign_passivbot_orders(
                new_detections, unique_count=len(self.foreign_passivbot_seen)
            )

    async def execute_to_exchange(self):
        """运行一个执行周期，包括配置同步和订单下达/取消。"""
        await self.execution_cycle()
        # await self.update_EMAs()
        await self.update_exchange_configs()
        to_cancel, to_create = await self.calc_orders_to_cancel_and_create()

        # 调试重复项
        seen = set()
        for elm in to_cancel:
            key = str(elm["price"]) + str(elm["qty"])
            if key in seen:
                logging.debug("duplicate cancel candidate: %s", elm)
            seen.add(key)

        seen = set()
        for elm in to_create:
            key = str(elm["price"]) + str(elm["qty"])
            if key in seen:
                logging.debug("duplicate create candidate: %s", elm)
            seen.add(key)
        # 格式化 custom_id
        if self.debug_mode:
            if to_cancel:
                print(f"would cancel {len(to_cancel)} order{'s' if len(to_cancel) > 1 else ''}")
        else:
            res = await self.execute_cancellations_parent(to_cancel)
        if self.debug_mode:
            if to_create:
                print(f"would create {len(to_create)} order{'s' if len(to_create) > 1 else ''}")
        elif self.get_raw_balance() < self.balance_threshold:
            logging.info(
                "[balance] too low: %.2f %s; not creating orders", self.get_raw_balance(), self.quote
            )
        else:
            # to_create_mod = [x for x in to_create if not order_has_match(x, to_cancel)]
            to_create_mod = []
            for x in to_create:
                xf = f"{x['symbol']} {x['side']} {x['position_side']} {x['qty']} @ {x['price']}"
                if order_has_match(x, to_cancel):
                    logging.debug(
                        "matching order cancellation found; will be delayed until next cycle: %s",
                        xf,
                    )
                elif delay_time_ms := self.order_was_recently_updated(x):
                    logging.info(
                        "[order] recent execution found; delaying for up to %.1f secs: %s",
                        delay_time_ms / 1000,
                        xf,
                    )
                else:
                    to_create_mod.append(x)
            if self.state_change_detected_by_symbol:
                logging.info(
                    "[order] state change detected; skipping order creation for %s until next cycle",
                    self.state_change_detected_by_symbol,
                )
                to_create_mod = [
                    x
                    for x in to_create_mod
                    if x["symbol"] not in self.state_change_detected_by_symbol
                ]
            res = None
            try:
                res = await self.execute_orders_parent(to_create_mod)
            except RestartBotException:
                raise  # 传播重启请求，不增加错误计数
            except Exception as e:
                logging.error(f"error executing orders {to_create_mod} {e}")
                print_async_exception(res)
                traceback.print_exc()
                await self.restart_bot_on_too_many_errors()
        if to_cancel or to_create:
            self.execution_scheduled = True
        if self.debug_mode:
            return to_cancel, to_create

    async def execute_orders_parent(self, orders: [dict]) -> [dict]:
        """在节流和记账后提交一批订单。"""
        orders = orders[: int(self.live_value("max_n_creations_per_batch"))]
        grouped_orders: dict[str, list[dict]] = defaultdict(list)
        emitted_ts = int(self.get_exchange_time()) if hasattr(self, "get_exchange_time") else utc_ms()
        for order in orders:
            self.add_to_recent_order_executions(order)
            self.log_order_action(
                order,
                "posting order",
                context=order.get("_context", "plan_sync"),
                level=logging.DEBUG,
                delta=order.get("_delta"),
            )
            grouped_orders[order["symbol"]].append(order)
        self._log_order_action_summary(grouped_orders, "post")
        res = await self.execute_orders(orders)
        if not res:
            return
        if len(orders) != len(res):
            print(
                f"debug unequal lengths execute_orders_parent: "
                f"{len(orders)} orders, {len(res)} executions",
                res,
            )
            return []
        to_return = []
        for ex, order in zip(res, orders):
            if not self.did_create_order(ex):
                print(f"debug did_create_order false {ex}")
                continue
            debug_prints = {}
            for key in order:
                if key not in ex:
                    debug_prints.setdefault("missing", []).append((key, order[key]))
                    ex[key] = order[key]
                elif ex[key] is None:
                    debug_prints.setdefault("is_none", []).append((key, order[key]))
                    ex[key] = order[key]
            if debug_prints and self.debug_mode:
                print("debug create_orders", debug_prints)
            Passivbot._record_emitted_order_custom_id(self, ex, emitted_ts=emitted_ts)
            to_return.append(ex)
        if to_return:
            for elm in to_return:
                self.add_new_order(elm, source="POST")
                self._monitor_record_event(
                    "order.opened",
                    ("order", "open"),
                    self._monitor_order_payload(elm, source="POST"),
                    symbol=elm.get("symbol"),
                    pside=elm.get("position_side"),
                )
            self._health_orders_placed += len(to_return)
        return to_return

    async def execute_cancellations_parent(self, orders: [dict]) -> [dict]:
        """提交一批取消请求，优先处理减仓订单。"""
        max_cancellations = int(self.live_value("max_n_cancellations_per_batch"))
        if len(orders) > max_cancellations:
            # 优先取消减仓订单
            try:
                reduce_only_orders = [
                    x for x in orders if x.get("reduce_only") or x.get("reduceOnly")
                ]
                rest = [x for x in orders if not x["reduce_only"]]
                orders = (reduce_only_orders + rest)[:max_cancellations]
            except Exception as e:
                logging.error(f"debug filter cancellations {e}")
                orders = orders[:max_cancellations]
        grouped_orders: dict[str, list[dict]] = defaultdict(list)
        for order in orders:
            self.add_to_recent_order_cancellations(order)
            self.log_order_action(
                order,
                "cancelling order",
                context=order.get("_context", "plan_sync"),
                level=logging.DEBUG,
                delta=order.get("_delta"),
            )
            grouped_orders[order["symbol"]].append(order)
        self._log_order_action_summary(grouped_orders, "cancel")
        res = await self.execute_cancellations(orders)
        to_return = []
        if len(orders) != len(res):
            self.execution_scheduled = True
            for od in orders:
                self.state_change_detected_by_symbol.add(od["symbol"])
            print(
                f"debug unequal lengths execute_cancellations_parent: "
                f"{len(orders)} orders, {len(res)} executions",
                res,
            )
            return []
        for ex, od in zip(res, orders):
            if not self.did_cancel_order(ex, od):
                self.state_change_detected_by_symbol.add(od["symbol"])
                print(f"debug did_cancel_order false {ex} {od}")
                continue
            debug_prints = {}
            for key in od:
                if key not in ex:
                    debug_prints.setdefault("missing", []).append((key, od[key]))
                    ex[key] = od[key]
                elif ex[key] is None:
                    debug_prints.setdefault("is_none", []).append((key, od[key]))
                    ex[key] = od[key]
            if debug_prints and self.debug_mode:
                print("debug cancel_orders", debug_prints)
            to_return.append(ex)
        if to_return:
            for elm in to_return:
                self.remove_order(elm, source="POST")
                self._monitor_record_event(
                    "order.canceled",
                    ("order", "cancel"),
                    self._monitor_order_payload(elm, source="POST"),
                    symbol=elm.get("symbol"),
                    pside=elm.get("position_side"),
                )
            self._health_orders_cancelled += len(to_return)
        return to_return

    def log_order_action(
        self,
        order,
        action,
        source="passivbot",
        *,
        level=logging.DEBUG,
        context: str | None = None,
        delta: dict | None = None,
    ):
        """记录描述订单操作的结构化消息。"""
        pb_order_type = self._resolve_pb_order_type(order)

        def _fmt(val):
            try:
                return f"{float(val):g}"
            except (TypeError, ValueError):
                return str(val)

        side = order.get("side", "?")
        qty = _fmt(order.get("qty", "?"))
        position_side = order.get("position_side", "?")
        price = _fmt(order.get("price", "?"))
        symbol = order.get("symbol", "?")
        coin = symbol_to_coin(symbol, verbose=False) or symbol
        details = f"{side} {qty} {position_side}@{price}"
        extra_parts = []
        if context:
            extra_parts.append(f"context={context}")
        elif order.get("_context"):
            extra_parts.append(f"context={order.get('_context')}")
        if delta:
            parts = []
            po, pn = delta.get("price_old"), delta.get("price_new")
            qo, qn = delta.get("qty_old"), delta.get("qty_new")
            if po is not None and pn is not None:
                parts.append(f"price {po} -> {pn} ({delta.get('price_pct_diff','?')}%)")
            if qo is not None and qn is not None:
                parts.append(f"qty {qo} -> {qn} ({delta.get('qty_pct_diff','?')}%)")
            if parts:
                extra_parts.append("delta=" + "; ".join(parts))
        msg = f"[order] {action: >{self.action_str_max_len}} {coin} | {details} | type={pb_order_type} | src={source}"
        if extra_parts:
            msg += " | " + " ".join(extra_parts)
        logging.log(level, msg)

    def _log_order_action_summary(self, grouped_orders: dict[str, list[dict]], action: str) -> None:
        """为批量订单操作输出精简的 INFO 摘要，跳过重复。"""
        max_entries = 4
        for symbol, orders in grouped_orders.items():
            if not orders:
                continue
            descriptors = []
            for order in orders:
                pb_order_type = self._resolve_pb_order_type(order)
                qty = order.get("qty")
                price = order.get("price")
                qty_str = f"{float(qty):g}" if isinstance(qty, (int, float)) else str(qty)
                price_str = f"{float(price):g}" if isinstance(price, (int, float)) else str(price)
                desc = (
                    f"{order.get('side','?')} {order.get('position_side','?')} "
                    f"{qty_str}@{price_str} {pb_order_type}"
                )
                extras = []
                context = order.get("_context")
                reason = order.get("_reason")
                if context:
                    extras.append(context)
                if reason and reason != context:
                    extras.append(f"reason={reason}")
                delta = order.get("_delta") or {}
                price_diff = delta.get("price_pct_diff")
                qty_diff = delta.get("qty_pct_diff")
                delta_parts = []
                if isinstance(price_diff, (int, float)) and price_diff:
                    delta_parts.append(f"Δp={price_diff:.3g}%")
                if isinstance(qty_diff, (int, float)) and qty_diff:
                    delta_parts.append(f"Δq={qty_diff:.3g}%")
                extras.extend(delta_parts)
                if extras:
                    desc += f" [{' '.join(extras)}]"
                descriptors.append(desc)
            if not descriptors:
                continue
            display = "; ".join(descriptors[:max_entries])
            if len(descriptors) > max_entries:
                display += f"; ... +{len(descriptors) - max_entries} more"
            key = (symbol, action)
            if self._last_action_summary.get(key) == display:
                continue
            self._last_action_summary[key] = display
            reason_counts = Counter(order.get("_reason") for order in orders if order.get("_reason"))
            reason_str = ""
            if reason_counts:
                reason_str = " | reasons=" + ", ".join(
                    f"{reason}:{count}" for reason, count in sorted(reason_counts.items())
                )
            coin = symbol_to_coin(symbol, verbose=False) or symbol
            logging.info("[order] %6s %s | %s%s", action, coin, display, reason_str)

    def _resolve_pb_order_type(self, order) -> str:
        """尽力解码 Passivbot 订单类型用于日志。"""
        if not isinstance(order, dict):
            return "unknown"
        pb_type = order.get("pb_order_type")
        if pb_type:
            return str(pb_type)
        symbol = order.get("symbol")
        if symbol and symbol in self.open_orders:
            for existing in self.open_orders[symbol]:
                if order_has_match(order, [existing], tolerance_price=0.0, tolerance_qty=0.0):
                    existing_type = existing.get("pb_order_type")
                    if existing_type:
                        return str(existing_type)
                    candidate = self._decode_pb_type_from_ids(existing)
                    if candidate:
                        return candidate
        candidate_ids = [
            order.get("custom_id"),
            order.get("customId"),
            order.get("client_order_id"),
            order.get("clientOrderId"),
            order.get("client_oid"),
            order.get("clientOid"),
            order.get("order_link_id"),
            order.get("orderLinkId"),
        ]
        candidate = self._decode_pb_type_from_ids(order, candidate_ids)
        if candidate:
            return candidate
        return "unknown"

    def _decode_pb_type_from_ids(
        self, order: dict, candidate_ids: Optional[list] = None
    ) -> Optional[str]:
        """从候选 ID 列表中解码订单的 Passivbot 类型。"""
        ids = candidate_ids
        if ids is None:
            ids = [
                order.get("custom_id"),
                order.get("customId"),
                order.get("client_order_id"),
                order.get("clientOrderId"),
                order.get("client_oid"),
                order.get("clientOid"),
                order.get("order_link_id"),
                order.get("orderLinkId"),
            ]
        for cid in ids:
            if not cid:
                continue
            snake = custom_id_to_snake(str(cid))
            if snake and snake != "unknown":
                return snake
        return None

    def did_create_order(self, executed) -> bool:
        """若交易所确认订单创建则返回 True。"""
        try:
            return "id" in executed and executed["id"] is not None
        except:
            return False
        # 更多测试在子类中定义

    def did_cancel_order(self, executed, order=None) -> bool:
        """当交易所响应确认取消时返回 True。"""
        if isinstance(executed, list) and len(executed) == 1:
            return self.did_cancel_order(executed[0], order)
        try:
            return "id" in executed and executed["id"] is not None
        except:
            return False
        # 更多测试在子类中定义

    def is_forager_mode(self, pside=None):
        """当配置允许该方向部署 forager 网格时返回 True。"""
        if pside is None:
            return self.is_forager_mode("long") or self.is_forager_mode("short")
        if self.bot_value(pside, "total_wallet_exposure_limit") <= 0.0:
            return False
        if self.live_value(f"forced_mode_{pside}"):
            return False
        n_positions = self.get_max_n_positions(pside)
        if n_positions == 0:
            return False
        if n_positions >= len(self.approved_coins_minus_ignored_coins[pside]):
            return False
        return True

    def pad_sym(self, symbol):
        """返回左对齐到配置日志宽度的交易对字符串。"""
        return f"{symbol: <{self.sym_padding}}"

    def _apply_endpoint_override(self, client) -> None:
        """将配置的 REST 端点覆盖应用到 ccxt 客户端。"""
        if client is None:
            return
        apply_rest_overrides_to_ccxt(client, self.endpoint_override)

    def _compute_fetch_budget_ttls(
        self, syms: list, max_age_ms: Optional[int], max_network_fetches: Optional[int]
    ) -> Tuple[Dict[str, int], set]:
        """计算带抓取预算的逐交易对 TTL，返回 (per_sym_ttl, cache_only_never_fetched)。

        抓取预算内的交易对使用真实 max_age_ms；超出预算的交易对获得巨大 TTL
        以仅使用缓存数据。被分配仅缓存 TTL 且从未抓取过的交易对收集到跳过集合中
        （get_candles 将 last_refresh_ms==0 视为"需要刷新"，无论 TTL 如何）。
        """
        CACHE_ONLY_TTL = 365 * 24 * 3600 * 1000  # 约 1 年 — 实质上仅使用缓存
        per_sym_ttl: Dict[str, int] = {}
        if max_network_fetches is not None and max_network_fetches >= 0 and max_age_ms is not None:
            now = utc_ms()
            staleness = []
            for s in syms:
                try:
                    last_ref = self.cm.get_last_refresh_ms(s)
                except Exception:
                    last_ref = 0
                staleness.append((s, int(now - last_ref) if last_ref > 0 else now))
            staleness.sort(key=lambda x: x[1], reverse=True)  # 最陈旧的优先
            fetch_set = set(s for s, _ in staleness[:max_network_fetches])
            for s in syms:
                per_sym_ttl[s] = int(max_age_ms) if s in fetch_set else CACHE_ONLY_TTL
        else:
            for s in syms:
                per_sym_ttl[s] = int(max_age_ms) if max_age_ms is not None else 0

        cache_only_never_fetched: set = set()
        for s in syms:
            if per_sym_ttl.get(s) == CACHE_ONLY_TTL:
                try:
                    if self.cm.get_last_refresh_ms(s) == 0:
                        cache_only_never_fetched.add(s)
                except Exception:
                    cache_only_never_fetched.add(s)

        return per_sym_ttl, cache_only_never_fetched

    def _get_fetch_delay_seconds(self) -> float:
        """返回配置的每次抓取延迟秒数。

        Bybit 和 Hyperliquid 默认 200ms（严格的基于 IP 的速率限制），
        其他交易所默认 0ms。
        可通过配置中的 live.warmup_fetch_delay_ms 覆盖。
        """
        fetch_delay_ms = get_optional_live_value(self.config, "warmup_fetch_delay_ms", None)
        try:
            fetch_delay_ms = float(fetch_delay_ms) if fetch_delay_ms is not None else None
        except Exception:
            fetch_delay_ms = None
        if fetch_delay_ms is None:
            exchange_lower = self.exchange.lower() if self.exchange else ""
            fetch_delay_ms = 200.0 if exchange_lower in ("bybit", "hyperliquid") else 0.0
        return max(0.0, float(fetch_delay_ms) / 1000.0)

    def stop_data_maintainers(self, verbose=True):
        """取消后台 K线/订单簿任务并记录结果。"""
        if not hasattr(self, "maintainers"):
            return
        res = {}
        for key in self.maintainers:
            try:
                res[key] = self.maintainers[key].cancel()
            except Exception as e:
                logging.error(f"error stopping maintainer {key} {e}")
        if hasattr(self, "WS_ohlcvs_1m_tasks"):
            res0s = {}
            for key in self.WS_ohlcvs_1m_tasks:
                try:
                    res0 = self.WS_ohlcvs_1m_tasks[key].cancel()
                    res0s[key] = res0
                except Exception as e:
                    logging.error(f"error stopping WS_ohlcvs_1m_tasks {key} {e}")
            if res0s:
                if verbose:
                    logging.info(f"stopped ohlcvs watcher tasks {res0s}")
        if verbose:
            logging.info(f"stopped data maintainers: {res}")
        return res

    def has_position(self, pside=None, symbol=None):
        """若机器人当前持有指定方向和交易对的仓位则返回 True。"""
        if pside is None:
            return self.has_position("long", symbol) or self.has_position("short", symbol)
        if symbol is None:
            return any([self.has_position(pside, s) for s in self.positions])
        return symbol in self.positions and self.positions[symbol][pside]["size"] != 0.0

    def is_trailing(self, symbol, pside=None):
        """若指定交易对和方向的追踪逻辑处于活跃状态则返回 True。"""
        if pside is None:
            return self.is_trailing(symbol, "long") or self.is_trailing(symbol, "short")
        return (
            self.bp(pside, "entry_trailing_grid_ratio", symbol) != 0.0
            or self.bp(pside, "close_trailing_grid_ratio", symbol) != 0.0
        )

    def get_last_position_changes(self, symbol=None):
        """返回每个交易对/方向最近成交时间戳，用于追踪逻辑。"""
        last_position_changes = defaultdict(dict)
        if self._pnls_manager is None:
            return last_position_changes

        events = self._pnls_manager.get_events()
        for symbol in self.positions:
            for pside in ["long", "short"]:
                if self.has_position(pside, symbol) and self.is_trailing(symbol, pside):
                    last_position_changes[symbol][pside] = utc_ms() - 1000 * 60 * 60 * 24 * 7
                    for ev in reversed(events):
                        try:
                            if ev.symbol == symbol and ev.position_side == pside:
                                last_position_changes[symbol][pside] = ev.timestamp
                                break
                        except Exception as e:
                            logging.error(f"Error in get_last_position_changes: {e}")
        return last_position_changes

    # 传统：wait_for_ohlcvs_1m_to_update 已移除（CandlestickManager 处理新鲜度）

    # 传统：get_ohlcvs_1m_filepath 已移除

    # 传统：trim_ohlcvs_1m 已移除

    # 传统：dump_ohlcvs_1m_to_cache 已移除

    async def update_trailing_data(self) -> None:
        """使用 CandlestickManager K线更新追踪价格指标。

        对每个有追踪仓位的交易对和方向，自上次仓位变动起遍历 K线并计算：
        - max_since_open：开仓以来最高价
        - min_since_max：最近新高后的最低价
        - min_since_open：开仓以来最低价
        - max_since_min：最近新低后的最高价（或按传统的收盘价）
        并发获取逐交易对 K线以减少延迟。
        """
        if not hasattr(self, "trailing_prices"):
            self.trailing_prices = {}
        last_position_changes = self.get_last_position_changes()
        symbols = set(self.trailing_prices) | set(last_position_changes) | set(self.active_symbols)

        # 先初始化所有交易对的容器
        for symbol in symbols:
            self.trailing_prices[symbol] = {
                "long": _trailing_bundle_default_dict(),
                "short": _trailing_bundle_default_dict(),
            }

        # 为有仓位变动的交易对构建并发抓取
        fetch_plan = {}
        for symbol in symbols:
            if symbol not in last_position_changes:
                continue
            # 确定方向中最早的起始时间以避免重复抓取
            starts = [last_position_changes[symbol][ps] for ps in last_position_changes[symbol]]
            if not starts:
                continue
            start_ts = int(min(starts))
            fetch_plan[symbol] = start_ts

        tasks = {
            sym: asyncio.create_task(self.cm.get_candles(sym, start_ts=st, end_ts=None, strict=False))
            for sym, st in fetch_plan.items()
        }

        results = {}
        for sym, task in tasks.items():
            try:
                results[sym] = await task
            except Exception as e:
                logging.debug("failed to fetch candles for trailing %s: %s", sym, e)
                results[sym] = None

        # 计算每个交易对/方向的追踪指标
        for symbol, arr in results.items():
            if arr is None or arr.size == 0:
                continue
            if symbol not in last_position_changes:
                continue
            arr = np.sort(arr, order="ts")
            for pside, changed_ts in last_position_changes[symbol].items():
                mask = arr["ts"] > int(changed_ts)
                if not np.any(mask):
                    continue
                subset = arr[mask]
                try:
                    bundle = _trailing_bundle_from_arrays(subset["h"], subset["l"], subset["c"])
                    self.trailing_prices[symbol][pside] = bundle
                except Exception as e:
                    logging.debug("failed to compute trailing bundle for %s %s: %s", symbol, pside, e)

    def symbol_is_eligible(self, symbol):
        """当交易对通过交易所特定的资格规则时返回 True。"""
        return True

    def set_market_specific_settings(self):
        """初始化逐交易对市场元数据（步长、ID、乘数）。"""
        self.symbol_ids = {symbol: self.markets_dict[symbol]["id"] for symbol in self.markets_dict}
        self.symbol_ids_inv = {v: k for k, v in self.symbol_ids.items()}

    def get_symbol_id(self, symbol):
        """返回 `symbol` 的交易所原生标识符，缓存默认值。"""
        try:
            return self.symbol_ids[symbol]
        except:
            logging.debug("symbol %s missing from self.symbol_ids. Using raw symbol.", symbol)
            self.symbol_ids[symbol] = symbol
            return symbol

    def to_ccxt_symbol(self, symbol: str) -> str:
        """转换为 ccxt 标准化的交易对"""
        candidates = []
        try:
            candidates.append(self.get_symbol_id_inv(symbol))
        except:
            pass
        try:
            candidates.append(self.coin_to_symbol(symbol))
        except:
            pass
        if candidates:
            return candidates[0]
        else:
            logging.info(f"failed to convert {symbol} to ccxt symbol. Using {symbol} as is.")

    def get_symbol_id_inv(self, symbol):
        """返回交易所原生标识符对应的人可读交易对。"""
        try:
            if symbol in self.symbol_ids_inv:
                return self.symbol_ids_inv[symbol]
            else:
                return self.coin_to_symbol(symbol)
        except:
            logging.info(f"failed to convert {symbol} to ccxt symbol. Using {symbol} as is.")
            self.symbol_ids_inv[symbol] = symbol
            return symbol

    def is_approved(self, pside, symbol) -> bool:
        """当交易对已批准、未忽略且足够年代可交易时返回 True。"""
        if symbol not in self.approved_coins_minus_ignored_coins[pside]:
            return False
        if symbol in self.ignored_coins[pside]:
            return False
        if not self.is_old_enough(pside, symbol):
            return False
        return True

    async def update_exchange_configs(self):
        """确保所有活跃交易对的交易所特定设置已初始化。"""
        if not hasattr(self, "already_updated_exchange_config_symbols"):
            self.already_updated_exchange_config_symbols = set()
        if not hasattr(self, "_exchange_config_retry_attempts"):
            self._exchange_config_retry_attempts = {}
        if not hasattr(self, "_exchange_config_retry_after_ms"):
            self._exchange_config_retry_after_ms = {}
        symbols_not_done = [
            x for x in self.active_symbols if x not in self.already_updated_exchange_config_symbols
        ]
        if symbols_not_done:
            for symbol in symbols_not_done:
                retry_after_ms = int(self._exchange_config_retry_after_ms.get(symbol, 0) or 0)
                if retry_after_ms > utc_ms():
                    continue
                try:
                    await self.update_exchange_config_by_symbols([symbol])
                    self.already_updated_exchange_config_symbols.add(symbol)
                    self._exchange_config_retry_attempts.pop(symbol, None)
                    self._exchange_config_retry_after_ms.pop(symbol, None)
                except RestartBotException:
                    raise
                except Exception as e:
                    attempts = int(self._exchange_config_retry_attempts.get(symbol, 0) or 0) + 1
                    self._exchange_config_retry_attempts[symbol] = attempts
                    backoff_s = self._exchange_config_backoff_seconds(attempts)
                    self._exchange_config_retry_after_ms[symbol] = (
                        utc_ms() + int(backoff_s * 1000.0)
                    )
                    if self._is_rate_limit_like_exception(e):
                        self._health_rate_limits += 1
                        logging.warning(
                            "[rate] exchange config update hit rate limit for %s; retrying in %.1fs",
                            symbol,
                            backoff_s,
                        )
                        break
                    logging.warning(
                        "[config] exchange config update failed for %s; retrying in %.1fs: %s",
                        symbol,
                        backoff_s,
                        e,
                    )
                else:
                    pause_s = self._exchange_config_success_pause_seconds()
                    if pause_s > 0.0:
                        await asyncio.sleep(pause_s)

    def _is_rate_limit_like_exception(self, exc: Exception) -> bool:
        if isinstance(exc, RateLimitExceeded):
            return True
        msg = str(exc).lower()
        return any(token in msg for token in ("rate limit", "too many", "429", "10006"))

    def _exchange_config_backoff_seconds(self, attempt: int) -> float:
        base = 2.0
        if getattr(self, "exchange", "") in {"bybit", "hyperliquid"}:
            base = 5.0
        return min(base * (2 ** max(int(attempt) - 1, 0)), 60.0) + random.uniform(0.0, 0.5)

    def _exchange_config_success_pause_seconds(self) -> float:
        if getattr(self, "exchange", "") in {"bybit", "hyperliquid", "okx", "kucoin", "bitget"}:
            return 0.2
        return 0.05

    async def update_exchange_config_by_symbols(self, symbols):
        """交易所特定钩子，刷新指定交易对的配置。"""
        # 由各交易所子类定义
        pass

    async def update_exchange_config(self):
        """交易所特定钩子，刷新全局配置状态。"""
        # 由各交易所子类定义
        pass

    def is_old_enough(self, pside, symbol):
        """若市场年龄超过 forager 模式配置的最低要求则返回 True。"""
        if self.is_forager_mode(pside) and self.minimum_market_age_millis > 0:
            first_timestamp = self.get_first_timestamp(symbol)
            if first_timestamp:
                return utc_ms() - first_timestamp > self.minimum_market_age_millis
            else:
                return False
        else:
            return True

    async def update_tickers(self):
        """获取最新行情数据并填充缺失的买/卖/最新价。"""
        if not hasattr(self, "tickers"):
            self.tickers = {}
        tickers = None
        try:
            tickers = await self.cca.fetch_tickers()
            for symbol in tickers:
                if tickers[symbol]["last"] is None:
                    if tickers[symbol]["bid"] is not None and tickers[symbol]["ask"] is not None:
                        tickers[symbol]["last"] = np.mean(
                            [tickers[symbol]["bid"], tickers[symbol]["ask"]]
                        )
                else:
                    for oside in ["bid", "ask"]:
                        if tickers[symbol][oside] is None and tickers[symbol]["last"] is not None:
                            tickers[symbol][oside] = tickers[symbol]["last"]
            self.tickers = tickers
        except Exception as e:
            logging.error(f"Error with {get_function_name()} {e}")

    async def execution_cycle(self):
        """在执行循环中与交易所交互前准备机器人状态。"""
        await self.update_effective_min_cost()
        self.refresh_approved_ignored_coins_lists()
        self.set_wallet_exposure_limits()
        if any(self.is_forager_mode(pside) for pside in ("long", "short")):
            await self.update_first_timestamps()
        self._assert_supported_live_state()
        self.active_symbols = self._build_live_symbol_universe()
        for symbol in self.active_symbols:
            if symbol not in self.positions:
                self.positions[symbol] = {
                    "long": {"size": 0.0, "price": 0.0},
                    "short": {"size": 0.0, "price": 0.0},
                }
            if symbol not in self.open_orders:
                self.open_orders[symbol] = []
        self.set_wallet_exposure_limits()
        await self.update_trailing_data()

    def _log_mode_changes(self, res: dict, previous_PB_modes: dict) -> None:
        """记录模式变更，DEBUG 级别记录全部详情，INFO 级别记录用户相关事件。

        DEBUG：所有模式变更（完整详情，无节流）
        INFO：选择性记录：
          - "added" 且为 "normal" -> forager 选中（含槽位上下文）
          - "added" 且为 "graceful_stop" -> 仅在首次运行时
          - "removed" -> 币种退出（有价值）
          - "changed" normal<->graceful_stop -> 抑制（振荡噪音）
          - "changed" 到/从 tp_only/manual/panic -> 重要，始终记录
        """
        is_first_run = previous_PB_modes is None

        # 收集槽位信息作为上下文
        slot_info = {}
        for pside in ["long", "short"]:
            try:
                max_n = self.get_max_n_positions(pside)
                current_n = self.get_current_n_positions(pside)
                slots_open = max_n > current_n
                slot_info[pside] = {"max": max_n, "current": current_n, "open": slots_open}
            except Exception:
                slot_info[pside] = {"max": 0, "current": 0, "open": False}

        # 如需要则初始化节流缓存（仅用于 INFO 级别）
        if not hasattr(self, "_mode_change_last_log_ms"):
            self._mode_change_last_log_ms = {}
        mode_change_throttle_ms = 300_000  # INFO 级别节流间隔 5 分钟
        now_ms = utc_ms()

        for change_type, changes in res.items():
            for elm in changes:
                # DEBUG 级别始终记录（完整详情）
                logging.debug("[mode] %s %s", change_type, elm)

                # 确定是否应在 INFO 级别记录
                should_log_info = False
                info_suffix = ""

                try:
                    # 解析元素："long.XRP/USDT:USDT: normal" 或 "long.XRP/USDT:USDT: old -> new"
                    parts = elm.split(".")
                    pside = parts[0] if parts else "long"
                    pside_info = slot_info.get(pside, {"max": 0, "current": 0, "open": False})

                    if change_type == "added":
                        # 新币种进入模式系统
                        if ": normal" in elm:
                            # Forager 选中 — 始终有价值
                            should_log_info = True
                            if pside_info["open"]:
                                info_suffix = (
                                    f" (forager slot {pside_info['current']+1}/{pside_info['max']})"
                                )
                            else:
                                info_suffix = f" (slot {pside_info['current']}/{pside_info['max']})"
                        elif is_first_run:
                            # 首次运行 — 显示所有模式以提高可见性
                            should_log_info = True
                        # 否则："added" 且为 graceful_stop 但非首次运行 -> 跳过 INFO

                    elif change_type == "removed":
                        # 币种退出 — 始终有价值
                        should_log_info = True

                    elif change_type == "changed":
                        # 模式变更 — 检查是振荡还是重要变更
                        is_oscillation = (
                            "normal -> graceful_stop" in elm or "graceful_stop -> normal" in elm
                        )
                        if is_oscillation:
                            # 振荡 — 在 INFO 级别抑制（已在 DEBUG 级别记录）
                            should_log_info = False
                        else:
                            # 重要模式变更（tp_only、manual、panic 等）
                            should_log_info = True

                except Exception:
                    # 解析错误时在 INFO 级别记录以确保安全
                    should_log_info = True

                if should_log_info:
                    # 对 INFO 级别应用节流
                    try:
                        symbol_part = elm.split(":")[0]
                        throttle_key = f"info:{change_type}:{symbol_part}"
                        last_log_ms = self._mode_change_last_log_ms.get(throttle_key, 0)
                        if (now_ms - last_log_ms) < mode_change_throttle_ms:
                            continue
                        self._mode_change_last_log_ms[throttle_key] = now_ms
                    except Exception:
                        pass
                    logging.info("[mode] %s %s%s", change_type, elm, info_suffix)

    async def get_filtered_coins(
        self, pside: str, *, max_network_fetches: Optional[int] = None
    ) -> List[str]:
        """使用基于 EMA 的成交量和对数范围过滤选择某方向的理想币种。

        步骤（forager 模式）：
        - 按年龄和有效最小成本过滤
        - 按 1m EMA 报价成交量排序
        - 去除最低的 filter_volume_drop_pct 比例
        - 对剩余按 1m EMA 对数范围排序
        - 返回最多 n_positions 个最波动的交易对
        非 forager 模式返回所有已批准候选项。
        """
        # 按年龄过滤币种
        # 按有效最小成本过滤
        # 按相对成交量过滤
        # 按对数范围过滤
        if self.get_forced_PB_mode(pside):
            return []
        candidates = self.approved_coins_minus_ignored_coins[pside]
        candidates = [s for s in candidates if self.is_old_enough(pside, s)]
        min_cost_flags = {s: self.effective_min_cost_is_low_enough(pside, s) for s in candidates}
        if not any(min_cost_flags.values()):
            if self.live_value("filter_by_min_effective_cost"):
                self.warn_on_high_effective_min_cost(pside)
            return []
        try:
            slots_open = self.get_max_n_positions(pside) > self.get_current_n_positions(pside)
        except Exception:
            slots_open = False
        if self.is_forager_mode(pside):
            # 按相对成交量和对数范围过滤
            clip_pct = self.bot_value(pside, "forager_volume_drop_pct")
            if not clip_pct:
                clip_pct = self.bot_value(pside, "filter_volume_drop_pct")
            volatility_drop = self.bot_value(pside, "filter_volatility_drop_pct")
            weights = self.bot_value(pside, "forager_score_weights")
            if not isinstance(weights, dict):
                weights = {
                    "volume": 0.0,
                    "ema_readiness": 0.0,
                    "volatility": 1.0,
                }
            max_n_positions = self.get_max_n_positions(pside)
            # 在所有情况下应用 max_ohlcv_fetches_per_minute（无论槽位是否开放）。
            max_calls = get_optional_live_value(self.config, "max_ohlcv_fetches_per_minute", 0)
            try:
                max_calls = int(max_calls) if max_calls is not None else 0
            except Exception:
                max_calls = 0
            if slots_open:
                rate_limit_age_ms = self._forager_target_staleness_ms(len(candidates), max_calls)
                # 即使有开放槽位也尊重速率限制；响应性最低设为 60s。
                max_age_ms = max(60_000, rate_limit_age_ms) if max_calls > 0 else 60_000
            else:
                max_age_ms = self._forager_target_staleness_ms(len(candidates), max_calls)
            # 使用调用方预计算的每方向预算（若可用）；
            # 否则在此处计算（向后兼容）。
            if max_network_fetches is None:
                fetch_budget = self._forager_refresh_budget(max_calls) if max_calls > 0 else None
            else:
                try:
                    fetch_budget = max(0, int(max_network_fetches))
                except Exception:
                    fetch_budget = 0
            if clip_pct > 0.0:
                volumes, log_ranges = await self.calc_volumes_and_log_ranges(
                    pside,
                    symbols=candidates,
                    max_age_ms=max_age_ms,
                    max_network_fetches=fetch_budget,
                )
            else:
                volumes = {
                    symbol: float(len(candidates) - idx) for idx, symbol in enumerate(candidates)
                }
                log_ranges = await self.calc_log_range(
                    pside,
                    eligible_symbols=candidates,
                    max_age_ms=max_age_ms,
                    max_network_fetches=fetch_budget,
                )
            if volatility_drop > 0.0:
                ranked = sorted(
                    candidates,
                    key=lambda symbol: float(log_ranges.get(symbol, 0.0)),
                    reverse=True,
                )
                keep_from = min(
                    len(ranked),
                    max(0, int(round(len(ranked) * float(volatility_drop)))),
                )
                candidates = ranked[keep_from:]
                if not candidates:
                    return []
            features = [
                {
                    "index": idx,
                    "enabled": min_cost_flags.get(symbol, True),
                    "volume_score": volumes.get(symbol, 0.0),
                    "volatility_score": log_ranges.get(symbol, 0.0),
                    "ema_readiness_score": 0.0,
                }
                for idx, symbol in enumerate(candidates)
            ]
            selected = pbr.select_coin_indices_py(
                features,
                max_n_positions,
                clip_pct,
                weights,
                True,
            )
            ideal_coins = [candidates[i] for i in selected]
            if not ideal_coins and self.live_value("filter_by_min_effective_cost"):
                if any(not flag for flag in min_cost_flags.values()):
                    self.warn_on_high_effective_min_cost(pside)
        else:
            eligible = [s for s in candidates if min_cost_flags.get(s, True)]
            if not eligible:
                if self.live_value("filter_by_min_effective_cost"):
                    self.warn_on_high_effective_min_cost(pside)
                return []
            # 所有已批准的币种均被选中，不过滤成交量和日志范围
            ideal_coins = sorted(eligible)
        return ideal_coins

    async def calc_volumes_and_log_ranges(
        self,
        pside: str,
        symbols: Optional[Iterable[str]] = None,
        *,
        max_age_ms: Optional[int] = 60_000,
        max_network_fetches: Optional[int] = None,
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        """通过一次 K线抓取计算每个交易对的 1m EMA 报价成交量和 1m EMA 对数范围。

        使用 CandlestickManager.get_latest_ema_metrics() 来避免每个交易对调用两次
        get_candles()（一次获取成交量，一次获取对数范围）。

        若设置了 *max_network_fetches*，最多允许指定数量的交易对
        触发网络抓取。剩余交易对获得非常大的 TTL，仅返回缓存数据（若未缓存则返回 0.0）而不访问 API。
        """
        span_volume = int(round(self.bot_value(pside, "forager_volume_ema_span")))
        span_volatility = int(round(self.bot_value(pside, "forager_volatility_ema_span")))
        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0
        span_buffer = 1.0 + max(0.0, warmup_ratio)
        max_span = max(span_volume, span_volatility)
        window_candles = max(1, int(math.ceil(max_span * span_buffer))) if max_span > 0 else 1
        if max_warmup_minutes > 0:
            window_candles = min(int(window_candles), int(max_warmup_minutes))
        if symbols is None:
            symbols = self.get_symbols_approved_or_has_pos(pside)

        syms = list(symbols)

        per_sym_ttl, cache_only_never_fetched = self._compute_fetch_budget_ttls(
            syms, max_age_ms, max_network_fetches
        )

        async def one(symbol: str):
            """获取单个交易对的最新 OHLCV 数据。"""
            try:
                if symbol in cache_only_never_fetched:
                    return (0.0, 0.0)
                ttl = per_sym_ttl.get(symbol)
                if ttl is None or ttl == 0:
                    if max_age_ms is not None:
                        ttl = int(max_age_ms)
                    else:
                        has_pos = self.has_position(symbol)
                        has_oo = (
                            bool(self.open_orders.get(symbol)) if hasattr(self, "open_orders") else False
                        )
                        ttl = (
                            60_000
                            if (has_pos or has_oo)
                            else int(getattr(self, "inactive_coin_candle_ttl_ms", 600_000))
                        )
                res = await self.cm.get_latest_ema_metrics(
                    symbol,
                    {"qv": span_volume, "log_range": span_volatility},
                    max_age_ms=ttl,
                    window_candles=window_candles,
                    timeframe=None,
                )
                vol = float(res.get("qv", float("nan")))
                lr = float(res.get("log_range", float("nan")))
                return (0.0 if not np.isfinite(vol) else vol, 0.0 if not np.isfinite(lr) else lr)
            except Exception:
                return (0.0, 0.0)

        tasks = {s: asyncio.create_task(one(s)) for s in syms}
        volumes: Dict[str, float] = {}
        log_ranges: Dict[str, float] = {}
        started_ms = utc_ms()
        for sym, task in tasks.items():
            try:
                vol, lr = await task
            except Exception:
                vol, lr = 0.0, 0.0
            volumes[sym] = float(vol)
            log_ranges[sym] = float(lr)

        # EMA 排名日志节流：每个指标最多每 5 分钟记录一次。
        # 仅在排名发生变化时记录。
        elapsed_s = max(0.001, (utc_ms() - started_ms) / 1000.0)
        now_ms = utc_ms()
        ema_log_throttle_ms = (
            300_000  # 5 minutes between logs per metric (reduced from 60s to reduce forager noise)
        )

        if volumes:
            top_n = min(8, len(volumes))
            top = sorted(volumes.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            top_syms = tuple(sym for sym, _ in top)
            if not hasattr(self, "_volume_top_cache"):
                self._volume_top_cache = {}
            if not hasattr(self, "_volume_top_last_log_ms"):
                self._volume_top_last_log_ms = {}
            cache_key = (pside, span_volume)
            last_top = self._volume_top_cache.get(cache_key)
            last_log_ms = self._volume_top_last_log_ms.get(cache_key, 0)
            # 同时满足：排名变化 AND 足够时间已过
            if last_top != top_syms and (now_ms - last_log_ms) >= ema_log_throttle_ms:
                self._volume_top_cache[cache_key] = top_syms
                self._volume_top_last_log_ms[cache_key] = now_ms
                summary = ", ".join(f"{symbol_to_coin(sym)}={val:.2f}" for sym, val in top)
                logging.info(
                    f"[ranking] volume EMA span {span_volume}: {len(syms)} coins elapsed={int(elapsed_s)}s, top{top_n}: {summary}"
                )
        if log_ranges:
            top_n = min(8, len(log_ranges))
            top = sorted(log_ranges.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            top_syms = tuple(sym for sym, _ in top)
            if not hasattr(self, "_log_range_top_cache"):
                self._log_range_top_cache = {}
            if not hasattr(self, "_log_range_top_last_log_ms"):
                self._log_range_top_last_log_ms = {}
            cache_key = (pside, span_volatility)
            last_top = self._log_range_top_cache.get(cache_key)
            last_log_ms = self._log_range_top_last_log_ms.get(cache_key, 0)
            # 同时满足：排名变化 AND 足够时间已过
            if last_top != top_syms and (now_ms - last_log_ms) >= ema_log_throttle_ms:
                self._log_range_top_cache[cache_key] = top_syms
                self._log_range_top_last_log_ms[cache_key] = now_ms
                summary = ", ".join(f"{symbol_to_coin(sym)}={val:.6f}" for sym, val in top)
                logging.info(
                    f"[ranking] log_range EMA span {span_volatility}: {len(syms)} coins elapsed={int(elapsed_s)}s, top{top_n}: {summary}"
                )

        return volumes, log_ranges

    def warn_on_high_effective_min_cost(self, pside):
        """若有效最小成本过滤移除了所有候选则发出警告。"""
        if not self.live_value("filter_by_min_effective_cost"):
            return
        if not self.is_pside_enabled(pside):
            return
        approved_coins_filtered = [
            x
            for x in self.approved_coins_minus_ignored_coins[pside]
            if self.effective_min_cost_is_low_enough(pside, x)
        ]
        if len(approved_coins_filtered) == 0:
            logging.info(
                f"Warning: No {pside} symbols are approved due to min effective cost too high. "
                + f"Suggestions: 1) increase account balance, 2) "
                + f"set 'filter_by_min_effective_cost' to false, 3) reduce n_{pside}s"
            )

    def get_max_n_positions(self, pside):
        """返回该方向配置的最大并发持仓数。"""
        max_n_positions = min(
            self.bot_value(pside, "n_positions"),
            len(self.approved_coins_minus_ignored_coins[pside]),
        )
        return max(0, int(round(max_n_positions)))

    def get_current_n_positions(self, pside):
        """统计该方向的未平仓位数，排除非活跃强制模式。"""
        n_positions = 0
        for symbol in self.positions:
            if self.positions[symbol][pside]["size"] != 0.0:
                forced_mode = self.get_forced_PB_mode(pside, symbol)
                if forced_mode in ["normal", "graceful_stop"]:
                    n_positions += 1
                else:
                    n_positions += 1
        return n_positions

    def get_forced_PB_mode(self, pside, symbol=None):
        """返回方向或交易对的显式强制模式（若已配置）。"""
        if self._equity_hard_stop_enabled(pside):
            state = self._hsl_state(pside)
            if self._equity_hard_stop_runtime_red_latched(pside) and not state["halted"]:
                return "panic"
            if state["halted"]:
                if symbol is None:
                    return "graceful_stop"
                return self._equity_hard_stop_halted_mode(pside, symbol)
        if symbol is not None:
            runtime_forced = getattr(self, "_runtime_forced_modes", {}).get(pside, {}).get(symbol)
            if runtime_forced:
                return str(runtime_forced)
        mode = self.config_get(["live", f"forced_mode_{pside}"], symbol)
        if mode:
            return expand_PB_mode(mode)
        elif symbol and not self.markets_dict[symbol]["active"]:
            return "tp_only"
        return None

    def set_wallet_exposure_limits(self):
        """重新计算双向的钱包敞口限制和逐交易对覆盖。"""
        for pside in ["long", "short"]:
            self.config["bot"][pside]["wallet_exposure_limit"] = self.get_wallet_exposure_limit(pside)
            for symbol in self.coin_overrides:
                ov_conf = self.coin_overrides[symbol].get("bot", {}).get(pside, {})
                if "wallet_exposure_limit" in ov_conf:
                    self.coin_overrides[symbol]["bot"][pside]["wallet_exposure_limit"] = (
                        self.get_wallet_exposure_limit(pside, symbol)
                    )

    def get_wallet_exposure_limit(self, pside, symbol=None):
        """从固定配置分母返回方向 WEL，遵循逐交易对覆盖。"""
        if symbol:
            fwel = (
                self.coin_overrides.get(symbol, {})
                .get("bot", {})
                .get(pside, {})
                .get("wallet_exposure_limit")
            )
            if fwel is not None:
                return fwel
        twel = self.bot_value(pside, "total_wallet_exposure_limit")
        if twel <= 0.0:
            return 0.0
        n_positions = int(round(self.bot_value(pside, "n_positions")))
        if n_positions <= 0:
            return 0.0
        return round(twel / n_positions, 8)

    def is_pside_enabled(self, pside):
        """若当前配置中启用了指定方向的交易则返回 True。"""
        return (
            self.bot_value(pside, "total_wallet_exposure_limit") > 0.0
            and self.bot_value(pside, "n_positions") > 0.0
        )

    def effective_min_cost_is_low_enough(self, pside, symbol):
        """检查交易对是否满足有效最小成本要求。"""
        if not self.live_value("filter_by_min_effective_cost"):
            return True
        base_limit = self.get_wallet_exposure_limit(pside, symbol)
        allowance_pct = float(self.bp(pside, "risk_we_excess_allowance_pct", symbol))
        allowance_multiplier = 1.0 + max(0.0, allowance_pct)
        effective_limit = base_limit * allowance_multiplier
        return (
            self.get_hysteresis_snapped_balance()
            * effective_limit
            * self.bp(pside, "entry_initial_qty_pct", symbol)
            >= self.effective_min_cost.get(symbol, float("inf"))
        )

    def get_hysteresis_snapped_balance(self) -> float:
        """返回用于仓位计算的滞后吸附余额。"""
        return float(getattr(self, "balance", 0.0) or 0.0)

    def get_raw_balance(self) -> float:
        """返回原始钱包余额（对于传统测试桩回退到吸附值）。"""
        if hasattr(self, "balance_raw"):
            return float(getattr(self, "balance_raw", 0.0) or 0.0)
        return self.get_hysteresis_snapped_balance()

    def add_new_order(self, order, source="WS"):
        """空操作占位；子类通过 REST 同步更新挂单。"""
        return  # 仅通过 self.update_open_orders() 中的 REST 添加新订单

    def remove_order(self, order: dict, source="WS", reason="cancelled"):
        """空操作占位；子类通过 REST 同步移除挂单。"""
        return  # 仅通过 self.update_open_orders() 中的 REST 移除订单

    def handle_order_update(self, upd_list):
        """当 websocket 订单更新到来时标记执行循环为脏。"""
        if upd_list:
            self.execution_scheduled = True
        return

    async def handle_balance_update(self, source="REST"):
        """处理余额更新事件，重新计算权益硬止损指标。"""
        if not hasattr(self, "_previous_balance_raw"):
            self._previous_balance_raw = 0.0
        if not hasattr(self, "_previous_balance_snapped"):
            self._previous_balance_snapped = 0.0
        if not hasattr(self, "_last_raw_only_log_time"):
            self._last_raw_only_log_time = 0.0
        balance_raw = self.get_raw_balance()
        balance_snapped = self.get_hysteresis_snapped_balance()
        if (
            balance_raw != self._previous_balance_raw
            or balance_snapped != self._previous_balance_snapped
        ):
            snap_changed = balance_snapped != self._previous_balance_snapped
            raw_only = not snap_changed
            now = time.time()
            should_log = snap_changed or (now - self._last_raw_only_log_time >= 900.0)
            try:
                equity = balance_raw + (await self.calc_upnl_sum())
                self._monitor_last_equity = float(equity)
                if should_log:
                    logging.info(
                        "[balance] raw %.6f -> %.6f | snap %.6f -> %.6f | equity: %.4f source: %s",
                        self._previous_balance_raw,
                        balance_raw,
                        self._previous_balance_snapped,
                        balance_snapped,
                        equity,
                        source,
                    )
                    if raw_only:
                        self._last_raw_only_log_time = now
                self._monitor_record_event(
                    "account.balance",
                    ("account", "balance"),
                    {
                        "previous_balance_raw": float(self._previous_balance_raw),
                        "balance_raw": float(balance_raw),
                        "previous_balance_snapped": float(self._previous_balance_snapped),
                        "balance_snapped": float(balance_snapped),
                        "equity": float(equity),
                        "source": str(source),
                    },
                )
            except Exception as e:
                logging.error(f"error with handle_balance_update {e}")
                traceback.print_exc()
            finally:
                self._previous_balance_raw = balance_raw
                self._previous_balance_snapped = balance_snapped
                self.execution_scheduled = True

    async def calc_upnl_sum(self):
        """使用最新价格计算所有已获取持仓的未实现 PnL。"""
        upnl_sum = 0.0
        last_prices = await self.cm.get_last_prices(
            set([x["symbol"] for x in self.fetched_positions]), max_age_ms=60_000
        )
        for elm in self.fetched_positions:
            try:
                upnl = calc_pnl(
                    elm["position_side"],
                    elm["price"],
                    last_prices[elm["symbol"]],
                    elm["size"],
                    self.inverse,
                    self.c_mults[elm["symbol"]],
                )
                if upnl:
                    upnl_sum += upnl
            except Exception as e:
                logging.error(f"error calculating upnl sum {e}")
                traceback.print_exc()
                return 0.0
        return upnl_sum

    async def init_pnls(self):
        """初始化 FillEventsManager 用于 PnL 跟踪。"""
        if self._pnls_initialized:
            return

        try:
            logging.debug("[fills] initializing FillEventsManager")

            # 从配置中提取交易对池
            symbol_pool = _extract_symbol_pool(self.config, None)

            # 构建此机器人的抓取器
            fetcher = _build_fetcher_for_bot(self, symbol_pool)

            # 创建带有独立缓存路径的 FillEventsManager
            cache_path = Path(f"caches/fill_events/{self.exchange}/{self.user}")

            self._pnls_manager = FillEventsManager(
                exchange=self.exchange,
                user=self.user,
                fetcher=fetcher,
                cache_path=cache_path,
            )

            # 加载缓存的事件
            await self._pnls_manager.ensure_loaded()

            # Bybit 缓存医生默认在启动时运行，自行修复已知的重复成交问题。
            doctor_mode = str(os.getenv("PASSIVBOT_FILL_EVENTS_DOCTOR", "")).strip().lower()
            if self.exchange == "bybit":
                if doctor_mode not in ("0", "false", "off", "disable", "disabled"):
                    auto_repair = doctor_mode not in ("check", "scan", "detect")
                    report = await self._pnls_manager.run_doctor(auto_repair=auto_repair)
                    logging.info(
                        "[fills-doctor] startup report anomalies=%s repaired=%s mode=%s",
                        report.get("anomaly_events", 0),
                        report.get("repaired", False),
                        doctor_mode or ("repair" if auto_repair else "check"),
                    )
            elif doctor_mode:
                auto_repair = doctor_mode in ("1", "true", "yes", "repair", "fix", "auto")
                report = await self._pnls_manager.run_doctor(auto_repair=auto_repair)
                logging.info(
                    "[fills-doctor] startup report anomalies=%s repaired=%s mode=%s",
                    report.get("anomaly_events", 0),
                    report.get("repaired", False),
                    doctor_mode,
                )

            cached_count = len(self._pnls_manager._events)
            logging.info("[fills] cache ready: %d cached events loaded", cached_count)

            self._pnls_initialized = True

        except Exception as e:
            logging.error("Failed to initialize FillEventsManager: %s", e)
            traceback.print_exc()
            raise

    async def update_pnls(self):
        """使用 FillEventsManager 获取最新成交并更新缓存。"""
        if self.stop_signal_received:
            return False

        await self.init_pnls()  # 若已初始化则无操作

        if self._pnls_manager is None:
            return False

        try:
            # 使用相同的回看窗口
            lookback = parse_pnls_max_lookback_days(
                self.live_value("pnls_max_lookback_days"),
                field_name="live.pnls_max_lookback_days",
            )
            age_limit = lookback.fill_cache_age_limit_ms(self.get_exchange_time())

            # 刷新前获取现有事件 ID 和源 ID
            existing_ids: set[str] = set()
            existing_source_ids: set[str] = set()
            for ev in self._pnls_manager.get_events():
                if getattr(ev, "id", None):
                    existing_ids.add(ev.id)
                src_ids = getattr(ev, "source_ids", None)
                if src_ids:
                    existing_source_ids.update(str(x) for x in src_ids if x)
                elif getattr(ev, "id", None):
                    existing_source_ids.add(ev.id)

            # 检查是否需要全量刷新（缓存为空或过旧）
            events = self._pnls_manager.get_events()
            needs_full_refresh = not events
            history_scope = self._pnls_manager.get_history_scope()
            if lookback.is_all and events and history_scope != "all":
                needs_full_refresh = True
                cache_key = "_fills_full_refresh_logged"
                if not getattr(self, cache_key, False):
                    setattr(self, cache_key, True)
                    logging.debug(
                        "[fills] Cache history scope %s is narrower than requested full history; doing full refresh",
                        history_scope,
                    )
            elif events and age_limit is not None:
                oldest_event_ts = events[0].timestamp
                if oldest_event_ts > age_limit + 1000 * 60 * 60 * 24:  # 比限制新超过 1 天
                    needs_full_refresh = True
                    # 每个会话仅记录一次以避免刷屏
                    cache_key = "_fills_full_refresh_logged"
                    if not getattr(self, cache_key, False):
                        setattr(self, cache_key, True)
                        logging.debug(
                            "[fills] Cache oldest event (%s) is newer than lookback (%s), doing full refresh",
                            ts_to_date(oldest_event_ts)[:19],
                            ts_to_date(age_limit)[:19],
                        )

            if needs_full_refresh:
                # 全量刷新，使用合适的回看窗口
                if not getattr(self, "_fills_full_refresh_logged", False):
                    if age_limit is None:
                        logging.debug("[fills] Performing full refresh from full available history")
                    else:
                        logging.debug(
                            "[fills] Performing full refresh from %s", ts_to_date(age_limit)[:19]
                        )
                await self._pnls_manager.refresh(
                    start_ms=None if age_limit is None else int(age_limit),
                    end_ms=None,
                )
                self._pnls_manager.set_history_scope("all" if lookback.is_all else "window")
            else:
                # 增量刷新
                await self._pnls_manager.refresh_latest(overlap=20)

            # 查找并记录新事件（刷新前不在缓存中的）
            all_events = self._pnls_manager.get_events()
            new_events = []
            seen_new_source_ids: set[str] = set()
            for ev in all_events:
                src_ids = getattr(ev, "source_ids", None)
                if src_ids:
                    src_ids = [str(x) for x in src_ids if x]
                else:
                    src_ids = [ev.id] if getattr(ev, "id", None) else []
                if not src_ids:
                    continue
                if any(src_id in existing_source_ids for src_id in src_ids):
                    continue
                if any(src_id in seen_new_source_ids for src_id in src_ids):
                    continue
                new_events.append(ev)
                seen_new_source_ids.update(src_ids)
            if new_events:
                self._log_new_fill_events(new_events)

            return True

        except RateLimitExceeded:
            self._health_rate_limits += 1
            self._monitor_record_event(
                "error.exchange",
                ("error", "exchange", "rate_limit"),
                {"source": "update_pnls", "message": "rate limit exceeded"},
            )
            logging.warning("[rate] hit rate limit while fetching fill events; retrying next cycle")
            return False
        except Exception as e:
            self._monitor_record_error(
                "error.exchange",
                e,
                tags=("error", "exchange"),
                payload={"source": "update_pnls"},
            )
            logging.error("[fills] Failed to update FillEventsManager: %s", e)
            if self.logging_level >= 2:
                traceback.print_exc()
            raise

    # -------------------------------------------------------------------------
    # FillEventsManager 辅助方法
    # -------------------------------------------------------------------------

    def _log_fill_event(self, event) -> str:
        """格式化 FillEvent 用于日志。

        格式：[fill] BTC long entry +0.001 @ 100000.00 id=abc123
        平仓：[fill] BTC long close -0.001 @ 100500.00, pnl=+5.50 USDT id=abc123
        未知订单：[fill] BTC long unknown -0.2 @ 2.05, pnl=-0.005 USDT (coid=abc123) id=xyz789
        """
        coin = symbol_to_coin(event.symbol, verbose=False) or event.symbol
        pside = event.position_side.lower()
        order_type = event.pb_order_type.lower() if event.pb_order_type else "fill"

        # 格式化数量符号（买入为 +，卖出为 -）
        qty_sign = "+" if event.side.lower() == "buy" else "-"
        qty_str = f"{qty_sign}{abs(event.qty):.6g}"

        # 包含时间戳使历史成交在日志中一目了然
        fill_ts = ""
        if getattr(event, "timestamp", 0):
            fill_ts = ts_to_date(event.timestamp)[:19]
        elif getattr(event, "datetime", ""):
            fill_ts = str(event.datetime)[:19]

        if fill_ts:
            msg = f"[fill] {fill_ts} {coin} {pside} {order_type} {qty_str} @ {event.price:.2f}"
        else:
            msg = f"[fill] {coin} {pside} {order_type} {qty_str} @ {event.price:.2f}"

        # 为平仓订单添加 pnl（始终显示，即使为 0.0）
        # 平仓订单的类型中包含 "close"（如 close_grid_long、close_unstuck_long）
        is_close = "close" in order_type
        if is_close or event.pnl != 0.0:
            pnl_sign = "+" if event.pnl >= 0 else ""
            msg += f", pnl={pnl_sign}{round_dynamic(event.pnl, 3)} USDT"

        # 为未知订单添加 client_order_id
        if order_type == "unknown" and event.client_order_id:
            msg += f" (coid={event.client_order_id})"

        # 始终在末尾添加成交 ID 以便追踪
        fill_id = getattr(event, "id", None)
        if fill_id:
            # 截断长 ID 以提高可读性（显示前 12 个字符）
            short_id = str(fill_id)[:12] if len(str(fill_id)) > 12 else str(fill_id)
            msg += f" id={short_id}"

        return msg

    def _log_new_fill_events(self, new_events: list) -> None:
        """记录新成交事件。超过 20 个则截断为摘要。"""
        if not new_events:
            return

        # 跟踪成交和 PnL 用于健康摘要
        self._health_fills += len(new_events)
        self._health_pnl += sum(ev.pnl for ev in new_events)

        if len(new_events) > 20:
            # 截断为摘要
            total_pnl = sum(ev.pnl for ev in new_events)
            pnl_sign = "+" if total_pnl >= 0 else ""
            logging.info(
                "[fill] %d fills, pnl=%s%s USDT",
                len(new_events),
                pnl_sign,
                round_dynamic(total_pnl, 3),
            )
        else:
            # 记录每个事件
            for event in sorted(new_events, key=lambda e: e.timestamp):
                logging.info(self._log_fill_event(event))
        for event in sorted(new_events, key=lambda e: e.timestamp):
            self._monitor_record_fill_history(event)
            self._monitor_record_event(
                "order.filled",
                ("order", "fill"),
                self._monitor_fill_payload(event),
                symbol=getattr(event, "symbol", None),
                pside=str(getattr(event, "position_side", "") or "").lower() or None,
                ts=int(getattr(event, "timestamp", 0) or 0) or None,
            )

    def _calc_unstuck_allowances(self, allow_new_unstuck: bool) -> dict[str, float]:
        """使用 FillEventsManager 数据计算解套额度。"""
        if not allow_new_unstuck or self._pnls_manager is None:
            return {"long": 0.0, "short": 0.0}

        events = self._get_effective_pnl_events()
        if not events:
            return {"long": 0.0, "short": 0.0}

        pnls_cumsum = np.array([float(ev.pnl) for ev in events], dtype=float).cumsum()
        pnls_cumsum_max, pnls_cumsum_last = pnls_cumsum.max(), pnls_cumsum[-1]
        out = {}
        balance_raw = self.get_raw_balance()
        for pside in ["long", "short"]:
            pct = float(self.bot_value(pside, "unstuck_loss_allowance_pct") or 0.0)
            if pct > 0.0:
                out[pside] = float(
                    pbr.calc_auto_unstuck_allowance(
                        balance_raw,
                        pct * float(self.bot_value(pside, "total_wallet_exposure_limit") or 0.0),
                        float(pnls_cumsum_max),
                        float(pnls_cumsum_last),
                    )
                )
            else:
                out[pside] = 0.0
        return out

    def _get_realized_pnl_cumsum_stats(self) -> dict[str, float]:
        """从 FillEventsManager 历史返回总已实现 PnL 累积峰值/当前值。"""
        if self._pnls_manager is None:
            return {"max": 0.0, "last": 0.0}
        events = self._get_effective_pnl_events()
        if not events:
            return {"max": 0.0, "last": 0.0}
        pnls_cumsum = np.array([float(ev.pnl) for ev in events], dtype=float).cumsum()
        return {"max": float(pnls_cumsum.max()), "last": float(pnls_cumsum[-1])}

    def _log_realized_loss_gate_blocks(self, out: dict, idx_to_symbol: dict[int, str]) -> None:
        """为被已实现亏损门限阻止的平仓订单发出可见警告。"""
        diagnostics = out.get("diagnostics", {}) if isinstance(out, dict) else {}
        blocks = diagnostics.get("loss_gate_blocks", [])
        if not isinstance(blocks, list) or not blocks:
            return
        now_ms = utc_ms()
        for block in blocks:
            if not isinstance(block, dict):
                continue
            symbol = idx_to_symbol.get(int(block.get("symbol_idx", -1)), "unknown")
            pside = str(block.get("pside", "unknown"))
            order_type = str(block.get("order_type", "unknown"))
            throttle_key = f"{symbol}:{pside}:{order_type}"
            last_log_ms = self._loss_gate_last_log_ms.get(throttle_key, 0)
            if (now_ms - last_log_ms) < self._loss_gate_log_interval_ms:
                continue
            self._loss_gate_last_log_ms[throttle_key] = now_ms
            qty = float(block.get("qty", 0.0) or 0.0)
            price = float(block.get("price", 0.0) or 0.0)
            projected_pnl = float(block.get("projected_pnl", 0.0) or 0.0)
            projected_balance = float(block.get("projected_balance_after", 0.0) or 0.0)
            balance_floor = float(block.get("balance_floor", 0.0) or 0.0)
            max_loss_pct = float(block.get("max_realized_loss_pct", 1.0) or 1.0)
            logging.warning(
                "[risk] order blocked by realized-loss gate | %s %s %s qty=%.10g price=%.10g "
                "projected_pnl=%.6f projected_balance=%.6f floor=%.6f max_realized_loss_pct=%.6f | "
                "adjust live.max_realized_loss_pct to change behavior",
                symbol,
                pside,
                order_type,
                qty,
                price,
                projected_pnl,
                projected_balance,
                balance_floor,
                max_loss_pct,
            )

    # 传统 init_fill_events、update_fill_events 等已移除 - 使用 FillEventsManager

    async def get_balance_equity_history(
        self, fill_events: Optional[List[dict]] = None, current_balance: Optional[float] = None
    ) -> Dict[str, Any]:
        """回放标准成交事件以生成历史余额/权益曲线。"""
        await self.init_pnls()

        def _safe_float(val: Any, default: float = 0.0) -> float:
            try:
                if val is None:
                    return default
                return float(val)
            except Exception:
                return default

        def _normalize_symbol(symbol: Any) -> str:
            sym = str(symbol) if symbol else ""
            if not sym:
                return ""
            if sym in self.c_mults:
                return sym
            try:
                converted = self.get_symbol_id_inv(sym)
                if converted:
                    return converted
            except Exception:
                pass
            return sym

        def _ensure_slot(container: Dict[str, Dict[str, Dict[str, float]]], symbol: str):
            if symbol not in container:
                container[symbol] = {
                    "long": {"size": 0.0, "price": 0.0},
                    "short": {"size": 0.0, "price": 0.0},
                }
            return container[symbol]

        def _determine_action(
            pside: str, side: str, qty_signed: Optional[float], explicit: Optional[str]
        ):
            if explicit in ("increase", "decrease"):
                return explicit
            if qty_signed is not None and qty_signed != 0.0:
                return "increase" if qty_signed > 0 else "decrease"
            side = side.lower()
            if pside == "long":
                return "increase" if side == "buy" else "decrease"
            return "increase" if side == "sell" else "decrease"

        def _extract_events(source: List[dict]) -> List[dict]:
            """从原始成交记录中提取并规范化事件数据。"""
            out = []
            for fill in source:
                ts_raw = fill.get("timestamp")
                if ts_raw is None:
                    continue
                try:
                    ts = int(ensure_millis(ts_raw))
                except Exception:
                    continue
                symbol = _normalize_symbol(fill.get("symbol"))
                if not symbol:
                    continue
                pside = str(fill.get("position_side", fill.get("pside", "long"))).lower()
                if pside not in ("long", "short"):
                    pside = "long"
                qty_signed = fill.get("qty_signed")
                qty_fallback_keys = ("qty", "amount", "size", "contracts")
                qty_val = _safe_float(
                    (
                        qty_signed
                        if qty_signed is not None
                        else next(
                            (fill.get(k) for k in qty_fallback_keys if fill.get(k) is not None), 0.0
                        )
                    ),
                    0.0,
                )
                qty = abs(qty_val)
                if qty <= 0.0:
                    continue
                price_keys = ("price", "avgPrice", "average", "avg_price", "execPrice")
                price = next((fill.get(k) for k in price_keys if fill.get(k) is not None), None)
                if price is None:
                    info = fill.get("info", {})
                    price = (
                        info.get("avgPrice") or info.get("execPrice") or info.get("avg_exec_price")
                    )
                price = _safe_float(price, 0.0)
                if price <= 0.0:
                    continue
                pnl_val = _safe_float(fill.get("pnl", 0.0), 0.0)
                fee_cost = 0.0
                fee_obj = fill.get("fee")
                if isinstance(fee_obj, dict):
                    fee_cost = _safe_float(fee_obj.get("cost", 0.0), 0.0)
                elif isinstance(fee_obj, (int, float, str)):
                    fee_cost = _safe_float(fee_obj, 0.0)
                elif isinstance(fill.get("fees"), (list, tuple)):
                    fee_cost = sum(
                        _safe_float(x.get("cost", 0.0), 0.0)
                        for x in fill["fees"]
                        if isinstance(x, dict)
                    )
                side = str(fill.get("side", "")).lower()
                action = _determine_action(pside, side, qty_signed, fill.get("action"))
                out.append(
                    {
                        "timestamp": ts,
                        "symbol": symbol,
                        "pside": pside,
                        "qty": qty,
                        "price": price,
                        "action": action,
                        "pnl": pnl_val,
                        "fee": fee_cost,
                        "pb_order_type": str(fill.get("pb_order_type") or "").lower(),
                        "c_mult": float(self.c_mults.get(symbol, 1.0)),
                    }
                )
            return sorted(out, key=lambda x: x["timestamp"])

        def _current_position_state() -> Dict[Tuple[str, str], Tuple[float, float]]:
            out: Dict[Tuple[str, str], Tuple[float, float]] = {}
            for symbol, slots in (self.positions or {}).items():
                norm_symbol = _normalize_symbol(symbol)
                if not norm_symbol or not isinstance(slots, dict):
                    continue
                for pside in ("long", "short"):
                    pos = slots.get(pside, {})
                    if not isinstance(pos, dict):
                        continue
                    size = abs(_safe_float(pos.get("size"), 0.0))
                    price = _safe_float(pos.get("price"), 0.0) if size > 1e-12 else 0.0
                    out[(norm_symbol, pside)] = (size, price)
            return out

        if fill_events is None:
            if self._pnls_manager:
                fill_events = [ev.to_dict() for ev in self._pnls_manager.get_events()]
            else:
                fill_events = []

        events = _extract_events(fill_events)
        current_position_state = _current_position_state()
        if events:
            try:
                compute_psize_pprice(
                    events,
                    final_state=current_position_state,
                    log_discrepancies=True,
                    log_prefix=f"{self.exchange}:{self.user} balance-equity replay",
                )
            except TypeError:
                compute_psize_pprice(events)
        if not events:
            ts_now = self.get_exchange_time()
            balance_now = (
                float(current_balance) if current_balance is not None else self.get_raw_balance()
            )
            point = {
                "timestamp": ts_now,
                "balance": balance_now,
                "equity": balance_now,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl_long": 0.0,
                "unrealized_pnl_short": 0.0,
                "realized_pnl_long": 0.0,
                "realized_pnl_short": 0.0,
                "is_flat": True,
                "is_flat_long": True,
                "is_flat_short": True,
                "panic_fill_count": 0,
            }
            return {
                "timeline": [point],
                "panic_flatten_events": [],
                "fill_events": [],
                "balances": [{"timestamp": point["timestamp"], "balance": balance_now}],
                "equities": [
                    {
                        "timestamp": point["timestamp"],
                        "equity": balance_now,
                        "unrealized_pnl": 0.0,
                    }
                ],
                "metadata": {
                    "lookback_days": parse_pnls_max_lookback_days(
                        self.live_value("pnls_max_lookback_days"),
                        field_name="live.pnls_max_lookback_days",
                    ).display_value,
                    "resolution_ms": ONE_MIN_MS,
                    "events_used": 0,
                    "symbols_covered": [],
                    "missing_price_symbols": [],
                },
            }

        lookback = parse_pnls_max_lookback_days(
            self.live_value("pnls_max_lookback_days"),
            field_name="live.pnls_max_lookback_days",
        )
        ts_now = self.get_exchange_time()
        lookback_start = lookback.balance_history_start_ms(ts_now)

        balance_now = (
            float(current_balance) if current_balance is not None else self.get_raw_balance()
        )
        balance_now = max(balance_now, 0.0)
        total_realised = sum(
            evt["pnl"] + evt.get("fee", 0.0) for evt in events if evt["timestamp"] <= ts_now
        )
        baseline_balance = balance_now - total_realised

        if lookback_start is None:
            start_ts = ensure_millis(events[0]["timestamp"])
            record_start_minute = int(math.floor(start_ts / ONE_MIN_MS) * ONE_MIN_MS)
        else:
            start_ts = min(ensure_millis(events[0]["timestamp"]), lookback_start)
            record_start_minute = int(math.floor(lookback_start / ONE_MIN_MS) * ONE_MIN_MS)
        start_minute = int(math.floor(start_ts / ONE_MIN_MS) * ONE_MIN_MS)
        end_minute = int(math.floor(ts_now / ONE_MIN_MS) * ONE_MIN_MS)
        if end_minute < record_start_minute:
            end_minute = record_start_minute

        symbols = {evt["symbol"] for evt in events if evt["symbol"]}
        price_lookup: Dict[str, Dict[int, float]] = {}
        approximate_price_sources: Dict[str, Dict[str, int]] = {}
        if symbols and getattr(self, "cm", None) is not None:
            tasks = {
                sym: asyncio.create_task(
                    self.cm.get_candles(sym, start_ts=start_minute, end_ts=end_minute, strict=False)
                )
                for sym in symbols
            }
            for sym, task in tasks.items():
                try:
                    arr = await task
                except Exception as exc:
                    logging.error(f"error fetching candles for {sym} {exc}")
                    arr = np.empty((0,), dtype=CANDLE_DTYPE)
                price_lookup[sym] = {
                    int(row["ts"]): float(row["c"]) for row in arr if float(row["c"]) > 0.0
                }
            is_hyperliquid = str(getattr(self, "exchange", "")).lower() == "hyperliquid"
            if is_hyperliquid:
                lookback_minutes = int(max(0, (end_minute - start_minute) // ONE_MIN_MS)) + 1
                tf_plan: list[tuple[str, int]] = []
                if lookback_minutes > 5000:
                    tf_plan.append(("5m", 5))
                if lookback_minutes > 5000 * 5:
                    tf_plan.append(("15m", 15))
                for timeframe, tf_minutes in tf_plan:
                    tf_tasks = {
                        sym: asyncio.create_task(
                            self.cm.get_candles(
                                sym,
                                start_ts=start_minute,
                                end_ts=end_minute,
                                strict=False,
                                timeframe=timeframe,
                            )
                        )
                        for sym in symbols
                    }
                    for sym, task in tf_tasks.items():
                        try:
                            arr = await task
                        except Exception as exc:
                            logging.error(
                                "error fetching %s candles for %s during equity history replay: %s",
                                timeframe,
                                sym,
                                exc,
                            )
                            continue
                        if arr is None or arr.size == 0:
                            continue
                        synth = synthesize_1m_from_higher_tf(arr, tf_minutes)
                        if synth.size == 0:
                            continue
                        added = 0
                        lookup = price_lookup.setdefault(sym, {})
                        for row in synth:
                            ts = int(row["ts"])
                            close = float(row["c"])
                            if ts < start_minute or ts > end_minute:
                                continue
                            if ts in lookup:
                                continue
                            lookup[ts] = float(close)
                            added += 1
                        if added > 0:
                            approximate_price_sources.setdefault(sym, {})[timeframe] = added
        else:
            price_lookup = {sym: {} for sym in symbols}

        positions: Dict[str, Dict[str, Dict[str, float]]] = {}
        active_symbols: set[str] = set()
        timeline: List[Dict[str, float]] = []
        panic_flatten_events: List[Dict[str, Any]] = []
        missing_price_symbols: set[str] = set()
        realized_pnl_pside_running = {"long": 0.0, "short": 0.0}
        actual_pside_flat = {
            pside: not any(
                size > 1e-12
                for (sym, ps), (size, _price) in current_position_state.items()
                if ps == pside
            )
            for pside in ("long", "short")
        }
        last_event_ts_by_pside = {
            pside: max((evt["timestamp"] for evt in events if evt["pside"] == pside), default=None)
            for pside in ("long", "short")
        }

        def _pside_is_flat(pside: str) -> bool:
            return not any(
                positions.get(sym, {}).get(pside, {}).get("size", 0.0) > 1e-12 for sym in positions
            )

        def _apply_event(evt: dict):
            """将单个事件应用到仓位追踪状态。"""
            slot = _ensure_slot(positions, evt["symbol"])[evt["pside"]]
            qty = evt["qty"]
            price = evt["price"]
            if evt["action"] == "increase":
                old_size = slot["size"]
                new_size = old_size + qty
                if new_size <= 0.0:
                    slot["size"], slot["price"] = 0.0, 0.0
                elif old_size <= 0.0:
                    slot["size"], slot["price"] = new_size, price
                else:
                    slot["price"] = max(
                        (old_size * slot["price"] + qty * price) / new_size,
                        0.0,
                    )
                    slot["size"] = new_size
            else:
                slot["size"] = max(slot["size"] - qty, 0.0)
                if slot["size"] <= 0.0:
                    slot["price"] = 0.0
            has_pos = slot["size"] > 1e-12
            if has_pos:
                active_symbols.add(evt["symbol"])
            elif not any(
                positions[evt["symbol"]][ps]["size"] > 1e-12 for ps in ("long", "short")
            ):
                active_symbols.discard(evt["symbol"])

        balance = baseline_balance
        event_idx = 0
        last_price: Dict[str, float] = {}

        minute = start_minute
        while minute <= end_minute:
            boundary = minute + ONE_MIN_MS
            panic_fill_count = 0
            while event_idx < len(events) and events[event_idx]["timestamp"] < boundary:
                evt = events[event_idx]
                _apply_event(evt)
                realized_delta = evt["pnl"] + evt.get("fee", 0.0)
                balance += realized_delta
                realized_pnl_pside_running[evt["pside"]] += realized_delta
                if "panic" in str(evt.get("pb_order_type") or ""):
                    panic_fill_count += 1
                    after_psize = _safe_float(evt.get("psize"), math.nan)
                    authoritative_flat_override = (
                        actual_pside_flat.get(evt["pside"], False)
                        and last_event_ts_by_pside.get(evt["pside"]) == evt["timestamp"]
                    )
                    if authoritative_flat_override and (
                        not math.isfinite(after_psize) or after_psize > 1e-12
                    ):
                        logging.warning(
                            "[risk] balance-equity replay trusting current flat %s state over residual panic replay size | timestamp=%s replay_after_psize=%s symbol=%s",
                            evt["pside"],
                            evt["timestamp"],
                            f"{after_psize:.12f}" if math.isfinite(after_psize) else "nan",
                            evt["symbol"],
                        )
                    if (
                        (math.isfinite(after_psize) and after_psize <= 1e-12)
                        or authoritative_flat_override
                        or _pside_is_flat(evt["pside"])
                    ):
                        panic_flatten_events.append(
                            {
                                "timestamp": int(evt["timestamp"]),
                                "minute_timestamp": int(minute),
                                "pside": str(evt["pside"]),
                                "symbol": str(evt["symbol"]),
                            }
                        )
                event_idx += 1
            upnl = 0.0
            upnl_by_pside = {"long": 0.0, "short": 0.0}
            for symbol in list(active_symbols):
                price = price_lookup.get(symbol, {}).get(minute)
                if price is None:
                    price = last_price.get(symbol)
                else:
                    last_price[symbol] = price
                if price is None or price <= 0.0:
                    missing_price_symbols.add(symbol)
                    continue
                slot = positions.get(symbol)
                if not slot:
                    continue
                for pside in ("long", "short"):
                    size = slot[pside]["size"]
                    if size <= 0.0:
                        continue
                    avg_price = slot[pside]["price"]
                    if avg_price <= 0.0:
                        continue
                    c_mult = self.c_mults.get(symbol, 1.0)
                    pside_upnl = calc_pnl(pside, avg_price, price, size, self.inverse, c_mult)
                    upnl += pside_upnl
                    upnl_by_pside[pside] += pside_upnl
            if minute >= record_start_minute:
                timeline.append(
                    {
                        "timestamp": minute,
                        "balance": balance,
                        "equity": balance + upnl,
                        "unrealized_pnl": upnl,
                        "realized_pnl": balance - baseline_balance,
                        "unrealized_pnl_long": upnl_by_pside["long"],
                        "unrealized_pnl_short": upnl_by_pside["short"],
                        "realized_pnl_long": realized_pnl_pside_running["long"],
                        "realized_pnl_short": realized_pnl_pside_running["short"],
                        "is_flat": len(active_symbols) == 0,
                        "is_flat_long": not any(
                            positions.get(sym, {}).get("long", {}).get("size", 0.0) > 1e-12
                            for sym in positions
                        ),
                        "is_flat_short": not any(
                            positions.get(sym, {}).get("short", {}).get("size", 0.0) > 1e-12
                            for sym in positions
                        ),
                        "panic_fill_count": int(panic_fill_count),
                    }
                )
            minute += ONE_MIN_MS

        if not timeline:
            point = {
                "timestamp": ts_now,
                "balance": balance_now,
                "equity": balance_now,
                "unrealized_pnl": 0.0,
                "realized_pnl": 0.0,
                "unrealized_pnl_long": 0.0,
                "unrealized_pnl_short": 0.0,
                "realized_pnl_long": 0.0,
                "realized_pnl_short": 0.0,
                "is_flat": True,
                "is_flat_long": True,
                "is_flat_short": True,
            }
            timeline = [point]

        balances = [{"timestamp": row["timestamp"], "balance": row["balance"]} for row in timeline]
        equities = [
            {
                "timestamp": row["timestamp"],
                "equity": row["equity"],
                "unrealized_pnl": row["unrealized_pnl"],
            }
            for row in timeline
        ]
        metadata = {
            "lookback_days": lookback.display_value,
            "resolution_ms": ONE_MIN_MS,
            "events_used": len(events),
            "symbols_covered": sorted(symbols),
            "missing_price_symbols": sorted(missing_price_symbols),
            "approximate_price_sources": approximate_price_sources,
        }
        return {
            "timeline": timeline,
            "panic_flatten_events": panic_flatten_events,
            "fill_events": events,
            "balances": balances,
            "equities": equities,
            "metadata": metadata,
        }

    async def update_open_orders(self):
        """从交易所刷新挂单并协调本地缓存。"""
        if not hasattr(self, "open_orders"):
            self.open_orders = {}
        if self.stop_signal_received:
            return False
        res = None
        try:
            res = await self.fetch_open_orders()
            if res in [None, False]:
                return False
            self.fetched_open_orders = res
            open_orders = res
            oo_ids_old = {elm["id"] for sublist in self.open_orders.values() for elm in sublist}
            oo_ids_new = {elm["id"] for elm in open_orders}
            added_orders = [oo for oo in open_orders if oo["id"] not in oo_ids_old]
            removed_orders = [
                oo
                for oo in [elm for sublist in self.open_orders.values() for elm in sublist]
                if oo["id"] not in oo_ids_new
            ]
            schedule_update_positions = False
            if len(removed_orders) > 20:
                logging.info(f"removed {len(removed_orders)} orders")
            else:
                for order in removed_orders:
                    if not self.order_was_recently_cancelled(order):
                        # 订单不再在挂单中，但不是被机器人取消的
                        # 可能有成交
                        # 强制再次更新仓位
                        schedule_update_positions = True
                        self.log_order_action(
                            order, "missing order", "fetch_open_orders", level=logging.INFO
                        )
                    else:
                        self.log_order_action(
                            order, "removed order", "fetch_open_orders", level=logging.DEBUG
                        )
            if len(added_orders) > 20:
                logging.info(f"[order] added {len(added_orders)} new orders")
            else:
                for order in added_orders:
                    self.log_order_action(
                        order, "added order", "fetch_open_orders", level=logging.DEBUG
                    )
            self.open_orders = {}
            for elm in open_orders:
                if elm["symbol"] not in self.open_orders:
                    self.open_orders[elm["symbol"]] = []
                self.open_orders[elm["symbol"]].append(elm)
            await self._detect_foreign_passivbot_orders(open_orders)
            balance_reconciled = self._reconcile_balance_after_open_orders_refresh()
            if balance_reconciled:
                await self.handle_balance_update(source="REST+open_orders")
            if schedule_update_positions:
                await asyncio.sleep(1.5)
                await self.update_positions_and_balance()
            return True
        except RateLimitExceeded:
            self._health_rate_limits += 1
            logging.warning("[rate] hit rate limit while fetching open orders; retrying next cycle")
            return False
        except Exception as e:
            logging.error(f"error with {get_function_name()} {e}")
            print_async_exception(res)
            traceback.print_exc()
            raise

    def get_exchange_time(self):
        """返回当前交易所时间（毫秒）。"""
        return utc_ms()

    async def log_position_changes(self, positions_old, positions_new, rd=6):
        """检测到差异时记录仓位变更用于调试。"""
        psold = {
            (x["symbol"], x["position_side"]): {k: x[k] for k in ["size", "price"]}
            for x in positions_old
        }
        psnew = {
            (x["symbol"], x["position_side"]): {k: x[k] for k in ["size", "price"]}
            for x in positions_new
        }

        if psold == psnew:
            return  # 无变更

        # 确保两个字典包含所有键
        for k in psnew:
            if k not in psold:
                psold[k] = {"size": 0.0, "price": 0.0}
        for k in psold:
            if k not in psnew:
                psnew[k] = {"size": 0.0, "price": 0.0}

        changed = []
        for k in psnew:
            if psold[k] != psnew[k]:
                changed.append(k)

        if not changed:
            return

        # 预计算每方向总 WE 用于 TWEL% 显示
        total_we_by_pside = {"long": 0.0, "short": 0.0}
        balance_raw = self.get_raw_balance()
        for pos in positions_new:
            sym = pos["symbol"]
            ps = pos["position_side"]
            sz = pos.get("size", 0.0)
            px = pos.get("price", 0.0)
            if sz != 0 and balance_raw > 0 and sym in self.c_mults:
                total_we_by_pside[ps] += pbr.qty_to_cost(sz, px, self.c_mults[sym]) / balance_raw

        # 创建 PrettyTable 用于对齐输出
        table = PrettyTable()
        table.border = False
        table.header = False
        table.padding_width = 0

        for symbol, pside in changed:
            old = psold[(symbol, pside)]
            new = psnew[(symbol, pside)]

            # 分类操作 ------------------------------------------------
            if old["size"] == 0.0 and new["size"] != 0.0:
                action = "    new"
            elif new["size"] == 0.0:
                action = " closed"
            elif new["size"] > old["size"]:
                action = "  added"
            elif new["size"] < old["size"]:
                action = "reduced"
            else:
                action = "unknown"

            # 计算新仓位的指标
            wallet_exposure = (
                pbr.qty_to_cost(new["size"], new["price"], self.c_mults[symbol]) / balance_raw
                if new["size"] != 0 and balance_raw > 0
                else 0.0
            )
            wel = float(self.bp(pside, "wallet_exposure_limit", symbol))
            allowance_pct = float(self.bp(pside, "risk_we_excess_allowance_pct", symbol))
            effective_wel = wel * (1.0 + max(0.0, allowance_pct))
            # WEL% = 相对于基础 WEL 的比率，WELe% = 相对于有效 WEL（含超额额度）的比率
            WEL_ratio = wallet_exposure / wel if wel > 0.0 else 0.0
            WELe_ratio = wallet_exposure / effective_wel if effective_wel > 0.0 else 0.0

            last_price = await self.cm.get_current_close(symbol, max_age_ms=60_000)
            try:
                pprice_diff = (
                    pbr.calc_pprice_diff_int(self.pside_int_map[pside], new["price"], last_price)
                    if last_price
                    else 0.0
                )
            except:
                pprice_diff = 0.0

            try:
                upnl = (
                    calc_pnl(
                        pside,
                        new["price"],
                        last_price,
                        new["size"],
                        self.inverse,
                        self.c_mults[symbol],
                    )
                    if last_price
                    else 0.0
                )
            except:
                upnl = 0.0

            coin = symbol_to_coin(symbol, verbose=False) or symbol
            # 格式化 WEL 百分比并填充以便对齐
            wel_pct = round(WEL_ratio * 100)
            wele_pct = round(WELe_ratio * 100)
            # TWEL% = 该方向总 WE / TWEL
            twel = float(self.bot_value(pside, "total_wallet_exposure_limit") or 0.0)
            twel_pct = round(total_we_by_pside[pside] / twel * 100) if twel > 0.0 else 0
            wel_str = f"| {wel_pct:3d}% WEL, {wele_pct:3d}% WELe, {twel_pct:3d}% TWEL |"
            table.add_row(
                [
                    action + " ",
                    coin + " ",
                    pside + " ",
                    round_dynamic(old["size"], rd),
                    " @ ",
                    round_dynamic(old["price"], rd),
                    " -> ",
                    round_dynamic(new["size"], rd),
                    " @ ",
                    round_dynamic(new["price"], rd),
                    " WE: ",
                    pbr.round_dynamic(wallet_exposure, 3),
                    " ",
                    wel_str,
                    " PA dist: ",
                    round(pprice_diff, 4),
                    " upnl: ",
                    pbr.round_dynamic(upnl, 3),
                ]
            )

        # 打印带 [pos] 前缀的对齐表格
        for line in table.get_string().splitlines():
            logging.info("[pos] %s", line)

    async def _fetch_and_apply_positions(self):
        """获取原始仓位，应用到本地状态并返回快照。

        返回：
            (成功: bool, 旧仓位, 新仓位) 的元组。

        抛出：
            Exception：API 错误（调用方通过 restart_bot_on_too_many_errors 处理）。
        """
        if not hasattr(self, "positions"):
            self.positions = {}
        res = await self.fetch_positions()
        if res is None:
            return False, None, None
        positions_list_new = res
        fetched_positions_old = deepcopy(self.fetched_positions)
        self.fetched_positions = positions_list_new
        positions_new = {
            sym: {
                "long": {"size": 0.0, "price": 0.0},
                "short": {"size": 0.0, "price": 0.0},
            }
            for sym in set(list(self.positions) + list(self.active_symbols))
        }
        for elm in positions_list_new:
            symbol, pside, pprice = elm["symbol"], elm["position_side"], elm["price"]
            psize = abs(elm["size"]) * (-1.0 if elm["position_side"] == "short" else 1.0)
            if symbol not in positions_new:
                positions_new[symbol] = {
                    "long": {"size": 0.0, "price": 0.0},
                    "short": {"size": 0.0, "price": 0.0},
                }
            positions_new[symbol][pside] = {"size": psize, "price": pprice}
        self.positions = positions_new
        return True, fetched_positions_old, self.fetched_positions

    async def update_positions(self, *, log_changes: bool = True):
        """获取仓位，更新本地缓存，可选记录变更。"""
        ok, fetched_positions_old, fetched_positions_new = await self._fetch_and_apply_positions()
        if not ok:
            return False
        if log_changes and fetched_positions_old is not None:
            try:
                await self.log_position_changes(fetched_positions_old, fetched_positions_new)
            except Exception as e:
                logging.error(f"error logging position changes {e}")
        return True

    async def update_balance(self):
        """获取并应用最新的钱包余额。

        返回：
            bool：成功为 True，balance_override 无效时为 False。

        抛出：
            Exception：API 错误（调用方通过 restart_bot_on_too_many_errors 处理）。
        """
        if not hasattr(self, "balance_override"):
            self.balance_override = None
        if not hasattr(self, "_balance_override_logged"):
            self._balance_override_logged = False
        if not hasattr(self, "previous_hysteresis_balance"):
            self.previous_hysteresis_balance = None
        if not hasattr(self, "balance_hysteresis_snap_pct"):
            self.balance_hysteresis_snap_pct = 0.02
        if not hasattr(self, "balance_raw"):
            self.balance_raw = self.get_raw_balance()
        if not hasattr(self, "_exchange_reported_balance_raw"):
            self._exchange_reported_balance_raw = self.balance_raw

        if self.balance_override is not None:
            balance_raw = float(self.balance_override)
            if not self._balance_override_logged:
                logging.info("Using balance override: %.6f", balance_raw)
                self._balance_override_logged = True
        else:
            if not hasattr(self, "fetch_balance"):
                logging.debug("update_balance: no fetch_balance implemented")
                return False
            balance_raw = await self.fetch_balance()

        # 仅接受数值余额；失败时保留先前值
        if balance_raw is None:
            logging.warning("balance fetch returned None; keeping previous balance")
            return False
        try:
            balance_raw = float(balance_raw)
        except (TypeError, ValueError):
            logging.warning("non-numeric balance fetch result; keeping previous balance")
            return False
        if not math.isfinite(balance_raw):
            logging.warning("non-finite balance fetch result; keeping previous balance")
            return False

        self._exchange_reported_balance_raw = balance_raw
        balance_snapped = balance_raw
        if self.balance_override is None:
            if self.previous_hysteresis_balance is None:
                self.previous_hysteresis_balance = balance_raw
            balance_snapped = pbr.hysteresis(
                balance_raw, self.previous_hysteresis_balance, self.balance_hysteresis_snap_pct
            )
            self.previous_hysteresis_balance = balance_snapped
        self.balance_raw = balance_raw
        self.balance = balance_snapped
        return True

    def _reconcile_balance_after_open_orders_refresh(self) -> bool:
        """交易所钩子：若需要在刷新挂单后调整余额。"""
        return False

    def _reconcile_balance_after_positions_and_balance_refresh(self) -> bool:
        """交易所钩子：若需要在刷新仓位和余额后调整余额。"""
        return False

    async def update_positions_and_balance(self):
        """同时刷新仓位和余额的便捷助手。"""
        balance_task = asyncio.create_task(self.update_balance())
        positions_task = asyncio.create_task(self._fetch_and_apply_positions())
        try:
            balance_ok, positions_res = await asyncio.gather(balance_task, positions_task)
        except Exception:
            for task in (balance_task, positions_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(balance_task, positions_task, return_exceptions=True)
            raise
        positions_ok, fetched_positions_old, fetched_positions_new = positions_res
        if positions_ok and fetched_positions_old is not None:
            try:
                await self.log_position_changes(fetched_positions_old, fetched_positions_new)
            except Exception as e:
                logging.error(f"error logging position changes {e}")
        if balance_ok and positions_ok:
            self._reconcile_balance_after_positions_and_balance_refresh()
            await self.handle_balance_update(source="REST")
        return balance_ok, positions_ok

    def _calc_effective_min_cost_at_price(self, symbol: str, price: float) -> float:
        """返回给定价格处的可执行最小订单成本，用于过滤/门控逻辑。"""
        qty_step = float(self.qty_steps[symbol])
        min_qty = float(self.min_qtys[symbol])
        min_cost = float(self.min_costs[symbol])
        c_mult = float(self.c_mults[symbol])
        if min_qty <= 0.0 and qty_step > 0.0:
            min_qty = qty_step
        calc_min_entry_qty = getattr(pbr, "calc_min_entry_qty_py", None)
        if calc_min_entry_qty is not None:
            min_entry_qty = float(calc_min_entry_qty(price, c_mult, qty_step, min_qty, min_cost))
        else:
            if price <= 0.0 or c_mult <= 0.0:
                min_entry_qty = min_qty
            else:
                min_cost_qty = min_cost / price / c_mult
                if qty_step > 0.0:
                    min_cost_qty = math.ceil(max(0.0, min_cost_qty) / qty_step) * qty_step
                min_entry_qty = max(min_qty, min_cost_qty)
        return float(pbr.qty_to_cost(min_entry_qty, price, c_mult))

    async def update_effective_min_cost(self, symbol=None):
        """更新一个或所有交易对的有效最小订单成本。"""
        if not hasattr(self, "effective_min_cost"):
            self.effective_min_cost = {}
        if symbol is None:
            symbols = sorted(self.get_symbols_approved_or_has_pos())
        else:
            symbols = [symbol]
        last_prices = await self.cm.get_last_prices(symbols, max_age_ms=600_000)
        for symbol in symbols:
            try:
                self.effective_min_cost[symbol] = self._calc_effective_min_cost_at_price(
                    symbol, float(last_prices[symbol])
                )
            except Exception as e:
                logging.error(f"error with {get_function_name()} for {symbol}: {e}")
                traceback.print_exc()

    async def calc_ideal_orders(self):
        """计算每个活跃交易对的期望入场和出场订单。"""
        return await self.calc_ideal_orders_orchestrator()

    def _bot_params_to_rust_dict(self, pside: str, symbol: str | None) -> dict:
        """构建匹配 Rust `BotParams` 的字典用于 JSON 编排器输入。"""
        # 全局配置的值（非逐交易对）位于 bot_value 下。
        global_keys = {
            "n_positions",
            "total_wallet_exposure_limit",
            "risk_twel_enforcer_threshold",
            "unstuck_loss_allowance_pct",
        }
        # 与 `passivbot-rust/src/types.rs BotParams` 保持 1:1 字段覆盖。
        fields = [
            "close_grid_markup_end",
            "close_grid_markup_start",
            "close_grid_qty_pct",
            "close_trailing_retracement_pct",
            "close_trailing_grid_ratio",
            "close_trailing_qty_pct",
            "close_trailing_threshold_pct",
            "entry_grid_double_down_factor",
            "entry_grid_spacing_volatility_weight",
            "entry_grid_spacing_we_weight",
            "entry_grid_spacing_pct",
            "entry_volatility_ema_span_hours",
            "entry_initial_ema_dist",
            "entry_initial_qty_pct",
            "entry_trailing_double_down_factor",
            "entry_trailing_retracement_pct",
            "entry_trailing_retracement_we_weight",
            "entry_trailing_retracement_volatility_weight",
            "entry_trailing_grid_ratio",
            "entry_trailing_threshold_pct",
            "entry_trailing_threshold_we_weight",
            "entry_trailing_threshold_volatility_weight",
            "forager_volatility_ema_span",
            "forager_volume_ema_span",
            "forager_volume_drop_pct",
            "forager_score_weights",
            "ema_span_0",
            "ema_span_1",
            "n_positions",
            "total_wallet_exposure_limit",
            "wallet_exposure_limit",
            "risk_wel_enforcer_threshold",
            "risk_twel_enforcer_threshold",
            "risk_we_excess_allowance_pct",
            "unstuck_close_pct",
            "unstuck_ema_dist",
            "unstuck_loss_allowance_pct",
            "unstuck_threshold",
        ]
        out: dict[str, object] = {}
        for key in fields:
            if key in global_keys:
                val = self.bot_value(pside, key)
            else:
                val = self.bp(pside, key, symbol) if symbol is not None else self.bp(pside, key)
            out_key = key
            if key == "forager_volatility_ema_span":
                out_key = "filter_volatility_ema_span"
            elif key == "forager_volume_ema_span":
                out_key = "filter_volume_ema_span"
            if key == "forager_score_weights":
                if not isinstance(val, dict):
                    raise TypeError(
                        f"bot.{pside}.forager_score_weights must be a dict, got {type(val).__name__}"
                    )
                out[out_key] = {
                    "volume": float(val["volume"]),
                    "ema_readiness": float(val["ema_readiness"]),
                    "volatility": float(val["volatility"]),
                }
            elif key == "n_positions":
                out[out_key] = int(round(val or 0.0))
            else:
                out[out_key] = float(val or 0.0)
        out.update(
            {
                "hsl_enabled": bool(self.bot_value(pside, "hsl_enabled")),
                "hsl_red_threshold": float(self.bot_value(pside, "hsl_red_threshold")),
                "hsl_ema_span_minutes": float(self.bot_value(pside, "hsl_ema_span_minutes")),
                "hsl_cooldown_minutes_after_red": float(
                    self.bot_value(pside, "hsl_cooldown_minutes_after_red")
                ),
                "hsl_no_restart_drawdown_threshold": float(
                    self.bot_value(pside, "hsl_no_restart_drawdown_threshold")
                ),
                "hsl_tier_ratio_yellow": float(self.bot_value(pside, "hsl_tier_ratios.yellow")),
                "hsl_tier_ratio_orange": float(self.bot_value(pside, "hsl_tier_ratios.orange")),
                "hsl_orange_tier_mode": str(self.bot_value(pside, "hsl_orange_tier_mode")),
                "hsl_panic_close_order_type": str(
                    self.bot_value(pside, "hsl_panic_close_order_type")
                ),
            }
        )
        return out

    def _pb_mode_to_orchestrator_mode(self, mode: str) -> str:
        m = (mode or "").strip().lower()
        if m == "tp_only_with_active_entry_cancellation":
            return "tp_only"
        if m in {"normal", "panic", "graceful_stop", "tp_only", "manual"}:
            return m
        return "manual"

    def _pside_blocks_new_entries(self, pside: str) -> bool:
        forced_mode = self.get_forced_PB_mode(pside)
        return forced_mode in {
            "panic",
            "graceful_stop",
            "tp_only",
            "tp_only_with_active_entry_cancellation",
            "manual",
        }

    def _build_live_symbol_universe(self) -> list[str]:
        symbols: set[str] = set()
        symbols |= set(getattr(self, "positions", {}))
        symbols |= set(getattr(self, "open_orders", {}))
        symbols |= set(getattr(self, "coin_overrides", {}))
        for pside in ("long", "short"):
            if self._pside_blocks_new_entries(pside):
                continue
            approved = self.approved_coins_minus_ignored_coins.get(pside, set())
            for symbol in approved:
                if self.is_approved(pside, symbol):
                    symbols.add(symbol)
        return sorted(symbols)

    def _mode_override_to_orchestrator_mode(self, mode: Optional[str]) -> Optional[str]:
        if mode is None:
            return None
        m = str(mode).strip().lower()
        if m == "tp_only_with_active_entry_cancellation":
            return "tp_only"
        if m in {"normal", "panic", "graceful_stop", "tp_only", "manual"}:
            return m
        return "manual"

    def _python_mode_from_orchestrator_state(
        self,
        pside: str,
        symbol: str,
        side_state: dict,
        explicit_override: Optional[str],
    ) -> str:
        if explicit_override:
            return str(explicit_override)
        if bool(side_state.get("active", False)):
            return "normal"
        return self.PB_mode_stop[pside]

    def _apply_orchestrator_symbol_states(
        self,
        diagnostics: dict,
        idx_to_symbol: dict[int, str],
        explicit_overrides: dict[str, dict[str, Optional[str]]],
    ) -> None:
        """将编排器返回的交易对状态应用到 PB_modes 和模式覆盖配置。"""
        previous_PB_modes = deepcopy(self.PB_modes) if hasattr(self, "PB_modes") else None
        symbol_states = diagnostics.get("symbol_states", []) if isinstance(diagnostics, dict) else []
        if not symbol_states:
            return
        pb_modes = {"long": {}, "short": {}}
        for row in symbol_states:
            if not isinstance(row, dict):
                continue
            symbol = idx_to_symbol.get(int(row.get("symbol_idx", -1)))
            if symbol is None:
                continue
            for pside in ("long", "short"):
                side_state = row.get(pside, {})
                explicit_override = explicit_overrides.get(pside, {}).get(symbol)
                pb_modes[pside][symbol] = self._python_mode_from_orchestrator_state(
                    pside,
                    symbol,
                    side_state if isinstance(side_state, dict) else {},
                    explicit_override,
                )

        for symbol in set(self.positions) | set(self.open_orders) | set(pb_modes["long"]) | set(pb_modes["short"]):
            for pside in ("long", "short"):
                if symbol not in pb_modes[pside]:
                    explicit_override = explicit_overrides.get(pside, {}).get(symbol)
                    if explicit_override:
                        pb_modes[pside][symbol] = str(explicit_override)
                    else:
                        pb_modes[pside][symbol] = self.PB_mode_stop[pside]

        self.PB_modes = pb_modes
        self.active_symbols = sorted(set(pb_modes["long"]) | set(pb_modes["short"]) | set(self.open_orders))
        res = log_dict_changes(previous_PB_modes, self.PB_modes)
        self._log_mode_changes(res, previous_PB_modes)

    def _orchestrator_mode_override(self, pside: str, symbol: str) -> Optional[str]:
        """获取编排器对指定交易对的模式覆盖设置。"""
        if self._equity_hard_stop_enabled(pside):
            state = self._hsl_state(pside)
            if self._equity_hard_stop_runtime_red_latched(pside) and not state["halted"]:
                return "panic"
            if state["halted"]:
                return self._equity_hard_stop_halted_mode(pside, symbol)
            if self._equity_hard_stop_runtime_tier(pside) == "orange":
                orange_mode = str(self.hsl[pside]["orange_tier_mode"])
                if orange_mode == "graceful_stop":
                    return "graceful_stop"
                if orange_mode == "tp_only_with_active_entry_cancellation":
                    size = float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0) or 0.0)
                    if size != 0.0:
                        return "tp_only_with_active_entry_cancellation"

        runtime_forced = getattr(self, "_runtime_forced_modes", {}).get(pside, {}).get(symbol)
        if runtime_forced:
            return str(runtime_forced)

        forced_mode = self.config_get(["live", f"forced_mode_{pside}"], symbol)
        if forced_mode:
            return expand_PB_mode(forced_mode)
        if not self.markets_dict.get(symbol, {}).get("active", True):
            return "tp_only"
        ineligible_reason = getattr(self, "ineligible_symbols", {}).get(symbol)
        if ineligible_reason is not None:
            return "tp_only" if ineligible_reason == "not active" else "manual"
        return None

    def _build_orchestrator_mode_overrides(
        self, symbols: Iterable[str]
    ) -> dict[str, dict[str, Optional[str]]]:
        overrides: dict[str, dict[str, Optional[str]]] = {"long": {}, "short": {}}
        for pside in ("long", "short"):
            for symbol in symbols:
                overrides[pside][symbol] = self._orchestrator_mode_override(pside, symbol)
        return overrides

    def _build_orchestrator_mode_overrides_fallback(
        self, symbols: Iterable[str]
    ) -> dict[str, dict[str, Optional[str]]]:
        overrides: dict[str, dict[str, Optional[str]]] = {"long": {}, "short": {}}
        pb_modes = getattr(self, "PB_modes", {})
        for pside in ("long", "short"):
            pside_modes = pb_modes.get(pside, {}) if isinstance(pb_modes, dict) else {}
            for symbol in symbols:
                mode = pside_modes.get(symbol)
                overrides[pside][symbol] = (
                    Passivbot._pb_mode_to_orchestrator_mode(self, mode) if mode else None
                )
        return overrides

    def _calc_unstuck_allowances_live(self, allow_new_unstuck: bool) -> dict[str, float]:
        """使用 FillEventsManager 计算 unstuck 限额。"""
        return self._calc_unstuck_allowances(allow_new_unstuck)

    async def calc_ideal_orders_orchestrator_from_snapshot(
        self, snapshot: dict, *, return_snapshot: bool
    ):
        """从快照数据计算编排器理想订单，包括仓位同步和模式覆盖应用。"""
        symbols = snapshot["symbols"]
        last_prices = snapshot["last_prices"]
        Passivbot._monitor_record_price_ticks(self, last_prices, ts=utc_ms(), source="orchestrator_snapshot")
        m1_close_emas = snapshot["m1_close_emas"]
        m1_volume_emas = snapshot["m1_volume_emas"]
        m1_log_range_emas = snapshot["m1_log_range_emas"]
        h1_log_range_emas = snapshot["h1_log_range_emas"]

        unstuck_allowances = snapshot.get("unstuck_allowances", {"long": 0.0, "short": 0.0})
        realized_pnl_cumsum = snapshot.get("realized_pnl_cumsum", {"max": 0.0, "last": 0.0})
        max_realized_loss_pct = float(self.live_value("max_realized_loss_pct") or 1.0)
        if hasattr(self, "_build_orchestrator_mode_overrides"):
            mode_overrides = self._build_orchestrator_mode_overrides(symbols)
        else:
            mode_overrides = Passivbot._build_orchestrator_mode_overrides_fallback(self, symbols)

        global_bp = {
            "long": self._bot_params_to_rust_dict("long", None),
            "short": self._bot_params_to_rust_dict("short", None),
        }
        # 有效 hedge_mode = 配置设置 AND 交易所能力。
        # 若任一为 False，则在编排器中阻止同币种对冲。
        effective_hedge_mode = self._config_hedge_mode and self.hedge_mode
        input_dict = {
            "balance": self.get_hysteresis_snapped_balance(),
            "balance_raw": self.get_raw_balance(),
            "global": {
                "filter_by_min_effective_cost": bool(self.live_value("filter_by_min_effective_cost")),
                "market_orders_allowed": bool(self.live_value("market_orders_allowed")),
                "market_order_near_touch_threshold": float(
                    self.live_value("market_order_near_touch_threshold")
                ),
                "panic_close_market": bool(
                    any(
                        Passivbot._equity_hard_stop_panic_close_order_type(self, pside) == "market"
                        for pside in ("long", "short")
                        if Passivbot._equity_hard_stop_enabled(self, pside)
                    )
                ),
                "unstuck_allowance_long": float(unstuck_allowances.get("long", 0.0)),
                "unstuck_allowance_short": float(unstuck_allowances.get("short", 0.0)),
                "max_realized_loss_pct": max_realized_loss_pct,
                "realized_pnl_cumsum_max": float(realized_pnl_cumsum.get("max", 0.0) or 0.0),
                "realized_pnl_cumsum_last": float(realized_pnl_cumsum.get("last", 0.0) or 0.0),
                "sort_global": True,
                "global_bot_params": global_bp,
                "hedge_mode": effective_hedge_mode,
            },
            "symbols": [],
            "peek_hints": None,
        }

        symbol_to_idx: dict[str, int] = {s: i for i, s in enumerate(symbols)}
        idx_to_symbol: dict[int, str] = {i: s for s, i in symbol_to_idx.items()}

        for symbol in symbols:
            idx = symbol_to_idx[symbol]
            mprice = float(last_prices.get(symbol, 0.0))
            if not math.isfinite(mprice) or mprice <= 0.0:
                raise Exception(f"invalid market price for {symbol}: {mprice}")

            active = bool(self.markets_dict.get(symbol, {}).get("active", True))
            effective_min_cost = float(
                getattr(self, "effective_min_cost", {}).get(symbol, 0.0) or 0.0
            )
            if effective_min_cost <= 0.0:
                effective_min_cost = self._calc_effective_min_cost_at_price(symbol, mprice)

            def side_input(pside: str) -> dict:
                """构建指定方向和交易对的编排器输入数据。"""
                mode = Passivbot._mode_override_to_orchestrator_mode(
                    self, mode_overrides[pside].get(symbol)
                )
                pos = self.positions.get(symbol, {}).get(pside, {"size": 0.0, "price": 0.0})
                trailing = self.trailing_prices.get(symbol, {}).get(pside)
                if not trailing:
                    trailing = _trailing_bundle_default_dict()
                else:
                    trailing = dict(trailing)
                return {
                    "mode": mode,
                    "position": {"size": float(pos["size"]), "price": float(pos["price"])},
                    "trailing": {
                        "min_since_open": float(trailing.get("min_since_open", 0.0)),
                        "max_since_min": float(trailing.get("max_since_min", 0.0)),
                        "max_since_open": float(trailing.get("max_since_open", 0.0)),
                        "min_since_max": float(trailing.get("min_since_max", 0.0)),
                    },
                    "bot_params": self._bot_params_to_rust_dict(pside, symbol),
                }

            m1_close_pairs = [[float(k), float(v)] for k, v in sorted(m1_close_emas[symbol].items())]
            m1_volume_pairs = [
                [float(k), float(v)] for k, v in sorted(m1_volume_emas[symbol].items())
            ]
            m1_lr_pairs = [[float(k), float(v)] for k, v in sorted(m1_log_range_emas[symbol].items())]
            h1_lr_pairs = [[float(k), float(v)] for k, v in sorted(h1_log_range_emas[symbol].items())]

            input_dict["symbols"].append(
                {
                    "symbol_idx": int(idx),
                    "order_book": {"bid": mprice, "ask": mprice},
                    "exchange": {
                        "qty_step": float(self.qty_steps[symbol]),
                        "price_step": float(self.price_steps[symbol]),
                        "min_qty": float(self.min_qtys[symbol]),
                        "min_cost": float(self.min_costs[symbol]),
                        "c_mult": float(self.c_mults[symbol]),
                        "maker_fee": float(
                            self.markets_dict.get(symbol, {}).get("maker", 0.0) or 0.0
                        ),
                        "taker_fee": float(
                            self.markets_dict.get(symbol, {}).get("taker", 0.0) or 0.0
                        ),
                    },
                    "tradable": bool(active),
                    "next_candle": None,
                    "effective_min_cost": float(effective_min_cost),
                    "emas": {
                        "m1": {
                            "close": m1_close_pairs,
                            "log_range": m1_lr_pairs,
                            "volume": m1_volume_pairs,
                        },
                        "h1": {"close": [], "log_range": h1_lr_pairs, "volume": []},
                    },
                    "long": side_input("long"),
                    "short": side_input("short"),
                }
            )

        try:
            out_json = pbr.compute_ideal_orders_json(json.dumps(input_dict))
        except Exception as e:
            msg = str(e)
            if "MissingEma" in msg:
                match = re.search(r"symbol_idx\s*:\s*(\d+)", msg)
                if match:
                    idx = int(match.group(1))
                    symbol = idx_to_symbol.get(idx)
                    if symbol:
                        logging.error("[ema] Missing EMA for %s (symbol_idx=%d)", symbol, idx)
            raise
        out = json.loads(out_json)
        self._log_realized_loss_gate_blocks(out, idx_to_symbol)
        if hasattr(self, "_apply_orchestrator_symbol_states"):
            self._apply_orchestrator_symbol_states(
                out.get("diagnostics", {}),
                idx_to_symbol,
                mode_overrides,
            )
        orders = out.get("orders", [])

        ideal_orders: dict[str, list] = {}
        for o in orders:
            symbol = idx_to_symbol.get(int(o["symbol_idx"]))
            if symbol is None:
                continue
            order_type = str(o["order_type"])
            order_type_id = int(pbr.order_type_snake_to_id(order_type))
            execution_type = str(o.get("execution_type", "limit"))
            tup = (float(o["qty"]), float(o["price"]), order_type, order_type_id, execution_type)
            ideal_orders.setdefault(symbol, []).append(tup)

        # 记录解套币种选择
        for o in orders:
            order_type_str = o.get("order_type", "")
            if "close_unstuck" in order_type_str:
                symbol = idx_to_symbol.get(int(o.get("symbol_idx", -1)))
                if symbol:
                    pside = "long" if "long" in order_type_str else "short"
                    pos = self.positions.get(symbol, {}).get(pside, {})
                    entry_price = pos.get("price", 0.0)
                    current_price = last_prices.get(symbol, 0.0)
                    if entry_price > 0 and current_price > 0:
                        price_diff_pct = (current_price / entry_price - 1.0) * 100
                        sign = "+" if price_diff_pct >= 0 else ""
                    else:
                        price_diff_pct = 0.0
                        sign = ""
                    coin = symbol.split("/")[0] if "/" in symbol else symbol
                    allowance = unstuck_allowances.get(pside, 0.0)
                    logging.info(
                        "[unstuck] selecting %s %s | entry=%.2f now=%.2f (%s%.1f%%) | allowance=%.2f",
                        coin,
                        pside,
                        entry_price,
                        current_price,
                        sign,
                        price_diff_pct,
                        allowance,
                    )
                break  # 每个周期仅一个解套订单

        # 记录正常模式无持仓且无初始入场交易对的 EMA 门控
        self._log_ema_gating(ideal_orders, m1_close_emas, last_prices, symbols)

        ideal_orders_f, _wel_blocked = self._to_executable_orders(ideal_orders, last_prices)
        ideal_orders_f = self._finalize_reduce_only_orders(ideal_orders_f, last_prices)

        if return_snapshot:
            snapshot_out = {
                "ts_ms": int(utc_ms()),
                "exchange": str(getattr(self, "exchange", "")),
                "user": str(self.config_get(["live", "user"]) or ""),
                "active_symbols": list(symbols),
                "orchestrator_input": input_dict,
                "orchestrator_output": out,
            }
            return ideal_orders_f, snapshot_out
        return ideal_orders_f, None

    async def _load_orchestrator_ema_bundle(
        self, symbols: list[str], modes: dict[str, dict[str, str]]
    ) -> tuple[
        dict[str, dict[float, float]],
        dict[str, dict[float, float]],
        dict[str, dict[float, float]],
        dict[str, dict[float, float]],
        dict[str, float],
        dict[str, float],
    ]:
        """获取 Rust 编排器所需的指定交易对的 EMA 值。

        返回：
        - m1_close_emas[交易对][span] = ema_close
        - m1_volume_emas[交易对][span] = ema_quote_volume
        - m1_log_range_emas[交易对][span] = ema_log_range (1m)
        - h1_log_range_emas[交易对][span] = ema_log_range (1h)
        - volumes_long[交易对], log_ranges_long[交易对]（便利值）
        """
        # 收集实盘交易对全集的 EMA 上下文。
        # Python 提供市场状态数据包；Rust 决定使用哪些分支。
        need_close_spans: dict[str, set[float]] = {s: set() for s in symbols}
        need_h1_lr_spans: dict[str, set[float]] = {s: set() for s in symbols}

        for pside in ["long", "short"]:
            for symbol in symbols:
                span0 = float(self.bp(pside, "ema_span_0", symbol))
                span1 = float(self.bp(pside, "ema_span_1", symbol))
                span2 = float((span0 * span1) ** 0.5) if span0 > 0.0 and span1 > 0.0 else 0.0
                for sp in (span0, span1, span2):
                    if sp > 0.0 and math.isfinite(sp):
                        need_close_spans[symbol].add(sp)
                h1_span = float(self.bp(pside, "entry_volatility_ema_span_hours", symbol) or 0.0)
                if h1_span > 0.0 and math.isfinite(h1_span):
                    need_h1_lr_spans[symbol].add(h1_span)

        # Forager 指标使用全局 span（按方向）；为所有交易对包含它们。
        vol_span_long = float(self.bot_value("long", "forager_volume_ema_span") or 0.0)
        lr_span_long = float(self.bot_value("long", "forager_volatility_ema_span") or 0.0)
        vol_span_short = float(self.bot_value("short", "forager_volume_ema_span") or 0.0)
        lr_span_short = float(self.bot_value("short", "forager_volatility_ema_span") or 0.0)
        m1_volume_spans = sorted(
            {s for s in (vol_span_long, vol_span_short) if s > 0.0 and math.isfinite(s)}
        )
        m1_lr_spans = sorted(
            {s for s in (lr_span_long, lr_span_short) if s > 0.0 and math.isfinite(s)}
        )
        if not hasattr(self, "_orchestrator_prev_close_ema"):
            self._orchestrator_prev_close_ema = {}
        if not hasattr(self, "_orchestrator_close_ema_fallback_counts"):
            self._orchestrator_close_ema_fallback_counts = {}

        async def fetch_map(symbol: str, spans: list[float], fn, ema_type: str):
            """获取指定交易对多种跨度 EMA 指标的映射。"""
            out: dict[float, float] = {}
            if not spans:
                return out
            for sp in spans:
                span = float(sp)
                try:
                    val = float(await fn(symbol, span))
                except Exception as e:
                    logging.warning(
                        "[ema] dropping %s span for %s span=%.8g reason=%s: %s",
                        ema_type,
                        symbol,
                        span,
                        type(e).__name__,
                        e,
                    )
                    continue
                if math.isfinite(val):
                    out[span] = val
                else:
                    logging.warning(
                        "[ema] dropping %s span for %s span=%.8g reason=non-finite value %s",
                        ema_type,
                        symbol,
                        span,
                        val,
                    )
            return out

        async def fetch_required_map(symbol: str, spans: list[float], fn, ema_type: str):
            """获取必需跨度 EMA 指标的映射（省略可选指标）。"""
            out: dict[float, float] = {}
            if not spans:
                return out
            missing: list[tuple[float, str]] = []
            for sp in spans:
                span = float(sp)
                try:
                    val = float(await fn(symbol, span))
                except Exception as e:
                    reason = f"{type(e).__name__}: {e}"
                else:
                    if math.isfinite(val):
                        out[span] = val
                        continue
                    reason = f"non-finite {ema_type} value {val}"
                logging.warning(
                    "[ema] missing required %s span for %s span=%.8g reason=%s",
                    ema_type,
                    symbol,
                    span,
                    reason,
                )
                missing.append((span, reason))
            if missing:
                detail = "; ".join([f"span={sp:.8g} reason={why}" for sp, why in missing])
                raise RuntimeError(f"[ema] missing required {ema_type} EMA for {symbol}: {detail}")
            return out

        async def fetch_close_map(symbol: str, spans: list[float]) -> dict[float, float]:
            """获取收盘价 EMA 映射。"""
            out: dict[float, float] = {}
            if not spans:
                return out
            now_ms = int(utc_ms())
            prev_by_span = self._orchestrator_prev_close_ema.setdefault(symbol, {})
            missing: list[tuple[float, str]] = []
            for sp in spans:
                span = float(sp)
                key = (symbol, span)
                reason = None
                try:
                    val = float(await ema_close(symbol, span))
                except Exception as e:
                    reason = f"{type(e).__name__}: {e}"
                else:
                    if math.isfinite(val):
                        out[span] = val
                        prev_by_span[span] = (val, now_ms)
                        prev_fallback_count = int(
                            self._orchestrator_close_ema_fallback_counts.get(key, 0)
                        )
                        if prev_fallback_count > 0:
                            logging.info(
                                "[ema] close EMA recovered %s span=%.8g after %d fallback(s)",
                                symbol,
                                span,
                                prev_fallback_count,
                            )
                        self._orchestrator_close_ema_fallback_counts[key] = 0
                    else:
                        reason = f"non-finite close EMA value {val}"
                if reason is None:
                    continue
                prev = prev_by_span.get(span)
                if prev is not None:
                    prev_val = float(prev[0])
                    prev_ts = int(prev[1])
                    if math.isfinite(prev_val):
                        out[span] = prev_val
                        n_fallbacks = int(
                            self._orchestrator_close_ema_fallback_counts.get(key, 0)
                        ) + 1
                        self._orchestrator_close_ema_fallback_counts[key] = n_fallbacks
                        age_ms = max(0, now_ms - prev_ts)
                        logging.warning(
                            "[ema] close EMA fallback %s span=%.8g ema=%.12g age_ms=%d"
                            " n_fallbacks=%d reason=%s",
                            symbol,
                            span,
                            prev_val,
                            age_ms,
                            n_fallbacks,
                            reason,
                        )
                        continue
                missing.append((span, reason))
            if missing:
                detail = "; ".join([f"span={sp:.8g} reason={why}" for sp, why in missing])
                raise RuntimeError(
                    f"[ema] missing required close EMA for {symbol}; no previous EMA fallback available: {detail}"
                )
            return out

        async def ema_close(symbol: str, span: float) -> float:
            # 1m K线每分钟定稿一次；60s TTL 避免冗余网络抓取。
            return float(await self.cm.get_latest_ema_close(symbol, span=span, max_age_ms=60_000))

        async def ema_qv(symbol: str, span: float) -> float:
            return float(
                await self.cm.get_latest_ema_quote_volume(symbol, span=span, max_age_ms=60_000)
            )

        async def ema_lr_1m(symbol: str, span: float) -> float:
            return float(await self.cm.get_latest_ema_log_range(symbol, span=span, max_age_ms=60_000))

        async def ema_lr_1h(symbol: str, span: float) -> float:
            return float(
                await self.cm.get_latest_ema_log_range(symbol, span=span, tf="1h", max_age_ms=600_000)
            )

        async def load_symbol_bundle(sym: str):
            close = await fetch_close_map(sym, sorted(need_close_spans[sym]))
            h1 = await fetch_required_map(
                sym, sorted(need_h1_lr_spans[sym]), ema_lr_1h, "h1_log_range"
            )
            vol = await fetch_map(sym, m1_volume_spans, ema_qv, "m1_volume")
            lr1m = await fetch_map(sym, m1_lr_spans, ema_lr_1m, "m1_log_range")
            return close, vol, lr1m, h1

        # 排序：有持仓的交易对优先（它们需要 EMA 数据进行正确的订单计算），
        # 剩余交易对随机打乱以避免字母顺序饥饿。
        symbols_with_pos = [s for s in symbols if self.has_position(symbol=s)]
        symbols_without_pos = [s for s in symbols if s not in symbols_with_pos]
        random.shuffle(symbols_without_pos)
        ordered_symbols = symbols_with_pos + symbols_without_pos

        get_fetch_delay_seconds = getattr(self, "_get_fetch_delay_seconds", None)
        if callable(get_fetch_delay_seconds):
            fetch_delay_s = float(get_fetch_delay_seconds())
        elif hasattr(self, "config") or hasattr(self, "exchange"):
            fetch_delay_s = float(Passivbot._get_fetch_delay_seconds(self))
        else:
            fetch_delay_s = 0.0
        if fetch_delay_s > 0:
            # 严格的交易所在所有交易对 TTL 同时到小时边界时，
            # 受益于节奏化高代价的 1h 刷新。
            symbol_results = []
            for sym in ordered_symbols:
                try:
                    res = await load_symbol_bundle(sym)
                except Exception as e:
                    res = e
                symbol_results.append(res)
                await asyncio.sleep(fetch_delay_s)
        else:
            symbol_tasks = [asyncio.create_task(load_symbol_bundle(sym)) for sym in ordered_symbols]
            symbol_results = await asyncio.gather(*symbol_tasks, return_exceptions=True)

        m1_close_emas: dict[str, dict[float, float]] = {}
        m1_volume_emas: dict[str, dict[float, float]] = {}
        m1_log_range_emas: dict[str, dict[float, float]] = {}
        h1_log_range_emas: dict[str, dict[float, float]] = {}
        errors: list[tuple[str, Exception]] = []
        for sym, res in zip(ordered_symbols, symbol_results):
            if isinstance(res, Exception):
                errors.append((sym, res))
                continue
            close, vol, lr1m, h1 = res
            m1_close_emas[sym] = close
            m1_volume_emas[sym] = vol
            m1_log_range_emas[sym] = lr1m
            h1_log_range_emas[sym] = h1
        if errors:
            for sym, err in errors[1:]:
                logging.debug(
                    "[ema] additional symbol EMA bundle failure %s: %s: %s",
                    sym,
                    type(err).__name__,
                    err,
                )
            raise errors[0][1]

        # 便利：计算传统 forager 日志使用的单 span 值。
        volumes_long = {s: m1_volume_emas[s].get(vol_span_long, 0.0) for s in symbols}
        log_ranges_long = {s: m1_log_range_emas[s].get(lr_span_long, 0.0) for s in symbols}

        return (
            m1_close_emas,
            m1_volume_emas,
            m1_log_range_emas,
            h1_log_range_emas,
            volumes_long,
            log_ranges_long,
        )

    async def calc_ideal_orders_orchestrator(self, *, return_snapshot: bool = False):
        """使用 Rust 编排器（JSON API）计算期望订单。"""
        symbols = sorted(set(getattr(self, "active_symbols", []) or self._build_live_symbol_universe()))
        if not symbols:
            return ({}, None) if return_snapshot else {}
        mode_overrides = self._build_orchestrator_mode_overrides(symbols)
        last_prices = await self._get_orchestrator_last_prices(symbols)
        refresh_mode = str(
            ((self.config.get("live") or {}).get("authoritative_refresh_mode")) or "legacy"
        )
        monitor_source = (
            "orchestrator_live_cm_staged"
            if refresh_mode == "staged"
            else "orchestrator_live"
        )
        Passivbot._monitor_record_price_ticks(self, last_prices, ts=utc_ms(), source=monitor_source)

        # 确保有效最小成本是最新的。
        if not hasattr(self, "effective_min_cost") or not self.effective_min_cost:
            await self.update_effective_min_cost()

        (
            m1_close_emas,
            m1_volume_emas,
            m1_log_range_emas,
            h1_log_range_emas,
            _volumes_long,
            _log_ranges_long,
        ) = await self._load_orchestrator_ema_bundle(symbols, mode_overrides)

        unstuck_allowances = self._calc_unstuck_allowances_live(
            allow_new_unstuck=not self.has_open_unstuck_order()
        )
        realized_pnl_cumsum = self._get_realized_pnl_cumsum_stats()
        max_realized_loss_pct = float(self.live_value("max_realized_loss_pct") or 1.0)

        global_bp = {
            "long": self._bot_params_to_rust_dict("long", None),
            "short": self._bot_params_to_rust_dict("short", None),
        }
        # 有效 hedge_mode = 配置设置 AND 交易所能力。
        # 若任一为 False，则在编排器中阻止同币种对冲。
        effective_hedge_mode = self._config_hedge_mode and self.hedge_mode
        input_dict = {
            "balance": self.get_hysteresis_snapped_balance(),
            "balance_raw": self.get_raw_balance(),
            "global": {
                "filter_by_min_effective_cost": bool(self.live_value("filter_by_min_effective_cost")),
                "market_orders_allowed": bool(self.live_value("market_orders_allowed")),
                "market_order_near_touch_threshold": float(
                    self.live_value("market_order_near_touch_threshold")
                ),
                "panic_close_market": bool(
                    any(
                        Passivbot._equity_hard_stop_panic_close_order_type(self, pside) == "market"
                        for pside in ("long", "short")
                        if Passivbot._equity_hard_stop_enabled(self, pside)
                    )
                ),
                "unstuck_allowance_long": float(unstuck_allowances.get("long", 0.0)),
                "unstuck_allowance_short": float(unstuck_allowances.get("short", 0.0)),
                "max_realized_loss_pct": max_realized_loss_pct,
                "realized_pnl_cumsum_max": float(realized_pnl_cumsum.get("max", 0.0) or 0.0),
                "realized_pnl_cumsum_last": float(realized_pnl_cumsum.get("last", 0.0) or 0.0),
                "sort_global": True,
                "global_bot_params": global_bp,
                "hedge_mode": effective_hedge_mode,
            },
            "symbols": [],
            "peek_hints": None,
        }

        symbol_to_idx: dict[str, int] = {s: i for i, s in enumerate(symbols)}
        idx_to_symbol: dict[int, str] = {i: s for s, i in symbol_to_idx.items()}

        for symbol in symbols:
            idx = symbol_to_idx[symbol]
            mprice = float(last_prices.get(symbol, 0.0))
            if not math.isfinite(mprice) or mprice <= 0.0:
                raise Exception(f"invalid market price for {symbol}: {mprice}")

            active = bool(self.markets_dict.get(symbol, {}).get("active", True))
            effective_min_cost = float(self.effective_min_cost.get(symbol, 0.0) or 0.0)
            if effective_min_cost <= 0.0:
                effective_min_cost = self._calc_effective_min_cost_at_price(symbol, mprice)

            def side_input(pside: str) -> dict:
                """构建指定方向的编排器输入。"""
                mode = self._mode_override_to_orchestrator_mode(mode_overrides[pside].get(symbol))
                pos = self.positions.get(symbol, {}).get(pside, {"size": 0.0, "price": 0.0})
                trailing = self.trailing_prices.get(symbol, {}).get(pside)
                if not trailing:
                    trailing = _trailing_bundle_default_dict()
                else:
                    trailing = dict(trailing)
                return {
                    "mode": mode,
                    "position": {"size": float(pos["size"]), "price": float(pos["price"])},
                    "trailing": {
                        "min_since_open": float(trailing.get("min_since_open", 0.0)),
                        "max_since_min": float(trailing.get("max_since_min", 0.0)),
                        "max_since_open": float(trailing.get("max_since_open", 0.0)),
                        "min_since_max": float(trailing.get("min_since_max", 0.0)),
                    },
                    "bot_params": self._bot_params_to_rust_dict(pside, symbol),
                }

            # 构建此交易对的 EMA 数据包。
            m1_close_pairs = [[float(k), float(v)] for k, v in sorted(m1_close_emas[symbol].items())]
            m1_volume_pairs = [
                [float(k), float(v)] for k, v in sorted(m1_volume_emas[symbol].items())
            ]
            m1_lr_pairs = [[float(k), float(v)] for k, v in sorted(m1_log_range_emas[symbol].items())]
            h1_lr_pairs = [[float(k), float(v)] for k, v in sorted(h1_log_range_emas[symbol].items())]

            input_dict["symbols"].append(
                {
                    "symbol_idx": int(idx),
                    "order_book": {"bid": mprice, "ask": mprice},
                    "exchange": {
                        "qty_step": float(self.qty_steps[symbol]),
                        "price_step": float(self.price_steps[symbol]),
                        "min_qty": float(self.min_qtys[symbol]),
                        "min_cost": float(self.min_costs[symbol]),
                        "c_mult": float(self.c_mults[symbol]),
                        "maker_fee": float(
                            self.markets_dict.get(symbol, {}).get("maker", 0.0) or 0.0
                        ),
                        "taker_fee": float(
                            self.markets_dict.get(symbol, {}).get("taker", 0.0) or 0.0
                        ),
                    },
                    "tradable": bool(active),
                    "next_candle": None,
                    "effective_min_cost": float(effective_min_cost),
                    "emas": {
                        "m1": {
                            "close": m1_close_pairs,
                            "log_range": m1_lr_pairs,
                            "volume": m1_volume_pairs,
                        },
                        "h1": {"close": [], "log_range": h1_lr_pairs, "volume": []},
                    },
                    "long": side_input("long"),
                    "short": side_input("short"),
                }
            )

        try:
            out_json = pbr.compute_ideal_orders_json(json.dumps(input_dict))
        except Exception as e:
            msg = str(e)
            if "MissingEma" in msg:
                match = re.search(r"symbol_idx\s*:\s*(\d+)", msg)
                if match:
                    idx = int(match.group(1))
                    symbol = idx_to_symbol.get(idx)
                    if symbol:
                        logging.error("[ema] Missing EMA for %s (symbol_idx=%d)", symbol, idx)
            raise
        out = json.loads(out_json)
        self._log_realized_loss_gate_blocks(out, idx_to_symbol)
        if hasattr(self, "_apply_orchestrator_symbol_states"):
            self._apply_orchestrator_symbol_states(
                out.get("diagnostics", {}),
                idx_to_symbol,
                mode_overrides,
            )
        orders = out.get("orders", [])
        if hasattr(self, "_update_monitor_runtime_hints"):
            self._update_monitor_runtime_hints(
                symbols=symbols,
                last_prices=last_prices,
                m1_close_emas=m1_close_emas,
                h1_log_range_emas=h1_log_range_emas,
                idx_to_symbol=idx_to_symbol,
                orders=orders,
            )

        ideal_orders: dict[str, list] = {}
        for o in orders:
            symbol = idx_to_symbol.get(int(o["symbol_idx"]))
            if symbol is None:
                continue
            order_type = str(o["order_type"])
            order_type_id = int(pbr.order_type_snake_to_id(order_type))
            execution_type = str(o.get("execution_type", "limit"))
            tup = (float(o["qty"]), float(o["price"]), order_type, order_type_id, execution_type)
            ideal_orders.setdefault(symbol, []).append(tup)

        # 记录解套币种选择
        for o in orders:
            order_type_str = o.get("order_type", "")
            if "close_unstuck" in order_type_str:
                symbol = idx_to_symbol.get(int(o.get("symbol_idx", -1)))
                if symbol:
                    pside = "long" if "long" in order_type_str else "short"
                    pos = self.positions.get(symbol, {}).get(pside, {})
                    entry_price = pos.get("price", 0.0)
                    current_price = last_prices.get(symbol, 0.0)
                    if entry_price > 0 and current_price > 0:
                        price_diff_pct = (current_price / entry_price - 1.0) * 100
                        sign = "+" if price_diff_pct >= 0 else ""
                    else:
                        price_diff_pct = 0.0
                        sign = ""
                    coin = symbol.split("/")[0] if "/" in symbol else symbol
                    allowance = unstuck_allowances.get(pside, 0.0)
                    logging.info(
                        "[unstuck] selecting %s %s | entry=%.2f now=%.2f (%s%.1f%%) | allowance=%.2f",
                        coin,
                        pside,
                        entry_price,
                        current_price,
                        sign,
                        price_diff_pct,
                        allowance,
                    )
                break  # 每个周期仅一个解套订单

        # 记录正常模式无持仓且无初始入场交易对的 EMA 门控
        self._log_ema_gating(ideal_orders, m1_close_emas, last_prices, symbols)

        ideal_orders_f, _wel_blocked = self._to_executable_orders(ideal_orders, last_prices)
        ideal_orders_f = self._finalize_reduce_only_orders(ideal_orders_f, last_prices)

        if return_snapshot:
            snapshot = {
                "ts_ms": int(utc_ms()),
                "exchange": str(getattr(self, "exchange", "")),
                "user": str(self.config_get(["live", "user"]) or ""),
                "active_symbols": list(symbols),
                "realized_pnl_cumsum": realized_pnl_cumsum,
                "orchestrator_input": input_dict,
                "orchestrator_output": out,
            }
            return ideal_orders_f, snapshot
        return ideal_orders_f

    async def _get_orchestrator_last_prices(self, symbols: list[str]) -> dict[str, float]:
        """返回编排器规划所需的最新价格。

        在分阶段模式下，市价读取仅通过 CandlestickManager，以便 CM 统一管理
        缓存、TTL 和远程抓取经济性。传统模式保留现有的直接批量行情路径，
        对缺失的交易对回退到 CM。
        """
        ttl_ms = 10_000
        refresh_mode = str(
            ((self.config.get("live") or {}).get("authoritative_refresh_mode")) or "legacy"
        )
        if refresh_mode == "staged":
            logging.debug(
                "[state] staged orchestrator requesting cm last prices | symbols=%s | ttl=%sms",
                len(symbols),
                ttl_ms,
            )
            last_prices = await self.cm.get_last_prices(symbols, max_age_ms=ttl_ms)
            invalid = []
            normalized = {}
            for symbol in symbols:
                raw = last_prices.get(symbol, 0.0)
                try:
                    price = float(raw)
                except (TypeError, ValueError):
                    price = 0.0
                normalized[symbol] = price
                if not math.isfinite(price) or price <= 0.0:
                    invalid.append(symbol)
            if invalid:
                logging.debug(
                    "[state] staged orchestrator cm last prices ready | symbols=%s | ok=%s | invalid=%s | invalid_symbols=%s",
                    len(symbols),
                    len(symbols) - len(invalid),
                    len(invalid),
                    ",".join(invalid[:12]),
                )
            else:
                logging.debug(
                    "[state] staged orchestrator cm last prices ready | symbols=%s | ok=%s | invalid=0",
                    len(symbols),
                    len(symbols),
                )
            return normalized

        # 遗留模式：优先使用直接批量交易所价格，然后通过 CM 填补缺失。
        last_prices = {}
        try:
            if (
                hasattr(self, "cca")
                and self.cca is not None
                and self.exchange
                and self.exchange.lower() == "hyperliquid"
            ):
                fetched = await self.cca.fetch(
                    self._hl_info_url(),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                    body=json.dumps({"type": "allMids"}),
                )
                coin_to_sym = {v: k for k, v in self.symbol_ids.items()} if self.symbol_ids else {}
                for coin, mid_str in fetched.items():
                    sym = coin_to_sym.get(coin)
                    if sym and sym in symbols:
                        try:
                            last_prices[sym] = float(mid_str)
                        except (ValueError, TypeError):
                            pass
            elif hasattr(self, "fetch_tickers"):
                tickers = await self.fetch_tickers()
                for sym in symbols:
                    tick = tickers.get(sym)
                    if tick and tick.get("last") is not None:
                        last_prices[sym] = float(tick["last"])
            if last_prices:
                now_ms = int(utc_ms())
                for sym, price in last_prices.items():
                    self.cm.set_current_close(sym, price, now_ms)
        except Exception as e:
            logging.debug("bulk price fetch failed, falling back to CM: %s", e)
            last_prices = {}
        missing = [s for s in symbols if s not in last_prices or last_prices[s] <= 0.0]
        if missing:
            cm_prices = await self.cm.get_last_prices(missing, max_age_ms=ttl_ms)
            last_prices.update(cm_prices)
        return last_prices

    def _to_executable_orders(
        self, ideal_orders: dict, last_prices: Dict[str, float]
    ) -> tuple[Dict[str, list], set[str]]:
        """将原始订单元组转换为 API 就绪的字典，并识别受 WEL 限制的交易对。"""
        ideal_orders_f: Dict[str, list] = {}
        wel_blocked_symbols: set[str] = set()

        for symbol, orders in ideal_orders.items():
            ideal_orders_f[symbol] = []
            last_mprice = last_prices[symbol]
            seen = set()
            with_mprice_diff = []
            for order in orders:
                side = determine_side_from_order_tuple(order)
                diff = order_market_diff(side, order[1], last_mprice)
                with_mprice_diff.append((diff, order, side))
                if (
                    isinstance(order, tuple)
                    and isinstance(order[2], str)
                    and "close_auto_reduce_wel" in order[2]
                ):
                    wel_blocked_symbols.add(symbol)
            any_partial = any("partial" in order[2] for _, order, _ in with_mprice_diff)
            for mprice_diff, order, order_side in sorted(with_mprice_diff, key=lambda item: item[0]):
                position_side = "long" if "long" in order[2] else "short"
                if order[0] == 0.0:
                    continue
                if mprice_diff > float(self.live_value("price_distance_threshold")):
                    if any_partial and "entry" in order[2]:
                        logging.debug(
                            "gated by price_distance_threshold (partial) | %s %s %s diff=%.5f",
                            symbol,
                            position_side,
                            order[2],
                            mprice_diff,
                        )
                        continue
                    if any(token in order[2] for token in ("initial", "unstuck")):
                        logging.debug(
                            "gated by price_distance_threshold (initial/unstuck) | %s %s %s diff=%.5f",
                            symbol,
                            position_side,
                            order[2],
                            mprice_diff,
                        )
                        continue
                    if not self.has_position(position_side, symbol):
                        logging.debug(
                            "gated by price_distance_threshold (no position) | %s %s %s diff=%.5f",
                            symbol,
                            position_side,
                            order[2],
                            mprice_diff,
                        )
                        continue
                seen_key = str(abs(order[0])) + str(order[1]) + order[2]
                if seen_key in seen:
                    logging.debug("duplicate ideal order for %s skipped: %s", symbol, order)
                    continue
                pb_order_type = snake_of(order[3])
                if len(order) >= 5:
                    execution_type = str(order[4]).lower()
                else:
                    execution_type = "limit"
                    panic_close_pref = self._equity_hard_stop_panic_close_order_type(
                        position_side
                    )
                    if "panic" in pb_order_type:
                        execution_type = "market" if panic_close_pref == "market" else "limit"
                if execution_type not in {"limit", "market"}:
                    execution_type = "limit"
                ideal_orders_f[symbol].append(
                    {
                        "symbol": symbol,
                        "side": order_side,
                        "position_side": position_side,
                        "qty": abs(order[0]),
                        "price": order[1],
                        "reduce_only": "close" in order[2],
                        "custom_id": self.format_custom_id_single(order[3]),
                        "type": execution_type,
                        "pb_order_type": pb_order_type,
                    }
                )
                seen.add(seen_key)
        return self._finalize_reduce_only_orders(ideal_orders_f, last_prices), wel_blocked_symbols

    def _finalize_reduce_only_orders(
        self, orders_by_symbol: Dict[str, list], last_prices: Dict[str, float]
    ) -> Dict[str, list]:
        """限制 reduce-only 数量，确保其不超过当前持仓量（单笔及合计）。"""
        for symbol, orders in orders_by_symbol.items():
            market_price = float(last_prices.get(symbol, 0.0))

            # 1) 将每笔 reduce-only 订单数量限制在持仓量以内
            for order in orders:
                if not order.get("reduce_only"):
                    continue
                pos = self.positions.get(order["symbol"], {}).get(order["position_side"], {})
                pos_size_abs = abs(float(pos.get("size", 0.0)))
                if abs(order["qty"]) > pos_size_abs:
                    logging.warning(
                        "trimmed reduce-only qty to position size | order=%s | position=%s",
                        order,
                        pos,
                    )
                    order["qty"] = pos_size_abs

            # 2) 限制 reduce_only 数量总和不超过持仓量，优先缩减离市价最远的平仓单
            for pside in ("long", "short"):
                pos_size_abs = abs(
                    float(self.positions.get(symbol, {}).get(pside, {}).get("size", 0.0))
                )
                if pos_size_abs <= 0.0:
                    continue
                ro = [o for o in orders if o.get("reduce_only") and o.get("position_side") == pside]
                if not ro:
                    continue
                total = sum(float(o.get("qty", 0.0)) for o in ro)
                if total <= pos_size_abs + 1e-12:
                    continue
                excess = total - pos_size_abs
                # 优先缩减离市价最远的：更大的 order_market_diff
                ro_sorted = sorted(
                    ro,
                    key=lambda o: order_market_diff(
                        o.get("side", ""), float(o.get("price", 0.0)), market_price
                    ),
                    reverse=True,
                )
                for o in ro_sorted:
                    if excess <= 0.0:
                        break
                    q = float(o.get("qty", 0.0))
                    if q <= 0.0:
                        continue
                    reduce_by = min(q, excess)
                    new_q = q - reduce_by
                    o["qty"] = float(round(new_q, 12))
                    excess -= reduce_by
                # 移除数量归零的 reduce-only 订单
                orders_by_symbol[symbol] = [
                    o
                    for o in orders_by_symbol[symbol]
                    if not (o.get("reduce_only") and float(o.get("qty", 0.0)) <= 0.0)
                ]

        return orders_by_symbol

    async def calc_orders_to_cancel_and_create(self):
        """确定需要取消的现有订单和需要创建的新订单。"""
        if not hasattr(self, "_last_plan_detail"):
            self._last_plan_detail = {}
        ideal_orders = await self.calc_ideal_orders()

        actual_orders = self._snapshot_actual_orders()
        keys = ("symbol", "side", "position_side", "qty", "price")
        to_cancel, to_create = [], []
        plan_summaries = []
        for symbol, symbol_orders in actual_orders.items():
            ideal_list = ideal_orders.get(symbol, []) if isinstance(ideal_orders, dict) else []
            cancel_, create_ = self._reconcile_symbol_orders(symbol, symbol_orders, ideal_list, keys)
            cancel_, create_ = self._annotate_order_deltas(cancel_, create_)
            pre_cancel = len(cancel_)
            pre_create = len(create_)
            cancel_, create_, skipped = self._apply_order_match_tolerance(cancel_, create_)
            plan_summaries.append(
                (symbol, pre_cancel, len(cancel_), pre_create, len(create_), skipped)
            )
            to_cancel += cancel_
            to_create += create_

        to_cancel = await self._sort_orders_by_market_diff(to_cancel, "to_cancel")
        to_create = await self._sort_orders_by_market_diff(to_create, "to_create")
        if plan_summaries:
            total_pre_cancel = sum(p[1] for p in plan_summaries)
            total_cancel = sum(p[2] for p in plan_summaries)
            total_pre_create = sum(p[3] for p in plan_summaries)
            total_create = sum(p[4] for p in plan_summaries)
            total_skipped = sum(p[5] for p in plan_summaries)
            detail_parts = []
            untouched_cancel = total_pre_cancel - total_cancel
            untouched_create = total_pre_create - total_create
            for symbol, pre_c, c, pre_cr, cr, skipped in plan_summaries:
                prev = self._last_plan_detail.get(symbol)
                current = (c, cr, skipped)
                self._last_plan_detail[symbol] = current
                if c or cr or skipped:
                    if prev != current:
                        detail_parts.append(f"{symbol}:c{pre_c}->{c} cr{pre_cr}->{cr} skip{skipped}")
            detail = " | ".join(detail_parts[:6])
            summary_key = (
                total_pre_cancel,
                total_cancel,
                total_pre_create,
                total_create,
                total_skipped,
                untouched_cancel,
                untouched_create,
                detail,
            )
            if summary_key != getattr(self, "_last_order_plan_summary", None):
                self._last_order_plan_summary = summary_key
                if total_cancel or total_create or total_skipped:
                    extra = []
                    if untouched_cancel:
                        extra.append(f"unchanged_cancel={untouched_cancel}")
                    if untouched_create:
                        extra.append(f"unchanged_create={untouched_create}")
                    # 无实际操作时（所有订单被跳过/未变化）使用 DEBUG 级别
                    log_level = logging.INFO if (total_cancel or total_create) else logging.DEBUG
                    logging.log(
                        log_level,
                        "[order] order plan summary | cancel %d->%d | create %d->%d | skipped=%d%s%s",
                        total_pre_cancel,
                        total_cancel,
                        total_pre_create,
                        total_create,
                        total_skipped,
                        f" | {' '.join(extra)}" if extra else "",
                        f" | details: {detail}" if detail else "",
                    )
        return to_cancel, to_create

    def _snapshot_actual_orders(self) -> dict[str, list[dict]]:
        """返回按交易对分组的当前挂单标准化快照。"""
        actual_orders: dict[str, list[dict]] = {}
        for symbol in self.active_symbols:
            symbol_orders = []
            for order in self.open_orders.get(symbol, []):
                try:
                    symbol_orders.append(
                        {
                            "symbol": order["symbol"],
                            "side": order["side"],
                            "position_side": order["position_side"],
                            "qty": abs(order["qty"]),
                            "price": order["price"],
                            "reduce_only": (
                                order["position_side"] == "long" and order["side"] == "sell"
                            )
                            or (order["position_side"] == "short" and order["side"] == "buy"),
                            "id": order.get("id"),
                            "custom_id": order.get("custom_id"),
                        }
                    )
                except Exception as exc:
                    logging.error(f"error in calc_orders_to_cancel_and_create {exc}")
                    traceback.print_exc()
                    print(order)
            actual_orders[symbol] = symbol_orders
        return actual_orders

    def _reconcile_symbol_orders(
        self,
        symbol: str,
        actual_orders: list[dict],
        ideal_orders: list,
        keys: tuple[str, ...],
    ) -> tuple[list[dict], list[dict]]:
        """对单个交易对进行模式过滤后，返回取消/创建列表。"""
        to_cancel, to_create = filter_orders(actual_orders, ideal_orders, keys)
        to_cancel, to_create = self._apply_mode_filters(symbol, to_cancel, to_create)
        return to_cancel, to_create

    def _annotate_order_deltas(
        self, to_cancel: list[dict], to_create: list[dict]
    ) -> tuple[list[dict], list[dict]]:
        """
        为现有订单与目标订单之间附加最佳匹配的差量信息，辅助日志记录。

        按交易对/方向/持仓方向匹配订单，选取价格距离最近的配对。
        """
        remaining_create = list(to_create)
        for order in to_create:
            order.setdefault("_context", "new")
            order.setdefault("_reason", "new")
        for cancel_order in to_cancel:
            cancel_order.setdefault("_context", "retire")
            cancel_order.setdefault("_reason", "retire")

        def pct(a: float, b: float) -> float:
            if a == 0 and b == 0:
                return 0.0
            if a == 0:
                return float("inf")
            return abs(b - a) / abs(a) * 100.0

        # 标注取消订单
        for cancel_order in to_cancel:
            candidates = [
                (idx, co)
                for idx, co in enumerate(remaining_create)
                if co.get("symbol") == cancel_order.get("symbol")
                and co.get("side") == cancel_order.get("side")
                and co.get("position_side") == cancel_order.get("position_side")
            ]
            if not candidates:
                continue
            # 选取价格差异最小的
            best_idx, best_order = min(
                candidates,
                key=lambda c: abs(
                    float(c[1].get("price", 0.0)) - float(cancel_order.get("price", 0.0))
                ),
            )
            raw_price_diff = pct(
                float(cancel_order.get("price", 0.0)), float(best_order.get("price", 0.0))
            )
            raw_qty_diff = pct(float(cancel_order.get("qty", 0.0)), float(best_order.get("qty", 0.0)))
            price_diff = round(raw_price_diff, 4) if math.isfinite(raw_price_diff) else raw_price_diff
            qty_diff = round(raw_qty_diff, 4) if math.isfinite(raw_qty_diff) else raw_qty_diff
            reason_parts = []
            if price_diff > 0:
                reason_parts.append("price")
            if qty_diff > 0:
                reason_parts.append("qty")
            reason = "+".join(reason_parts) if reason_parts else "adjustment"
            cancel_order["_delta"] = {
                "price_old": cancel_order.get("price"),
                "price_new": best_order.get("price"),
                "price_pct_diff": price_diff,
                "qty_old": cancel_order.get("qty"),
                "qty_new": best_order.get("qty"),
                "qty_pct_diff": qty_diff,
            }
            cancel_order["_context"] = "replace"
            cancel_order["_reason"] = reason
            # 同时标注匹配的创建订单
            best_order["_delta"] = {
                "price_old": cancel_order.get("price"),
                "price_new": best_order.get("price"),
                "price_pct_diff": price_diff,
                "qty_old": cancel_order.get("qty"),
                "qty_new": best_order.get("qty"),
                "qty_pct_diff": qty_diff,
            }
            best_order["_context"] = "replace"
            best_order["_reason"] = reason
            remaining_create.pop(best_idx)

        for ord in remaining_create:
            ord.setdefault("_context", "new")
            ord.setdefault("_reason", "fresh")
        return to_cancel, to_create

    def _apply_order_match_tolerance(
        self, to_cancel: list[dict], to_create: list[dict]
    ) -> tuple[list[dict], list[dict], int]:
        """丢弃在容差范围内的取消/创建配对，避免无谓的订单替换。

        返回 (remaining_cancel, remaining_create, skipped_pairs)
        """
        tolerance = float(self.live_value("order_match_tolerance_pct"))
        if tolerance <= 0.0:
            return to_cancel, to_create, 0

        used_cancel: set[int] = set()
        kept_create: list[dict] = []
        skipped = 0

        def pct_diff(a: float, b: float) -> float:
            if b == 0:
                return 0.0 if a == 0 else float("inf")
            return abs(a - b) / abs(b) * 100.0

        for order in to_create:
            match_idx = None
            for idx, existing in enumerate(to_cancel):
                if idx in used_cancel:
                    continue
                try:
                    if orders_matching(
                        order,
                        existing,
                        tolerance_qty=tolerance,
                        tolerance_price=tolerance,
                    ):
                        match_idx = idx
                        break
                except Exception:
                    continue
            if match_idx is None:
                kept_create.append(order)
            else:
                used_cancel.add(match_idx)
                skipped += 1
                try:
                    price_diff = pct_diff(float(order["price"]), float(to_cancel[match_idx]["price"]))
                    qty_diff = pct_diff(float(order["qty"]), float(to_cancel[match_idx]["qty"]))
                    logging.debug(
                        "skipped_recreate | %s | tolerance=%.4f%% price_diff=%.4f%% qty_diff=%.4f%%",
                        order.get("symbol", "?"),
                        tolerance * 100.0,
                        price_diff,
                        qty_diff,
                    )
                except Exception:
                    logging.debug(
                        "skipped_recreate | %s | tolerance=%.4f%%",
                        order.get("symbol", "?"),
                        tolerance * 100.0,
                    )

        remaining_cancel = [o for i, o in enumerate(to_cancel) if i not in used_cancel]
        return remaining_cancel, kept_create, skipped

    def _apply_mode_filters(
        self,
        symbol: str,
        to_cancel: list[dict],
        to_create: list[dict],
    ) -> tuple[list[dict], list[dict]]:
        """应用模式相关的取消/创建过滤规则。"""
        for pside in ["long", "short"]:
            mode = self.PB_modes[pside].get(symbol)
            if mode == "manual":
                to_cancel = [x for x in to_cancel if x["position_side"] != pside]
                to_create = [x for x in to_create if x["position_side"] != pside]
            elif mode == "tp_only":
                to_cancel = [
                    x
                    for x in to_cancel
                    if (
                        x["position_side"] != pside
                        or (x["position_side"] == pside and x["reduce_only"])
                    )
                ]
                to_create = [
                    x
                    for x in to_create
                    if (
                        x["position_side"] != pside
                        or (x["position_side"] == pside and x["reduce_only"])
                    )
                ]
            elif mode == "tp_only_with_active_entry_cancellation":
                # 保留活跃的平仓订单管理，同时允许取消开仓订单。
                # 永不创建新的开仓订单，但已有的开仓订单可出现在取消列表中。
                to_create = [
                    x
                    for x in to_create
                    if (
                        x["position_side"] != pside
                        or (x["position_side"] == pside and x["reduce_only"])
                    )
                ]
        return to_cancel, to_create

    async def _sort_orders_by_market_diff(self, orders: list[dict], log_label: str) -> list[dict]:
        """按市价差异排序订单，并发获取价格。"""
        if not orders:
            return []
        market_prices = await self._fetch_market_prices({order["symbol"] for order in orders})
        entries = []
        for order in orders:
            market_price = market_prices.get(order["symbol"])
            if market_price is None:
                logging.debug("price missing sort %s by mprice_diff %s", log_label, order)
                diff = 0.0
            else:
                diff = order_market_diff(order["side"], order["price"], market_price)
            entries.append((diff, order))
        entries.sort(key=lambda item: item[0])
        return [order for _, order in entries]

    async def _fetch_market_prices(self, symbols: set[str]) -> dict[str, float | None]:
        """获取指定交易对的当前收盘价。"""
        results: dict[str, float | None] = {}
        tasks: dict[str, asyncio.Task] = {}
        for symbol in symbols:
            try:
                fetch_result = self.cm.get_current_close(symbol, max_age_ms=10_000)
                if inspect.isawaitable(fetch_result):
                    tasks[symbol] = asyncio.create_task(fetch_result)
                else:
                    results[symbol] = fetch_result
            except Exception as exc:
                logging.debug("failed fetching mprice for %s: %s", symbol, exc)
                results[symbol] = None
        for symbol, task in tasks.items():
            try:
                results[symbol] = await task
            except Exception as exc:
                logging.debug("failed fetching mprice for %s: %s", symbol, exc)
                results[symbol] = None
        return results

    async def restart_bot_on_too_many_errors(self):
        """当每小时执行错误预算耗尽时重启机器人。"""
        if not hasattr(self, "error_counts"):
            self.error_counts = []
        now = utc_ms()
        self.error_counts = [x for x in self.error_counts if x > now - 1000 * 60 * 60] + [now]
        max_n_errors_per_hour = 10
        logging.info(
            f"error count: {len(self.error_counts)} of {max_n_errors_per_hour} errors per hour"
        )
        if len(self.error_counts) >= max_n_errors_per_hour:
            await self.restart_bot()
            raise Exception("too many errors... restarting bot.")

    def format_custom_id_single(self, order_type_id: int) -> str:
        """构建包含订单类型标记和 UUID 后缀的自定义 ID。"""
        token = type_token(order_type_id, with_marker=True)  # "0xABCD" 类型标记
        return (token + uuid4().hex)[: self.custom_id_max_length]

    def debug_dump_bot_state_to_disk(self):
        """将内部状态快照持久化到磁盘，用于调试。"""
        if not hasattr(self, "tmp_debug_ts"):
            self.tmp_debug_ts = 0
            self.tmp_debug_cache = make_get_filepath(f"caches/{self.exchange}/{self.user}_debug/")
        if utc_ms() - self.tmp_debug_ts > 1000 * 60 * 3:
            logging.info(f"debug dumping bot state to disk")
            for k, v in vars(self).items():
                try:
                    json.dump(
                        denumpyize(v), open(os.path.join(self.tmp_debug_cache, k + ".json"), "w")
                    )
                except Exception as e:
                    logging.error(f"debug failed to dump to disk {k} {e}")
            self.tmp_debug_ts = utc_ms()

    # 遗留 EMA 维护逻辑（init_EMAs_single/update_EMAs）已移除，改用 CandlestickManager

    def get_symbols_with_pos(self, pside=None):
        """返回指定方向上有持仓的交易对集合。"""
        if pside is None:
            return self.get_symbols_with_pos("long") | self.get_symbols_with_pos("short")
        return set([s for s in self.positions if self.positions[s][pside]["size"] != 0.0])

    def get_symbols_approved_or_has_pos(self, pside=None) -> set:
        """返回已批准交易或当前有持仓的交易对。"""
        if pside is None:
            return self.get_symbols_approved_or_has_pos(
                "long"
            ) | self.get_symbols_approved_or_has_pos("short")
        return (
            self.approved_coins_minus_ignored_coins[pside]
            | self.get_symbols_with_pos(pside)
            | {s for s in self.coin_overrides if self.get_forced_PB_mode(pside, s) == "normal"}
        )

    # 遗留 get_ohlcvs_1m_file_mods 已移除

    async def restart_bot(self):
        """停止所有任务并抛出异常以触发外部重启。"""
        logging.info("Initiating bot restart...")
        # 注意：不要在此设置 stop_signal_received=True —— 那会导致主循环退出而非重启。
        # 该标志仅用于用户主动停止（SIGINT/SIGTERM）。
        self.stop_data_maintainers()
        await self.cca.close()
        if self.ccp is not None:
            await self.ccp.close()
        raise RestartBotException("Bot will restart.")

    def _forager_refresh_budget(self, max_calls_per_minute: int) -> int:
        """Forager K 线刷新的令牌桶预算。"""
        try:
            max_calls = int(max_calls_per_minute)
        except Exception:
            max_calls = 0
        if max_calls <= 0:
            return 0
        now = utc_ms()
        state = getattr(self, "_forager_refresh_state", None)
        if not isinstance(state, dict):
            state = {"tokens": float(max_calls), "last_ms": now}
        last_ms = int(state.get("last_ms", now) or now)
        tokens = float(state.get("tokens", max_calls))
        elapsed = max(0.0, (now - last_ms) / 60_000.0)
        tokens = min(float(max_calls), tokens + float(max_calls) * elapsed)
        budget = int(tokens)
        state["tokens"] = float(tokens - budget)
        state["last_ms"] = int(now)
        self._forager_refresh_state = state
        return max(0, budget)

    def _split_forager_budget_by_side(
        self, total_budget: int, sides: Iterable[str]
    ) -> Dict[str, int]:
        """按方向公平分配周期预算，余数使用轮询方式分配。"""
        side_list = [s for s in sides if s in ("long", "short")]
        out = {s: 0 for s in side_list}
        try:
            total = int(total_budget)
        except Exception:
            total = 0
        if total <= 0 or not side_list:
            return out
        n = len(side_list)
        base = total // n
        rem = total % n
        for s in side_list:
            out[s] = base
        start = int(getattr(self, "_forager_budget_rr", 0) or 0) % n
        for i in range(rem):
            out[side_list[(start + i) % n]] += 1
        self._forager_budget_rr = (start + 1) % n
        return out

    def _forager_target_staleness_ms(self, n_symbols: int, max_calls_per_minute: int) -> int:
        """根据刷新预算计算 Forager 候选交易对的最大可接受陈旧度。"""
        try:
            n_syms = int(n_symbols)
        except Exception:
            n_syms = 0
        try:
            max_calls = int(max_calls_per_minute)
        except Exception:
            max_calls = 0
        if n_syms <= 0 or max_calls <= 0:
            return int(getattr(self, "inactive_coin_candle_ttl_ms", 600_000))
        minutes = max(1.0, float(n_syms) / float(max_calls))
        return int(minutes * 60_000)

    def _maybe_log_candle_refresh(
        self,
        context: str,
        symbols: Iterable[str],
        *,
        target_age_ms: Optional[int] = None,
        refreshed: Optional[int] = None,
        throttle_ms: int = 60_000,
    ) -> None:
        """以节流方式记录指定交易对的 K 线陈旧度摘要。"""
        try:
            now = utc_ms()
            boot_delay_ms = int(getattr(self, "candle_refresh_log_boot_delay_ms", 300_000) or 0)
            boot_elapsed = int(now - getattr(self, "start_time_ms", now))
            if boot_elapsed < boot_delay_ms:
                return
            last = int(getattr(self, "_candle_refresh_log_last_ms", 0) or 0)
            if (now - last) < int(throttle_ms):
                return
            sym_list = list(symbols)
            if not sym_list:
                return
            ages = []
            for sym in sym_list:
                try:
                    last_final = self.cm.get_last_final_ts(sym)
                except Exception:
                    last_final = 0
                if last_final:
                    ages.append(max(0, now - int(last_final)))
            if not ages:
                return
            ages.sort()
            median_ms = ages[len(ages) // 2]
            max_ms = ages[-1]
            target_s = f"{int(target_age_ms/1000)}s" if target_age_ms else "n/a"
            refreshed_str = f", refreshed={refreshed}" if refreshed is not None else ""
            logging.debug(
                "[candle] %s symbols=%d%s max_stale=%ds median_stale=%ds target=%s",
                context,
                len(sym_list),
                refreshed_str,
                int(max_ms / 1000),
                int(median_ms / 1000),
                target_s,
            )
            self._candle_refresh_log_last_ms = int(now)
        except Exception:
            return

    async def _refresh_forager_candidate_candles(self) -> None:
        """尽可能刷新 Forager 候选交易对的 K 线，避免大批量集中请求。"""
        if not self.is_forager_mode():
            return
        max_calls = get_optional_live_value(self.config, "max_ohlcv_fetches_per_minute", 0)
        try:
            max_calls = int(max_calls) if max_calls is not None else 0
        except Exception:
            max_calls = 0

        candidates_by_side: Dict[str, set] = {}
        slots_open_any = False
        for pside in ("long", "short"):
            if not self.is_forager_mode(pside):
                continue
            syms = set(self.approved_coins_minus_ignored_coins.get(pside, set()))
            if not syms:
                continue
            candidates_by_side[pside] = syms
            try:
                max_n = int(self.get_max_n_positions(pside))
            except Exception:
                max_n = 0
            try:
                current_n = int(self.get_current_n_positions(pside))
            except Exception:
                current_n = len(self.get_symbols_with_pos(pside))
            if max_n > current_n:
                slots_open_any = True

        if not candidates_by_side:
            return

        all_candidates = set().union(*candidates_by_side.values())
        if not all_candidates:
            return

        if slots_open_any:
            if max_calls > 0:
                # 即使有空位也要遵守速率限制；使用令牌桶预算。
                budget = self._forager_refresh_budget(max_calls)
                if budget <= 0:
                    return
            else:
                budget = len(all_candidates)
        else:
            if max_calls <= 0:
                return
            budget = self._forager_refresh_budget(max_calls)
            if budget <= 0:
                return

        # 跳过活跃交易对；它们在 update_ohlcvs_1m_for_actives 中刷新
        active = set(self.active_symbols) if hasattr(self, "active_symbols") else set()
        candidates = sorted(all_candidates - active)
        if not candidates:
            return

        if slots_open_any:
            rate_limit_age_ms = self._forager_target_staleness_ms(len(all_candidates), max_calls)
            # 即使有空位也要遵守速率限制；最低 60 秒以保证响应速度。
            target_age_ms = max(60_000, rate_limit_age_ms) if max_calls > 0 else 60_000
        else:
            target_age_ms = self._forager_target_staleness_ms(len(all_candidates), max_calls)
        now = utc_ms()
        stale: List[Tuple[float, str]] = []
        for sym in candidates:
            try:
                last_final = self.cm.get_last_final_ts(sym)
            except Exception:
                last_final = 0
            age_ms = now - int(last_final) if last_final else float("inf")
            if age_ms > target_age_ms:
                stale.append((age_ms, sym))
        if not stale:
            return

        stale.sort(reverse=True)
        to_refresh = [sym for _, sym in stale[:budget]]
        if not to_refresh:
            return

        # Forager 刷新行为的节流可见性（仅调试）。
        try:
            now = utc_ms()
            boot_delay_ms = int(getattr(self, "candle_refresh_log_boot_delay_ms", 300_000) or 0)
            boot_elapsed = int(now - getattr(self, "start_time_ms", now))
            if boot_elapsed >= boot_delay_ms:
                last_log = int(getattr(self, "_forager_refresh_log_last_ms", 0) or 0)
                if (now - last_log) >= 90_000:
                    oldest_ms = int(stale[0][0]) if stale else 0
                    logging.debug(
                        "[candle] forager refresh slots_open=%s candidates=%d stale=%d budget=%d oldest=%ds target=%ds",
                        "yes" if slots_open_any else "no",
                        len(all_candidates),
                        len(stale),
                        len(to_refresh),
                        int(oldest_ms / 1000),
                        int(target_age_ms / 1000),
                    )
                    self._forager_refresh_log_last_ms = int(now)
        except Exception:
            pass

        end_ts = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
        try:
            default_win = int(getattr(self.cm, "default_window_candles", 120) or 120)
        except Exception:
            default_win = 120
        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0
        span_buffer = 1.0 + max(0.0, warmup_ratio)

        fetch_delay_s = self._get_fetch_delay_seconds()

        for sym in to_refresh:
            try:
                max_span = 0.0
                for pside, syms in candidates_by_side.items():
                    if sym not in syms:
                        continue
                    try:
                        span_v = self.bp(pside, "forager_volume_ema_span", sym)
                    except Exception:
                        span_v = None
                    try:
                        span_lr = self.bp(pside, "forager_volatility_ema_span", sym)
                    except Exception:
                        span_lr = None
                    for span in (span_v, span_lr):
                        if span is not None:
                            try:
                                max_span = max(max_span, float(span))
                            except Exception:
                                pass
                win = (
                    max(default_win, int(math.ceil(max_span * span_buffer)))
                    if max_span > 0.0
                    else default_win
                )
                if max_warmup_minutes > 0:
                    win = min(int(win), int(max_warmup_minutes))
                start_ts = end_ts - ONE_MIN_MS * max(1, win)
                await self.cm.get_candles(
                    sym,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    max_age_ms=0,
                    strict=False,
                    max_lookback_candles=win,
                )
                if fetch_delay_s > 0:
                    await asyncio.sleep(fetch_delay_s)
            except TimeoutError as exc:
                logging.warning(
                    "Timed out acquiring candle lock for %s; forager refresh will retry (%s)",
                    sym,
                    exc,
                )
            except Exception as exc:
                logging.error("error refreshing forager candles for %s: %s", sym, exc, exc_info=True)

    async def update_ohlcvs_1m_for_actives(self):
        """确保活跃交易对在 CandlestickManager 中有新鲜的 1 分钟 K 线（不超过 60 秒）。

        使用 CandlestickManager.get_candles 的 max_age_ms=60_000 参数，仅当内部
        上次刷新时间超过 TTL 时才会刷新。获取一个小的最近窗口，结束于最新已定型的分钟。
        """
        # 1 分钟 K 线每分钟仅定型一次；更频繁的刷新会浪费 API 预算。
        # 使用 60 秒 TTL，确保每个交易对最多每分钟获取一次。
        max_age_ms = 60_000
        try:
            now = utc_ms()
            end_ts = (now // ONE_MIN_MS) * ONE_MIN_MS - ONE_MIN_MS
            # 如可用则使用管理器默认窗口，否则使用合理回退值
            try:
                window = int(getattr(self.cm, "default_window_candles", 120))
            except Exception:
                window = 120
            start_ts = end_ts - ONE_MIN_MS * window

            fetch_delay_s = self._get_fetch_delay_seconds()

            symbols = sorted(set(self.active_symbols))
            # 优先处理有持仓的交易对（需要新鲜 K 线以正确计算订单），
            # 对其余交易对随机打乱，避免 429 导致后半部分持续缓存饥饿。
            symbols_with_pos = [s for s in symbols if self.has_position(symbol=s)]
            symbols_without_pos = [s for s in symbols if s not in symbols_with_pos]
            random.shuffle(symbols_without_pos)
            ordered_symbols = symbols_with_pos + symbols_without_pos
            self._maybe_log_candle_refresh(
                "active refresh",
                symbols,
                target_age_ms=max_age_ms,
                refreshed=len(symbols),
                throttle_ms=60_000,
            )
            for sym in ordered_symbols:
                # 若 429 触发了 CandlestickManager 的全局退避，则提前终止循环；
                # 剩余交易对都会命中相同的退避。它们将在下一个周期被处理；
                # 持仓优先 + 随机打乱的顺序可防止系统性饥饿。
                if self.cm.is_rate_limited():
                    logging.debug("[candle] active refresh breaking early: rate limit backoff active")
                    break
                try:
                    await self.cm.get_candles(
                        sym,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        max_age_ms=max_age_ms,
                        strict=False,
                        max_lookback_candles=window,
                    )
                    if fetch_delay_s > 0:
                        await asyncio.sleep(fetch_delay_s)
                except TimeoutError as exc:
                    logging.warning(
                        "Timed out acquiring candle lock for %s; will retry next cycle (%s)",
                        sym,
                        exc,
                    )
                except Exception as exc:
                    logging.error("error refreshing candles for %s: %s", sym, exc, exc_info=True)
            # 尽力刷新 Forager 候选交易对（懒加载 & 有预算限制）
            await self._refresh_forager_candidate_candles()
        except Exception as e:
            logging.error(f"error with {get_function_name()} {e}")
            traceback.print_exc()

    async def maintain_hourly_cycle(self):
        """在机器人运行期间定期刷新市场元数据。"""
        # 随机抖动（0-120 秒），避免同一 VPS 上的多个机器人同时触发
        # init_markets 而耗尽基于 IP 的速率限额。
        jitter_s = random.uniform(0, 120)
        logging.info("[hourly] starting maintenance cycle (jitter=%.1fs)", jitter_s)
        while not self.stop_signal_received:
            try:
                now = utc_ms()
                mem_prev = getattr(self, "_mem_log_prev", None)
                last_mem_log_ts = None
                if isinstance(mem_prev, dict):
                    last_mem_log_ts = mem_prev.get("timestamp")
                interval = getattr(self, "memory_snapshot_interval_ms", 3_600_000)
                if last_mem_log_ts is None or now - last_mem_log_ts >= interval:
                    self._log_memory_snapshot(now_ms=now)
                candle_check_interval = int(getattr(self, "candle_disk_check_interval_ms", 0) or 0)
                last_candle_check = int(getattr(self, "_candle_disk_check_last_ms", 0) or 0)
                boot_delay_ms = int(getattr(self, "candle_disk_check_boot_delay_ms", 300_000) or 0)
                boot_elapsed = int(now - getattr(self, "start_time_ms", now))
                if (
                    candle_check_interval > 0
                    and boot_elapsed >= boot_delay_ms
                    and (last_candle_check == 0 or now - last_candle_check >= candle_check_interval)
                ):
                    self._candle_disk_check_last_ms = now
                    try:
                        await self.audit_required_candle_disk_coverage()
                    except Exception as exc:
                        logging.error(
                            "error running candle disk coverage audit: %s", exc, exc_info=True
                        )
                # 每小时更新一次 markets 字典，附带每个实例的抖动
                hourly_interval_ms = 1000 * 60 * 60 + int(jitter_s * 1000)
                if now - self.init_markets_last_update_ms > hourly_interval_ms:
                    try:
                        await self.init_markets(verbose=False)
                    except RateLimitExceeded:
                        self._health_rate_limits += 1
                        logging.warning(
                            "[rate] hourly init_markets hit rate limit; will retry next cycle"
                        )
                        await asyncio.sleep(10)
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"error with {get_function_name()} {e}")
                traceback.print_exc()
                await self.restart_bot_on_too_many_errors()
                await asyncio.sleep(5)

    async def start_data_maintainers(self):
        """启动负责市场元数据和订单监控的后台任务。"""
        if hasattr(self, "maintainers"):
            self.stop_data_maintainers()
        maintainer_names = ["maintain_hourly_cycle"]
        if self.ws_enabled:
            maintainer_names.append("watch_orders")
        else:
            logging.info("Websocket 维护器已跳过（通过自定义端点禁用了 ws）。")
        self.maintainers = {
            name: asyncio.create_task(getattr(self, name)()) for name in maintainer_names
        }

    # 遗留 websocket 1 分钟 OHLCV 监控已移除；CandlestickManager 为权威来源

    async def calc_log_range(
        self,
        pside: str,
        eligible_symbols: Optional[Iterable[str]] = None,
        *,
        max_age_ms: Optional[int] = 60_000,
        max_network_fetches: Optional[int] = None,
    ) -> Dict[str, float]:
        """计算每个交易对的 1 分钟 EMA 对数范围：EMA(ln(high/low))。

        返回交易对到 ema_log_range 的映射；非有限值或计算失败返回 0.0。

        若设置了 *max_network_fetches*，则最多允许该数量的交易对触发网络请求；
        其余交易对仅使用缓存数据。
        """
        if eligible_symbols is None:
            eligible_symbols = self.eligible_symbols
        span = int(round(self.bot_value(pside, "forager_volatility_ema_span")))
        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0
        span_buffer = 1.0 + max(0.0, warmup_ratio)
        window_candles = max(1, int(math.ceil(span * span_buffer))) if span > 0 else 1
        if max_warmup_minutes > 0:
            window_candles = min(int(window_candles), int(max_warmup_minutes))

        syms = list(eligible_symbols)

        per_sym_ttl, cache_only_never_fetched = self._compute_fetch_budget_ttls(
            syms, max_age_ms, max_network_fetches
        )

        # 在 1 分钟 K 线上计算对数范围的 EMA：ln(high/low)
        async def one(symbol: str):
            """计算单个交易对的对数范围 EMA。"""
            try:
                if symbol in cache_only_never_fetched:
                    return 0.0
                ttl = per_sym_ttl.get(symbol)
                if ttl is None or ttl == 0:
                    # 若调用方传入 TTL 则使用；否则按交易对选择 TTL
                    if max_age_ms is not None:
                        ttl = int(max_age_ms)
                    else:
                        # 对非交易中的交易对使用更宽松的 TTL
                        has_pos = self.has_position(symbol)
                        has_oo = (
                            bool(self.open_orders.get(symbol)) if hasattr(self, "open_orders") else False
                        )
                        ttl = (
                            60_000
                            if (has_pos or has_oo)
                            else int(getattr(self, "inactive_coin_candle_ttl_ms", 600_000))
                        )
                res = await self.cm.get_latest_ema_metrics(
                    symbol,
                    {"log_range": span},
                    max_age_ms=ttl,
                    window_candles=window_candles,
                    timeframe=None,
                )
                val = float(res.get("log_range", float("nan")))
                return float(val) if np.isfinite(val) else 0.0
            except Exception:
                return 0.0

        tasks = {s: asyncio.create_task(one(s)) for s in syms}
        out = {}
        n = len(syms)
        started_ms = utc_ms()
        for sym, task in tasks.items():
            try:
                out[sym] = await task
            except Exception:
                out[sym] = 0.0
        elapsed_s = max(0.001, (utc_ms() - started_ms) / 1000.0)
        now_ms = utc_ms()
        ema_log_throttle_ms = 300_000  # 每个指标日志间隔 5 分钟
        if out:
            top_n = min(8, len(out))
            top = sorted(out.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            top_syms = tuple(sym for sym, _ in top)
            # 仅在排名变化（成员/顺序）时记录日志以减少噪音。
            # 同时限制每个指标最多每 5 分钟记录一次。
            if not hasattr(self, "_log_range_top_cache"):
                self._log_range_top_cache = {}
            if not hasattr(self, "_log_range_top_last_log_ms"):
                self._log_range_top_last_log_ms = {}
            cache_key = (pside, span)
            last_top = self._log_range_top_cache.get(cache_key)
            last_log_ms = self._log_range_top_last_log_ms.get(cache_key, 0)
            if last_top != top_syms and (now_ms - last_log_ms) >= ema_log_throttle_ms:
                self._log_range_top_cache[cache_key] = top_syms
                self._log_range_top_last_log_ms[cache_key] = now_ms
                summary = ", ".join(f"{symbol_to_coin(sym)}={val:.6f}" for sym, val in top)
                logging.info(
                    f"[ranking] log_range EMA span {span}: {n} coins elapsed={int(elapsed_s)}s, top{top_n}: {summary}"
                )
        return out

    async def calc_volumes(
        self,
        pside: str,
        symbols: Optional[Iterable[str]] = None,
        *,
        max_age_ms: Optional[int] = 60_000,
    ) -> Dict[str, float]:
        """计算每个交易对的 1 分钟 EMA 报价量。

        返回交易对到 ema_quote_volume 的映射；非有限值或计算失败返回 0.0。
        """
        span = int(round(self.bot_value(pside, "forager_volume_ema_span")))
        try:
            warmup_ratio = float(get_optional_live_value(self.config, "warmup_ratio", 0.0))
        except Exception:
            warmup_ratio = 0.0
        try:
            max_warmup_minutes = int(
                get_optional_live_value(self.config, "max_warmup_minutes", 0) or 0
            )
        except Exception:
            max_warmup_minutes = 0
        span_buffer = 1.0 + max(0.0, warmup_ratio)
        window_candles = max(1, int(math.ceil(span * span_buffer))) if span > 0 else 1
        if max_warmup_minutes > 0:
            window_candles = min(int(window_candles), int(max_warmup_minutes))
        if symbols is None:
            symbols = self.get_symbols_approved_or_has_pos(pside)

        # 在 1 分钟 K 线上计算报价量的 EMA
        async def one(symbol: str):
            """计算单个交易对的报价量 EMA。"""
            try:
                if max_age_ms is not None:
                    ttl = int(max_age_ms)
                else:
                    has_pos = self.has_position(symbol)
                    has_oo = (
                        bool(self.open_orders.get(symbol)) if hasattr(self, "open_orders") else False
                    )
                    ttl = (
                        60_000
                        if (has_pos or has_oo)
                        else int(getattr(self, "inactive_coin_candle_ttl_ms", 600_000))
                    )
                res = await self.cm.get_latest_ema_metrics(
                    symbol,
                    {"qv": span},
                    max_age_ms=ttl,
                    window_candles=window_candles,
                    timeframe=None,
                )
                val = float(res.get("qv", float("nan")))
                return float(val) if np.isfinite(val) else 0.0
            except Exception:
                return 0.0

        syms = list(symbols)
        tasks = {s: asyncio.create_task(one(s)) for s in syms}
        out = {}
        n = len(syms)
        started_ms = utc_ms()
        for sym, task in tasks.items():
            try:
                out[sym] = await task
            except Exception:
                out[sym] = 0.0
        elapsed_s = max(0.001, (utc_ms() - started_ms) / 1000.0)
        now_ms = utc_ms()
        ema_log_throttle_ms = 300_000  # 每个指标日志间隔 5 分钟
        if out:
            top_n = min(8, len(out))
            top = sorted(out.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            top_syms = tuple(sym for sym, _ in top)
            # 限制每个指标最多每 5 分钟记录一次。
            if not hasattr(self, "_volume_top_cache"):
                self._volume_top_cache = {}
            if not hasattr(self, "_volume_top_last_log_ms"):
                self._volume_top_last_log_ms = {}
            cache_key = (pside, span)
            last_top = self._volume_top_cache.get(cache_key)
            last_log_ms = self._volume_top_last_log_ms.get(cache_key, 0)
            if last_top != top_syms and (now_ms - last_log_ms) >= ema_log_throttle_ms:
                self._volume_top_cache[cache_key] = top_syms
                self._volume_top_last_log_ms[cache_key] = now_ms
                summary = ", ".join(f"{symbol_to_coin(sym)}={val:.2f}" for sym, val in top)
                logging.info(
                    f"[ranking] volume EMA span {span}: {n} coins elapsed={int(elapsed_s)}s, top{top_n}: {summary}"
                )
        return out

    async def execute_multiple(self, orders: [dict], type_: str):
        """按顺序执行一组订单操作，同时追踪失败情况。"""
        if not orders:
            return []
        executions = []
        any_exceptions = False
        for order in orders:  # 按距 PA 距离排序
            task = None
            try:
                task = asyncio.create_task(getattr(self, type_)(order))
                executions.append((order, task))
            except Exception as e:
                logging.error(f"error executing {type_} {order} {e}")
                print_async_exception(task)
                traceback.print_exc()
                executions.append((order, e))
                any_exceptions = True
        results = []
        for order, execution in executions:
            if isinstance(execution, Exception):
                # 任务创建时即已失败
                results.append(execution)
                continue
            result = None
            try:
                result = await execution
                results.append(result)
            except Exception as e:
                logging.error(f"error executing {type_} {execution} {e}")
                print_async_exception(result)
                results.append(e)
                traceback.print_exc()
                any_exceptions = True
        if any_exceptions:
            await self.restart_bot_on_too_many_errors()
        return results

    # 遗留 maintain_ohlcvs_1m_REST 已移除；CandlestickManager 负责缓存和 TTL

    # 遗留 update_ohlcvs_1m_single_from_exchange 已移除

    # 遗留 update_ohlcvs_1m_single_from_disk 已移除

    # 遗留 update_ohlcvs_1m_single 已移除

    # 遗留文件锁辅助函数已移除

    async def close(self):
        """停止后台任务并关闭交易所客户端。"""
        logging.info(f"Stopped data maintainers: {self.stop_data_maintainers()}")
        await self.cca.close()
        if self.ccp is not None:
            await self.ccp.close()

    def add_to_coins_lists(self, content, k_coins, log_psides=None):
        """从配置内容更新已批准/已忽略币种集合。"""
        if log_psides is None:
            log_psides = set(content.keys())
        symbols = None
        result = {"added": {}, "removed": {}}
        psides_equal = content["long"] == content["short"]
        for pside in content:
            if not psides_equal or symbols is None:
                coins = content[pside]
                if k_coins == "approved_coins" and _coins_source_side_is_all(coins):
                    symbols = set(getattr(self, "eligible_symbols", set()))
                else:
                    # 检查 coins 是否为需要拆分的单个字符串
                    if isinstance(coins, str):
                        coins = coins.split(",")
                    # 处理列表元素中包含逗号分隔值的情况
                    elif isinstance(coins, (list, tuple)):
                        expanded_coins = []
                        for item in coins:
                            if isinstance(item, str) and "," in item:
                                expanded_coins.extend(item.split(","))
                            else:
                                expanded_coins.append(item)
                        coins = expanded_coins

                    symbols = [self.coin_to_symbol(coin, verbose=False) for coin in coins if coin]
                    symbols = {s for s in symbols if s}
                    eligible = getattr(self, "eligible_symbols", None)
                    if eligible:
                        skipped = [sym for sym in symbols if sym not in eligible]
                        if skipped:
                            coin_list = ", ".join(
                                sorted(symbol_to_coin(sym, verbose=False) or sym for sym in skipped)
                            )
                            symbol_list = ", ".join(sorted(skipped))
                            warned = getattr(self, "_unsupported_coin_warnings", None)
                            if warned is None:
                                warned = set()
                                setattr(self, "_unsupported_coin_warnings", warned)
                            warn_key = (self.exchange, coin_list, symbol_list, k_coins)
                            if warn_key not in warned:
                                logging.info(
                                    "[config] skipping unsupported markets for %s: coins=%s symbols=%s exchange=%s",
                                    k_coins,
                                    coin_list,
                                    symbol_list,
                                    getattr(self, "exchange", "?"),
                                )
                                warned.add(warn_key)
                            symbols = symbols - set(skipped)
            symbols_already = getattr(self, k_coins)[pside]
            if symbols_already != symbols:
                added = symbols - symbols_already
                removed = symbols_already - symbols
                if added and pside in log_psides:
                    result["added"][pside] = added
                if removed and pside in log_psides:
                    result["removed"][pside] = removed
                getattr(self, k_coins)[pside] = symbols
        return result

    def refresh_approved_ignored_coins_lists(self):
        """从配置源重新加载已批准和已忽略的币种列表。"""
        try:
            added_summary = {}
            removed_summary = {}
            for k in ("approved_coins", "ignored_coins"):
                if not hasattr(self, k):
                    setattr(self, k, {"long": set(), "short": set()})
                config_sources = self.config.get("_coins_sources", {})
                if k in config_sources:
                    raw_source = config_sources[k]
                else:
                    raw_source = self.live_value(k)
                parsed = normalize_coins_source(raw_source, allow_all=(k == "approved_coins"))
                if k == "approved_coins":
                    log_psides = {ps for ps in parsed if self.is_pside_enabled(ps)}
                else:
                    log_psides = set(parsed.keys())
                add_res = self.add_to_coins_lists(parsed, k, log_psides=log_psides)
                if add_res:
                    added_summary.setdefault(k, {}).update(add_res.get("added", {}))
                    removed_summary.setdefault(k, {}).update(add_res.get("removed", {}))
            self.approved_coins_minus_ignored_coins = {}
            for pside in self.approved_coins:
                if not self.is_pside_enabled(pside):
                    if pside not in self._disabled_psides_logged:
                        if self.approved_coins[pside]:
                            logging.info(
                                f"{pside} side disabled (zero exposure or positions); clearing approved list."
                            )
                        else:
                            logging.info(
                                f"{pside} side disabled (zero exposure or positions); approved list already empty."
                            )
                        self._disabled_psides_logged.add(pside)
                    self.approved_coins[pside] = set()
                    self.approved_coins_minus_ignored_coins[pside] = set()
                    continue
                else:
                    if pside in self._disabled_psides_logged:
                        logging.info(f"{pside} side re-enabled; restoring approved coin handling.")
                        self._disabled_psides_logged.discard(pside)
                self.approved_coins_minus_ignored_coins[pside] = self._filter_approved_symbols(
                    pside, self.approved_coins[pside] - self.ignored_coins[pside]
                )
            # 聚合新增/移除日志以提高可读性
            for k, summary in (("added", added_summary.get("approved_coins", {})),):
                if summary:
                    parts = []
                    for pside, coins in summary.items():
                        if coins:
                            parts.append(
                                f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}"
                            )
                    if parts:
                        logging.info("added to approved_coins | %s", " | ".join(parts))
            for k, summary in (("removed", removed_summary.get("approved_coins", {})),):
                if summary:
                    parts = []
                    for pside, coins in summary.items():
                        if coins:
                            parts.append(
                                f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}"
                            )
                    if parts:
                        logging.info("removed from approved_coins | %s", " | ".join(parts))
            for k, summary in (("added", added_summary.get("ignored_coins", {})),):
                if summary:
                    parts = []
                    for pside, coins in summary.items():
                        if coins:
                            parts.append(
                                f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}"
                            )
                    if parts:
                        logging.info("added to ignored_coins | %s", " | ".join(parts))
            for k, summary in (("removed", removed_summary.get("ignored_coins", {})),):
                if summary:
                    parts = []
                    for pside, coins in summary.items():
                        if coins:
                            parts.append(
                                f"{pside}: {','.join(sorted(symbol_to_coin(x) for x in coins))}"
                            )
                    if parts:
                        logging.info("removed from ignored_coins | %s", " | ".join(parts))
            try:
                if not getattr(self, "_stock_perps_warning_logged", False):
                    stock_syms = set()
                    for syms in self.approved_coins_minus_ignored_coins.values():
                        for sym in syms:
                            base = sym.split("/")[0] if "/" in sym else sym
                            if base.startswith(("xyz:", "XYZ-", "XYZ:")) or sym.startswith(
                                ("xyz:", "XYZ-", "XYZ:")
                            ):
                                stock_syms.add(sym)
                    if stock_syms:
                        coins = sorted(
                            {
                                symbol_to_coin(s) or (s.split("/")[0] if "/" in s else s)
                                for s in stock_syms
                            }
                        )
                        logging.warning(
                            "Stock perps detected in approved_coins (%s). On Hyperliquid, HIP-3/non-standard perps require unifiedAccount mode; non-unified accounts will fail loudly.",
                            ",".join(coins),
                        )
                        self._stock_perps_warning_logged = True
            except Exception:
                pass
            self._log_coin_symbol_fallback_summary()
        except Exception as e:
            logging.error(f"error with refresh_approved_ignored_coins_lists {e}")
            traceback.print_exc()

    def _log_coin_symbol_fallback_summary(self):
        """输出交易对/币种映射回退的简要摘要（每次变化时输出一次）。"""
        counts = coin_symbol_warning_counts()
        if counts != self._last_coin_symbol_warning_counts:
            if counts["symbol_to_coin_fallbacks"] or counts["coin_to_symbol_fallbacks"]:
                logging.info(
                    "[mapping] fallbacks: symbol->coin=%d | coin->symbol=%d (unique)",
                    counts["symbol_to_coin_fallbacks"],
                    counts["coin_to_symbol_fallbacks"],
                )
            self._last_coin_symbol_warning_counts = dict(counts)

    def _build_order_params(self, order: dict) -> dict:
        """钩子：构建下单的执行参数。

        在子类中重写以实现交易所特定逻辑。
        """
        return {}

    async def execute_order(self, order: dict) -> dict:
        """通过交易所客户端下单个订单。"""
        params = {
            "symbol": order["symbol"],
            "type": order.get("type", "limit"),
            "side": order["side"],
            "amount": abs(order["qty"]),
            "price": order["price"],
            "params": self._build_order_params(order),
        }
        executed = await self.cca.create_order(**params)
        return executed

    async def execute_orders(self, orders: [dict]) -> [dict]:
        """使用辅助管道批量执行订单创建。"""
        return await self.execute_multiple(orders, "execute_order")

    async def execute_cancellation(self, order: dict) -> dict:
        """通过交易所客户端取消单个订单。"""
        executed = None
        try:
            executed = await self.cca.cancel_order(order["id"], symbol=order["symbol"])
            return executed
        except Exception as e:
            err_str = str(e).lower()
            # 检测"订单已成交/已取消"错误 —— 无害，仅为竞态条件
            already_gone_indicators = [
                "100004",  # KuCoin: "订单无法取消"
                "110001",  # Bybit: "订单不存在或取消太晚"
                "order not exists",
                "order does not exist",
                "order not found",
                "too late to cancel",
                "already filled",
                "already cancelled",
                "already canceled",
                "-2011",  # Binance: "未知订单"
                "51400",  # OKX: "订单不存在"
                "order_not_found",
            ]
            if any(ind in err_str for ind in already_gone_indicators):
                logging.info(
                    "[order] cancel skipped: %s %s - order likely already filled or cancelled",
                    order.get("symbol", "?"),
                    order.get("id", "?")[:12],
                )
            else:
                logging.error(f"error cancelling order {order} {e}")
                print_async_exception(executed)
                traceback.print_exc()
            return {}

    async def execute_cancellations(self, orders: [dict]) -> [dict]:
        """使用辅助管道批量执行取消操作。"""
        return await self.execute_multiple(orders, "execute_cancellation")


def setup_bot(config):
    """根据配置实例化正确的交易所机器人实现。"""
    user_info = load_user_info(require_live_value(config, "user"))
    if user_info["exchange"] == "bybit":
        from exchanges.bybit import BybitBot

        bot = BybitBot(config)
    elif user_info["exchange"] == "bitget":
        from exchanges.bitget import BitgetBot

        bot = BitgetBot(config)
    elif user_info["exchange"] == "binance":
        from exchanges.binance import BinanceBot

        bot = BinanceBot(config)
    elif user_info["exchange"] == "okx":
        from exchanges.okx import OKXBot

        bot = OKXBot(config)
    elif user_info["exchange"] == "hyperliquid":
        from exchanges.hyperliquid import HyperliquidBot

        bot = HyperliquidBot(config)
    elif user_info["exchange"] == "gateio":
        from exchanges.gateio import GateIOBot

        bot = GateIOBot(config)
    elif user_info["exchange"] == "defx":
        from exchanges.defx import DefxBot

        bot = DefxBot(config)
    elif user_info["exchange"] == "kucoin":
        from exchanges.kucoin import KucoinBot

        bot = KucoinBot(config)
    elif user_info["exchange"] == "paradex":
        from exchanges.paradex import ParadexBot

        bot = ParadexBot(config)
    elif user_info["exchange"] == "fake":
        from exchanges.fake import FakeBot

        bot = FakeBot(config)
    else:
        # 通用 CCXTBot，适用于任何 CCXT 支持的交易所
        from exchanges.ccxt_bot import CCXTBot

        bot = CCXTBot(config)
        logging.info(
            f"正在为 '{user_info['exchange']}' 使用通用 CCXTBot（无自定义实现）"
        )
    return bot


async def shutdown_bot(bot):
    """停止后台任务并优雅地关闭交易所客户端。"""
    print("Shutting down bot...")
    bot.stop_data_maintainers()
    try:
        await asyncio.wait_for(bot.close(), timeout=3.0)
    except asyncio.TimeoutError:
        print("Shutdown timed out after 3 seconds. Forcing exit.")
    except Exception as e:
        print(f"Error during shutdown: {e}")


async def main():
    """入口点：解析命令行参数、加载配置并启动机器人生命周期。"""
    raw_argv = sys.argv[1:]
    help_all = help_all_requested(raw_argv)
    parser = build_command_parser(
        prog=get_cli_prog("passivbot"),
        description="run passivbot",
        usage="%(prog)s [config_path] [options]",
        epilog=(
            "Examples:\n"
            "  passivbot live configs/live/my_account.json\n"
            "  passivbot live configs/live/my_account.json -s BTC,ETH --log-level info\n"
            "\n"
            "Use --help-all to show every config override flag."
        ),
    )
    parser.add_argument(
        "config_path",
        type=str,
        nargs="?",
        default=None,
        help="path to json/hjson passivbot config (defaults to in-code schema defaults if omitted)",
    )
    add_help_all_argument(
        parser,
        help_all=help_all,
        help_text="Show all live-trading override flags, including advanced config overrides.",
    )

    logging_group = parser.add_argument_group("Logging")
    logging_group.add_argument(
        "--log-level",
        dest="log_level",
        default=None,
        help="Logging verbosity (warning, info, debug, trace or 0-3).",
    )
    logging_group.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Enable verbose (debug) logging. Equivalent to --log-level debug.",
    )

    runtime_group = parser.add_argument_group("Runtime")
    runtime_group.add_argument(
        "--custom-endpoints",
        dest="custom_endpoints",
        default=None,
        help=(
            "Path to custom endpoints JSON for this run. "
            "Use 'none' to disable overrides even if a default file exists."
        ),
    )

    group_map = {
        "Coin Selection": parser.add_argument_group("Coin Selection"),
        "Behavior": parser.add_argument_group("Behavior"),
        "Runtime": runtime_group,
        "Logging": logging_group,
        "Advanced Overrides": parser.add_argument_group("Advanced Overrides"),
    }

    template_config = get_template_config()
    del template_config["optimize"]
    del template_config["backtest"]
    if "logging" in template_config and isinstance(template_config["logging"], dict):
        template_config["logging"].pop("level", None)
    allowed_config_keys = add_config_arguments(
        parser,
        template_config,
        command="live",
        help_all=help_all,
        group_map=group_map,
    )
    raw_args = merge_negative_cli_values(expand_help_all_argv(raw_argv))
    args = parser.parse_args(raw_args)
    # --verbose 标志覆盖 --log-level 为 debug（级别 2）
    cli_log_level = "debug" if args.verbose else args.log_level
    initial_log_level = resolve_log_level(cli_log_level, None, fallback=1)
    configure_logging(debug=initial_log_level)
    source_config, base_config_path, raw_snapshot = load_input_config(args.config_path)
    update_config_with_args(source_config, args, verbose=True, allowed_keys=allowed_config_keys)
    config = prepare_config(
        source_config,
        base_config_path=base_config_path,
        live_only=True,
        verbose=True,
        target="live",
        runtime="live",
        raw_snapshot=raw_snapshot,
    )
    config_logging_value = get_optional_config_value(config, "logging.level", None)
    effective_log_level = resolve_log_level(cli_log_level, config_logging_value, fallback=1)
    logging_section = config.get("logging")
    if not isinstance(logging_section, dict):
        logging_section = {}
    config["logging"] = logging_section
    logging_section["level"] = effective_log_level
    live_user = require_live_value(config, "user")
    log_file_settings = resolve_live_log_file_settings(
        config,
        user=live_user,
        command_args=[sys.argv[0], *raw_argv],
    )
    if effective_log_level != initial_log_level or log_file_settings["log_file"]:
        configure_logging(debug=effective_log_level, **log_file_settings)

    custom_endpoints_cli = args.custom_endpoints
    live_section = config.get("live") if isinstance(config.get("live"), dict) else {}
    custom_endpoints_cfg = live_section.get("custom_endpoints_path") if live_section else None

    override_path = None
    autodiscover = True
    preloaded_override = None

    def _sanitize(value):
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return "none"
            return stripped
        return str(value)

    cli_value = _sanitize(custom_endpoints_cli) if custom_endpoints_cli is not None else None
    cfg_value = _sanitize(custom_endpoints_cfg) if custom_endpoints_cfg is not None else None

    if cli_value is not None:
        if cli_value.lower() in {"none", "off", "disable"}:
            override_path = None
            autodiscover = False
            logging.info("Custom endpoints disabled via CLI argument.")
        else:
            override_path = cli_value
            autodiscover = False
            preloaded_override = load_custom_endpoint_config(override_path)
            logging.info("Using custom endpoints from CLI path: %s", override_path)
    elif cfg_value:
        if cfg_value.lower() in {"none", "off", "disable"}:
            override_path = None
            autodiscover = False
            logging.info("Custom endpoints disabled via config live.custom_endpoints_path.")
        else:
            override_path = cfg_value
            autodiscover = False
            preloaded_override = load_custom_endpoint_config(override_path)
            logging.info(
                "Using custom endpoints from config live.custom_endpoints_path: %s", override_path
            )
    else:
        logging.debug("Custom endpoints not specified; falling back to auto-discovery.")

    configure_custom_endpoint_loader(
        override_path,
        autodiscover=autodiscover,
        preloaded=preloaded_override,
    )

    user_info = load_user_info(live_user)
    # 现在已知交易所，使用交易所前缀重新配置日志
    exchange_prefix = user_info["exchange"]
    configure_logging(debug=effective_log_level, prefix=exchange_prefix, **log_file_settings)
    await load_markets(user_info["exchange"], verbose=True)

    config = parse_overrides(config, verbose=True)
    cooldown_secs = 60
    restarts = []
    while True:

        bot = setup_bot(config)
        globals()["bot"] = bot
        fatal_error = None
        try:
            await bot.start_bot()
        except FatalBotException as e:
            fatal_error = e
            logging.error(f"passivbot fatal error {e}")
        except Exception as e:
            logging.error(f"passivbot error {e}")
            traceback.print_exc()
        finally:
            try:
                if bot.stop_signal_received or getattr(bot, "_shutdown_in_progress", False):
                    shutdown_task = getattr(bot, "_shutdown_task", None)
                    if shutdown_task is not None:
                        await shutdown_task
                    else:
                        await bot.shutdown_gracefully()
                else:
                    bot.stop_data_maintainers()
                if bot.ccp is not None:
                    await bot.ccp.close()
                    bot.ccp = None
                if bot.cca is not None:
                    await bot.cca.close()
                    bot.cca = None
            except:
                pass
            if bot is not None and getattr(bot, "_shutdown_in_progress", False):
                logging.info("[%s] [shutdown] cleanup complete", getattr(bot, "exchange", "?"))
        if bot.stop_signal_received:
            logging.info("Bot stopped via signal; exiting main loop.")
            break
        if fatal_error is not None:
            break

        logging.info(f"restarting bot...")
        print()
        for z in range(cooldown_secs, -1, -1):
            if bot is not None and getattr(bot, "stop_signal_received", False):
                break
            print(f"\rcountdown {z}...  ")
            await asyncio.sleep(1)
        print()
        if bot is not None and getattr(bot, "stop_signal_received", False):
            logging.info("Bot stopped via signal during restart cooldown; exiting main loop.")
            break

        restarts.append(utc_ms())
        restarts = [x for x in restarts if x > utc_ms() - 1000 * 60 * 60 * 24]
        max_restarts = int(require_live_value(bot.config, "max_n_restarts_per_day"))
        if len(restarts) > max_restarts:
            logging.info(f"过去 24 小时重启次数超过 {max_restarts}")
            break


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot shutdown complete.")
