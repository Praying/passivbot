"""成交事件管理模块。

提供一个可复用的管理器，在本地缓存规范化的成交事件，
按需从交易所获取新数据，并暴露便捷的查询 API
（PnL 汇总、累计 PnL、最后成交时间戳等）。

目前实现了 Bitget 获取器；设计可扩展到其他交易所。
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import logging
import os
import random
import tempfile
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from importlib import import_module
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypedDict

from ccxt.base.errors import RateLimitExceeded
from config import load_input_config, prepare_config

try:
    from utils import ts_to_date  # type: ignore
except ImportError:  # pragma: no cover - fallback for package-relative execution
    from .utils import ts_to_date

from logging_setup import configure_logging
from procedures import load_user_info
from pure_funcs import ensure_millis

logger = logging.getLogger(__name__)

# 频繁警告的节流状态
_pnl_discrepancy_last_log: Dict[str, float] = {}  # exchange:user -> 上次日志时间
_pnl_discrepancy_last_delta: Dict[str, float] = {}  # exchange:user -> 上次差值
_PNL_DISCREPANCY_THROTTLE_SECONDS = 3600.0  # 如果差值未变，每小时最多记录一次
_PNL_DISCREPANCY_CHANGE_THRESHOLD = 0.10  # 差值变化超过10%视为"已变化"
_PNL_DISCREPANCY_MIN_SECONDS = 900.0  # 即使差值变化，日志之间的最小间隔秒数


# ---------------------------------------------------------------------------
# 速率限制协调
# ---------------------------------------------------------------------------

# 每个交易所的默认速率限制（每分钟调用次数）
_DEFAULT_RATE_LIMITS: Dict[str, Dict[str, int]] = {
    "binance": {"fetch_my_trades": 1200, "fetch_income_history": 120, "default": 1200},
    "bybit": {"fetch_my_trades": 120, "fetch_positions_history": 120, "default": 120},
    "bitget": {"fill_history": 120, "fetch_order": 60, "default": 120},
    "hyperliquid": {"fetch_my_trades": 120, "default": 120},
    "gateio": {"fetch_closed_orders": 120, "default": 120},
    "kucoin": {
        "fetch_my_trades": 120,
        "fetch_positions_history": 120,
        "fetch_order": 60,
        "default": 120,
    },
    # OKX: /fills = 60 req/2s, /fills-history = 10 req/2s（保守估计）
    "okx": {"fetch_my_trades": 1800, "fills_history": 300, "default": 300},
}

# 速率限制跟踪窗口（毫秒）
_RATE_LIMIT_WINDOW_MS = 60_000

# 启动时交错抖动的默认范围（秒）
_STARTUP_JITTER_MIN = 0.0
_STARTUP_JITTER_MAX = 30.0


class RateLimitCoordinator:
    """通过共享临时文件协调多个机器人实例的速率限制。

    每个交易所都有一个临时文件记录最近的 API 调用。实例在发起 API 调用前检查此文件，
    如果接近速率限制则添加抖动。
    """

    def __init__(
        self,
        exchange: str,
        user: str,
        *,
        temp_dir: Optional[Path] = None,
        window_ms: int = _RATE_LIMIT_WINDOW_MS,
        limits: Optional[Dict[str, int]] = None,
    ) -> None:
        """初始化速率限制协调器。"""
        self.exchange = exchange.lower()
        self.user = user
        self.window_ms = window_ms
        self.limits = limits or _DEFAULT_RATE_LIMITS.get(self.exchange, {"default": 120})

        if temp_dir is None:
            temp_dir = Path(tempfile.gettempdir()) / "passivbot_rate_limits"
        self.temp_dir = temp_dir
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.temp_file = self.temp_dir / f"{self.exchange}.json"

    def _load_calls(self) -> List[Dict[str, object]]:
        """从临时文件加载最近的 API 调用。"""
        if not self.temp_file.exists():
            return []
        try:
            with self.temp_file.open("r") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                try:
                    data = json.load(f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                return data.get("calls", [])
        except Exception as exc:
            logger.debug("RateLimitCoordinator: failed to load %s: %s", self.temp_file, exc)
            return []

    def _save_calls(self, calls: List[Dict[str, object]]) -> None:
        """原子性地将 API 调用保存到临时文件。"""
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        # 修剪旧条目
        cutoff = now_ms - self.window_ms
        calls = [c for c in calls if c.get("timestamp_ms", 0) > cutoff]

        data = {
            "calls": calls,
            "window_ms": self.window_ms,
            "limits": self.limits,
            "last_update": now_ms,
        }

        tmp_file = self.temp_file.with_suffix(".tmp")
        try:
            with tmp_file.open("w") as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                try:
                    json.dump(data, f)
                finally:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            os.replace(tmp_file, self.temp_file)
        except Exception as exc:
            logger.debug("RateLimitCoordinator: failed to save %s: %s", self.temp_file, exc)

    def get_current_usage(self, endpoint: str) -> int:
        """获取当前窗口内某个端点的调用次数。"""
        calls = self._load_calls()
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        cutoff = now_ms - self.window_ms
        return sum(
            1 for c in calls if c.get("endpoint") == endpoint and c.get("timestamp_ms", 0) > cutoff
        )

    def get_limit(self, endpoint: str) -> int:
        """获取某个端点的速率限制。"""
        return self.limits.get(endpoint, self.limits.get("default", 120))

    def record_call(self, endpoint: str) -> None:
        """记录一次 API 调用。"""
        calls = self._load_calls()
        calls.append(
            {
                "endpoint": endpoint,
                "timestamp_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                "user": self.user,
            }
        )
        self._save_calls(calls)

    async def wait_if_needed(self, endpoint: str) -> float:
        """检查速率限制，必要时等待。返回等待时间（秒）。"""
        current = self.get_current_usage(endpoint)
        limit = self.get_limit(endpoint)

        if current >= limit:
            # 达到或超过限制 - 等待完整窗口
            wait_time = self.window_ms / 1000.0
            logger.info(
                "RateLimitCoordinator: %s:%s at limit (%d/%d), waiting %.1fs",
                self.exchange,
                endpoint,
                current,
                limit,
                wait_time,
            )
            await asyncio.sleep(wait_time)
            return wait_time
        elif current >= limit * 0.8:
            # 接近限制 - 添加抖动
            jitter = random.uniform(0.1, 2.0)
            logger.debug(
                "RateLimitCoordinator: %s:%s approaching limit (%d/%d), jitter %.2fs",
                self.exchange,
                endpoint,
                current,
                limit,
                jitter,
            )
            await asyncio.sleep(jitter)
            return jitter

        return 0.0

    @staticmethod
    async def startup_jitter(
        min_seconds: float = _STARTUP_JITTER_MIN,
        max_seconds: float = _STARTUP_JITTER_MAX,
    ) -> float:
        """在启动时应用随机抖动，以错开多个机器人启动。"""
        jitter = random.uniform(min_seconds, max_seconds)
        if jitter > 0:
            logger.info("RateLimitCoordinator: startup jitter %.2fs", jitter)
            await asyncio.sleep(jitter)
        return jitter


def _format_ms(ts: Optional[int]) -> str:
    if ts is None:
        return "None"
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _day_key(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _merge_fee_lists(
    fees_a: Optional[Sequence], fees_b: Optional[Sequence]
) -> Optional[List[Dict[str, object]]]:
    """按币种合并两组手续费列表，cost 值累加。"""
    def to_list(fees):
        if not fees:
            return []
        if isinstance(fees, dict):
            return [fees]
        return list(fees)

    merged: Dict[str, Dict[str, object]] = {}
    for entry in to_list(fees_a) + to_list(fees_b):
        if not isinstance(entry, dict):
            continue
        currency = str(entry.get("currency") or entry.get("code") or "")
        if currency not in merged:
            merged[currency] = dict(entry)
            try:
                merged[currency]["cost"] = float(entry.get("cost", 0.0))
            except Exception:
                merged[currency]["cost"] = 0.0
        else:
            try:
                merged[currency]["cost"] += float(entry.get("cost", 0.0))
            except Exception:
                pass
    if not merged:
        return None
    return [dict(value) for value in merged.values()]


def _fee_cost(fees: Optional[Sequence]) -> float:
    """防御性地求和手续费成本，容忍缺失/不完整的结构。"""
    total = 0.0
    if not fees:
        return total
    items: Sequence
    if isinstance(fees, dict):
        items = [fees]
    else:
        try:
            items = list(fees)
        except Exception:
            return total
    for entry in items:
        if not isinstance(entry, dict):
            continue
        try:
            total += float(entry.get("cost", 0.0))
        except Exception:
            continue
    return total


def ensure_qty_signage(events: List[Dict[str, object]]) -> None:
    """标准化数量符号约定：买入为正，卖出为负。"""
    for ev in events:
        side = str(ev.get("side") or "").lower()
        qty = float(ev.get("qty") or ev.get("amount") or 0.0)
        if qty == 0.0:
            continue
        if side == "buy" and qty < 0:
            ev["qty"] = abs(qty)
        elif side == "sell" and qty > 0:
            ev["qty"] = -abs(qty)


def _compute_add_reduce(pos_side: str, qty_signed: float) -> Tuple[float, float]:
    """根据持仓方向和有符号数量计算加仓/减仓量。

    Args:
        pos_side: "long" 或 "short"
        qty_signed: 有符号数量（买入 +，卖出 -）

    Returns:
        (add_amt, reduce_amt) 元组
    """
    if pos_side == "short":
        add_amt = max(-qty_signed, 0.0)  # 卖出为负 -> 加仓
        reduce_amt = max(qty_signed, 0.0)  # 买入为正 -> 减空仓
    else:
        add_amt = max(qty_signed, 0.0)  # 买入加多仓
        reduce_amt = max(-qty_signed, 0.0)  # 卖出减多仓
    return add_amt, reduce_amt


def compute_psize_pprice(
    events: List[Dict[str, object]],
    initial_state: Optional[Dict[Tuple[str, str], Tuple[float, float]]] = None,
) -> Dict[Tuple[str, str], Tuple[float, float]]:
    """
    使用两阶段算法计算每个成交事件的 psize/pprice。

    阶段1：正向迭代计算最终持仓状态，存储每次成交前的状态供阶段2使用。
    阶段2：反向迭代为每个事件标注"成交后"状态。

    此方法比多遍对账更简洁，因为从已知最终状态反向工作是确定性的——无需对账。

    Args:
        events: 成交事件字典列表（必须包含: symbol, position_side, side, qty, price）
                数量符号必须已标准化（买入 +，卖出 -）。
        initial_state: 可选的起始持仓 {(symbol, pside): (size, price)}

    Returns:
        所有成交后的最终持仓状态: {(symbol, pside): (size, price)}
    """
    if not events:
        return {}

    # 按 (symbol, position_side) 分组事件
    grouped: Dict[Tuple[str, str], List[Dict[str, object]]] = defaultdict(list)
    for ev in events:
        key = (
            str(ev.get("symbol") or ""),
            str(ev.get("position_side") or ev.get("pside") or "long").lower(),
        )
        grouped[key].append(ev)

    final_state: Dict[Tuple[str, str], Tuple[float, float]] = {}

    for key, evs in grouped.items():
        evs.sort(key=lambda x: x.get("timestamp", 0))

        # 阶段1：正向计算最终状态，存储每次成交前的状态
        psize = initial_state.get(key, (0.0, 0.0))[0] if initial_state else 0.0
        pprice = initial_state.get(key, (0.0, 0.0))[1] if initial_state else 0.0

        # 为每次成交存储 (before_psize, before_pprice, after_psize, after_pprice)
        states: List[Tuple[float, float, float, float]] = []

        for ev in evs:
            qty_signed = float(ev.get("qty") or ev.get("amount") or 0.0) * float(
                ev.get("c_mult", 1.0) or 1.0
            )
            price = float(ev.get("price") or 0.0)
            add_amt, reduce_amt = _compute_add_reduce(key[1], qty_signed)

            before_psize = psize
            before_pprice = pprice

            # 更新持仓
            if add_amt > 0:
                if psize <= 0:
                    pprice = price
                else:
                    pprice = ((psize * pprice) + (add_amt * price)) / (psize + add_amt)
                psize += add_amt
            if reduce_amt > 0:
                psize = max(0.0, psize - reduce_amt)
                if psize <= 1e-12:
                    psize = 0.0
                    pprice = 0.0

            states.append((before_psize, before_pprice, psize, pprice))

        final_state[key] = (psize, pprice)

        # 阶段2：为每个事件标注成交后状态
        # states 列表已包含每次成交的成交后状态
        for ev, (_, _, after_psize, after_pprice) in zip(evs, states):
            ev["psize"] = round(after_psize, 12)
            ev["pprice"] = after_pprice

    return final_state


def annotate_positions_inplace(
    events: List[Dict[str, object]],
    state: Optional[Dict[Tuple[str, str], Tuple[float, float]]] = None,
    *,
    recompute_pnl: bool = False,
) -> Dict[Tuple[str, str], Tuple[float, float]]:
    """
    compute_psize_pprice 的旧版包装器，用于向后兼容。

    注意：简化算法中不再支持 recompute_pnl。
    获取器负责在获取期间计算正确的 PnL 值。
    """
    if recompute_pnl:
        logger.warning("annotate_positions_inplace: recompute_pnl=True is deprecated and ignored")
    return compute_psize_pprice(events, state)


def compute_realized_pnls_from_trades(
    trades: List[Dict[str, object]],
) -> Tuple[Dict[str, float], Dict[Tuple[str, str], Tuple[float, float]]]:
    """
    通过从成交重建持仓来计算每笔交易的已实现 PnL。

    按 (symbol, position_side) 分别跟踪持仓，以便对冲的多/空头寸互不干扰。
    Position_size 始终保持为给定方向的正数绝对值；减仓触发已实现 PnL。

    Returns:
        per_trade_pnl: 映射 trade_id -> 已实现 pnl（总值，不含手续费）
        final_positions: 映射 (symbol, position_side) -> (pos_size, vwap)
    """
    per_trade: Dict[str, float] = {}
    positions: Dict[Tuple[str, str], Tuple[float, float]] = {}

    for trade in sorted(trades, key=lambda x: x.get("timestamp", 0)):
        trade_id = str(trade.get("id") or "")
        if not trade_id:
            continue
        symbol = str(trade.get("symbol") or "")
        side = str(trade.get("side") or "").lower()
        pos_side = str(trade.get("position_side") or trade.get("pside") or "long").lower()
        qty = abs(float(trade.get("qty") or trade.get("amount") or 0.0))
        price = float(trade.get("price") or 0.0)
        if qty <= 0 or price <= 0 or not symbol:
            per_trade[trade_id] = 0.0
            continue

        key = (symbol, pos_side)
        pos_size, vwap = positions.get(key, (0.0, 0.0))

        # 判断此交易对该方向是加仓还是减仓
        if pos_side == "short":
            adds = side == "sell"
        else:  # long or unknown
            adds = side == "buy"

        realized = 0.0
        if not adds:
            # 减仓
            if pos_size > 0:
                closing_qty = min(pos_size, qty)
                if pos_side == "short":
                    realized += (vwap - price) * closing_qty
                else:
                    realized += (price - vwap) * closing_qty
                pos_size -= closing_qty
                if pos_size < 1e-12:
                    pos_size = 0.0
                    vwap = 0.0
                leftover = qty - closing_qty
                if leftover > 0:
                    # 交易超出并成为交易方向的新持仓
                    pos_size = leftover
                    vwap = price
        else:
            # 加仓
            new_size = pos_size + qty
            if pos_size == 0.0:
                vwap = price
            else:
                vwap = ((pos_size * vwap) + (qty * price)) / (pos_size + qty)
            pos_size = new_size

        positions[key] = (pos_size, vwap)
        per_trade[trade_id] = realized

    return per_trade, positions


def _coalesce_events(events: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """将共享 timestamp/symbol/pb_type/side/position 的事件合并分组。"""
    aggregated: Dict[Tuple, Dict[str, object]] = {}
    order: List[Tuple] = []

    def _event_source_ids(ev: Dict[str, object]) -> List[str]:
        ids = ev.get("source_ids")
        if ids:
            return [str(x) for x in ids if x]
        return []

    for ev in events:
        key = (
            ev.get("timestamp"),
            ev.get("symbol"),
            ev.get("pb_order_type"),
            ev.get("side"),
            ev.get("position_side"),
        )
        if key not in aggregated:
            aggregated[key] = dict(ev)
            aggregated[key]["id"] = str(ev.get("id", ""))
            src_ids = _event_source_ids(ev)
            if src_ids:
                aggregated[key]["source_ids"] = src_ids
            aggregated[key]["qty"] = float(ev.get("qty", 0.0))
            aggregated[key]["pnl"] = float(ev.get("pnl", 0.0))
            aggregated[key]["fees"] = _merge_fee_lists(ev.get("fees"), None)
            aggregated[key]["raw"] = _normalize_raw_field(ev.get("raw"))
            aggregated[key]["_price_numerator"] = float(ev.get("price", 0.0)) * float(
                ev.get("qty", 0.0)
            )
            order.append(key)
        else:
            agg = aggregated[key]
            agg["id"] = f"{agg['id']}+{ev.get('id', '')}".strip("+")
            src_ids = _event_source_ids(ev)
            if src_ids:
                merged_ids = set(agg.get("source_ids") or [])
                merged_ids.update(src_ids)
                agg["source_ids"] = sorted(merged_ids)
            agg["qty"] = float(agg.get("qty", 0.0)) + float(ev.get("qty", 0.0))
            agg["pnl"] = float(agg.get("pnl", 0.0)) + float(ev.get("pnl", 0.0))
            agg["fees"] = _merge_fee_lists(agg.get("fees"), ev.get("fees"))
            agg["raw"] = _normalize_raw_field(agg.get("raw")) + _normalize_raw_field(ev.get("raw"))
            agg["_price_numerator"] = float(agg.get("_price_numerator", 0.0)) + float(
                ev.get("price", 0.0)
            ) * float(ev.get("qty", 0.0))
            if not agg.get("client_order_id") and ev.get("client_order_id"):
                agg["client_order_id"] = ev.get("client_order_id")
            if not agg.get("pb_order_type"):
                agg["pb_order_type"] = ev.get("pb_order_type")
    coalesced: List[Dict[str, object]] = []
    for key in order:
        agg = aggregated[key]
        qty = float(agg.get("qty", 0.0))
        price_numerator = float(agg.get("_price_numerator", 0.0))
        if qty > 0:
            agg["price"] = price_numerator / qty
        agg.pop("_price_numerator", None)
        fees = agg.get("fees")
        if isinstance(fees, list) and len(fees) == 1:
            agg["fees"] = fees[0]
        coalesced.append(agg)
    return coalesced


def _check_pagination_progress(
    previous: Optional[Tuple[Tuple[str, object], ...]],
    params: Dict[str, object],
    context: str,
) -> Optional[Tuple[Tuple[str, object], ...]]:
    params_key = tuple(sorted(params.items()))
    if previous == params_key:
        logger.warning(
            "%s: repeated params detected; aborting pagination (%s)",
            context,
            dict(params),
        )
        return None
    logger.debug("%s: fetching with params %s", context, dict(params))
    return params_key


# ---------------------------------------------------------------------------
# 数据模型
# ---------------------------------------------------------------------------


def _normalize_raw_field(raw: object) -> List[Dict[str, object]]:
    """将 raw 字段标准化为 List[Dict] 格式。

    处理从旧的 Dict 格式到新的 List[Dict] 格式的迁移。
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        # 已经是新格式 - 验证后返回
        return [dict(item) if isinstance(item, dict) else {"data": item} for item in raw]
    if isinstance(raw, dict):
        # 旧格式：单个字典 -> 包装为带 "legacy" 来源的列表
        return [{"source": "legacy", "data": raw}]
    # 未知格式
    return [{"source": "unknown", "data": str(raw)}]


def _extract_source_ids(raw: object, fallback_id: Optional[object]) -> List[str]:
    """从原始载荷中提取稳定的来源 ID，回退到事件 id。"""
    ids: set[str] = set()
    raw_items = _normalize_raw_field(raw)
    for item in raw_items:
        data = item.get("data") if isinstance(item, dict) else item
        if isinstance(data, dict):
            # 优先使用规范交易 id（如果存在）
            for key in ("id", "tradeId", "trade_id", "execId"):
                val = data.get(key)
                if val:
                    ids.add(str(val))
            info = data.get("info")
            if isinstance(info, dict):
                for key in ("tid", "id", "tradeId", "trade_id", "execId"):
                    val = info.get(key)
                    if val:
                        ids.add(str(val))
    if not ids and fallback_id:
        ids.add(str(fallback_id))
    return sorted(ids)


def _bybit_trade_dedupe_key(trade: Dict[str, object]) -> Optional[Tuple[object, ...]]:
    """为 Bybit fetch_my_trades 行构建稳定的去重键。"""
    info = trade.get("info")
    info = info if isinstance(info, dict) else {}
    exec_id = trade.get("id") or info.get("execId")
    if exec_id:
        return ("exec_id", str(exec_id))
    # 缺少显式 exec id 的畸形行的回退。
    timestamp = int(trade.get("timestamp") or info.get("execTime") or 0)
    symbol = str(trade.get("symbol") or info.get("symbol") or "")
    side = str(trade.get("side") or info.get("side") or "").lower()
    order_id = str(trade.get("order") or info.get("orderId") or "")
    amount = float(trade.get("amount") or info.get("execQty") or 0.0)
    price = float(trade.get("price") or info.get("execPrice") or 0.0)
    if timestamp <= 0 or not symbol or not side or not order_id or amount <= 0.0 or price <= 0.0:
        return None
    return ("fallback", timestamp, symbol, side, order_id, amount, price)


def _bybit_trade_qty_abs(trade: Dict[str, object]) -> float:
    info = trade.get("info")
    info = info if isinstance(info, dict) else {}
    return abs(float(trade.get("amount") or info.get("execQty") or 0.0))


def _bybit_trade_qty_signed(trade: Dict[str, object]) -> float:
    info = trade.get("info")
    info = info if isinstance(info, dict) else {}
    side = str(trade.get("side") or info.get("side") or "").lower()
    qty = _bybit_trade_qty_abs(trade)
    if side == "sell":
        return -qty
    return qty


def _bybit_event_group_key(event: FillEvent) -> Tuple[int, str, str, str, str]:
    return (
        int(event.timestamp),
        str(event.symbol),
        str(event.pb_order_type),
        str(event.side).lower(),
        str(event.position_side).lower(),
    )


@dataclass(frozen=True)
class FillEvent:
    """单个成交事件的规范表示。"""

    id: str
    timestamp: int
    datetime: str
    symbol: str
    side: str
    qty: float
    price: float
    pnl: float
    fees: Optional[Sequence]
    pb_order_type: str
    position_side: str
    client_order_id: str
    source_ids: List[str] = field(default_factory=list)
    psize: float = 0.0
    pprice: float = 0.0
    raw: List[Dict[str, object]] = None  # 来自多个来源的原始载荷列表

    @property
    def key(self) -> str:
        return self.id

    def to_dict(self) -> Dict[str, object]:
        """将成交事件序列化为字典。"""
        return {
            "id": self.id,
            "source_ids": list(self.source_ids) if self.source_ids is not None else [],
            "timestamp": self.timestamp,
            "datetime": self.datetime,
            "symbol": self.symbol,
            "side": self.side,
            "qty": self.qty,
            "price": self.price,
            "pnl": self.pnl,
            "fees": self.fees,
            "pb_order_type": self.pb_order_type,
            "position_side": self.position_side,
            "client_order_id": self.client_order_id,
            "psize": self.psize,
            "pprice": self.pprice,
            "raw": self.raw if self.raw is not None else [],
        }

    @classmethod
    def from_dict(cls, data: Dict[str, object]) -> "FillEvent":
        """从字典反序列化为 FillEvent 实例。"""
        required = [
            "id",
            "timestamp",
            "symbol",
            "side",
            "qty",
            "price",
            "pnl",
            "pb_order_type",
            "position_side",
            "client_order_id",
        ]
        missing = [key for key in required if key not in data]
        if missing:
            raise ValueError(f"Fill event missing required keys: {missing}")
        return cls(
            id=str(data["id"]),
            source_ids=(
                _extract_source_ids(data.get("raw"), data.get("id"))
                if not data.get("source_ids")
                else [str(x) for x in data.get("source_ids") if x]
            ),
            timestamp=int(data["timestamp"]),
            datetime=str(data.get("datetime") or ts_to_date(int(data["timestamp"]))),
            symbol=str(data["symbol"]),
            side=str(data["side"]).lower(),
            qty=float(data["qty"]),
            price=float(data["price"]),
            pnl=float(data["pnl"]),
            fees=data.get("fees"),
            pb_order_type=str(data["pb_order_type"]),
            position_side=str(data["position_side"]).lower(),
            client_order_id=str(data["client_order_id"]),
            psize=float(data.get("psize", 0.0)),
            pprice=float(data.get("pprice", 0.0)),
            raw=_normalize_raw_field(data.get("raw")),
        )


# ---------------------------------------------------------------------------
# 缓存
# ---------------------------------------------------------------------------

# 将间隙标记为持久之前的最大重试次数
_GAP_MAX_RETRIES = 3

# 间隙置信度级别
GAP_CONFIDENCE_UNKNOWN = 0.0
GAP_CONFIDENCE_SUSPICIOUS = 0.3
GAP_CONFIDENCE_LIKELY_LEGITIMATE = 0.7
GAP_CONFIDENCE_CONFIRMED = 1.0

# 间隙原因
GAP_REASON_AUTO = "auto_detected"
GAP_REASON_FETCH_FAILED = "fetch_failed"
GAP_REASON_CONFIRMED = "confirmed_legitimate"
GAP_REASON_MANUAL = "manual"


class KnownGap(TypedDict, total=False):
    """存储在 metadata.json known_gaps 中的间隙元数据。"""

    start_ts: int  # 间隙开始时间戳（毫秒）
    end_ts: int  # 间隙结束时间戳（毫秒）
    retry_count: int  # 获取尝试次数（最多3次）
    reason: str  # auto_detected, fetch_failed, confirmed_legitimate, manual
    added_at: int  # 间隙首次检测到的时间戳
    confidence: float  # 0.0=未知, 0.3=可疑, 0.7=可能正常, 1.0=已确认


class CacheMetadata(TypedDict, total=False):
    """存储在 metadata.json 中的缓存元数据。"""

    last_refresh_ms: int  # 上次成功刷新的时间戳
    oldest_event_ts: int  # 缓存中最旧事件的时间戳
    newest_event_ts: int  # 缓存中最新事件的时间戳
    covered_start_ms: int  # 已确认的最早开放式回溯起始时间
    known_gaps: List[KnownGap]  # 已知间隙列表
    history_scope: str  # unknown, window, all


class FillEventCache:
    """按 UTC 日期分割存储成交的 JSON 缓存。"""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._metadata: Optional[CacheMetadata] = None

    def load(self) -> List[FillEvent]:
        """从磁盘加载所有 JSON 日志文件中的成交事件。"""
        files = sorted(self.root.glob("*.json"))
        events: List[FillEvent] = []
        for path in files:
            try:
                with path.open("r", encoding="utf-8") as fh:
                    payload = json.load(fh) or []
            except Exception as exc:
                logger.warning("[fills] cache load: failed to read %s (%s)", path, exc)
                continue
            for raw in payload:
                try:
                    events.append(FillEvent.from_dict(raw))
                except Exception:
                    logger.debug("[fills] cache load: skipping malformed record in %s", path)
        events.sort(key=lambda ev: ev.timestamp)
        logger.debug(
            "[fills] cache loaded: %d events from %d files in %s",
            len(events),
            len(files),
            self.root,
        )
        return events

    def save(self, events: Sequence[FillEvent]) -> None:
        day_map: Dict[str, List[FillEvent]] = defaultdict(list)
        for event in events:
            day_map[_day_key(event.timestamp)].append(event)
        for day in day_map:
            day_map[day].sort(key=lambda ev: ev.timestamp)
        self.save_days(day_map)

    def save_days(self, day_events: Dict[str, Sequence[FillEvent]]) -> None:
        """按天原子性地保存成交事件到独立的 JSON 文件。"""
        for day, events in day_events.items():
            path = self.root / f"{day}.json"
            payload = [event.to_dict() for event in sorted(events, key=lambda ev: ev.timestamp)]
            if path.exists():
                try:
                    with path.open("r", encoding="utf-8") as fh:
                        current = json.load(fh)
                except Exception:
                    current = None
                if current == payload:
                    logger.debug("FillEventCache.save_days: %s unchanged", path.name)
                    continue
            tmp_path = path.with_suffix(".tmp")
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp_path, path)
            logger.debug(
                "[fills] cache wrote %d events to %s",
                len(payload),
                path.name,
            )

    @property
    def metadata_path(self) -> Path:
        return self.root / "metadata.json"

    def load_metadata(self) -> CacheMetadata:
        """从磁盘加载缓存元数据。"""
        if self._metadata is not None:
            return self._metadata

        default: CacheMetadata = {
            "last_refresh_ms": 0,
            "oldest_event_ts": 0,
            "newest_event_ts": 0,
            "covered_start_ms": 0,
            "known_gaps": [],
            "history_scope": "unknown",
        }

        if not self.metadata_path.exists():
            self._metadata = default
            return self._metadata

        try:
            with self.metadata_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
            if not isinstance(data, dict):
                data = default
            # 确保所有键存在
            for key in default:
                data.setdefault(key, default[key])
            self._metadata = data
        except Exception as exc:
            logger.warning("[fills] cache metadata: failed to read %s (%s)", self.metadata_path, exc)
            self._metadata = default

        return self._metadata

    def save_metadata(self, metadata: Optional[CacheMetadata] = None) -> None:
        """原子性地将缓存元数据保存到磁盘。"""
        if metadata is not None:
            self._metadata = metadata

        if self._metadata is None:
            return

        tmp_path = self.metadata_path.with_suffix(".tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(self._metadata, fh, indent=2)
            os.replace(tmp_path, self.metadata_path)
            logger.debug("FillEventCache.save_metadata: wrote to %s", self.metadata_path)
        except Exception as exc:
            logger.error(
                "FillEventCache.save_metadata: failed to write %s (%s)", self.metadata_path, exc
            )

    def update_metadata_from_events(self, events: Sequence[FillEvent]) -> None:
        """根据事件更新元数据时间戳。"""
        if not events:
            return

        metadata = self.load_metadata()
        timestamps = [ev.timestamp for ev in events]
        oldest = min(timestamps)
        newest = max(timestamps)

        current_oldest = metadata.get("oldest_event_ts", 0)
        current_newest = metadata.get("newest_event_ts", 0)

        if current_oldest == 0 or oldest < current_oldest:
            metadata["oldest_event_ts"] = oldest
        if newest > current_newest:
            metadata["newest_event_ts"] = newest

        metadata["last_refresh_ms"] = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        self.save_metadata(metadata)

    def get_known_gaps(self) -> List[KnownGap]:
        """返回已知间隙列表。"""
        return self.load_metadata().get("known_gaps", [])

    def get_covered_start_ms(self) -> int:
        """返回已确认的最早开放式回溯起始时间。"""
        metadata = self.load_metadata()
        return int(metadata.get("covered_start_ms", 0) or 0)

    def mark_covered_start(self, start_ts: int) -> None:
        """持久化已确认的最早开放式回溯起始时间。"""
        metadata = self.load_metadata()
        start_ts = int(start_ts)
        current = int(metadata.get("covered_start_ms", 0) or 0)
        if current == 0 or start_ts < current:
            metadata["covered_start_ms"] = start_ts
        metadata["last_refresh_ms"] = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        self.save_metadata(metadata)

    def get_history_scope(self) -> str:
        """返回缓存的历史覆盖范围约定。"""
        scope = str(self.load_metadata().get("history_scope", "unknown") or "unknown").lower()
        return scope if scope in {"unknown", "window", "all"} else "unknown"

    def set_history_scope(self, scope: str) -> None:
        """持久化缓存历史覆盖范围约定。"""
        normalized = str(scope or "unknown").lower()
        if normalized not in {"unknown", "window", "all"}:
            raise ValueError(f"invalid history scope {scope!r}")
        metadata = self.load_metadata()
        if metadata.get("history_scope") == normalized:
            return
        metadata["history_scope"] = normalized
        self.save_metadata(metadata)

    def add_known_gap(
        self,
        start_ts: int,
        end_ts: int,
        *,
        reason: str = GAP_REASON_AUTO,
        confidence: float = GAP_CONFIDENCE_UNKNOWN,
    ) -> None:
        """添加或更新已知间隙。"""
        metadata = self.load_metadata()
        gaps = metadata.get("known_gaps", [])
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        # 检查重叠间隙以更新
        for gap in gaps:
            if gap["start_ts"] <= end_ts and gap["end_ts"] >= start_ts:
                # 重叠 - 合并
                gap["start_ts"] = min(gap["start_ts"], start_ts)
                gap["end_ts"] = max(gap["end_ts"], end_ts)
                gap["retry_count"] = gap.get("retry_count", 0) + 1
                if gap["retry_count"] >= _GAP_MAX_RETRIES:
                    gap["confidence"] = max(
                        gap.get("confidence", 0), GAP_CONFIDENCE_LIKELY_LEGITIMATE
                    )
                logger.info(
                    "FillEventCache.add_known_gap: updated gap %s → %s (retry_count=%d)",
                    _format_ms(gap["start_ts"]),
                    _format_ms(gap["end_ts"]),
                    gap["retry_count"],
                )
                self.save_metadata(metadata)
                return

        # 新间隙
        new_gap: KnownGap = {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "retry_count": 0,
            "reason": reason,
            "added_at": now_ms,
            "confidence": confidence,
        }
        gaps.append(new_gap)
        metadata["known_gaps"] = gaps
        logger.info(
            "FillEventCache.add_known_gap: added new gap %s → %s (reason=%s)",
            _format_ms(start_ts),
            _format_ms(end_ts),
            reason,
        )
        self.save_metadata(metadata)

    def clear_gap(self, start_ts: int, end_ts: int) -> bool:
        """移除已填充的间隙。如果移除了间隙则返回 True。"""
        metadata = self.load_metadata()
        gaps = metadata.get("known_gaps", [])
        original_count = len(gaps)

        # 移除完全包含在已填充范围内的间隙
        remaining = []
        for gap in gaps:
            if gap["start_ts"] >= start_ts and gap["end_ts"] <= end_ts:
                logger.info(
                    "FillEventCache.clear_gap: removed gap %s → %s",
                    _format_ms(gap["start_ts"]),
                    _format_ms(gap["end_ts"]),
                )
                continue
            # 部分重叠 - 裁剪间隙
            if gap["start_ts"] < start_ts < gap["end_ts"]:
                gap["end_ts"] = start_ts
            if gap["start_ts"] < end_ts < gap["end_ts"]:
                gap["start_ts"] = end_ts
            if gap["start_ts"] < gap["end_ts"]:
                remaining.append(gap)

        if len(remaining) != original_count:
            metadata["known_gaps"] = remaining
            self.save_metadata(metadata)
            return True
        return False

    def should_retry_gap(self, gap: KnownGap) -> bool:
        """检查间隙是否应重试（retry_count < max）。"""
        return gap.get("retry_count", 0) < _GAP_MAX_RETRIES

    def get_coverage_summary(self) -> Dict[str, object]:
        """返回缓存覆盖摘要用于调试。"""
        metadata = self.load_metadata()
        gaps = metadata.get("known_gaps", [])

        persistent_gaps = [g for g in gaps if not self.should_retry_gap(g)]
        retryable_gaps = [g for g in gaps if self.should_retry_gap(g)]

        total_gap_ms = sum(g["end_ts"] - g["start_ts"] for g in gaps)

        return {
            "oldest_event_ts": metadata.get("oldest_event_ts", 0),
            "newest_event_ts": metadata.get("newest_event_ts", 0),
            "covered_start_ms": metadata.get("covered_start_ms", 0),
            "last_refresh_ms": metadata.get("last_refresh_ms", 0),
            "history_scope": self.get_history_scope(),
            "total_gaps": len(gaps),
            "persistent_gaps": len(persistent_gaps),
            "retryable_gaps": len(retryable_gaps),
            "total_gap_hours": total_gap_ms / (1000 * 60 * 60) if total_gap_ms > 0 else 0,
            "gaps": [
                {
                    "start": _format_ms(g["start_ts"]),
                    "end": _format_ms(g["end_ts"]),
                    "retry_count": g.get("retry_count", 0),
                    "reason": g.get("reason", "unknown"),
                    "confidence": g.get("confidence", 0),
                }
                for g in gaps
            ],
        }


# ---------------------------------------------------------------------------
# 获取器基础设施
# ---------------------------------------------------------------------------


class BaseFetcher:
    """交易所特定成交获取器的抽象接口。"""

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        raise NotImplementedError


class FakeFetcher(BaseFetcher):
    """从模拟交易所账本获取规范成交事件。"""

    def __init__(self, api) -> None:
        self.api = api

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        """从模拟交易所账本获取成交事件。"""
        events = list(self.api.get_fill_events(since_ms, until_ms))
        for event in events:
            cache_entry = detail_cache.get(event["id"])
            if cache_entry:
                event["client_order_id"], event["pb_order_type"] = cache_entry
            elif event["client_order_id"]:
                event["pb_order_type"] = custom_id_to_snake(event["client_order_id"])
            if not event["pb_order_type"]:
                event["pb_order_type"] = "unknown"
        if on_batch and events:
            on_batch(events)
        return events


class BitgetFetcher(BaseFetcher):
    """从 Bitget 获取并丰富成交事件。"""

    def __init__(
        self,
        api,
        *,
        product_type: str = "USDT-FUTURES",
        history_limit: int = 100,
        detail_calls_per_minute: int = 120,
        detail_concurrency: int = 10,
        now_func: Optional[Callable[[], int]] = None,
        symbol_resolver: Optional[Callable[[Optional[str]], str]] = None,
    ) -> None:
        """初始化 Bitget 获取器。"""
        self.api = api
        self.product_type = product_type
        self.history_limit = history_limit
        self.detail_calls_per_minute = max(1, detail_calls_per_minute)
        self._detail_call_timestamps: deque[int] = deque()
        self.detail_concurrency = max(1, detail_concurrency)
        self._rate_lock = asyncio.Lock()
        self._now_func = now_func or (lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000))
        if symbol_resolver is None:
            raise ValueError("BitgetFetcher requires a symbol_resolver callable")
        self._symbol_resolver = symbol_resolver

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        """从 Bitget 获取成交历史并富化订单详情。"""
        buffer_step_ms = 24 * 60 * 60 * 1000
        end_time = int(until_ms) if until_ms is not None else self._now_func() + buffer_step_ms
        params: Dict[str, object] = {
            "productType": self.product_type,
            "limit": self.history_limit,
            "endTime": end_time,
        }
        events: Dict[str, Dict[str, object]] = {}

        detail_hits = 0
        detail_fetches = 0
        max_fetches = 400
        fetch_count = 0

        logger.debug(
            "BitgetFetcher.fetch: start (since=%s, until=%s, limit=%d)",
            _format_ms(since_ms),
            _format_ms(until_ms),
            self.history_limit,
        )

        while True:
            if fetch_count >= max_fetches:
                logger.warning(
                    "BitgetFetcher.fetch: reached maximum pagination depth (%d)",
                    max_fetches,
                )
                break
            fetch_count += 1
            payload = await self.api.private_mix_get_v2_mix_order_fill_history(dict(params))
            fill_list = payload.get("data", {}).get("fillList") or []
            if fetch_count > 1:
                logger.debug(
                    "BitgetFetcher.fetch: fetch #%d endTime=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("endTime")),
                    len(fill_list),
                )
            if not fill_list:
                if since_ms is None:
                    logger.debug("BitgetFetcher.fetch: empty batch without start bound; stopping")
                    break
                end_param = int(params.get("endTime", self._now_func()))
                if end_param <= since_ms:
                    logger.debug(
                        "BitgetFetcher.fetch: empty batch and cursor reached start; stopping"
                    )
                    break
                new_end_time = max(since_ms, end_param - buffer_step_ms)
                if new_end_time == end_param:
                    new_end_time = max(since_ms, end_param - 1)
                params["endTime"] = new_end_time
                logger.debug(
                    "BitgetFetcher.fetch: empty batch, continuing with endTime=%s",
                    _format_ms(params["endTime"]),
                )
                continue
            logger.debug(
                "BitgetFetcher.fetch: received batch size=%d endTime=%s",
                len(fill_list),
                params.get("endTime"),
            )
            batch_ids: List[str] = []
            pending_tasks: List[asyncio.Task[int]] = []
            for raw in fill_list:
                event = self._normalize_fill(raw)
                event_id = event["id"]
                if not event_id:
                    continue
                batch_ids.append(event_id)
                if event_id in detail_cache:
                    client_oid, pb_type = detail_cache[event_id]
                    event["client_order_id"] = client_oid
                    event["pb_order_type"] = pb_type
                    detail_hits += 1
                if not event.get("client_order_id"):
                    pending_tasks.append(
                        asyncio.create_task(self._enrich_with_details(event, detail_cache))
                    )
                    if len(pending_tasks) >= self.detail_concurrency:
                        detail_fetches += await self._flush_detail_tasks(pending_tasks)
                events[event_id] = event
            detail_fetches += await self._flush_detail_tasks(pending_tasks)
            if on_batch:
                batch_events = [
                    dict(events[event_id])
                    for event_id in batch_ids
                    if events[event_id].get("client_order_id")
                ]
                if batch_events:
                    on_batch(batch_events)
            oldest = min(int(raw["cTime"]) for raw in fill_list)
            if len(fill_list) < self.history_limit:
                if since_ms is None:
                    logger.debug(
                        "BitgetFetcher.fetch: short batch size=%d without start bound; stopping",
                        len(fill_list),
                    )
                    break
                end_param = int(params.get("endTime", oldest))
                if end_param - since_ms < buffer_step_ms:
                    logger.debug(
                        "BitgetFetcher.fetch: short batch size=%d close to requested start; stopping",
                        len(fill_list),
                    )
                    break
                new_end_time = max(since_ms, min(end_param, oldest) - 1)
                if new_end_time <= since_ms:
                    logger.debug(
                        "BitgetFetcher.fetch: rewound endTime to start boundary; stopping",
                    )
                    break
                params["endTime"] = new_end_time
                logger.debug(
                    "BitgetFetcher.fetch: short batch size=%d, continuing with endTime=%s",
                    len(fill_list),
                    _format_ms(params["endTime"]),
                )
                continue
            first_ts = min(ev["timestamp"] for ev in events.values()) if events else None
            if since_ms is not None and first_ts is not None and first_ts <= since_ms:
                break
            params["endTime"] = max(since_ms or oldest, oldest - 1)

        ordered = sorted(events.values(), key=lambda ev: ev["timestamp"])
        if since_ms is not None:
            ordered = [ev for ev in ordered if ev["timestamp"] >= since_ms]
        if until_ms is not None:
            ordered = [ev for ev in ordered if ev["timestamp"] <= until_ms]
        logger.debug(
            "BitgetFetcher.fetch: done (events=%d, detail_cache_hits=%d, detail_fetches=%d)",
            len(ordered),
            detail_hits,
            detail_fetches,
        )
        return ordered

    async def _enrich_with_details(
        self,
        event: Dict[str, object],
        cache: Dict[str, Tuple[str, str]],
    ) -> int:
        """获取 Bitget 订单详情以填充 client_order_id 和 pb_order_type。"""
        if not event.get("order_id"):
            return 0
        logger.debug(
            "BitgetFetcher._enrich_with_details: fetching detail for order %s %s",
            event["order_id"],
            event.get("datetime"),
        )
        await self._respect_rate_limit()
        order_details = await self.api.private_mix_get_v2_mix_order_detail(
            {
                "productType": self.product_type,
                "orderId": event["order_id"],
                "symbol": event["symbol_external"],
            }
        )
        client_oid = (
            order_details.get("data", {}).get("clientOid")
            if isinstance(order_details, dict)
            else None
        )
        if client_oid:
            pb_type = custom_id_to_snake(client_oid)
            event["client_order_id"] = client_oid
            event["pb_order_type"] = pb_type
            cache[event["id"]] = (client_oid, pb_type)
            logger.debug(
                "BitgetFetcher._enrich_with_details: cached clientOid=%s for trade %s, pb_order_type %s",
                client_oid,
                event["id"],
                pb_type,
            )
            return 1
        else:
            logger.debug(
                "BitgetFetcher._enrich_with_details: no clientOid returned for order %s",
                event["order_id"],
            )
            return 1

    async def _respect_rate_limit(self) -> None:
        """滑动窗口限速：若窗口内调用已达上限则等待。"""
        window_ms = 60_000
        max_calls = self.detail_calls_per_minute
        q = self._detail_call_timestamps
        while True:
            async with self._rate_lock:
                now = self._now_func()
                window_start = now - window_ms
                while q and q[0] <= window_start:
                    q.popleft()
                if len(q) < max_calls:
                    q.append(now)
                    return
                wait_ms = q[0] + window_ms - now
            if wait_ms > 0:
                logger.debug(
                    "BitgetFetcher._respect_rate_limit: sleeping %.3fs to respect %d calls/min",
                    wait_ms / 1000,
                    max_calls,
                )
                await asyncio.sleep(wait_ms / 1000)
            else:
                await asyncio.sleep(0)

    async def _flush_detail_tasks(self, tasks: List[asyncio.Task[int]]) -> int:
        if not tasks:
            return 0
        results = await asyncio.gather(*tasks, return_exceptions=True)
        tasks.clear()
        total = 0
        for res in results:
            if isinstance(res, Exception):
                logger.error(
                    "BitgetFetcher._flush_detail_tasks: detail fetch failed: %s",
                    res,
                )
                continue
            total += res or 0
        return total

    def _normalize_fill(self, raw: Dict[str, object]) -> Dict[str, object]:
        """将 Bitget 原始成交数据标准化为内部事件格式。"""
        timestamp = int(raw["cTime"])
        side, position_side = deduce_side_pside(raw)
        return {
            "id": raw.get("tradeId"),
            "order_id": raw.get("orderId"),
            "timestamp": timestamp,
            "datetime": ts_to_date(timestamp),
            "symbol": self._resolve_symbol(raw.get("symbol")),
            "symbol_external": raw.get("symbol"),
            "side": side,
            "qty": float(raw.get("baseVolume", 0.0)),
            "price": float(raw.get("price", 0.0)),
            "pnl": float(raw.get("profit", 0.0)),
            "fees": raw.get("feeDetail"),
            "pb_order_type": raw.get("pb_order_type", ""),
            "position_side": position_side,
            "client_order_id": raw.get("client_order_id"),
            "raw": [{"source": "fill_history", "data": dict(raw)}],
        }

    def _resolve_symbol(self, market_symbol: Optional[str]) -> str:
        """解析交易对名称。"""
        if not market_symbol:
            return ""
        try:
            resolved = self._symbol_resolver(market_symbol)
        except Exception as exc:
            logger.warning(
                "BitgetFetcher._resolve_symbol: resolver failed for %s (%s); using fallback",
                market_symbol,
                exc,
            )
            resolved = None
        if resolved:
            return resolved
        logger.warning(
            "BitgetFetcher._resolve_symbol: unresolved symbol '%s'; falling back to raw value",
            market_symbol,
        )
        return str(market_symbol)


class BinanceFetcher(BaseFetcher):
    """通过合并收入和交易历史为 Binance 获取已实现 PnL 事件。"""

    def __init__(
        self,
        api,
        *,
        symbol_resolver: Callable[[str], str],
        now_func: Optional[Callable[[], int]] = None,
        positions_provider: Optional[Callable[[], Iterable[str]]] = None,
        open_orders_provider: Optional[Callable[[], Iterable[str]]] = None,
        income_limit: int = 1000,
        trade_limit: int = 1000,
    ) -> None:
        """初始化 Binance 获取器。"""
        self.api = api
        if symbol_resolver is None:
            raise ValueError("BinanceFetcher requires a symbol_resolver callable")
        self._symbol_resolver = symbol_resolver
        self._positions_provider = positions_provider or (lambda: ())
        self._open_orders_provider = open_orders_provider or (lambda: ())
        self.income_limit = min(1000, max(1, income_limit))  # 上限为 1000
        self._now_func = now_func or (lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000))
        self.trade_limit = max(1, trade_limit)
        self._unsupported_symbols: set[str] = set()
        self._market_symbols: Optional[set[str]] = None
        self._markets_loaded = False

    async def _get_market_symbols(self) -> Optional[set[str]]:
        """获取交易所支持的所有交易对符号集合。"""
        if self._market_symbols is not None:
            return self._market_symbols
        symbols = getattr(self.api, "symbols", None)
        markets = getattr(self.api, "markets", None)
        if (not symbols and not markets) and not self._markets_loaded:
            try:
                await self.api.load_markets()
                self._markets_loaded = True
            except Exception:
                return None
            symbols = getattr(self.api, "symbols", None)
            markets = getattr(self.api, "markets", None)
        if symbols:
            self._market_symbols = set(symbols)
        elif markets:
            self._market_symbols = set(markets.keys())
        else:
            self._market_symbols = None
        return self._market_symbols

    def _note_unsupported_symbol(self, symbol: str) -> None:
        if symbol in self._unsupported_symbols:
            return
        self._unsupported_symbols.add(symbol)
        logger.debug("[fills] BinanceFetcher skipping unsupported symbol %s", symbol)

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        """合并 income 和 trades 获取 Binance 已实现 PnL 事件。"""
        logger.debug(
            "BinanceFetcher.fetch: start since=%s until=%s",
            _format_ms(since_ms),
            _format_ms(until_ms),
        )
        income_events = await self._fetch_income(since_ms, until_ms)
        symbol_pool = set(self._collect_symbols(self._positions_provider))
        symbol_pool.update(self._collect_symbols(self._open_orders_provider))
        symbol_pool.update(ev["symbol"] for ev in income_events if ev.get("symbol"))
        if detail_cache is None:
            detail_cache = {}

        supported_symbols = await self._get_market_symbols()
        if supported_symbols:
            unsupported = [sym for sym in symbol_pool if sym not in supported_symbols]
            for sym in unsupported:
                self._note_unsupported_symbol(sym)
            symbol_pool = {sym for sym in symbol_pool if sym in supported_symbols}

        trade_events: Dict[str, Dict[str, object]] = {}
        trade_tasks: Dict[str, asyncio.Task[List[Dict[str, object]]]] = {}
        for symbol in sorted(symbol_pool):
            if not symbol:
                continue
            trade_tasks[symbol] = asyncio.create_task(
                self._fetch_symbol_trades(symbol, since_ms, until_ms)
            )
        for symbol, task in trade_tasks.items():
            try:
                trades = await task
            except RateLimitExceeded as exc:  # pragma: no cover - 依赖实盘 API
                logger.warning(
                    "BinanceFetcher.fetch: rate-limited fetching trades for %s (%s)", symbol, exc
                )
                trades = []
            except Exception as exc:
                logger.error("BinanceFetcher.fetch: error fetching trades for %s (%s)", symbol, exc)
                trades = []
            for trade in trades:
                event = self._normalize_trade(trade)
                cached = detail_cache.get(event["id"])
                if cached:
                    event.setdefault("client_order_id", cached[0])
                    if cached[1]:
                        event.setdefault("pb_order_type", cached[1])
                trade_events[event["id"]] = event

        merged: Dict[str, Dict[str, object]] = {}
        for ev in income_events:
            merged[ev["id"]] = ev

        def _event_from_trade(trade: Dict[str, object]) -> Dict[str, object]:
            """从原始成交构建事件字典。"""
            symbol = trade.get("symbol") or self._resolve_symbol(trade.get("info", {}).get("symbol"))
            timestamp = int(trade.get("timestamp") or 0)
            client_oid = trade.get("client_order_id") or ""
            event: Dict[str, object] = {
                "id": str(trade.get("id")),
                "timestamp": timestamp,
                "datetime": ts_to_date(timestamp) if timestamp else "",
                "symbol": symbol or "",
                "side": trade.get("side") or "",
                "qty": float(trade.get("qty") or 0.0),
                "price": float(trade.get("price") or 0.0),
                "pnl": float(trade.get("pnl") or 0.0),
                "fees": trade.get("fees"),
                "pb_order_type": trade.get("pb_order_type") or "",
                "position_side": trade.get("position_side") or "unknown",
                "client_order_id": client_oid,
                "order_id": trade.get("order_id") or "",
                "info": trade.get("info"),
            }
            return event

        def _merge_trade_into_event(event: Dict[str, object], trade: Dict[str, object]) -> None:
            """将成交数据合并到现有事件中，填充缺失字段。"""
            if not event.get("symbol") and trade.get("symbol"):
                event["symbol"] = trade["symbol"]
            if not event.get("side") and trade.get("side"):
                event["side"] = trade["side"]
            if float(event.get("qty", 0.0)) == 0.0 and trade.get("qty") is not None:
                event["qty"] = float(trade.get("qty", 0.0))
            if float(event.get("price", 0.0)) == 0.0 and trade.get("price") is not None:
                event["price"] = float(trade.get("price", 0.0))
            if not event.get("fees") and trade.get("fees"):
                event["fees"] = trade["fees"]
            if (event.get("position_side") in (None, "", "unknown")) and trade.get("position_side"):
                event["position_side"] = trade["position_side"]
            if trade.get("client_order_id"):
                event["client_order_id"] = trade["client_order_id"]
            if trade.get("order_id"):
                event["order_id"] = trade["order_id"]
            if trade.get("info"):
                event["info"] = trade["info"]
            if trade.get("pb_order_type"):
                event["pb_order_type"] = trade["pb_order_type"]

        if trade_events:
            for event_id, trade in trade_events.items():
                if event_id not in merged:
                    merged[event_id] = _event_from_trade(trade)
                event = merged[event_id]
                _merge_trade_into_event(event, trade)

        for event_id, event in merged.items():
            cached = detail_cache.get(event_id)
            if cached:
                client_oid, pb_type = cached
                if client_oid:
                    event["client_order_id"] = client_oid
                if pb_type and pb_type != "unknown":
                    event["pb_order_type"] = pb_type

        enrichment_tasks: List[asyncio.Task[Optional[Tuple[str, str]]]] = []
        enrichment_events: List[Tuple[Dict[str, object], str]] = []
        if merged:
            for event_id, event in merged.items():
                has_client = bool(event.get("client_order_id"))
                has_type = bool(event.get("pb_order_type")) and event["pb_order_type"] != "unknown"
                if has_client and has_type:
                    continue
                trade = trade_events.get(event_id)
                order_id = None
                symbol = None
                if trade:
                    order_id = trade.get("order_id")
                    symbol = trade.get("symbol") or event.get("symbol")
                else:
                    order_id = event.get("order_id")
                    symbol = event.get("symbol")
                if not order_id or not symbol:
                    continue
                enrichment_events.append((event, event_id))
                enrichment_tasks.append(
                    asyncio.create_task(
                        self._enrich_with_order_details(
                            str(order_id),
                            str(symbol),
                        )
                    )
                )
        if enrichment_tasks:
            detail_results = await asyncio.gather(*enrichment_tasks, return_exceptions=True)
            for (event, event_id), res in zip(enrichment_events, detail_results):
                if isinstance(res, Exception):
                    logger.debug(
                        "BinanceFetcher.fetch: fetch_order failed for %s (%s)",
                        event.get("id"),
                        res,
                    )
                    continue
                if not res:
                    continue
                client_oid, pb_type = res
                event["client_order_id"] = client_oid
                if pb_type:
                    event["pb_order_type"] = pb_type
                if event_id:
                    detail_cache[event_id] = (client_oid, pb_type or "")

        for event_id, ev in merged.items():
            client_oid = ev.get("client_order_id")
            if client_oid and not ev.get("pb_order_type"):
                ev["pb_order_type"] = custom_id_to_snake(str(client_oid))
            if not ev.get("pb_order_type"):
                ev["pb_order_type"] = ""
            ev["client_order_id"] = str(client_oid) if client_oid is not None else ""
            if event_id and ev.get("client_order_id"):
                detail_cache[event_id] = (ev["client_order_id"], ev["pb_order_type"])

        ordered = sorted(merged.values(), key=lambda ev: ev["timestamp"])
        if since_ms is not None:
            ordered = [ev for ev in ordered if ev["timestamp"] >= since_ms]
        if until_ms is not None:
            ordered = [ev for ev in ordered if ev["timestamp"] <= until_ms]

        if on_batch and ordered:
            on_batch(ordered)

        logger.debug(
            "BinanceFetcher.fetch: done events=%d (symbols=%d)",
            len(ordered),
            len(symbol_pool),
        )
        return ordered

    async def _enrich_with_order_details(
        self,
        order_id: Optional[str],
        symbol: Optional[str],
    ) -> Optional[Tuple[str, str]]:
        """获取 Binance 订单详情以提取 client_order_id。"""
        if not order_id or not symbol:
            return None
        try:
            detail = await self.api.fetch_order(order_id, symbol)
        except Exception as exc:  # pragma: no cover - 依赖实盘 API
            logger.debug(
                "BinanceFetcher._enrich_with_order_details: fetch_order failed for %s (%s)",
                order_id,
                exc,
            )
            return None
        info = detail.get("info") if isinstance(detail, dict) else detail
        if not isinstance(info, dict):
            return None
        client_oid = info.get("clientOrderId") or info.get("clientOrderID")
        if not client_oid:
            return None
        client_oid = str(client_oid)
        return client_oid, custom_id_to_snake(client_oid)

    async def _fetch_income(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
    ) -> List[Dict[str, object]]:
        """按周分页获取 Binance REALIZED_PNL 收入记录。"""
        params: Dict[str, object] = {"incomeType": "REALIZED_PNL", "limit": self.income_limit}
        if until_ms is None:
            if since_ms is None:
                logger.debug(f"BinanceFetcher._fetch_income.fapiprivate_get_income params={params}")
                payload = await self.api.fapiprivate_get_income(params=params)
                return sorted(
                    [self._normalize_income(x) for x in payload], key=lambda x: x["timestamp"]
                )
            until_ms = self._now_func() + 1000 * 60 * 60
        week_buffer_ms = 1000 * 60 * 60 * 24 * 6.95
        params["startTime"] = int(since_ms)
        params["endTime"] = int(min(until_ms, since_ms + week_buffer_ms))
        events = []
        previous_key: Optional[Tuple[Tuple[str, object], ...]] = None
        fetch_count = 0
        while True:
            key = _check_pagination_progress(
                previous_key,
                params,
                "BinanceFetcher._fetch_income",
            )
            if key is None:
                break
            previous_key = key
            fetch_count += 1
            payload = await self.api.fapiprivate_get_income(params=params)
            if fetch_count > 1:
                payload_size = len(payload) if payload else 0
                # 仅在有实际数据时记录 INFO；否则记录 DEBUG
                log_fn = logger.info if payload_size > 0 else logger.debug
                log_fn(
                    "BinanceFetcher._fetch_income: fetch #%d startTime=%s endTime=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("startTime")),
                    _format_ms(params.get("endTime")),
                    payload_size,
                )
            if payload == []:
                if params["startTime"] + week_buffer_ms >= until_ms:
                    break
                params["startTime"] = int(params["startTime"] + week_buffer_ms)
                params["endTime"] = int(min(until_ms, params["startTime"] + week_buffer_ms))
                continue
            events.extend(
                sorted([self._normalize_income(x) for x in payload], key=lambda x: x["timestamp"])
            )
            params["startTime"] = int(events[-1]["timestamp"]) + 1
            params["endTime"] = int(min(until_ms, params["startTime"] + week_buffer_ms))
            if params["startTime"] > until_ms:
                break
        return events

    async def _fetch_symbol_trades(
        self,
        ccxt_symbol: str,
        since_ms: Optional[int],
        until_ms: Optional[int],
    ) -> List[Dict[str, object]]:
        """按交易对获取 Binance 成交历史，支持时间范围分页。"""
        limit = min(1000, max(1, self.trade_limit))
        try:
            if since_ms is None and until_ms is None:
                return await self.api.fetch_my_trades(ccxt_symbol, limit=limit)

            end_bound = until_ms or self._now_func()
            start_bound = since_ms or max(0, end_bound - 7 * 24 * 60 * 60 * 1000)
            week_span = int(7 * 24 * 60 * 60 * 1000 * 0.99)
            params: Dict[str, object] = {}
            fetched: Dict[str, Dict[str, object]] = {}
            previous_key: Optional[Tuple[Tuple[str, object], ...]] = None
            fetch_count = 0

            cursor = int(start_bound)
            while cursor <= end_bound:
                window_end = int(min(end_bound, cursor + week_span))
                params["startTime"] = cursor
                params["endTime"] = window_end
                param_key = _check_pagination_progress(
                    previous_key,
                    params,
                    f"BinanceFetcher._fetch_symbol_trades({ccxt_symbol})",
                )
                if param_key is None:
                    break
                previous_key = param_key
                fetch_count += 1
                batch = await self.api.fetch_my_trades(
                    ccxt_symbol,
                    limit=limit,
                    params=dict(params),
                )
                if fetch_count > 1:
                    batch_size = len(batch) if batch else 0
                    # 仅在有实际数据时记录 INFO；否则记录 DEBUG
                    log_fn = logger.info if batch_size > 0 else logger.debug
                    log_fn(
                        "BinanceFetcher._fetch_symbol_trades: fetch #%d symbol=%s start=%s end=%s size=%d",
                        fetch_count,
                        ccxt_symbol,
                        _format_ms(params["startTime"]),
                        _format_ms(params["endTime"]),
                        batch_size,
                    )
                if not batch:
                    cursor = window_end + 1
                    continue
                for trade in batch:
                    trade_id = str(
                        trade.get("id")
                        or (trade.get("info") or {}).get("id")
                        or f"{trade.get('order')}-{trade.get('timestamp')}"
                    )
                    fetched[trade_id] = trade
                last_ts = int(
                    batch[-1].get("timestamp")
                    or (batch[-1].get("info") or {}).get("time")
                    or params["endTime"]
                )
                if last_ts >= end_bound or len(batch) < limit:
                    cursor = last_ts + 1
                    if cursor > end_bound:
                        break
                else:
                    cursor = last_ts + 1

            ordered = sorted(
                fetched.values(),
                key=lambda tr: int(tr.get("timestamp") or (tr.get("info") or {}).get("time") or 0),
            )
            return ordered
        except Exception as exc:  # pragma: no cover - 依赖实盘 API
            msg = str(exc).lower() if exc else ""
            if "does not have market symbol" in msg or "market symbol" in msg:
                self._note_unsupported_symbol(ccxt_symbol)
                return []
            logger.error("BinanceFetcher._fetch_symbol_trades: error %s (%s)", ccxt_symbol, exc)
            return []

    def _normalize_income(self, entry: Dict[str, object]) -> Dict[str, object]:
        """将 Binance income 记录标准化为内部事件格式。"""
        trade_id = entry.get("tradeId") or entry.get("id") or f"income-{entry.get('time')}"
        timestamp = int(entry.get("time") or entry.get("timestamp") or 0)
        raw_symbol = entry.get("symbol")
        ccxt_symbol = self._resolve_symbol(raw_symbol)
        pnl = float(entry.get("income") or entry.get("pnl") or 0.0)
        position_side = str(entry.get("positionSide") or entry.get("pside") or "unknown").lower()
        return {
            "id": str(trade_id),
            "timestamp": timestamp,
            "datetime": ts_to_date(timestamp),
            "symbol": ccxt_symbol,
            "side": entry.get("side") or "",
            "qty": 0.0,
            "price": 0.0,
            "pnl": pnl,
            "fees": None,
            "pb_order_type": "",
            "position_side": position_side or "unknown",
            "client_order_id": entry.get("clientOrderId") or "",
        }

    def _normalize_trade(self, trade: Dict[str, object]) -> Dict[str, object]:
        """将 Binance 成交记录标准化为内部事件格式。"""
        info = trade.get("info") or {}
        trade_id = trade.get("id") or info.get("id")
        timestamp = int(trade.get("timestamp") or info.get("time") or info.get("T") or 0)
        pnl = float(info.get("realizedPnl") or trade.get("pnl") or 0.0)
        position_side = str(
            info.get("positionSide") or trade.get("position_side") or "unknown"
        ).lower()
        fees = trade.get("fees") or trade.get("fee")
        client_order_id = (
            trade.get("clientOrderId")
            or info.get("clientOrderId")
            or info.get("origClientOrderId")
            or info.get("clientOrderID")
            or ""
        )
        symbol = trade.get("symbol")
        if symbol and "/" not in symbol:
            symbol = self._resolve_symbol(symbol)
        order_id = (
            trade.get("order")
            or info.get("orderId")
            or info.get("origClientOrderId")
            or info.get("orderID")
        )
        return {
            "id": str(trade_id),
            "timestamp": timestamp,
            "datetime": ts_to_date(timestamp),
            "symbol": symbol or "",
            "side": trade.get("side") or "",
            "qty": float(trade.get("amount") or trade.get("qty") or 0.0),
            "price": float(trade.get("price") or 0.0),
            "pnl": pnl,
            "fees": fees,
            "pb_order_type": "",
            "position_side": position_side or "unknown",
            "client_order_id": client_order_id,
            "order_id": str(order_id) if order_id else "",
            "info": info,
            "raw": [{"source": "fetch_my_trades", "data": dict(trade)}],
        }

    def _collect_symbols(self, provider: Callable[[], Iterable[str]]) -> List[str]:
        try:
            items = provider() or []
        except Exception as exc:
            logger.warning("BinanceFetcher._collect_symbols: provider failed (%s)", exc)
            return []
        symbols: List[str] = []
        for raw in items:
            normalized = self._resolve_symbol(raw)
            if normalized:
                symbols.append(normalized)
        return symbols

    def _resolve_symbol(self, value: Optional[str]) -> str:
        if not value:
            return ""
        try:
            resolved = self._symbol_resolver(value)
            if resolved:
                return resolved
        except Exception as exc:
            logger.warning("BinanceFetcher._resolve_symbol: resolver failed for %s (%s)", value, exc)
        return str(value)


# ---------------------------------------------------------------------------
# 管理器
# ---------------------------------------------------------------------------


class FillEventsManager:
    """围绕缓存/获取的成交事件的高级接口。"""

    def __init__(
        self,
        *,
        exchange: str,
        user: str,
        fetcher: BaseFetcher,
        cache_path: Path,
        rate_limit_coordinator: Optional[RateLimitCoordinator] = None,
    ) -> None:
        """初始化成交事件管理器。"""
        self.exchange = exchange
        self.user = user
        self.fetcher = fetcher
        self.cache = FillEventCache(cache_path)
        self.rate_limiter = rate_limit_coordinator or RateLimitCoordinator(exchange, user)
        self._events: List[FillEvent] = []
        self._loaded = False
        self._lock = asyncio.Lock()

    async def ensure_loaded(self) -> None:
        """从缓存加载成交事件（仅首次调用时执行），过滤无效记录并标注 psize/pprice。"""
        if self._loaded:
            return
        async with self._lock:
            if self._loaded:
                return
            cached = self.cache.load()
            filtered = []
            dropped = 0
            for ev in cached:
                if getattr(ev, "raw", None) is None:
                    dropped += 1
                    continue
                filtered.append(ev)
            self._events = sorted(filtered, key=lambda ev: ev.timestamp)

            # 为可能缺少这些值的旧版缓存标注 psize/pprice
            if self._events:
                payload = [ev.to_dict() for ev in self._events]
                ensure_qty_signage(payload)
                compute_psize_pprice(payload)
                self._events = [FillEvent.from_dict(ev) for ev in payload]

            logger.debug(
                "[fills] ensure_loaded: %d cached events (dropped %d without raw)",
                len(self._events),
                dropped,
            )
            self._loaded = True

    @staticmethod
    def _bybit_event_trade_rows(event: FillEvent) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for item in _normalize_raw_field(getattr(event, "raw", None)):
            if not isinstance(item, dict):
                continue
            if item.get("source") != "fetch_my_trades":
                continue
            data = item.get("data")
            if isinstance(data, dict):
                rows.append(data)
        return rows

    @staticmethod
    def _bybit_event_non_trade_raw(event: FillEvent) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for item in _normalize_raw_field(getattr(event, "raw", None)):
            if not isinstance(item, dict):
                continue
            if item.get("source") == "fetch_my_trades":
                continue
            rows.append(item)
        return rows

    @staticmethod
    def _bybit_group_stats(events: Sequence[FillEvent]) -> Dict[str, object]:
        """计算 Bybit 事件组的统计信息。"""
        unique_rows: Dict[Tuple[object, ...], Dict[str, object]] = {}
        fallback_idx = 0
        duplicate_rows = 0
        for ev in events:
            for row in FillEventsManager._bybit_event_trade_rows(ev):
                key = _bybit_trade_dedupe_key(row)
                if key is None:
                    key = ("__fallback__", fallback_idx)
                    fallback_idx += 1
                if key in unique_rows:
                    duplicate_rows += 1
                    continue
                unique_rows[key] = row

        unique_qty_abs = sum(_bybit_trade_qty_abs(row) for row in unique_rows.values())
        side = str(events[0].side).lower() if events else "buy"
        unique_qty_signed = -unique_qty_abs if side == "sell" else unique_qty_abs
        group_qty = sum(float(ev.qty) for ev in events)
        return {
            "duplicate_rows": duplicate_rows,
            "group_size": len(events),
            "group_qty": group_qty,
            "unique_qty_signed": unique_qty_signed,
            "unique_row_count": len(unique_rows),
        }

    @staticmethod
    def _scan_bybit_qty_inflation(events: Sequence[FillEvent]) -> List[Dict[str, object]]:
        """扫描 Bybit 数量膨胀异常。"""
        anomalies: List[Dict[str, object]] = []
        tolerance = 1e-9

        grouped: Dict[Tuple[int, str, str, str, str], List[FillEvent]] = defaultdict(list)
        for ev in events:
            grouped[_bybit_event_group_key(ev)].append(ev)

        for key, group in grouped.items():
            stats = FillEventsManager._bybit_group_stats(group)
            group_size = int(stats["group_size"])
            duplicate_rows = int(stats["duplicate_rows"])
            group_qty = float(stats["group_qty"])
            unique_qty_signed = float(stats["unique_qty_signed"])
            if group_size <= 1 and duplicate_rows <= 0:
                continue
            if abs(group_qty - unique_qty_signed) <= tolerance and group_size == 1:
                continue
            anomalies.append(
                {
                    "key": key,
                    "event_ids": [ev.id for ev in group],
                    "group_size": group_size,
                    "duplicate_rows": duplicate_rows,
                    "group_qty": group_qty,
                    "expected_qty": unique_qty_signed,
                    "unique_trade_rows": int(stats["unique_row_count"]),
                }
            )
        return anomalies

    @staticmethod
    def _normalize_fee_dict(fee: Optional[Dict[str, object]]) -> Optional[Dict[str, object]]:
        """标准化手续费字典，提取 currency、cost 和 rate。"""
        if not isinstance(fee, dict):
            return None
        out: Dict[str, object] = {}
        currency = fee.get("currency") or fee.get("code")
        if currency:
            out["currency"] = str(currency)
        try:
            out["cost"] = float(fee.get("cost", 0.0))
        except Exception:
            out["cost"] = 0.0
        if fee.get("rate") is not None:
            try:
                out["rate"] = float(fee.get("rate"))
            except Exception:
                pass
        return out

    @staticmethod
    def _extract_bybit_fee_from_trade_row(row: Dict[str, object]) -> Optional[Dict[str, object]]:
        """从 Bybit 成交行提取手续费信息，优先使用 ccxt fee，回退到 info 字段。"""
        fee = FillEventsManager._normalize_fee_dict(row.get("fee"))
        if fee is not None:
            return fee
        info = row.get("info")
        info = info if isinstance(info, dict) else {}
        fee_cost_raw = info.get("execFee")
        fee_ccy = info.get("feeCurrency")
        if fee_cost_raw is None:
            return None
        try:
            fee_cost = float(fee_cost_raw)
        except Exception:
            return None
        out: Dict[str, object] = {"cost": fee_cost}
        if fee_ccy:
            out["currency"] = str(fee_ccy)
        fee_rate_raw = info.get("feeRate")
        if fee_rate_raw is not None:
            try:
                out["rate"] = float(fee_rate_raw)
            except Exception:
                pass
        return out

    @staticmethod
    def _dedupe_raw_payloads(items: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
        deduped: List[Dict[str, object]] = []
        seen: set[str] = set()
        for item in items:
            try:
                marker = json.dumps(item, sort_keys=True, separators=(",", ":"))
            except Exception:
                marker = str(item)
            if marker in seen:
                continue
            seen.add(marker)
            deduped.append(item)
        return deduped

    @staticmethod
    def _build_consolidated_bybit_event(group: Sequence[FillEvent]) -> FillEvent:
        """将一组 Bybit 重复成交合并为单个规范化事件，重新计算 PnL 和手续费。"""
        # 选择最佳基线事件（优先内部去重的，然后是最大唯一覆盖的）。
        best_event = group[0]
        best_rank: Tuple[int, int, float] = (-1, -1, float("-inf"))
        mt_unique_by_key: Dict[Tuple[object, ...], Dict[str, object]] = {}
        non_mt_rows: List[Dict[str, object]] = []
        fallback_idx = 0

        for ev in group:
            mt_rows = FillEventsManager._bybit_event_trade_rows(ev)
            keys_seen: set[Tuple[object, ...]] = set()
            duplicates = 0
            unique_count = 0
            signed_qty_sum = 0.0
            for row in mt_rows:
                key = _bybit_trade_dedupe_key(row)
                if key is None:
                    key = ("__fallback__", fallback_idx)
                    fallback_idx += 1
                if key in keys_seen:
                    duplicates += 1
                    continue
                keys_seen.add(key)
                unique_count += 1
                signed_qty_sum += _bybit_trade_qty_signed(row)
                if key not in mt_unique_by_key:
                    mt_unique_by_key[key] = row
            qty_delta = -abs(float(ev.qty) - signed_qty_sum)
            rank = (1 if duplicates == 0 else 0, unique_count, qty_delta)
            if rank > best_rank:
                best_rank = rank
                best_event = ev
            non_mt_rows.extend(FillEventsManager._bybit_event_non_trade_raw(ev))

        mt_rows_unique = list(mt_unique_by_key.values())
        side = str(best_event.side).lower()
        qty_abs_sum = sum(_bybit_trade_qty_abs(row) for row in mt_rows_unique)
        qty_signed_sum = -qty_abs_sum if side == "sell" else qty_abs_sum

        price_num = 0.0
        for row in mt_rows_unique:
            info = row.get("info")
            info = info if isinstance(info, dict) else {}
            price = float(row.get("price") or info.get("execPrice") or 0.0)
            price_num += price * _bybit_trade_qty_abs(row)
        price = float(best_event.price)
        if qty_abs_sum > 0.0:
            price = price_num / qty_abs_sum

        fees_merged = None
        for row in mt_rows_unique:
            fee = FillEventsManager._extract_bybit_fee_from_trade_row(row)
            fees_merged = _merge_fee_lists(fees_merged, fee)
        fees_out: Optional[Sequence]
        if isinstance(fees_merged, list) and len(fees_merged) == 1:
            fees_out = fees_merged[0]
        else:
            fees_out = fees_merged

        source_ids: set[str] = set(best_event.source_ids or [])
        for row in mt_rows_unique:
            info = row.get("info")
            info = info if isinstance(info, dict) else {}
            trade_id = row.get("id") or info.get("execId")
            if trade_id:
                source_ids.add(str(trade_id))
        source_ids_sorted = sorted(source_ids)
        event_id = "+".join(source_ids_sorted) if source_ids_sorted else best_event.id

        # 尽可能从 positions_history + 唯一成交重新计算平仓 PnL。
        pnl: Optional[float] = None
        positions_items = [
            row
            for row in non_mt_rows
            if isinstance(row, dict) and str(row.get("source")) == "positions_history"
        ]
        for pos_item in positions_items:
            data = pos_item.get("data")
            if not isinstance(data, dict):
                continue
            info = data.get("info")
            info = info if isinstance(info, dict) else {}
            avg_entry = float(info.get("avgEntryPrice") or data.get("entryPrice") or 0.0)
            total_closed = float(info.get("closedSize") or data.get("contracts") or 0.0)
            if avg_entry <= 0.0 or total_closed <= 0.0:
                continue
            total_fees = float(info.get("closeFee") or 0.0) + float(info.get("openFee") or 0.0)
            recomputed = 0.0
            used = False
            for row in mt_rows_unique:
                info_row = row.get("info")
                info_row = info_row if isinstance(info_row, dict) else {}
                closed_size = float(info_row.get("closedSize") or info_row.get("closeSize") or 0.0)
                if closed_size <= 0.0:
                    continue
                exit_price = float(row.get("price") or info_row.get("execPrice") or 0.0)
                if exit_price <= 0.0:
                    continue
                if str(best_event.position_side).lower() == "long":
                    gross = (exit_price - avg_entry) * closed_size
                else:
                    gross = (avg_entry - exit_price) * closed_size
                fee_portion = (closed_size / total_closed) * total_fees if total_closed > 0.0 else 0.0
                recomputed += gross - fee_portion
                used = True
            if used:
                pnl = recomputed
                break
        if pnl is None:
            if abs(float(best_event.qty)) > 1e-12:
                pnl = float(best_event.pnl) * (qty_signed_sum / float(best_event.qty))
            else:
                pnl = float(best_event.pnl)

        raw_payload = [
            {"source": "fetch_my_trades", "data": dict(row)} for row in mt_rows_unique
        ] + FillEventsManager._dedupe_raw_payloads(non_mt_rows)

        return FillEvent(
            id=event_id,
            source_ids=source_ids_sorted,
            timestamp=int(best_event.timestamp),
            datetime=str(best_event.datetime),
            symbol=str(best_event.symbol),
            side=str(best_event.side).lower(),
            qty=float(qty_signed_sum),
            price=float(price),
            pnl=float(pnl),
            fees=fees_out,
            pb_order_type=str(best_event.pb_order_type),
            position_side=str(best_event.position_side).lower(),
            client_order_id=str(best_event.client_order_id),
            psize=float(best_event.psize),
            pprice=float(best_event.pprice),
            raw=raw_payload,
        )

    async def run_doctor(self, *, auto_repair: bool = False) -> Dict[str, object]:
        """检测并可选地自动修复已知的成交事件缓存异常。"""
        await self.ensure_loaded()
        report: Dict[str, object] = {
            "exchange": self.exchange,
            "user": self.user,
            "events_scanned": len(self._events),
            "anomaly_events": 0,
            "anomaly_examples": [],
            "auto_repair": bool(auto_repair),
            "repaired": False,
        }
        if self.exchange.lower() != "bybit":
            return report

        anomalies = self._scan_bybit_qty_inflation(self._events)
        report["anomaly_events"] = len(anomalies)
        report["anomaly_examples"] = anomalies[:5]
        if not anomalies or not auto_repair:
            return report

        grouped: Dict[Tuple[int, str, str, str, str], List[FillEvent]] = defaultdict(list)
        for ev in self._events:
            grouped[_bybit_event_group_key(ev)].append(ev)

        repaired_events: List[FillEvent] = []
        for key in sorted(grouped.keys()):
            group = grouped[key]
            if len(group) == 1:
                stats = self._bybit_group_stats(group)
                if int(stats["duplicate_rows"]) <= 0:
                    repaired_events.extend(group)
                    continue
            repaired_events.append(self._build_consolidated_bybit_event(group))

        repaired_events.sort(key=lambda ev: ev.timestamp)
        payload = [ev.to_dict() for ev in repaired_events]
        ensure_qty_signage(payload)
        compute_psize_pprice(payload)
        self._events = [FillEvent.from_dict(ev) for ev in payload]
        self.cache.save(self._events)
        self.cache.update_metadata_from_events(self._events)

        remaining = self._scan_bybit_qty_inflation(self._events)
        report["anomaly_events_after"] = len(remaining)
        report["anomaly_examples_after"] = remaining[:5]
        report["repaired"] = len(remaining) == 0
        if remaining:
            logger.warning(
                "[fills-doctor] repair incomplete: %d anomalies remain (continuing)",
                len(remaining),
            )
        else:
            logger.info("[fills-doctor] repair complete; no remaining Bybit anomalies")
        return report

    async def refresh(
        self,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
    ) -> None:
        """从交易所获取最新成交事件，增量合并到缓存并持久化受影响的日期文件。"""
        await self.ensure_loaded()
        logger.debug(
            "[fills] refresh: start=%s end=%s current_cache=%d",
            _format_ms(start_ms),
            _format_ms(end_ms),
            len(self._events),
        )
        detail_cache = {
            ev.id: (ev.client_order_id, ev.pb_order_type) for ev in self._events if ev.client_order_id
        }
        updated_map: Dict[str, FillEvent] = {ev.id: ev for ev in self._events}
        source_ids_index: Dict[Tuple[str, ...], set[str]] = defaultdict(set)
        for ev in self._events:
            if ev.source_ids:
                source_ids_index[tuple(ev.source_ids)].add(ev.id)
        added_ids: set[str] = set()
        all_days_persisted: set[str] = set()

        def handle_batch(batch: List[Dict[str, object]]) -> None:
            """增量处理获取到的成交批次：规范化、去重、持久化受影响的日期文件。"""
            ensure_qty_signage(batch)
            days_touched: set[str] = set()
            for raw in batch:
                raw.setdefault("raw", [])
                try:
                    event = FillEvent.from_dict(raw)
                except ValueError as exc:
                    logger.warning(
                        "[fills] skipping malformed event %s (error=%s)",
                        raw.get("id"),
                        exc,
                    )
                    continue
                source_key = tuple(event.source_ids) if event.source_ids else tuple()
                replaced_ids: set[str] = set()
                if source_key and source_key in source_ids_index:
                    replaced_ids = {eid for eid in source_ids_index[source_key] if eid != event.id}
                    for replaced_id in replaced_ids:
                        updated_map.pop(replaced_id, None)
                    source_ids_index[source_key] = {event.id}
                prev = updated_map.get(event.id)
                if prev is not None and event.timestamp < prev.timestamp:
                    continue
                updated_map[event.id] = event
                if source_key:
                    source_ids_index[source_key].add(event.id)
                if prev is None and not replaced_ids:
                    added_ids.add(event.id)
                day = _day_key(event.timestamp)
                days_touched.add(day)
            if not days_touched:
                return
            day_payload = self._events_for_days(updated_map.values(), days_touched)
            self.cache.save_days(day_payload)
            all_days_persisted.update(days_touched)

        try:
            await self.fetcher.fetch(start_ms, end_ms, detail_cache, on_batch=handle_batch)
        except RateLimitExceeded:
            # 将有界范围的失败保留为已知间隙，以便重试逻辑可以重新访问。
            # 我们仍然重新抛出以在关键输入时大声失败。
            if start_ms is not None and end_ms is not None:
                self.cache.add_known_gap(
                    start_ms,
                    end_ms,
                    reason=GAP_REASON_FETCH_FAILED,
                    confidence=GAP_CONFIDENCE_UNKNOWN,
                )
            raise

        self._events = sorted(updated_map.values(), key=lambda ev: ev.timestamp)

        # 为所有事件标注 psize/pprice
        if self._events:
            payload = [ev.to_dict() for ev in self._events]
            ensure_qty_signage(payload)
            compute_psize_pprice(payload)
            self._events = [FillEvent.from_dict(ev) for ev in payload]

            # 重新持久化受影响日期的 psize/pprice 标注值
            if all_days_persisted:
                day_payload = self._events_for_days(self._events, all_days_persisted)
                self.cache.save_days(day_payload)

        # 用时间戳更新缓存元数据
        if self._events:
            self.cache.update_metadata_from_events(self._events)

            # 如果成功获取了间隙范围的数据，清除它
            if start_ms is not None and end_ms is not None and added_ids:
                self.cache.clear_gap(start_ms, end_ms)

        # 合并的刷新摘要日志
        # 仅在有新成交时记录 INFO；常规刷新记录到 DEBUG
        if added_ids:
            days_list = sorted(all_days_persisted)
            days_preview = ", ".join(days_list[:5])
            if len(days_list) > 5:
                days_preview += f", ... ({len(days_list)} total)"
            logger.info(
                "[fills] refresh: events=%d (+%d) | persisted %d days (%s)",
                len(self._events),
                len(added_ids),
                len(all_days_persisted),
                days_preview,
            )
        else:
            logger.debug("[fills] refresh: events=%d (no changes)", len(self._events))

    async def refresh_latest(self, *, overlap: int = 20) -> None:
        """仅获取最近的成交，重叠 `overlap` 个事件。"""
        await self.ensure_loaded()
        if not self._events:
            logger.debug("[fills] refresh_latest: cache empty, falling back to full refresh")
        start_ms = None
        if self._events:
            idx = max(0, len(self._events) - overlap)
            start_ms = self._events[idx].timestamp
        await self.refresh(start_ms=start_ms, end_ms=None)

    async def refresh_for_lookback(
        self,
        start_ms: int,
        *,
        end_ms: Optional[int] = None,
        overlap: int = 20,
        gap_hours: float = 12.0,
        force_refetch_gaps: bool = False,
    ) -> None:
        """使用缓存派生的覆盖范围刷新请求的回溯窗口的成交。

        开放式回溯在缓存元数据中跟踪，以便机器人在重启后可以避免
        重新运行相同的历史引导，当回溯的早期部分确实没有成交时。
        """
        await self.ensure_loaded()
        start_ms = int(start_ms)
        if end_ms is not None:
            await self.refresh_range(
                start_ms=start_ms,
                end_ms=end_ms,
                gap_hours=gap_hours,
                overlap=overlap,
                force_refetch_gaps=force_refetch_gaps,
            )
            return

        metadata = self.cache.load_metadata()
        covered_start_ms = int(metadata.get("covered_start_ms", 0) or 0)
        oldest_event_ts = int(self._events[0].timestamp) if self._events else 0
        metadata_oldest_event_ts = int(metadata.get("oldest_event_ts", 0) or 0)
        metadata_newest_event_ts = int(metadata.get("newest_event_ts", 0) or 0)
        metadata_indicates_no_cached_fills = (
            metadata_oldest_event_ts <= 0 and metadata_newest_event_ts <= 0
        )
        metadata_claims_history_without_events = (
            not self._events
            and (metadata_oldest_event_ts > 0 or metadata_newest_event_ts > 0)
            and covered_start_ms > 0
            and covered_start_ms <= start_ms
        )
        lookback_covered = (
            covered_start_ms > 0 and covered_start_ms <= start_ms and bool(self._events)
        ) or (oldest_event_ts > 0 and oldest_event_ts <= start_ms) or (
            covered_start_ms > 0
            and covered_start_ms <= start_ms
            and metadata_indicates_no_cached_fills
        )

        if lookback_covered:
            logger.debug(
                "[fills] lookback already covered from %s (covered_start=%s oldest_event=%s); refreshing latest",
                _format_ms(start_ms),
                _format_ms(covered_start_ms) if covered_start_ms else "None",
                _format_ms(oldest_event_ts) if oldest_event_ts else "None",
            )
            await self.refresh_latest(overlap=overlap)
            return

        if metadata_claims_history_without_events:
            logger.warning(
                "[fills] cache metadata claims lookback coverage from %s, but no cached events were loaded; rebuilding from requested lookback",
                _format_ms(covered_start_ms),
            )

        if self._events:
            logger.info(
                "[fills] lookback uncovered from %s; refreshing missing range before latest",
                _format_ms(start_ms),
            )
            await self.refresh_range(
                start_ms=start_ms,
                end_ms=None,
                gap_hours=gap_hours,
                overlap=overlap,
                force_refetch_gaps=force_refetch_gaps,
            )
        else:
            logger.info("[fills] cache empty; refreshing full lookback from %s", _format_ms(start_ms))
            await self.refresh(start_ms=start_ms, end_ms=None)

        self.cache.mark_covered_start(start_ms)

    async def refresh_range(
        self,
        start_ms: int,
        end_ms: Optional[int],
        *,
        gap_hours: float = 12.0,
        overlap: int = 20,
        force_refetch_gaps: bool = False,
    ) -> None:
        """使用间隙启发式方法填充 `start_ms` 和 `end_ms` 之间的缺失数据。

        Args:
            start_ms: 开始时间戳（毫秒）
            end_ms: 结束时间戳（毫秒，或 None 表示当前时间）
            gap_hours: 检测间隙的阈值（默认12小时）
            overlap: 获取最新数据时重叠的事件数量
            force_refetch_gaps: 如果为 True，即使持久间隙也重试
        """
        await self.ensure_loaded()
        intervals: List[Tuple[int, int]] = []

        # 从缓存元数据获取已知间隙
        known_gaps = self.cache.get_known_gaps()

        def is_in_persistent_gap(ts_start: int, ts_end: int) -> bool:
            """检查区间是否完全在持久（最大重试次数）间隙内。"""
            if force_refetch_gaps:
                return False
            for gap in known_gaps:
                if ts_start >= gap["start_ts"] and ts_end <= gap["end_ts"]:
                    if not self.cache.should_retry_gap(gap):
                        return True
            return False

        if not self._events:
            logger.debug("[fills] refresh_range: cache empty, refreshing entire interval")
            await self.refresh(start_ms=start_ms, end_ms=end_ms)
            await self.refresh_latest(overlap=overlap)
            return

        events_sorted = self._events
        earliest = events_sorted[0].timestamp
        latest = events_sorted[-1].timestamp
        gap_ms = max(1, int(gap_hours * 60.0 * 60.0 * 1000.0))

        # 如果请求，获取最早缓存之前的旧数据
        if start_ms < earliest:
            upper = earliest if end_ms is None else min(earliest, end_ms)
            if start_ms < upper and not is_in_persistent_gap(start_ms, upper):
                intervals.append((start_ms, upper))

        # 检测缓存数据中的大间隙
        prev_ts = earliest
        for ev in events_sorted[1:]:
            cur_ts = ev.timestamp
            if end_ms is not None and cur_ts > end_ms:
                break
            if cur_ts - prev_ts >= gap_ms:
                gap_start = max(prev_ts, start_ms)
                gap_end = cur_ts
                if gap_start < gap_end:
                    if is_in_persistent_gap(gap_start, gap_end):
                        logger.debug(
                            "FillEventsManager.refresh_range: skipping persistent gap %s → %s",
                            _format_ms(gap_start),
                            _format_ms(gap_end),
                        )
                    else:
                        intervals.append((gap_start, gap_end))
                        # 记录为潜在间隙以供跟踪
                        self.cache.add_known_gap(
                            gap_start,
                            gap_end,
                            reason=GAP_REASON_AUTO,
                            confidence=GAP_CONFIDENCE_SUSPICIOUS,
                        )
            prev_ts = cur_ts

        # 如果请求，获取最新缓存之后的新数据（如果尚未覆盖）
        if end_ms is not None and end_ms > latest and (not intervals or intervals[-1][1] != end_ms):
            lower = max(latest, start_ms)
            if lower < end_ms and not is_in_persistent_gap(lower, end_ms):
                intervals.append((lower, end_ms))

        merged = self._merge_intervals(intervals)
        if merged:
            logger.debug(
                "[fills] refresh_range: refreshing %d intervals: %s",
                len(merged),
                ", ".join(f"{_format_ms(start)} → {_format_ms(end)}" for start, end in merged),
            )
        else:
            logger.debug("[fills] refresh_range: no gaps detected in requested interval")

        for start, end in merged:
            await self.refresh(start_ms=start, end_ms=end)

        await self.refresh_latest(overlap=overlap)

    def get_events(
        self,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> List[FillEvent]:
        """获取可选过滤的成交事件。

        事件返回时带有基于完整历史的预计算 psize/pprice 值
        （在 ensure_loaded/refresh 期间计算）。这些值反映
        按时间顺序每次成交后的持仓状态。
        """
        events = self._events
        if start_ms is not None:
            events = [ev for ev in events if ev.timestamp >= start_ms]
        if end_ms is not None:
            events = [ev for ev in events if ev.timestamp <= end_ms]
        if symbol:
            events = [ev for ev in events if ev.symbol == symbol]
        return list(events)

    def get_pnl_sum(
        self,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> float:
        events = self.get_events(start_ms, end_ms, symbol)
        return float(sum(ev.pnl for ev in events))

    def get_pnl_cumsum(
        self,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        symbol: Optional[str] = None,
    ) -> List[Tuple[int, float]]:
        events = self.get_events(start_ms, end_ms, symbol)
        total = 0.0
        result = []
        for ev in events:
            total += ev.pnl
            result.append((ev.timestamp, total))
        return result

    def get_last_timestamp(self, symbol: Optional[str] = None) -> Optional[int]:
        events = self._events
        if symbol:
            events = [ev for ev in events if ev.symbol == symbol]
        if not events:
            return None
        return max(ev.timestamp for ev in events)

    def reconstruct_positions(
        self, current_positions: Optional[Dict[str, float]] = None
    ) -> Dict[str, float]:
        positions: Dict[str, float] = dict(current_positions or {})
        for ev in self._events:
            key = f"{ev.symbol}:{ev.position_side}"
            positions[key] = positions.get(key, 0.0) + ev.qty
        return positions

    def reconstruct_equity_curve(self, starting_equity: float = 0.0) -> List[Tuple[int, float]]:
        total = starting_equity
        points: List[Tuple[int, float]] = []
        for ev in self._events:
            total += ev.pnl
            points.append((ev.timestamp, total))
        return points

    def get_coverage_summary(self) -> Dict[str, object]:
        """返回缓存覆盖范围和已知间隙的摘要。"""
        summary = self.cache.get_coverage_summary()
        summary["events_count"] = len(self._events)
        summary["exchange"] = self.exchange
        summary["user"] = self.user
        if self._events:
            summary["first_event"] = _format_ms(self._events[0].timestamp)
            summary["last_event"] = _format_ms(self._events[-1].timestamp)
            # 统计唯一交易对数量
            symbols = set(ev.symbol for ev in self._events)
            summary["symbols_count"] = len(symbols)
            summary["symbols"] = sorted(symbols)
        return summary

    def get_history_scope(self) -> str:
        return self.cache.get_history_scope()

    def set_history_scope(self, scope: str) -> None:
        self.cache.set_history_scope(scope)

    @staticmethod
    def _events_for_days(
        events: Iterable[FillEvent], days: Iterable[str]
    ) -> Dict[str, List[FillEvent]]:
        target = {day: [] for day in days}
        for event in events:
            day = _day_key(event.timestamp)
            if day in target:
                target[day].append(event)
        for day_events in target.values():
            day_events.sort(key=lambda ev: ev.timestamp)
        return target

    @staticmethod
    def _merge_intervals(intervals: Sequence[Tuple[int, int]]) -> List[Tuple[int, int]]:
        cleaned = [(int(start), int(end)) for start, end in intervals if end > start]
        if not cleaned:
            return []
        cleaned.sort(key=lambda x: x[0])
        merged: List[Tuple[int, int]] = []
        cur_start, cur_end = cleaned[0]
        for start, end in cleaned[1:]:
            if start <= cur_end:
                cur_end = max(cur_end, end)
            else:
                merged.append((cur_start, cur_end))
                cur_start, cur_end = start, end
        merged.append((cur_start, cur_end))
        return merged


class BybitFetcher(BaseFetcher):
    """通过合并交易和持仓历史从 Bybit 获取成交事件。"""

    def __init__(
        self,
        api,
        *,
        category: str = "linear",
        trade_limit: int = 100,
        position_limit: int = 100,
        overlap_days: float = 3.0,
        max_span_days: float = 6.5,
    ) -> None:
        """初始化 Bybit 获取器。"""
        self.api = api
        self.category = category
        self.trade_limit = max(1, min(trade_limit, 100))
        self.position_limit = max(1, min(position_limit, 100))
        self._default_span_ms = int(overlap_days * 24 * 60 * 60 * 1000)
        self._max_span_ms = int(max_span_days * 24 * 60 * 60 * 1000)

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        """获取 Bybit 成交和持仓历史，合并后返回规范化事件。"""
        end_ms = until_ms or (self._now_ms() + 60 * 60 * 1000)
        start_ms = since_ms or max(0, end_ms - self._default_span_ms)

        trades = await self._fetch_my_trades(start_ms, end_ms)
        positions = await self._fetch_positions_history(start_ms, end_ms)

        events = self._combine(trades, positions, detail_cache)
        events = [
            ev
            for ev in events
            if (since_ms is None or ev["timestamp"] >= since_ms)
            and (until_ms is None or ev["timestamp"] <= until_ms)
        ]
        events.sort(key=lambda ev: ev["timestamp"])
        events = _coalesce_events(events)

        if on_batch and events:
            day_map = defaultdict(list)
            for ev in events:
                day_map[_day_key(ev["timestamp"])].append(ev)
            for day in sorted(day_map):
                on_batch(day_map[day])

        logger.debug(
            "BybitFetcher.fetch: done (events=%d, trades=%d, positions=%d)",
            len(events),
            len(trades),
            len(positions),
        )
        return events

    async def _fetch_my_trades(self, start_ms: int, end_ms: int) -> List[Dict[str, object]]:
        """分页获取 Bybit 成交记录，自动去重。"""
        params = {
            "type": "swap",
            "subType": self.category,
            "limit": self.trade_limit,
            "endTime": int(end_ms),
        }
        results: List[Dict[str, object]] = []
        max_fetches = 200
        fetch_count = 0
        prev_params = None
        while True:
            new_key = _check_pagination_progress(
                prev_params,
                params,
                "BybitFetcher._fetch_my_trades",
            )
            if new_key is None:
                break
            prev_params = new_key
            fetch_count += 1
            batch = await self.api.fetch_my_trades(params=params)
            if fetch_count > 1:
                logger.debug(
                    "BybitFetcher._fetch_my_trades: fetch #%d endTime=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("endTime")),
                    len(batch) if batch else 0,
                )
            if not batch:
                break
            batch.sort(key=lambda x: x["timestamp"])
            results.extend(batch)
            if len(batch) < self.trade_limit:
                if params["endTime"] - start_ms < self._max_span_ms:
                    break
                params["endTime"] = max(start_ms, params["endTime"] - self._max_span_ms)
                continue
            first_ts = batch[0]["timestamp"]
            if first_ts <= start_ms:
                break
            if params["endTime"] == first_ts:
                break
            params["endTime"] = int(first_ts)
            if fetch_count >= max_fetches:
                logger.warning("BybitFetcher._fetch_my_trades: max fetches reached")
                break
        ordered = sorted(
            results,
            key=lambda x: int(x.get("info", {}).get("updatedTime") or x.get("timestamp") or 0),
        )
        deduped: List[Dict[str, object]] = []
        seen_keys: set[Tuple[object, ...]] = set()
        duplicate_rows = 0
        for trade in ordered:
            key = _bybit_trade_dedupe_key(trade)
            if key is None:
                deduped.append(trade)
                continue
            if key in seen_keys:
                duplicate_rows += 1
                continue
            seen_keys.add(key)
            deduped.append(trade)
        if duplicate_rows:
            logger.debug(
                "BybitFetcher._fetch_my_trades: dropped %d duplicate fill rows before canonicalization",
                duplicate_rows,
            )
        return deduped

    async def _fetch_positions_history(self, start_ms: int, end_ms: int) -> List[Dict[str, object]]:
        """使用 Bybit 原始 API 和混合分页获取已平仓 PnL 记录。

        使用两阶段方法：
        1. 游标分页用于最近记录（更高效，不会遗漏记录）
        2. 基于时间的滑动窗口用于较旧记录（游标无法回溯足够远）

        这是必要的，因为：
        - CCXT 的 fetch_positions_history 使用基于时间的分页，可能会遗漏记录
        - Bybit 的游标分页仅覆盖最近约7天的数据
        """
        results: Dict[str, Dict[str, object]] = {}  # 按 orderId 去重
        max_fetches = 500
        fetch_count = 0

        # 阶段1：使用游标分页获取最近记录
        params: Dict[str, object] = {
            "category": "linear",
            "limit": self.position_limit,
            "endTime": int(end_ms),
        }

        cursor_oldest_ts = end_ms

        while True:
            fetch_count += 1
            if fetch_count > max_fetches:
                logger.warning(
                    "BybitFetcher._fetch_positions_history: max fetches reached (%d)", max_fetches
                )
                break

            try:
                response = await self.api.private_get_v5_position_closed_pnl(params)
            except Exception as exc:
                logger.warning("BybitFetcher._fetch_positions_history: API error: %s", exc)
                break

            batch = response.get("result", {}).get("list", [])
            if not batch:
                break

            self._process_closed_pnl_batch(batch, start_ms, results)

            oldest_ts = int(batch[-1].get("updatedTime", 0)) if batch else 0
            cursor_oldest_ts = oldest_ts

            if oldest_ts <= start_ms:
                break

            cursor = response.get("result", {}).get("nextPageCursor")
            if not cursor:
                # 游标耗尽 - 切换到基于时间的滑动窗口
                break
            params["cursor"] = cursor

        # 阶段2：基于时间的滑动窗口用于较旧记录（如果游标未到达起始位置）
        if cursor_oldest_ts > start_ms:
            logger.debug(
                "BybitFetcher._fetch_positions_history: cursor exhausted at %s, switching to time-based",
                _format_ms(cursor_oldest_ts),
            )
            # 移除游标并继续使用基于时间的分页
            current_end = cursor_oldest_ts

            while current_end > start_ms and fetch_count < max_fetches:
                fetch_count += 1
                params = {
                    "category": "linear",
                    "limit": self.position_limit,
                    "endTime": int(current_end),
                }

                try:
                    response = await self.api.private_get_v5_position_closed_pnl(params)
                except Exception as exc:
                    logger.warning("BybitFetcher._fetch_positions_history: API error: %s", exc)
                    break

                batch = response.get("result", {}).get("list", [])
                if not batch:
                    # 没有更多记录，滑动窗口后移
                    current_end = max(start_ms, current_end - self._max_span_ms)
                    continue

                self._process_closed_pnl_batch(batch, start_ms, results)

                oldest_ts = int(batch[-1].get("updatedTime", 0)) if batch else 0
                if oldest_ts <= start_ms:
                    break

                # 滑动窗口：如果批次已满，使用最旧的时间戳；否则向后跳转
                if len(batch) >= self.position_limit:
                    current_end = oldest_ts
                else:
                    current_end = max(start_ms, oldest_ts - self._max_span_ms)

        logger.debug(
            "BybitFetcher._fetch_positions_history: fetched %d records in %d requests",
            len(results),
            fetch_count,
        )
        return list(results.values())

    def _process_closed_pnl_batch(
        self,
        batch: List[Dict[str, object]],
        start_ms: int,
        results: Dict[str, Dict[str, object]],
    ) -> None:
        """处理一批已平仓 PnL 记录并添加到结果字典。"""
        for record in batch:
            updated_ts = int(record.get("updatedTime", 0))
            created_ts = int(record.get("createdTime", 0))
            order_id = record.get("orderId", "")

            # 跳过超出时间范围或已处理的记录
            if updated_ts < start_ms or order_id in results:
                continue

            # 将 Bybit 交易对转换为 CCXT 格式
            raw_symbol = record.get("symbol", "")
            ccxt_symbol = raw_symbol
            if hasattr(self.api, "markets") and self.api.markets:
                for market_symbol, market in self.api.markets.items():
                    if market.get("id") == raw_symbol:
                        ccxt_symbol = market_symbol
                        break

            results[order_id] = {
                "info": record,
                "symbol": ccxt_symbol,
                "timestamp": created_ts,
                "datetime": datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc).isoformat(),
                "lastUpdateTimestamp": updated_ts,
                "realizedPnl": float(record.get("closedPnl", 0)),
                "contracts": float(record.get("closedSize", 0)),
                "entryPrice": float(record.get("avgEntryPrice", 0)),
                "lastPrice": float(record.get("avgExitPrice", 0)),
                "leverage": float(record.get("leverage", 1)),
                "side": "long" if record.get("side", "").lower() == "sell" else "short",
            }

    def _combine(
        self,
        trades: List[Dict[str, object]],
        positions: List[Dict[str, object]],
        detail_cache: Dict[str, Tuple[str, str]],
    ) -> List[Dict[str, object]]:
        """合并交易和持仓历史以计算每笔成交的 PnL。

        策略：对于每笔平仓成交，使用其已平仓 PnL 记录中的 avgEntryPrice
        计算准确的 PnL：(exitPrice - avgEntryPrice) * closedSize * direction。

        这确保每笔成交获得正确的 PnL，而不是按比例分配
        总订单 PnL（当成交有不同的退出价格时这是不正确的）。
        """
        # 按 orderId 索引已平仓 PnL 记录以快速查找
        # 每笔平仓成交都有自己的已平仓 PnL 记录（包含 avgEntryPrice）
        pnl_by_order: Dict[str, Dict] = {}
        raw_pnl_by_order: Dict[str, Dict] = {}  # 保留原始数据用于 raw 字段
        for entry in positions:
            info = entry.get("info", {})
            order_id = str(info.get("orderId", entry.get("orderId", "")))
            if not order_id:
                continue
            pnl_by_order[order_id] = {
                "closedPnl": float(entry.get("realizedPnl") or info.get("closedPnl") or 0.0),
                "avgEntryPrice": float(info.get("avgEntryPrice") or 0.0),
                "avgExitPrice": float(info.get("avgExitPrice") or 0.0),
                "closedSize": float(info.get("closedSize") or entry.get("contracts") or 0.0),
                "closeFee": float(info.get("closeFee") or 0.0),
                "openFee": float(info.get("openFee") or 0.0),
                "side": str(info.get("side") or "").lower(),
                "symbol": entry.get("symbol") or info.get("symbol"),
            }
            raw_pnl_by_order[order_id] = dict(entry)

        events: List[Dict[str, object]] = []
        matched_count = 0
        computed_count = 0

        for trade in trades:
            event = self._normalize_trade(trade)
            order_id = event.get("order_id")
            cache_entry = detail_cache.get(event["id"])

            # 从缓存或 client_order_id 设置 pb_order_type
            if cache_entry:
                event["client_order_id"], event["pb_order_type"] = cache_entry
                if not event["pb_order_type"]:
                    event["pb_order_type"] = "unknown"
            elif event["client_order_id"]:
                pb_type = custom_id_to_snake(event["client_order_id"])
                event["pb_order_type"] = pb_type or "unknown"
            else:
                event["pb_order_type"] = "unknown"

            # 使用 avgEntryPrice 计算平仓成交的 PnL
            closed_size = float(event.get("closed_size", 0))
            if closed_size > 0 and order_id and order_id in pnl_by_order:
                pnl_record = pnl_by_order[order_id]
                avg_entry = pnl_record["avgEntryPrice"]
                exit_price = event["price"]
                position_side = event["position_side"]

                if avg_entry > 0 and exit_price > 0:
                    # 根据持仓方向计算毛 PnL
                    # 多头平仓（卖出）：退出价 > 入场价时盈利
                    # 空头平仓（买入）：入场价 > 退出价时盈利
                    if position_side == "long":
                        gross_pnl = (exit_price - avg_entry) * closed_size
                    else:
                        gross_pnl = (avg_entry - exit_price) * closed_size

                    # 按比例分配手续费（当此成交属于更大的平仓订单时）
                    total_closed = pnl_record["closedSize"]
                    total_fees = pnl_record["closeFee"] + pnl_record["openFee"]
                    if total_closed > 0:
                        fee_portion = (closed_size / total_closed) * total_fees
                    else:
                        fee_portion = 0.0

                    event["pnl"] = gross_pnl - fee_portion
                    computed_count += 1
                else:
                    # avgEntryPrice 不可用时回退到 closedPnl
                    event["pnl"] = pnl_record["closedPnl"]

                matched_count += 1

                # 将 positions_history（已平仓 PnL）数据附加到 raw 字段
                if order_id in raw_pnl_by_order:
                    event["raw"].append(
                        {
                            "source": "positions_history",
                            "data": raw_pnl_by_order[order_id],
                        }
                    )

            events.append(event)

        if matched_count > 0:
            logger.debug(
                "[fills] PnL computed for %d/%d close fills using avgEntryPrice",
                computed_count,
                matched_count,
            )

        return events

    @staticmethod
    def _normalize_trade(trade: Dict[str, object]) -> Dict[str, object]:
        """将 Bybit 成交记录标准化为内部事件格式。"""
        info = trade.get("info", {})
        order_id = str(info.get("orderId", trade.get("order")))
        trade_id = str(trade.get("id") or info.get("execId") or order_id)
        timestamp = int(trade.get("timestamp") or info.get("execTime", 0))
        qty = float(trade.get("amount") or info.get("execQty", 0.0))
        side = str(trade.get("side") or info.get("side", "")).lower()
        price = float(trade.get("price") or info.get("execPrice", 0.0))
        closed_size = float(info.get("closedSize") or info.get("closeSize") or 0.0)
        position_side = BybitFetcher._determine_position_side(side, closed_size)
        pnl = float(trade.get("pnl") or 0.0)
        client_order_id = info.get("orderLinkId") or trade.get("clientOrderId")
        fee = trade.get("fee")
        symbol = trade.get("symbol") or info.get("symbol")

        return {
            "id": trade_id,
            "order_id": order_id,
            "timestamp": timestamp,
            "datetime": ts_to_date(timestamp),
            "symbol": symbol,
            "side": side,
            "qty": abs(qty),
            "price": price,
            "pnl": pnl,
            "fees": fee,
            "pb_order_type": "",
            "position_side": position_side,
            "client_order_id": client_order_id or "",
            "closed_size": closed_size,  # 用于 PnL 计算
            "raw": [{"source": "fetch_my_trades", "data": dict(trade)}],
        }

    @staticmethod
    def _determine_position_side(side: str, closed_size: float) -> str:
        if side == "buy":
            return "short" if closed_size else "long"
        if side == "sell":
            return "long" if closed_size else "short"
        return "long"

    @staticmethod
    def _now_ms() -> int:
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


class HyperliquidFetcher(BaseFetcher):
    """通过 ccxt.fetch_my_trades 获取 Hyperliquid 的成交事件。"""

    def __init__(
        self,
        api,
        *,
        trade_limit: int = 500,
        symbol_resolver: Optional[Callable[[Optional[str]], str]] = None,
    ) -> None:
        self.api = api
        self.trade_limit = max(1, trade_limit)
        self._symbol_resolver = symbol_resolver

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        """分页获取 Hyperliquid 成交事件，带速率限制重试。"""
        params: Dict[str, object] = {"limit": self.trade_limit}
        if since_ms is not None:
            params["since"] = int(since_ms)

        collected: Dict[str, Dict[str, object]] = {}
        max_fetches = 200
        fetch_count = 0

        prev_params = None
        rate_limit_retries = 0
        max_rate_limit_retries = 5
        while True:
            check_params = dict(params)
            check_params["_page"] = fetch_count
            new_key = _check_pagination_progress(
                prev_params,
                check_params,
                "HyperliquidFetcher.fetch",
            )
            if new_key is None:
                break
            prev_params = new_key
            try:
                trades = await self.api.fetch_my_trades(params=params)
            except RateLimitExceeded as exc:
                rate_limit_retries += 1
                if rate_limit_retries >= max_rate_limit_retries:
                    msg = (
                        "HyperliquidFetcher.fetch: too many consecutive rate-limit retries "
                        f"({rate_limit_retries}/{max_rate_limit_retries}); aborting fetch"
                    )
                    logger.warning("%s", msg)
                    raise RateLimitExceeded(msg) from exc
                logger.debug(
                    "HyperliquidFetcher.fetch: rate limit exceeded (retry %d/%d), sleeping (%s)",
                    rate_limit_retries,
                    max_rate_limit_retries,
                    exc,
                )
                await asyncio.sleep(min(30.0, 2.0 ** rate_limit_retries))
                # 重置 prev_params 以避免重试被标记为重复
                prev_params = None
                continue
            rate_limit_retries = 0
            fetch_count += 1
            if fetch_count > 1:
                logger.debug(
                    "HyperliquidFetcher.fetch: fetch #%d since=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("since")),
                    len(trades) if trades else 0,
                )
            if not trades:
                break
            before_count = len(collected)
            for trade in trades:
                event = self._normalize_trade(trade)
                ts = event["timestamp"]
                if since_ms is not None and ts < since_ms:
                    continue
                if until_ms is not None and ts > until_ms:
                    continue
                collected[event["id"]] = event
            added = len(collected) - before_count
            if len(trades) < self.trade_limit:
                break
            last_ts = int(
                trades[-1].get("timestamp")
                or trades[-1].get("info", {}).get("time")
                or trades[-1].get("info", {}).get("updatedTime")
                or 0
            )
            if last_ts <= 0:
                break
            if until_ms is not None and last_ts >= until_ms:
                break
            if added <= 0:
                logger.debug(
                    "HyperliquidFetcher.fetch: no new trades added on page (last_ts=%s), stopping",
                    last_ts,
                )
                break
            params["since"] = last_ts
            if fetch_count >= max_fetches:
                logger.warning(
                    "HyperliquidFetcher.fetch: reached maximum pagination depth (%d)",
                    max_fetches,
                )
                break

        events = sorted(collected.values(), key=lambda ev: ev["timestamp"])
        events = _coalesce_events(events)
        # 注意：psize/pprice 标注在 FillEventsManager.refresh() 中集中完成

        for event in events:
            cache_entry = detail_cache.get(event["id"])
            if cache_entry:
                event["client_order_id"], event["pb_order_type"] = cache_entry
            elif event["client_order_id"]:
                event["pb_order_type"] = custom_id_to_snake(event["client_order_id"])
            else:
                event["pb_order_type"] = "unknown"
            if not event["pb_order_type"]:
                event["pb_order_type"] = "unknown"

        if on_batch and events:
            on_batch(events)

        return events

    @staticmethod
    def _normalize_trade(trade: Dict[str, object]) -> Dict[str, object]:
        """将 Hyperliquid 成交记录标准化为内部事件格式。"""
        info = trade.get("info", {}) or {}
        trade_id = str(trade.get("id") or info.get("hash") or info.get("tid") or "")
        order_id = str(trade.get("order") or info.get("oid") or "")
        timestamp = int(
            trade.get("timestamp")
            or info.get("time")
            or info.get("tradeTime")
            or info.get("updatedTime")
            or 0
        )
        symbol_raw = trade.get("symbol") or info.get("symbol") or info.get("coin")
        side = str(trade.get("side") or info.get("side") or "").lower()
        qty = abs(float(trade.get("amount") or info.get("sz") or 0.0))
        price = float(trade.get("price") or info.get("px") or 0.0)
        pnl = float(trade.get("pnl") or info.get("closedPnl") or 0.0)
        fee = trade.get("fee") or {"currency": info.get("feeToken"), "cost": info.get("fee")}
        client_order_id = trade.get("clientOrderId") or info.get("cloid") or info.get("clOrdId") or ""
        direction = str(info.get("dir", "")).lower()
        if "short" in direction:
            position_side = "short"
        elif "long" in direction:
            position_side = "long"
        else:
            position_side = "long" if side == "buy" else "short"
        return {
            "id": trade_id,
            "order_id": order_id,
            "timestamp": timestamp,
            "datetime": ts_to_date(timestamp) if timestamp else "",
            "symbol": str(symbol_raw or ""),
            "side": side,
            "qty": qty,
            "price": price,
            "pnl": pnl,
            "fees": fee,
            "pb_order_type": "",
            "position_side": position_side,
            "client_order_id": str(client_order_id or ""),
            "raw": [{"source": "fetch_my_trades", "data": trade}],
            "c_mult": float(info.get("contractMultiplier") or info.get("multiplier") or 1.0),
        }


class GateioFetcher(BaseFetcher):
    """使用 trades + 订单 PnL 获取 Gate.io 的成交事件。

    使用 my_trades_timerange 端点获取成交级别数据（手续费、精确价格），
    因为标准 my_trades 端点有 7 天硬限制。使用 fetch_closed_orders 获取
    PnL。当订单有多笔成交时，按比例分配订单级别 PnL。
    """

    def __init__(
        self,
        api,
        *,
        trade_limit: int = 100,
        now_func: Optional[Callable[[], int]] = None,
    ) -> None:
        self.api = api
        self.trade_limit = max(1, min(100, trade_limit))
        self._now_func = now_func or (lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000))

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        """获取 Gate.io 成交并合并订单级别 PnL。"""
        logger.debug(
            "GateioFetcher.fetch: start (since=%s, until=%s)",
            _format_ms(since_ms),
            _format_ms(until_ms),
        )

        # 步骤 1：获取成交（包含手续费的成交级别数据）
        trades = await self._fetch_trades(since_ms, until_ms)
        if not trades:
            logger.debug("GateioFetcher.fetch: no trades found")
            return []

        # 步骤 2：收集唯一订单 ID
        order_ids: set[str] = set()
        for t in trades:
            oid = str(t.get("order") or t.get("info", {}).get("order_id") or "")
            if oid:
                order_ids.add(oid)

        # 步骤 3：获取已关闭订单以获取 PnL
        orders_by_id = await self._fetch_orders_for_pnl(order_ids)

        # 步骤 4：将成交与订单 PnL 合并
        events = self._merge_trades_with_orders(trades, orders_by_id, detail_cache)

        # 按时间范围过滤
        if since_ms is not None:
            events = [ev for ev in events if ev["timestamp"] >= since_ms]
        if until_ms is not None:
            events = [ev for ev in events if ev["timestamp"] <= until_ms]

        ordered = sorted(events, key=lambda ev: ev["timestamp"])

        if on_batch and ordered:
            on_batch(ordered)

        logger.debug(
            "GateioFetcher.fetch: done (events=%d, trades=%d, orders=%d)",
            len(ordered),
            len(trades),
            len(orders_by_id),
        )
        return ordered

    async def _fetch_trades(
        self, since_ms: Optional[int], until_ms: Optional[int]
    ) -> List[Dict[str, object]]:
        """使用 my_trades_timerange 端点获取成交。

        标准 my_trades 端点有约 7 天硬限制，因此使用时间范围端点，
        允许通过指定 from/to 时间戳获取历史数据。
        """
        now_ms = self._now_func()
        # 未提供 since_ms 时默认回溯 30 天
        default_lookback_ms = 30 * 24 * 60 * 60 * 1000
        from_s = int((since_ms or (now_ms - default_lookback_ms)) / 1000)
        to_s = int((until_ms or now_ms) / 1000)

        collected: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        fetch_count = 0
        offset = 0
        consecutive_rate_limits = 0

        while fetch_count < max_fetches:
            fetch_count += 1
            try:
                # 直接通过 CCXT 私有 API 使用时间范围端点
                batch = await self.api.private_futures_get_settle_my_trades_timerange(
                    {
                        "settle": "usdt",
                        "from": from_s,
                        "to": to_s,
                        "limit": self.trade_limit,
                        "offset": offset,
                    }
                )
                consecutive_rate_limits = 0
            except RateLimitExceeded as exc:
                consecutive_rate_limits += 1
                sleep_time = min(2**consecutive_rate_limits, 30)
                logger.debug(
                    "GateioFetcher._fetch_trades: rate-limited (%s); sleeping %.1fs", exc, sleep_time
                )
                await asyncio.sleep(sleep_time)
                continue
            except Exception as exc:
                # 检查是否为伪装的频率限制错误
                if "TOO_MANY_REQUESTS" in str(exc):
                    consecutive_rate_limits += 1
                    sleep_time = min(2**consecutive_rate_limits, 30)
                    logger.debug(
                        "GateioFetcher._fetch_trades: rate-limited (%s); sleeping %.1fs",
                        exc,
                        sleep_time,
                    )
                    await asyncio.sleep(sleep_time)
                    continue
                raise

            if fetch_count > 1:
                logger.info(
                    "GateioFetcher._fetch_trades: fetch #%d offset=%s size=%d",
                    fetch_count,
                    offset,
                    len(batch) if batch else 0,
                )

            if not batch:
                break

            for raw_trade in batch:
                # 将原始 Gate.io 响应转换为类 CCXT 格式
                trade = self._normalize_raw_trade(raw_trade)
                ts = trade.get("timestamp", 0)
                # 跳过时间范围外的成交（安全检查）
                if since_ms is not None and ts < since_ms:
                    continue
                if until_ms is not None and ts > until_ms:
                    continue
                trade_id = str(trade.get("id") or "")
                if trade_id:
                    collected[trade_id] = trade

            if len(batch) < self.trade_limit:
                break

            offset += self.trade_limit
            # 短暂延迟以避免频率限制
            await asyncio.sleep(0.15)

        if fetch_count >= max_fetches:
            logger.warning("GateioFetcher._fetch_trades: reached pagination cap (%d)", max_fetches)

        return list(collected.values())

    def _normalize_raw_trade(self, raw: Dict[str, object]) -> Dict[str, object]:
        """将原始 Gate.io my_trades_timerange 响应转换为类 CCXT 格式。

        my_trades_timerange 原始格式：
            price, text, fee, create_time（浮点秒数）, point_fee,
            trade_id, contract, role, order_id, size, close_size, biz_info, amend_text

        _normalize_trade 期望的类 CCXT 格式：
            id, order, timestamp, symbol, side, amount, price, fee, info
        """
        # 将时间戳从浮点秒数解析为毫秒
        create_time = raw.get("create_time", 0)
        timestamp_ms = int(float(create_time) * 1000) if create_time else 0

        # 获取合约并转换为 CCXT 交易对格式（例如 BNB_USDT -> BNB/USDT:USDT）
        contract = str(raw.get("contract") or "")
        symbol = contract.replace("_", "/") + ":USDT" if contract else ""

        # 根据 size 符号确定方向（正数 = 买入，负数 = 卖出）
        size = float(raw.get("size") or 0)
        side = "buy" if size >= 0 else "sell"

        # 构建手续费结构
        fee_cost = float(raw.get("fee") or 0)
        fee = {"cost": fee_cost, "currency": "USDT"} if fee_cost else None

        return {
            "id": str(raw.get("trade_id") or raw.get("id") or ""),
            "order": str(raw.get("order_id") or ""),
            "timestamp": timestamp_ms,
            "symbol": symbol,
            "side": side,
            "amount": abs(size),
            "price": float(raw.get("price") or 0),
            "fee": fee,
            "info": raw,  # 保留原始数据供 _normalize_trade 访问
        }

    async def _fetch_orders_for_pnl(self, order_ids: set[str]) -> Dict[str, Dict[str, object]]:
        """获取已关闭订单以获取 PnL 数据。"""
        orders_by_id: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        fetch_count = 0
        params: Dict[str, object] = {"status": "finished", "limit": 100, "offset": 0}

        while fetch_count < max_fetches:
            fetch_count += 1
            try:
                batch = await self.api.fetch_closed_orders(params=params)
            except RateLimitExceeded as exc:
                logger.debug("GateioFetcher._fetch_orders_for_pnl: rate-limited (%s); sleeping", exc)
                await asyncio.sleep(1.0)
                continue

            if not batch:
                break

            for order in batch:
                oid = str(order.get("id") or "")
                if oid:
                    orders_by_id[oid] = order

            # 检查是否已收集所有需要的订单
            if order_ids and order_ids.issubset(orders_by_id.keys()):
                break

            if len(batch) < 100:
                break

            params["offset"] = int(params.get("offset", 0)) + 100

        return orders_by_id

    def _merge_trades_with_orders(
        self,
        trades: List[Dict[str, object]],
        orders_by_id: Dict[str, Dict[str, object]],
        detail_cache: Dict[str, Tuple[str, str]],
    ) -> List[Dict[str, object]]:
        """将成交与订单级别 PnL 合并，按比例分配。"""
        # 按 order_id 分组成交
        trades_by_order: Dict[str, List[Dict[str, object]]] = defaultdict(list)
        for t in trades:
            oid = str(t.get("order") or t.get("info", {}).get("order_id") or "")
            trades_by_order[oid].append(t)

        events = []
        for order_id, order_trades in trades_by_order.items():
            order = orders_by_id.get(order_id, {})
            order_info = order.get("info", {}) if order else {}

            # 获取订单级别 PnL
            order_pnl = float(order_info.get("pnl") or 0.0)

            # 计算总数量用于按比例分配
            total_qty = sum(abs(float(t.get("amount", 0))) for t in order_trades)

            for t in order_trades:
                event = self._normalize_trade(t, order, order_pnl, total_qty, detail_cache)
                events.append(event)

        return events

    def _normalize_trade(
        self,
        trade: Dict[str, object],
        order: Dict[str, object],
        order_pnl: float,
        total_qty: float,
        detail_cache: Dict[str, Tuple[str, str]],
    ) -> Dict[str, object]:
        """将成交标准化为规范的成交事件格式。"""
        info = trade.get("info", {}) or {}
        order_info = order.get("info", {}) if order else {}

        trade_id = str(trade.get("id") or info.get("trade_id") or "")
        order_id = str(trade.get("order") or info.get("order_id") or "")

        ts_raw = trade.get("timestamp") or info.get("create_time") or 0
        try:
            timestamp = int(ensure_millis(float(ts_raw)))
        except Exception:
            timestamp = int(float(ts_raw)) if ts_raw else 0

        symbol = str(trade.get("symbol") or info.get("contract") or "")
        side = str(trade.get("side") or info.get("side") or "").lower()
        qty = abs(float(trade.get("amount") or info.get("size") or 0.0))
        price = float(trade.get("price") or info.get("price") or 0.0)
        fee = trade.get("fee")

        # 按比例分配 PnL
        proportion = qty / total_qty if total_qty > 0 else 0
        pnl = order_pnl * proportion

        # 从成交或订单获取客户端订单 ID
        client_order_id = str(
            info.get("text") or order.get("clientOrderId") or order_info.get("text") or ""
        )

        # 首先检查详情缓存
        if trade_id and trade_id in detail_cache:
            client_order_id, pb_type = detail_cache[trade_id]
        else:
            pb_type = custom_id_to_snake(client_order_id) if client_order_id else "unknown"
            if trade_id and client_order_id:
                detail_cache[trade_id] = (client_order_id, pb_type)

        # 确定持仓方向
        close_size = float(info.get("close_size", 0))
        is_reduce_only = order.get("reduceOnly", False) or order_info.get("is_reduce_only", False)
        is_close = close_size > 0 or is_reduce_only or abs(order_pnl) > 0
        position_side = self._determine_position_side(side, is_close)

        return {
            "id": trade_id,
            "order_id": order_id,
            "timestamp": timestamp,
            "datetime": ts_to_date(timestamp) if timestamp else "",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price,
            "pnl": pnl,
            "fees": fee,
            "pb_order_type": pb_type or "unknown",
            "position_side": position_side,
            "client_order_id": client_order_id,
            "raw": [{"source": "my_trades_timerange", "data": dict(trade)}],
        }

    @staticmethod
    def _determine_position_side(side: str, is_close: bool) -> str:
        side = side.lower()
        if is_close:
            if side == "buy":
                return "short"
            if side == "sell":
                return "long"
        else:
            if side == "buy":
                return "long"
            if side == "sell":
                return "short"
        return "long"


class KucoinFetcher(BaseFetcher):
    """通过组合成交和持仓历史获取 Kucoin 的成交事件。"""

    def __init__(
        self, api, *, trade_limit: int = 1000, now_func: Optional[Callable[[], int]] = None
    ) -> None:
        self.api = api
        self.trade_limit = max(1, trade_limit)
        self._symbol_resolver = None
        self._now_func = now_func or (lambda: int(datetime.now(tz=timezone.utc).timestamp() * 1000))

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        """获取 Kucoin 成交，用 positions_history 校准 PnL 后批量富化订单详情。"""
        trades = await self._fetch_trades(since_ms, until_ms)
        if not trades:
            return []

        # 从成交计算本地已实现 PnL（毛值），可用时减去手续费
        local_pnls, _ = compute_realized_pnls_from_trades(trades)

        closes = [
            t
            for t in trades
            if (t["side"] == "sell" and t["position_side"] == "long")
            or (t["side"] == "buy" and t["position_side"] == "short")
        ]
        events: Dict[str, Dict[str, object]] = {}
        for t in trades:
            ev = dict(t)
            fee_cost = _fee_cost(ev.get("fees"))
            ev["pnl"] = local_pnls.get(ev["id"], 0.0) - fee_cost
            events[ev["id"]] = ev

        if closes:
            ph = await self._fetch_positions_history(
                start_ms=closes[0]["timestamp"] - 60_000,
                end_ms=closes[-1]["timestamp"] + 60_000,
            )
            self._match_pnls(closes, ph, events)
            self._log_discrepancies(local_pnls, ph)

        ordered = sorted(events.values(), key=lambda ev: ev["timestamp"])
        await self._enrich_with_order_details_bulk(ordered, detail_cache)
        if on_batch and ordered:
            on_batch(ordered)
        return ordered

    async def _fetch_trades(
        self, since_ms: Optional[int], until_ms: Optional[int]
    ) -> List[Dict[str, object]]:
        """按时间窗口分页获取 Kucoin 成交记录。"""
        now_ms = self._now_func()
        until_ts = int(until_ms) if until_ms is not None else now_ms + 3_600_000
        since_ts = int(since_ms) if since_ms is not None else until_ts - 24 * 60 * 60 * 1000
        buffer_ms = int(24 * 60 * 60 * 1000 * 0.99)
        limit = min(self.trade_limit, 1000)

        collected: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        start_at = since_ts
        prev_params = None
        fetch_count = 0

        while start_at < until_ts and fetch_count < max_fetches:
            fetch_count += 1
            end_at = min(start_at + buffer_ms, until_ts)
            params: Dict[str, object] = {
                "startAt": int(start_at),
                "endAt": int(end_at),
                "limit": limit,
            }
            key = _check_pagination_progress(prev_params, dict(params), "KucoinFetcher._fetch_trades")
            if key is None:
                break
            prev_params = key
            batch = await self.api.fetch_my_trades(params=params)
            if fetch_count > 1:
                logger.debug(
                    "KucoinFetcher._fetch_trades: fetch #%d startAt=%s endAt=%s size=%d",
                    fetch_count,
                    _format_ms(params["startAt"]),
                    _format_ms(params["endAt"]),
                    len(batch) if batch else 0,
                )
            if not batch:
                start_at += buffer_ms
                continue

            batch_sorted = sorted(batch, key=lambda x: x.get("timestamp", 0))
            for trade in batch_sorted:
                event = self._normalize_trade(trade)
                ts = event["timestamp"]
                if ts < since_ts or ts > until_ts:
                    continue
                key = (event.get("id") or "", event.get("order_id") or "")
                collected[key] = event

            last_ts = int(batch_sorted[-1].get("timestamp", start_at))
            if last_ts <= start_at:
                start_at = start_at + buffer_ms
            else:
                start_at = last_ts + 1

        if fetch_count >= max_fetches:
            logger.warning("KucoinFetcher._fetch_trades: reached pagination cap (%d)", max_fetches)

        return sorted(collected.values(), key=lambda ev: ev["timestamp"])

    async def _fetch_positions_history(self, start_ms: int, end_ms: int) -> List[Dict[str, object]]:
        """按时间窗口分页获取 Kucoin 持仓历史记录。"""
        results: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        fetch_count = 0
        buffer_ms = int(24 * 60 * 60 * 1000 * 0.99)
        limit = 200
        now_ms = self._now_func()
        until_ts = int(end_ms) if end_ms is not None else now_ms + 3_600_000
        since_ts = int(start_ms) if start_ms is not None else until_ts - 24 * 60 * 60 * 1000

        start_at = since_ts
        prev_params = None
        while start_at < until_ts and fetch_count < max_fetches:
            end_at = min(start_at + buffer_ms, until_ts)
            params: Dict[str, object] = {"from": int(start_at), "to": int(end_at), "limit": limit}
            key = _check_pagination_progress(
                prev_params, dict(params), "KucoinFetcher._fetch_positions_history"
            )
            if key is None:
                break
            prev_params = key
            fetch_count += 1
            batch = await self.api.fetch_positions_history(params=params)
            if fetch_count > 1:
                logger.debug(
                    "KucoinFetcher._fetch_positions_history: fetch #%d from=%s to=%s size=%d",
                    fetch_count,
                    _format_ms(params.get("from")),
                    _format_ms(params.get("to")),
                    len(batch) if batch else 0,
                )
            if not batch:
                start_at += buffer_ms
                continue
            batch_sorted = sorted(batch, key=lambda x: x.get("lastUpdateTimestamp", 0))
            for pos in batch_sorted:
                close_id = str(pos.get("info", {}).get("closeId") or pos.get("id") or "")
                results[close_id] = pos
            last_ts = int(batch_sorted[-1].get("lastUpdateTimestamp", end_at))
            if last_ts <= start_at:
                start_at += buffer_ms
            else:
                start_at = last_ts + 1

        if fetch_count >= max_fetches:
            logger.warning(
                "KucoinFetcher._fetch_positions_history: reached pagination cap (%d)", max_fetches
            )

        return sorted(results.values(), key=lambda x: x.get("lastUpdateTimestamp", 0))

    def _match_pnls(
        self,
        closes: List[Dict[str, object]],
        positions: List[Dict[str, object]],
        events: Dict[str, Dict[str, object]],
    ) -> None:
        """将 positions_history 中的持仓平仓 PnL 匹配到成交。

        使用 5 分钟窗口查找可能属于同一持仓平仓的所有成交。
        当多笔成交匹配到同一持仓平仓时：
        - PnL 按成交数量按比例分配
        - 这确保无论有多少笔成交关闭了持仓，总 PnL 都正确求和
        """
        match_window_ms = 5 * 60 * 1000  # 5 分钟匹配窗口

        closes_by_symbol: Dict[str, List[Dict[str, object]]] = defaultdict(list)
        for c in closes:
            closes_by_symbol[c["symbol"]].append(c)
        positions_by_symbol: Dict[str, List[Dict[str, object]]] = defaultdict(list)
        for p in positions:
            positions_by_symbol[p.get("symbol", "")].append(p)

        # 跟踪哪些成交已被分配 PnL
        assigned_trade_ids: set[str] = set()
        unmatched_positions = []

        for symbol, pos_list in positions_by_symbol.items():
            if symbol not in closes_by_symbol:
                unmatched_positions.extend(pos_list)
                continue

            symbol_closes = closes_by_symbol[symbol]
            for p in pos_list:
                p_ts = p.get("lastUpdateTimestamp", 0)
                p_pnl = float(p.get("realizedPnl", 0.0) or 0.0)

                # 查找匹配窗口内尚未分配的所有成交
                matching_fills = [
                    c
                    for c in symbol_closes
                    if c["id"] not in assigned_trade_ids
                    and abs(c["timestamp"] - p_ts) < match_window_ms
                ]

                if not matching_fills:
                    # 尝试扩大此持仓的匹配窗口
                    unmatched_positions.append(p)
                    continue

                # 计算匹配成交的总数量
                total_qty = sum(
                    abs(float(f.get("qty", 0) or f.get("amount", 0) or 0)) for f in matching_fills
                )

                if total_qty <= 0:
                    # 回退：将所有 PnL 分配给最近的成交
                    closest = min(matching_fills, key=lambda c: abs(c["timestamp"] - p_ts))
                    events[closest["id"]]["pnl"] = p_pnl
                    assigned_trade_ids.add(closest["id"])
                else:
                    # 按数量按比例分配 PnL
                    for fill in matching_fills:
                        fill_qty = abs(float(fill.get("qty", 0) or fill.get("amount", 0) or 0))
                        proportion = fill_qty / total_qty if total_qty > 0 else 0
                        events[fill["id"]]["pnl"] = p_pnl * proportion
                        assigned_trade_ids.add(fill["id"])

        # 为未从 positions_history 分配到 PnL 的平仓设置 PnL 为 0
        for c in closes:
            if c["id"] not in assigned_trade_ids:
                # 此平仓未匹配到任何 positions_history 条目 - 将 local_pnl 设为 0
                # 因为没有可靠的入场数据来计算
                events[c["id"]]["pnl"] = 0.0

        # 记录未匹配的持仓用于调试
        if unmatched_positions:
            total_unmatched_pnl = sum(
                float(p.get("realizedPnl", 0) or 0) for p in unmatched_positions
            )
            logger.debug(
                "[pnl] KucoinFetcher._match_pnls: %d position closes (%s total PnL) "
                "could not be matched to any trade fills",
                len(unmatched_positions),
                f"{total_unmatched_pnl:.4f}",
            )

    def _log_discrepancies(
        self, local_pnls: Dict[str, float], positions: List[Dict[str, object]]
    ) -> None:
        """比较本地计算的 PnL 与 positions_history 的 PnL，节流记录显著差异。"""
        if not positions or not local_pnls:
            return
        # 按交易对聚合用于粗略对账
        pos_sum: Dict[str, float] = defaultdict(float)
        for p in positions:
            sym = p.get("symbol") or p.get("info", {}).get("symbol") or ""
            if not sym:
                continue
            try:
                pos_sum[sym] += float(p.get("realizedPnl", 0.0))
            except Exception:
                continue
        if not pos_sum:
            return
        # 此处无法从成交 ID 推断按交易对的本地聚合；报告全局总和
        local_total = sum(local_pnls.values())
        remote_total = sum(pos_sum.values())
        if abs(local_total - remote_total) > max(1e-8, 0.05 * (abs(remote_total) + 1e-8)):
            # 节流：每小时记录一次，或在 delta 显著变化时立即记录
            now = time.time()
            throttle_key = f"kucoin:{id(self.api)}"
            last_log = _pnl_discrepancy_last_log.get(throttle_key, 0.0)
            last_delta = _pnl_discrepancy_last_delta.get(throttle_key)
            current_delta = local_total - remote_total
            # 记录条件：(1) delta 显著变化，或 (2) 节流窗口已过期
            delta_changed = last_delta is None or abs(
                current_delta - last_delta
            ) > _PNL_DISCREPANCY_CHANGE_THRESHOLD * (abs(last_delta) + 1.0)
            time_since_last = now - last_log
            should_log = (
                delta_changed and time_since_last >= _PNL_DISCREPANCY_MIN_SECONDS
            ) or time_since_last >= _PNL_DISCREPANCY_THROTTLE_SECONDS
            if should_log:
                _pnl_discrepancy_last_log[throttle_key] = now
                _pnl_discrepancy_last_delta[throttle_key] = current_delta
                logger.warning(
                    "[pnl] KucoinFetcher: local sum %.2f differs from positions_history %.2f (delta=%.2f)",
                    local_total,
                    remote_total,
                    current_delta,
                )

    @staticmethod
    def _normalize_trade(trade: Dict[str, object]) -> Dict[str, object]:
        """将 Kucoin 成交记录标准化为内部事件格式。"""
        info = trade.get("info", {}) or {}
        trade_id = str(trade.get("id") or info.get("tradeId") or info.get("id") or "")
        order_id = str(trade.get("order") or info.get("orderId") or "")
        ts_raw = (
            info.get("tradeTime")
            or trade.get("timestamp")
            or info.get("createdAt")
            or info.get("updatedTime")
            or 0
        )
        try:
            timestamp = int(ensure_millis(float(ts_raw)))
        except Exception:
            try:
                timestamp = int(float(ts_raw))
            except Exception:
                timestamp = 0
        symbol = str(trade.get("symbol") or "")
        side = str(trade.get("side") or info.get("side") or "").lower()
        qty = abs(float(trade.get("amount") or info.get("size") or info.get("amount") or 0.0))
        price = float(trade.get("price") or info.get("price") or 0.0)
        fee = trade.get("fee")
        reduce_only = bool(trade.get("reduceOnly") or info.get("closeOrder") or False)
        close_fee_pay = float(info.get("closeFeePay") or 0.0)
        position_side = KucoinFetcher._determine_position_side(side, reduce_only, close_fee_pay)

        return {
            "id": trade_id,
            "order_id": order_id,
            "timestamp": timestamp,
            "datetime": ts_to_date(timestamp) if timestamp else "",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price,
            "pnl": 0.0,
            "fees": fee,
            "pb_order_type": "",
            "position_side": position_side,
            "client_order_id": str(trade.get("clientOrderId") or info.get("clientOid") or ""),
            "raw": [{"source": "fetch_my_trades", "data": dict(trade)}],
        }

    @staticmethod
    def _determine_position_side(side: str, reduce_only: bool, close_fee_pay: float) -> str:
        side = side.lower()
        if side == "buy":
            return "short" if close_fee_pay != 0.0 or reduce_only else "long"
        if side == "sell":
            return "long" if close_fee_pay != 0.0 or reduce_only else "short"
        return "long"

    async def _enrich_with_order_details_bulk(
        self, events: List[Dict[str, object]], detail_cache: Dict[str, Tuple[str, str]]
    ) -> None:
        """用订单详情中的 clientOid 富化事件。

        优化策略：
        1. 通过 tradeId 和 orderId 检查缓存
        2. 按 orderId 分组事件以避免重复 fetch_order 调用
        3. 在相同 orderId 的事件间共享结果
        """
        if events is None:
            return
        detail_cache = detail_cache or {}

        # 从缓存构建 orderId -> clientOid 查找表（用于已富化的事件）
        order_id_cache: Dict[str, Tuple[str, str]] = {}

        # 第一遍：应用缓存值并构建 orderId 查找表
        for ev in events:
            ev_id = ev.get("id")
            order_id = ev.get("order_id")

            # 通过 tradeId 检查缓存
            cached = detail_cache.get(ev_id) if ev_id else None
            if cached:
                ev["client_order_id"], ev["pb_order_type"] = cached
                # 同时为相同订单的其他事件填充 orderId 缓存
                if order_id:
                    order_id_cache[str(order_id)] = cached
                continue

            # 检查是否已知此 orderId 的 clientOid
            if order_id and str(order_id) in order_id_cache:
                client_oid, pb_type = order_id_cache[str(order_id)]
                ev["client_order_id"] = client_oid
                ev["pb_order_type"] = pb_type
                if ev_id:
                    detail_cache[ev_id] = (client_oid, pb_type)

        # 第二遍：收集仍需富化的事件，按 orderId 分组
        events_by_order: Dict[str, List[Dict[str, object]]] = defaultdict(list)
        for ev in events:
            has_client = bool(ev.get("client_order_id"))
            has_type = bool(ev.get("pb_order_type")) and ev["pb_order_type"] != "unknown"
            if has_client and has_type:
                continue
            order_id = ev.get("order_id")
            if not order_id:
                ev.setdefault("pb_order_type", "unknown")
                continue
            # 已获取此 orderId 则跳过
            if str(order_id) in order_id_cache:
                client_oid, pb_type = order_id_cache[str(order_id)]
                ev["client_order_id"] = client_oid
                ev["pb_order_type"] = pb_type
                ev_id = ev.get("id")
                if ev_id:
                    detail_cache[ev_id] = (client_oid, pb_type)
                continue
            events_by_order[str(order_id)].append(ev)

        unique_orders = list(events_by_order.keys())
        if unique_orders:
            # 获取每个 orderId 的交易对（使用第一个事件的交易对）
            order_symbols = {oid: evs[0].get("symbol") for oid, evs in events_by_order.items()}

            # 限制并发以避免 API 过载
            sem = asyncio.Semaphore(8)
            total = len(unique_orders)
            completed = 0
            last_log_time = time.time()
            log_interval = 5.0

            async def throttled_fetch(order_id: str) -> Tuple[str, Optional[Tuple[str, str]]]:
                """带并发限制的订单详情获取。"""
                nonlocal completed, last_log_time
                async with sem:
                    symbol = order_symbols.get(order_id)
                    result = await self._enrich_with_order_details(order_id, symbol)
                    completed += 1
                    now = time.time()
                    if total > 50 and (now - last_log_time >= log_interval):
                        last_log_time = now
                        pct = int(100 * completed / total)
                        logger.info(
                            "KucoinFetcher: enriching order details %d/%d (%d%%)",
                            completed,
                            total,
                            pct,
                        )
                    return order_id, result

            total_events = sum(len(evs) for evs in events_by_order.values())
            if total > 50:
                logger.info(
                    "KucoinFetcher: enriching %d events via %d unique orders (concurrency=8)...",
                    total_events,
                    total,
                )

            tasks = [throttled_fetch(oid) for oid in unique_orders]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            if total > 50:
                logger.info(
                    "KucoinFetcher: enrichment complete (%d orders, %d events)", total, total_events
                )

            # 将结果应用于共享相同 orderId 的所有事件
            for res in results:
                if isinstance(res, Exception):
                    continue
                order_id, detail = res
                if detail is None:
                    # 将此 orderId 的所有事件标记为未知
                    for ev in events_by_order.get(order_id, []):
                        ev.setdefault("pb_order_type", "unknown")
                    continue

                client_oid, pb_type = detail
                order_id_cache[order_id] = (client_oid, pb_type)

                # 应用于此 orderId 的所有事件
                for ev in events_by_order.get(order_id, []):
                    ev["client_order_id"] = client_oid or ev.get("client_order_id") or ""
                    ev["pb_order_type"] = pb_type or "unknown"
                    ev_id = ev.get("id")
                    if ev_id:
                        detail_cache[ev_id] = (ev["client_order_id"], ev["pb_order_type"])

        # 最终遍历：确保所有事件都有 pb_order_type
        for ev in events:
            if not ev.get("pb_order_type"):
                ev["pb_order_type"] = "unknown"

    async def _enrich_with_order_details(
        self, order_id: Optional[str], symbol: Optional[str]
    ) -> Optional[Tuple[str, str]]:
        """获取 Kucoin 订单详情以提取 client_order_id。"""
        if not order_id:
            return None
        try:
            detail = await self.api.fetch_order(order_id, symbol)
        except Exception as exc:  # pragma: no cover - 依赖实盘 API
            logger.debug(
                "KucoinFetcher._enrich_with_order_details: fetch_order failed for %s (%s)",
                order_id,
                exc,
            )
            return None
        info = detail.get("info") if isinstance(detail, dict) else detail
        if not isinstance(info, dict):
            return None
        client_oid = (
            detail.get("clientOrderId")
            or info.get("clientOrderId")
            or info.get("clientOid")
            or info.get("clientOid")
        )
        if not client_oid:
            return None
        client_oid = str(client_oid)
        return client_oid, custom_id_to_snake(client_oid)


# ---------------------------------------------------------------------------
# Bitget 集成工具
# ---------------------------------------------------------------------------


class OkxFetcher(BaseFetcher):
    """使用 fills 和 fills-history 端点从 OKX 获取成交事件。

    OKX 在单个端点中提供所有必需字段：
    - tradeId：唯一成交标识
    - fillPnl：已实现 PnL
    - posSide：持仓方向（long/short/net）
    - clOrdId：客户端订单 ID（passivbot 订单类型）
    - fillSz：成交数量
    - fillPx：成交价格

    端点：
    - /api/v5/trade/fills：最近 3 天（较高频率限制）
    - /api/v5/trade/fills-history：最近 3 个月（较低频率限制）

    分页：最新优先返回；使用 'after' 参数配合 billId 进行向后分页。
    """

    # 3 天的毫秒值 - 选择 /fills 和 /fills-history 的阈值
    _THREE_DAYS_MS = 3 * 24 * 60 * 60 * 1000

    def __init__(
        self,
        api,
        *,
        trade_limit: int = 100,
        inst_type: str = "SWAP",
    ) -> None:
        self.api = api
        self.trade_limit = max(1, min(100, trade_limit))  # OKX 最大为 100
        self.inst_type = inst_type

    async def fetch(
        self,
        since_ms: Optional[int],
        until_ms: Optional[int],
        detail_cache: Dict[str, Tuple[str, str]],
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]] = None,
    ) -> List[Dict[str, object]]:
        """从 OKX /fills 和 /fills-history 端点获取成交事件。"""
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        until_ms = until_ms or now_ms

        # 根据时间范围确定使用哪个端点
        three_days_ago = now_ms - self._THREE_DAYS_MS

        collected: Dict[str, Dict[str, object]] = {}
        max_fetches = 400
        fetch_count = 0

        logger.debug(
            "OkxFetcher.fetch: start (since=%s, until=%s)",
            _format_ms(since_ms),
            _format_ms(until_ms),
        )

        # 如果需要 3 天前的数据，从 fills-history 开始
        if since_ms is not None and since_ms < three_days_ago:
            # 使用 fills-history 获取较早的数据
            fetch_count, collected = await self._fetch_from_endpoint(
                endpoint="history",
                since_ms=since_ms,
                until_ms=min(until_ms, three_days_ago),
                collected=collected,
                max_fetches=max_fetches,
                start_fetch_count=fetch_count,
                on_batch=on_batch,
                detail_cache=detail_cache,
            )

        # 使用 /fills 获取最近数据（最近 3 天）
        recent_since = max(since_ms or 0, three_days_ago) if since_ms else three_days_ago
        if until_ms > three_days_ago:
            fetch_count, collected = await self._fetch_from_endpoint(
                endpoint="recent",
                since_ms=recent_since if since_ms else None,
                until_ms=until_ms,
                collected=collected,
                max_fetches=max_fetches,
                start_fetch_count=fetch_count,
                on_batch=on_batch,
                detail_cache=detail_cache,
            )

        # 排序和过滤结果
        events = sorted(collected.values(), key=lambda ev: ev["timestamp"])

        # 应用时间过滤
        if since_ms is not None:
            events = [ev for ev in events if ev["timestamp"] >= since_ms]
        if until_ms is not None:
            events = [ev for ev in events if ev["timestamp"] <= until_ms]

        # 合并重复事件
        events = _coalesce_events(events)
        # 注意：psize/pprice 标注在 FillEventsManager.refresh() 中集中完成

        # 从缓存应用 pb_order_type 或从 clOrdId 推导
        for event in events:
            cache_entry = detail_cache.get(event["id"])
            if cache_entry:
                event["client_order_id"], event["pb_order_type"] = cache_entry
            elif event["client_order_id"]:
                event["pb_order_type"] = custom_id_to_snake(event["client_order_id"])
            else:
                event["pb_order_type"] = "unknown"
            if not event["pb_order_type"]:
                event["pb_order_type"] = "unknown"

        logger.debug(
            "OkxFetcher.fetch: done (events=%d, fetches=%d)",
            len(events),
            fetch_count,
        )
        return events

    async def _fetch_from_endpoint(
        self,
        endpoint: str,
        since_ms: Optional[int],
        until_ms: int,
        collected: Dict[str, Dict[str, object]],
        max_fetches: int,
        start_fetch_count: int,
        on_batch: Optional[Callable[[List[Dict[str, object]]], None]],
        detail_cache: Dict[str, Tuple[str, str]],
    ) -> Tuple[int, Dict[str, Dict[str, object]]]:
        """从 /fills（最近）或 /fills-history（历史）端点获取成交。"""
        fetch_count = start_fetch_count
        after_cursor: Optional[str] = None

        endpoint_name = "fills" if endpoint == "recent" else "fills-history"
        logger.debug(
            "OkxFetcher: using /%s endpoint (since=%s, until=%s)",
            endpoint_name,
            _format_ms(since_ms),
            _format_ms(until_ms),
        )

        while fetch_count < max_fetches:
            params: Dict[str, object] = {
                "instType": self.inst_type,
                "limit": str(self.trade_limit),
            }

            # 时间窗口
            if since_ms is not None:
                params["begin"] = str(since_ms)
            if until_ms is not None:
                params["end"] = str(until_ms)

            # 分页游标
            if after_cursor:
                params["after"] = after_cursor

            try:
                if endpoint == "recent":
                    response = await self.api.private_get_trade_fills(params)
                else:
                    response = await self.api.private_get_trade_fills_history(params)
            except RateLimitExceeded as exc:
                logger.debug("OkxFetcher: rate limit hit, sleeping (%s)", exc)
                await asyncio.sleep(2.0)
                continue

            fetch_count += 1
            fills = response.get("data", [])

            if fetch_count > 1:
                logger.debug(
                    "OkxFetcher.fetch: /%s #%d after=%s size=%d",
                    endpoint_name,
                    fetch_count,
                    after_cursor,
                    len(fills),
                )

            if not fills:
                break

            batch_events = []
            oldest_ts = None
            for raw in fills:
                event = self._normalize_fill(raw)
                event_id = event["id"]
                if not event_id:
                    continue

                # 确保在批量回调之前填充 client_order_id/pb_order_type
                cached = detail_cache.get(event_id)
                if cached:
                    cached_client, cached_pb = cached
                    if cached_client:
                        event["client_order_id"] = cached_client
                    if cached_pb:
                        event["pb_order_type"] = cached_pb
                client_oid = str(event.get("client_order_id") or "")
                pb_type = str(event.get("pb_order_type") or "")
                if not pb_type and client_oid:
                    pb_type = custom_id_to_snake(client_oid)
                if not pb_type:
                    pb_type = "unknown"
                event["client_order_id"] = client_oid
                event["pb_order_type"] = pb_type
                if event_id and client_oid:
                    detail_cache[event_id] = (client_oid, pb_type)

                # 检查时间范围
                ts = event["timestamp"]
                if since_ms is not None and ts < since_ms:
                    continue
                if until_ms is not None and ts > until_ms:
                    continue

                # 跟踪最早时间用于边界检查
                if oldest_ts is None or ts < oldest_ts:
                    oldest_ts = ts

                # 从缓存富化
                if event_id in detail_cache:
                    event["client_order_id"], event["pb_order_type"] = detail_cache[event_id]

                collected[event_id] = event
                batch_events.append(event)

            # 增量处理的回调
            if on_batch and batch_events:
                on_batch(batch_events)

            # 检查是否到达起始边界
            if since_ms is not None and oldest_ts is not None and oldest_ts <= since_ms:
                logger.debug("OkxFetcher: reached since_ms boundary, stopping")
                break

            # 短批次表示无更多数据
            if len(fills) < self.trade_limit:
                break

            # 获取下一批次的分页游标（使用最早成交的 billId）
            last_fill = fills[-1]
            after_cursor = last_fill.get("billId")
            if not after_cursor:
                break

        return fetch_count, collected

    @staticmethod
    def _normalize_fill(raw: Dict[str, object]) -> Dict[str, object]:
        """将原始 OKX 成交标准化为规范的成交事件格式。"""
        trade_id = str(raw.get("tradeId") or "")
        order_id = str(raw.get("ordId") or "")
        timestamp = int(raw.get("ts") or raw.get("fillTime") or 0)
        inst_id = str(raw.get("instId") or "")

        # 将 instId（例如 "BTC-USDT-SWAP"）转换为 CCXT 交易对格式
        symbol = inst_id
        if "-SWAP" in inst_id:
            parts = inst_id.replace("-SWAP", "").split("-")
            if len(parts) == 2:
                base, quote = parts
                symbol = f"{base}/{quote}:{quote}"
        elif "-" in inst_id:
            parts = inst_id.split("-")
            if len(parts) >= 2:
                base, quote = parts[0], parts[1]
                symbol = f"{base}/{quote}:{quote}"

        side = str(raw.get("side") or "").lower()
        qty = abs(float(raw.get("fillSz") or 0.0))
        price = float(raw.get("fillPx") or 0.0)
        pnl = float(raw.get("fillPnl") or 0.0)

        # 持仓方向处理（支持对冲和净头寸模式）
        pos_side_raw = str(raw.get("posSide") or "").lower()
        if pos_side_raw == "net":
            # 净头寸模式：从 side + pnl 推断持仓方向
            # 如果平仓（有 PnL），交易方向的反向即为持仓方向
            if pnl != 0:
                position_side = "short" if side == "buy" else "long"
            else:
                # 开仓：与交易方向相同
                position_side = "long" if side == "buy" else "short"
        elif pos_side_raw in ("long", "short"):
            position_side = pos_side_raw
        else:
            # 回退
            position_side = "long" if side == "buy" else "short"

        client_order_id = str(raw.get("clOrdId") or "")
        fee_ccy = str(raw.get("feeCcy") or "")
        fee_amt = float(raw.get("fee") or 0.0)
        fee = {"currency": fee_ccy, "cost": abs(fee_amt)} if fee_ccy else None

        return {
            "id": trade_id,
            "order_id": order_id,
            "timestamp": timestamp,
            "datetime": ts_to_date(timestamp) if timestamp else "",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "price": price,
            "pnl": pnl,
            "fees": fee,
            "pb_order_type": "",
            "position_side": position_side,
            "client_order_id": client_order_id,
            "raw": [{"source": "okx_fills", "data": raw}],
            "c_mult": 1.0,
        }


def custom_id_to_snake(client_oid: str) -> str:
    """占位导入垫片；实际实现在 passivbot 中。"""
    try:
        from passivbot import custom_id_to_snake as _real

        return _real(client_oid)
    except Exception:
        return client_oid or ""


def deduce_side_pside(elm: dict) -> Tuple[str, str]:
    """可用时从 exchanges.bitget 导入辅助函数。"""
    try:
        from exchanges.bitget import deduce_side_pside as _real

        return _real(elm)
    except Exception:
        side = str(elm.get("side", "buy")).lower()
        return side or "buy", "long"


# ---------------------------------------------------------------------------
# CLI 辅助工具
# ---------------------------------------------------------------------------


EXCHANGE_BOT_CLASSES: Dict[str, Tuple[str, str]] = {
    "binance": ("exchanges.binance", "BinanceBot"),
    "bitget": ("exchanges.bitget", "BitgetBot"),
    "bybit": ("exchanges.bybit", "BybitBot"),
    "fake": ("exchanges.fake", "FakeBot"),
    "hyperliquid": ("exchanges.hyperliquid", "HyperliquidBot"),
    "gateio": ("exchanges.gateio", "GateIOBot"),
    "kucoin": ("exchanges.kucoin", "KucoinBot"),
    "okx": ("exchanges.okx", "OKXBot"),
}


def _parse_time_arg(value: Optional[str]) -> Optional[int]:
    """解析时间参数，支持毫秒时间戳、ISO 格式和 'now' 关键字。"""
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        ts = int(value)
        if ts < 10**11:
            ts *= 1000
        return ts
    except ValueError:
        pass
    try:
        if value.lower() == "now":
            dt = datetime.now(tz=timezone.utc)
        else:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        raise ValueError(f"Unable to parse datetime '{value}'")


def _parse_log_level(value: str) -> int:
    mapping = {"warning": 0, "warn": 0, "info": 1, "debug": 2, "trace": 3}
    if value is None:
        return 1
    value = str(value).strip().lower()
    if value in mapping:
        return mapping[value]
    try:
        lvl = int(float(value))
        return max(0, min(3, lvl))
    except Exception:
        return 1


def _extract_symbol_pool(config: dict, override: Optional[List[str]]) -> List[str]:
    if override:
        return sorted({sym for sym in override if sym})
    live = config.get("live", {})
    approved = live.get("approved_coins")
    symbols: List[str] = []
    if isinstance(approved, dict):
        for vals in approved.values():
            if isinstance(vals, list):
                symbols.extend(vals)
    elif isinstance(approved, list):
        symbols.extend(approved)
    return sorted({sym for sym in symbols if sym})


def _symbol_resolver(bot) -> Callable[[Optional[str]], str]:
    """构建交易对符号解析器，将交易所原生符号映射为 CCXT 格式。"""
    def resolver(raw: Optional[str]) -> str:
        """将交易所原生交易对符号解析为 CCXT 格式。"""
        if not raw:
            return ""
        if isinstance(raw, str) and "/" in raw:
            return raw
        value = "" if raw is None else str(raw)
        if not value:
            return ""
        # 优先使用 bot 的 coin_to_symbol 映射，它处理交易所的特殊规则
        try:
            mapped = bot.coin_to_symbol(value, verbose=False)
            if mapped:
                return mapped
        except Exception:
            pass
        upper = value.upper()
        for quote in ("USDT", "USDC", "USD"):
            if upper.endswith(quote) and len(upper) > len(quote):
                base = upper[: -len(quote)]
                if base:
                    return f"{base}/{quote}:{quote}"
        if ":" in value and "/" not in value:
            base, _, quote = value.partition(":")
            if base and quote:
                return f"{base}/{quote}:{quote}"
        return value

    return resolver


def _build_fetcher_for_bot(bot, symbols: List[str]) -> BaseFetcher:
    """根据机器人交易所类型创建对应的成交获取器。"""
    exchange = getattr(bot, "exchange", "").lower()
    resolver = _symbol_resolver(bot)
    static_provider = lambda: symbols  # noqa: E731
    if exchange == "binance":
        return BinanceFetcher(
            api=bot.cca,
            symbol_resolver=resolver,
            positions_provider=static_provider,
            open_orders_provider=static_provider,
        )
    if exchange == "bitget":
        return BitgetFetcher(
            api=bot.cca,
            symbol_resolver=lambda value: resolver(value),
        )
    if exchange == "bybit":
        return BybitFetcher(api=bot.cca)
    if exchange == "fake":
        return FakeFetcher(api=bot.cca)
    if exchange == "hyperliquid":
        return HyperliquidFetcher(
            api=bot.cca,
            symbol_resolver=lambda value: resolver(value),
        )
    if exchange == "gateio":
        return GateioFetcher(
            api=bot.cca,
        )
    if exchange == "kucoin":
        return KucoinFetcher(api=bot.cca)
    if exchange == "okx":
        return OkxFetcher(api=bot.cca)
    raise ValueError(f"Unsupported exchange '{exchange}' for fill events CLI")


def _instantiate_bot(config: dict):
    live = config.get("live", {})
    user = str(live.get("user") or "").strip()
    if not user:
        raise ValueError("Config missing live.user to determine bot exchange")
    user_info = load_user_info(user)
    exchange = str(user_info.get("exchange") or "").lower()
    if not exchange:
        raise ValueError(f"User '{user}' has no exchange configured in api-keys.json")
    bot_cls_info = EXCHANGE_BOT_CLASSES.get(exchange)
    if bot_cls_info is None:
        raise ValueError(f"No bot class registered for exchange '{exchange}'")
    module = import_module(bot_cls_info[0])
    bot_cls = getattr(module, bot_cls_info[1])
    return bot_cls(config)


async def _run_cli(args: argparse.Namespace) -> None:
    """CLI 入口：解析配置，初始化机器人和管理器，执行刷新操作。"""
    source_config, base_config_path, raw_snapshot = load_input_config(args.config)
    config = prepare_config(
        source_config,
        base_config_path=base_config_path,
        verbose=False,
        target="live",
        runtime="live",
        raw_snapshot=raw_snapshot,
    )
    live = config.setdefault("live", {})
    if args.user:
        live["user"] = args.user
    bot = _instantiate_bot(config)
    try:
        symbol_pool = _extract_symbol_pool(config, args.symbols)
        fetcher = _build_fetcher_for_bot(bot, symbol_pool)
        cache_root = Path(args.cache_root)
        cache_path = cache_root / bot.exchange / bot.user
        manager = FillEventsManager(
            exchange=bot.exchange,
            user=bot.user,
            fetcher=fetcher,
            cache_path=cache_path,
        )
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        start_ms = _parse_time_arg(args.start) or (
            now_ms - int(args.lookback_days * 24 * 60 * 60 * 1000)
        )
        end_ms = _parse_time_arg(args.end) or now_ms
        if start_ms >= end_ms:
            raise ValueError("start time must be earlier than end time")
        logger.info(
            "fill_events_manager CLI | exchange=%s user=%s start=%s end=%s cache=%s",
            bot.exchange,
            bot.user,
            _format_ms(start_ms),
            _format_ms(end_ms),
            cache_path,
        )
        await manager.refresh_range(start_ms, end_ms)
        events = manager.get_events(start_ms, end_ms)
        logger.info("fill_events_manager CLI: events=%d written to %s", len(events), cache_path)
    finally:
        try:
            await bot.close()
        except Exception:
            pass


def main() -> None:
    """CLI 主入口：解析命令行参数并运行成交事件缓存刷新。"""
    parser = argparse.ArgumentParser(description="Fill events cache refresher")
    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default=None,
        help="Config path (defaults to in-code schema defaults)",
    )
    parser.add_argument("--user", "-u", type=str, required=True, help="Live user identifier")
    parser.add_argument("--start", "-s", type=str, help="Start datetime (ms or ISO)")
    parser.add_argument("--end", "-e", type=str, help="End datetime (ms or ISO)")
    parser.add_argument(
        "--lookback-days",
        "-d",
        type=float,
        default=30.0,
        help="Default lookback window in days when start is omitted",
    )
    parser.add_argument(
        "--log-level",
        "-l",
        type=str,
        default="info",
        help="Logging verbosity (warning/info/debug/trace or 0-3)",
    )
    parser.add_argument(
        "--cache-root",
        "-r",
        type=str,
        default="caches/fill_events",
        help="Root directory for fill events cache (default: caches/fill_events)",
    )
    parser.add_argument(
        "--symbols",
        "-S",
        nargs="*",
        default=None,
        help="Optional explicit symbol list to fetch",
    )
    args = parser.parse_args()
    configure_logging(debug=_parse_log_level(args.log_level))
    try:
        asyncio.run(_run_cli(args))
    except KeyboardInterrupt:
        logger.info("fill_events_manager CLI interrupted by user")


if __name__ == "__main__":
    main()
