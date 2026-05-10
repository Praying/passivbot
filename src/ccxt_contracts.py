from __future__ import annotations

import asyncio
import json
import math
import re
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

import ccxt

from config.load import prepare_config
from config.schema import get_template_config
from passivbot import setup_bot
from utils import _build_coin_symbol_maps, filter_markets


DEFAULT_CAPTURE_SECTIONS = ("markets", "capabilities", "balance", "positions", "open_orders")
DEFAULT_CAPABILITY_KEYS = (
    "fetchBalance",
    "fetchPositions",
    "fetchOpenOrders",
    "fetchMyTrades",
    "setLeverage",
    "setMarginMode",
    "setPositionMode",
    "watchOrders",
)
DEFAULT_DIFF_IGNORE_PATHS = {"meta.captured_at"}


class _AsyncReturn:
    """异步调用时返回深拷贝的固定值。"""
    def __init__(self, value: Any):
        self.value = deepcopy(value)

    async def __call__(self, *args, **kwargs):
        return deepcopy(self.value)


def sanitize_for_json(value: Any) -> Any:
    """递归清洗值使其可 JSON 序列化，处理 NaN/Inf、bytes、Path 等类型。"""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return repr(value)
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [sanitize_for_json(v) for v in value]
    if hasattr(value, "item") and callable(getattr(value, "item")):
        try:
            return sanitize_for_json(value.item())
        except Exception:
            pass
    if hasattr(value, "isoformat") and callable(getattr(value, "isoformat")):
        try:
            return value.isoformat()
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        try:
            return sanitize_for_json(vars(value))
        except Exception:
            pass
    return repr(value)


def snapshot_slug(value: str) -> str:
    """将字符串转换为安全的文件名 slug。"""
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value).strip())
    slug = slug.strip("-")
    return slug or "snapshot"


def default_snapshot_path(base_dir: str | Path, exchange: str, label: str) -> Path:
    """构建默认快照路径：base_dir/exchange_label.json。"""
    return Path(base_dir) / snapshot_slug(exchange) / f"{snapshot_slug(label)}.json"


def get_bot_class(exchange: str):
    """根据交易所名称返回对应的 Bot 类。"""
    exchange = str(exchange).lower()
    if exchange == "binance":
        from exchanges.binance import BinanceBot

        return BinanceBot
    if exchange == "bitget":
        from exchanges.bitget import BitgetBot

        return BitgetBot
    if exchange == "bybit":
        from exchanges.bybit import BybitBot

        return BybitBot
    if exchange == "defx":
        from exchanges.defx import DefxBot

        return DefxBot
    if exchange == "gateio":
        from exchanges.gateio import GateIOBot

        return GateIOBot
    if exchange == "hyperliquid":
        from exchanges.hyperliquid import HyperliquidBot

        return HyperliquidBot
    if exchange == "kucoin":
        from exchanges.kucoin import KucoinBot

        return KucoinBot
    if exchange == "okx":
        from exchanges.okx import OKXBot

        return OKXBot
    if exchange == "paradex":
        from exchanges.paradex import ParadexBot

        return ParadexBot
    from exchanges.ccxt_bot import CCXTBot

    return CCXTBot


def build_contract_bot(exchange: str, quote: str = "USDT"):
    """构建用于合约探测的最小 Bot 实例，仅初始化必要属性。"""
    bot_cls = get_bot_class(exchange)
    bot = bot_cls.__new__(bot_cls)
    bot.exchange = str(exchange).lower()
    bot.user = "ccxt-contracts"
    bot.quote = quote
    bot.config = {
        "live": {
            "time_in_force": "gtc",
            "margin_mode_preference": "cross",
            "leverage": 3,
            "hedge_mode": True,
        }
    }
    bot.user_info = {"exchange": bot.exchange}
    bot.cca = SimpleNamespace(has={}, options={})
    bot.ccp = None
    bot.markets_dict = {}
    bot.symbol_ids = {}
    bot.symbol_ids_inv = {}
    bot.min_costs = {}
    bot.min_qtys = {}
    bot.qty_steps = {}
    bot.price_steps = {}
    bot.c_mults = {}
    bot.max_leverage = {}
    bot._live_margin_modes = {}
    bot._hl_live_margin_modes = {}
    bot._blocked_margin_symbols_warned = set()
    bot._margin_mode_preference_warned = False
    bot.positions = {}
    bot.open_orders = {}
    bot.active_symbols = []
    bot.log_once = lambda *args, **kwargs: None
    bot.coin_to_symbol = lambda coin, verbose=True: coin
    bot.get_symbol_id_inv = lambda symbol: symbol
    bot.has_position = lambda pside, symbol: abs(
        float(getattr(bot, "positions", {}).get(symbol, {}).get(pside, {}).get("size", 0.0) or 0.0)
    ) > 0.0
    bot.get_symbols_with_pos = lambda: [
        symbol
        for symbol, sides in getattr(bot, "positions", {}).items()
        if any(abs(float(sides.get(pside, {}).get("size", 0.0) or 0.0)) > 0.0 for pside in ("long", "short"))
    ]
    return bot


def derive_market_contracts(exchange: str, quote: str, markets: dict[str, dict]) -> dict[str, dict[str, Any]]:
    """推导市场合约参数，包括最小下单量、步长、合约乘数等。"""
    bot = build_contract_bot(exchange=exchange, quote=quote)
    bot.markets_dict = deepcopy(markets)
    bot.set_market_specific_settings()
    contracts = {}
    for symbol in sorted(markets):
        contracts[symbol] = {
            "id": bot.symbol_ids.get(symbol),
            "min_cost": bot.min_costs.get(symbol),
            "min_qty": bot.min_qtys.get(symbol),
            "qty_step": bot.qty_steps.get(symbol),
            "price_step": bot.price_steps.get(symbol),
            "contract_size": bot.c_mults.get(symbol),
            "max_leverage": bot.max_leverage.get(symbol),
            "requires_isolated_margin": bool(bot._requires_isolated_margin(symbol)),
            "margin_capability": bot._get_margin_capability(symbol),
        }
    return sanitize_for_json(contracts)


def summarize_market_snapshot(exchange: str, quote: str, markets: dict[str, dict]) -> dict[str, Any]:
    """生成市场快照摘要，包含合约参数、合格/不合格币种及映射关系。"""
    eligible, ineligible, reasons = filter_markets(markets, exchange, quote=quote)
    coin_to_symbol_map, symbol_to_coin_map = _build_coin_symbol_maps(markets, quote)
    return sanitize_for_json(
        {
            "contracts": derive_market_contracts(exchange, quote, markets),
            "eligible_symbols": sorted(eligible),
            "ineligible_symbols": sorted(ineligible),
            "ineligible_reasons": reasons,
            "coin_to_symbol_map": coin_to_symbol_map,
            "symbol_to_coin_map": symbol_to_coin_map,
        }
    )


def summarize_capabilities(has_map: dict[str, Any], keys: Iterable[str] = DEFAULT_CAPABILITY_KEYS) -> dict[str, Any]:
    """提取并清洗交易所能力映射中指定键的值。"""
    return {key: sanitize_for_json(has_map.get(key)) for key in keys}


def _flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    """递归展平嵌套结构为点分隔路径的扁平字典。"""
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key in sorted(value):
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            out.update(_flatten(value[key], child_prefix))
        return out
    if isinstance(value, list):
        out: dict[str, Any] = {}
        for i, item in enumerate(value):
            child_prefix = f"{prefix}[{i}]"
            out.update(_flatten(item, child_prefix))
        return out
    return {prefix: value}


def diff_snapshots(
    old_snapshot: dict[str, Any],
    new_snapshot: dict[str, Any],
    *,
    ignore_paths: Iterable[str] = DEFAULT_DIFF_IGNORE_PATHS,
) -> dict[str, Any]:
    """比较两个快照的差异，返回新增、删除和变更的路径列表。"""
    ignore = tuple(sorted(ignore_paths))
    old_flat = _flatten(sanitize_for_json(old_snapshot))
    new_flat = _flatten(sanitize_for_json(new_snapshot))
    added = []
    removed = []
    changed = []
    # 逐一比较展平后的键
    for key in sorted(set(old_flat) | set(new_flat)):
        # 跳过忽略路径
        if any(key == path or key.startswith(f"{path}.") or key.startswith(f"{path}[") for path in ignore):
            continue
        if key not in old_flat:
            added.append({"path": key, "new": new_flat[key]})
        elif key not in new_flat:
            removed.append({"path": key, "old": old_flat[key]})
        elif old_flat[key] != new_flat[key]:
            changed.append({"path": key, "old": old_flat[key], "new": new_flat[key]})
    return {
        "summary": {
            "added": len(added),
            "removed": len(removed),
            "changed": len(changed),
        },
        "added": added,
        "removed": removed,
        "changed": changed,
    }


def prepare_live_config_for_user(user: str) -> dict:
    """为指定用户准备 live 运行配置。"""
    config = get_template_config()
    config["live"]["user"] = user
    return prepare_config(
        config,
        live_only=True,
        verbose=False,
        target="live",
        runtime="live",
        raw_snapshot=config,
    )


async def capture_contract_snapshot(
    *,
    user: str,
    label: str | None = None,
    sections: Iterable[str] = DEFAULT_CAPTURE_SECTIONS,
    symbols: Iterable[str] = (),
    trades_limit: int = 25,
) -> dict[str, Any]:
    """异步捕获合约快照，包含市场、能力、余额、仓位等各节段数据。"""
    config = prepare_live_config_for_user(user)
    bot = setup_bot(config)
    try:
        exchange = str(bot.exchange).lower()
        symbols = [str(s) for s in symbols if str(s).strip()]
        snapshot: dict[str, Any] = {
            "meta": {
                "user_label": label or user,
                "exchange": exchange,
                "quote": getattr(bot, "quote", "USDT"),
                "ccxt_version": getattr(ccxt, "__version__", "unknown"),
                "python_version": sys.version.split()[0],
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "sections": list(sections),
                "symbols": symbols,
            }
        }
        section_set = set(sections)

        # 按节段分别捕获数据
        if "markets" in section_set:
            markets = await bot.cca.load_markets(True)
            snapshot["markets"] = {
                "raw": sanitize_for_json(markets),
                "summary": summarize_market_snapshot(exchange, bot.quote, markets),
            }
            bot.markets_dict = deepcopy(markets)
            bot.set_market_specific_settings()

        if "capabilities" in section_set:
            snapshot["capabilities"] = summarize_capabilities(getattr(bot.cca, "has", {}) or {})

        if "balance" in section_set:
            raw_balance, normalized_balance = await bot.capture_balance_snapshot()
            snapshot["balance"] = {
                "raw": sanitize_for_json(raw_balance),
                "normalized": sanitize_for_json(normalized_balance),
            }

        if "positions" in section_set:
            raw_positions, normalized_positions = await bot.capture_positions_snapshot()
            snapshot["positions"] = {
                "raw": sanitize_for_json(raw_positions),
                "normalized": sanitize_for_json(normalized_positions),
            }

        if "open_orders" in section_set:
            _, normalized_all_orders = await bot.capture_open_orders_snapshot()
            open_orders_section: dict[str, Any] = {"normalized_all": sanitize_for_json(normalized_all_orders)}
            # 按交易对分别获取挂单详情
            if symbols:
                open_orders_section["by_symbol"] = {}
                for symbol in symbols:
                    try:
                        raw_symbol_orders, normalized_symbol_orders = await bot.capture_open_orders_snapshot(
                            symbol=symbol
                        )
                    except Exception as exc:
                        raw_symbol_orders = {"error": f"{type(exc).__name__}: {exc}"}
                        normalized_symbol_orders = raw_symbol_orders
                    open_orders_section["by_symbol"][symbol] = {
                        "raw": sanitize_for_json(raw_symbol_orders),
                        "normalized": sanitize_for_json(normalized_symbol_orders),
                    }
            snapshot["open_orders"] = open_orders_section

        if "trades" in section_set:
            trades_section: dict[str, Any] = {}
            if symbols:
                trades_section["by_symbol"] = {}
                for symbol in symbols:
                    raw_trades = await bot.cca.fetch_my_trades(symbol=symbol, limit=trades_limit)
                    trades_section["by_symbol"][symbol] = sanitize_for_json(raw_trades)
            else:
                trades_section["raw"] = sanitize_for_json(
                    await bot.cca.fetch_my_trades(symbol=None, limit=trades_limit)
                )
            snapshot["trades"] = trades_section

        return snapshot
    finally:
        try:
            await asyncio.wait_for(bot.close(), timeout=3.0)
        except Exception:
            pass


def load_snapshot(path: str | Path) -> dict[str, Any]:
    """从 JSON 文件加载快照。"""
    with open(path, "r") as f:
        return json.load(f)


def dump_snapshot(snapshot: dict[str, Any], path: str | Path) -> Path:
    """将快照写入 JSON 文件，自动创建父目录。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(snapshot, f, indent=2, sort_keys=True)
        f.write("\n")
    return path
