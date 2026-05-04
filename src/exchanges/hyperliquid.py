import asyncio
import json
import random
import re
import traceback
from copy import deepcopy

import ccxt.pro as ccxt_pro
import ccxt.async_support as ccxt_async
import passivbot_rust as pbr
from ccxt.base.errors import RateLimitExceeded

from exchanges.ccxt_bot import CCXTBot, format_exchange_config_response
from passivbot import logging
from passivbot_exceptions import FatalBotException
from utils import ts_to_date, utc_ms
from config.access import require_live_value
from pure_funcs import calc_hash
from procedures import print_async_exception, assert_correct_ccxt_version

round_ = pbr.round_
round_dynamic = pbr.round_dynamic
round_dynamic_up = pbr.round_dynamic_up
round_dynamic_dn = pbr.round_dynamic_dn

_PASSIVBOT_CUSTOM_ID_MARKER_RE = re.compile(r"0x[0-9a-fA-F]{4}")

assert_correct_ccxt_version(ccxt=ccxt_async)


class HyperliquidBot(CCXTBot):
    # HIP-3 股票永续合约最大杠杆为 10x
    HIP3_MAX_LEVERAGE = 10
    # HIP-3 品种使用 "xyz:" 前缀（TradeXYZ 构建器）
    HIP3_PREFIX = "xyz:"
    HIP3_ALT_PREFIXES = ("XYZ-", "XYZ:")
    HIP3_ISOLATED_SUPPORTED = False
    HIP3_ORDER_MARGIN_BUFFER = 1.01

    def __init__(self, config: dict):
        super().__init__(config)
        self.quote = "USDC"
        self.hedge_mode = False
        self.significant_digits = {}
        self._hl_live_margin_modes = {}
        if "is_vault" not in self.user_info or self.user_info["is_vault"] == "":
            logging.info(
                f"用户 {self.user} 的 api-keys.json 中缺少参数 'is_vault'。设置为 false"
            )
            self.user_info["is_vault"] = False
        self.max_n_concurrent_ohlcvs_1m_updates = 2
        self.custom_id_max_length = 34
        self._hl_fetch_lock = asyncio.Lock()
        self._hl_cache_generation = 0
        self._hl_user_abstraction = "unknown"
        self._hl_unified_enabled = False

    def _hl_info_url(self) -> str:
        """从 CCXT 会话 URL 配置推导 Hyperliquid /info 端点。"""
        base = self.cca.urls.get("api", {}).get("public", "https://api.hyperliquid.xyz")
        hostname = getattr(self.cca, "hostname", "hyperliquid.xyz")
        return base.replace("{hostname}", hostname).rstrip("/") + "/info"

    def _normalize_hl_user_abstraction(self, raw) -> str:
        """将 Hyperliquid userAbstraction 响应规范化为稳定字符串。"""
        if raw is None:
            return "unknown"
        text = str(raw).strip()
        if len(text) >= 2 and text[0] == text[-1] == '"':
            text = text[1:-1]
        return text or "unknown"

    async def fetch_user_abstraction_state(self) -> str:
        """获取并缓存 Hyperliquid 账户抽象模式。"""
        wallet_address = str(self.user_info.get("wallet_address") or "")
        if not wallet_address:
            raise ValueError(f"用户 {self.user!r} 缺少用于 Hyperliquid 抽象的 wallet_address")
        raw = await self.cca.publicPostInfo({"type": "userAbstraction", "user": wallet_address})
        abstraction = self._normalize_hl_user_abstraction(raw)
        self._hl_user_abstraction = abstraction
        self._hl_unified_enabled = abstraction == "unifiedAccount"
        if getattr(self, "cca", None) is not None:
            self.cca.options["enableUnifiedMargin"] = bool(self._hl_unified_enabled)
        if getattr(self, "ccp", None) is not None:
            self.ccp.options["enableUnifiedMargin"] = bool(self._hl_unified_enabled)
        return abstraction

    async def refresh_and_log_user_abstraction_state(self) -> str:
        """刷新 Hyperliquid 账户抽象模式，记录首次发现或变更。"""
        abstraction = await self.fetch_user_abstraction_state()
        previous = getattr(self, "_hl_last_logged_user_abstraction", None)
        if previous is None:
            logging.info(
                "[account] Hyperliquid abstraction=%s | unified=%s",
                abstraction,
                "yes" if abstraction == "unifiedAccount" else "no",
            )
        elif previous != abstraction:
            logging.warning(
                "[account] Hyperliquid abstraction changed %s -> %s | unified=%s",
                previous,
                abstraction,
                "yes" if abstraction == "unifiedAccount" else "no",
            )
        self._hl_last_logged_user_abstraction = abstraction
        return abstraction

    def create_ccxt_sessions(self):
        creds = {
            "walletAddress": self.user_info["wallet_address"],
            "privateKey": self.user_info["private_key"],
        }
        # 配置 fetchMarkets 以包含来自 TradeXYZ 的 HIP-3 股票永续合约
        fetch_markets_config = {
            "types": ["swap", "hip3"],  # 包含 HIP-3 市场
            "hip3": {
                "dex": ["xyz"],  # TradeXYZ DEX，用于股票永续合约（TSLA、NVDA 等）
            },
        }
        if self.ws_enabled:
            self.ccp = getattr(ccxt_pro, self.exchange)(creds)
            self.ccp.options.update(self._build_ccxt_options())
            self.ccp.options["defaultType"] = "swap"
            self.ccp.options["fetchMarkets"] = fetch_markets_config
            self._apply_endpoint_override(self.ccp)
        elif self.endpoint_override:
            logging.info("由于自定义端点覆盖，跳过 Hyperliquid websocket 会话。")
        self.cca = getattr(ccxt_async, self.exchange)(creds)
        self.cca.options.update(self._build_ccxt_options())
        self.cca.options["defaultType"] = "swap"
        self.cca.options["fetchMarkets"] = fetch_markets_config
        self._apply_endpoint_override(self.cca)

    def set_market_specific_settings(self):
        super().set_market_specific_settings()
        isolated_count = 0
        for symbol in self.markets_dict:
            elm = self.markets_dict[symbol]
            self.symbol_ids[symbol] = elm["id"]
            self.min_costs[symbol] = (
                10.0 if elm["limits"]["cost"]["min"] is None else elm["limits"]["cost"]["min"]
            )
            self.min_costs[symbol] = pbr.round_(self.min_costs[symbol] * 1.01, 0.01)
            self.qty_steps[symbol] = elm["precision"]["amount"]
            self.min_qtys[symbol] = (
                self.qty_steps[symbol]
                if elm["limits"]["amount"]["min"] is None
                else elm["limits"]["amount"]["min"]
            )
            self.price_steps[symbol] = elm["precision"]["price"]
            self.c_mults[symbol] = elm["contractSize"]

            # 对于仅限逐仓的市场（HIP-3），杠杆上限为 10x
            if self._requires_isolated_margin(symbol):
                isolated_count += 1
                self.max_leverage[symbol] = min(
                    self.HIP3_MAX_LEVERAGE,
                    (
                        int(elm["info"]["maxLeverage"])
                        if "maxLeverage" in elm["info"]
                        else self.HIP3_MAX_LEVERAGE
                    ),
                )
            else:
                self.max_leverage[symbol] = (
                    int(elm["info"]["maxLeverage"]) if "maxLeverage" in elm["info"] else 0
                )
        self.n_decimal_places = 6
        self.n_significant_figures = 5
        if isolated_count:
            logging.debug(
                f"检测到 {isolated_count} 个仅限逐仓的品种（HIP-3/股票永续合约）"
            )

    def _hip3_margin_metadata(self, symbol: str) -> dict:
        market = getattr(self, "markets_dict", {}).get(symbol, {})
        info = market.get("info", {})
        margin_modes = market.get("marginModes", {})
        raw_mode = str(info.get("marginMode") or "").strip()
        raw_mode_l = raw_mode.lower()
        only_isolated = bool(info.get("onlyIsolated") or info.get("isolatedOnly"))
        cross_capable = not only_isolated and raw_mode_l not in {"strictisolated", "nocross"}
        if isinstance(margin_modes, dict) and margin_modes.get("cross") is False:
            cross_capable = False
        return {
            "cross_capable": cross_capable,
            "only_isolated": only_isolated,
        }

    def _requires_isolated_margin(self, symbol: str) -> bool:
        """检查品种是否需要逐仓模式。

        在 Hyperliquid 上，这包括：
        1. 根据元数据实际为仅限逐仓的 HIP-3 市场
        2. 带有 onlyIsolated=True 标志的其他市场

        Args:
            symbol: CCXT 格式的品种名（例如 "xyz:TSLA/USDC:USDC"）

        Returns:
            如果该品种需要逐仓模式则返回 True
        """
        prefixes = (self.HIP3_PREFIX,) + tuple(self.HIP3_ALT_PREFIXES)
        base = symbol.split("/")[0] if "/" in symbol else symbol
        if (
            self._get_hl_dex_for_symbol(symbol)
            or symbol.startswith(prefixes)
            or base.startswith(prefixes)
        ):
            return not self._hip3_margin_metadata(symbol)["cross_capable"]

        # 回退到基类检查（onlyIsolated 标志等）
        return super()._requires_isolated_margin(symbol)

    def _record_hl_live_margin_mode(self, symbol: str, margin_mode: str | None) -> None:
        if not symbol or not margin_mode:
            return
        normalized = str(margin_mode).lower()
        if normalized in {"cross", "isolated"}:
            self._hl_live_margin_modes[symbol] = normalized

    def _get_hl_dex_for_symbol(self, symbol: str) -> str | None:
        """返回品种对应的 HIP-3 dex 名称（如可用）。"""
        market = getattr(self, "markets_dict", {}).get(symbol, {})
        base_name = market.get("baseName") or market.get("info", {}).get("baseName", "")
        if isinstance(base_name, str) and ":" in base_name:
            dex_name = base_name.split(":", 1)[0]
            if dex_name:
                return dex_name
        return None

    def _get_hl_hip3_state_symbols(self) -> list[str]:
        """返回需要 dex 作用域状态查询的已跟踪 HIP-3 品种。"""
        tracked = set(getattr(self, "active_symbols", []) or [])
        tracked.update(getattr(self, "open_orders", {}).keys())
        tracked.update(getattr(self, "positions", {}).keys())
        return sorted(
            symbol
            for symbol in tracked
            if symbol in getattr(self, "markets_dict", {}) and self._get_hl_dex_for_symbol(symbol)
        )

    def _get_hl_hip3_dex_names(self) -> list[str]:
        dexes = set()
        for symbol in getattr(self, "markets_dict", {}) or {}:
            dex_name = self._get_hl_dex_for_symbol(symbol)
            if dex_name:
                dexes.add(dex_name)
        return sorted(dexes)

    def _normalize_ccxt_position(self, position: dict) -> dict:
        side = position.get("side")
        contracts = float(position.get("contracts") or 0.0)
        if side == "short":
            contracts = -contracts
        margin_mode = position.get("marginMode")
        if margin_mode is None and isinstance(position.get("info"), dict):
            leverage = position["info"].get("position", {}).get("leverage", {})
            if isinstance(leverage, dict):
                margin_mode = leverage.get("type")
        if margin_mode is None and position.get("isolated") is not None:
            margin_mode = "isolated" if position.get("isolated") else "cross"
        info_position = {}
        if isinstance(position.get("info"), dict):
            info_position = position["info"].get("position", {}) or {}
        return {
            "symbol": position["symbol"],
            "position_side": side,
            "size": contracts,
            "price": float(position.get("entryPrice") or 0.0),
            "margin_mode": str(margin_mode).lower() if margin_mode else None,
            "margin_used": float(
                position.get("initialMargin")
                or position.get("margin")
                or info_position.get("marginUsed")
                or 0.0
            ),
        }

    async def _fetch_hip3_positions(self, *, include_raw: bool = False):
        """通过 dex 作用域的 CCXT 路由获取 HIP-3 持仓。"""
        positions_by_key = {}
        raw_payloads = []
        fetch_specs = [{"params": {"dex": dex_name}} for dex_name in self._get_hl_hip3_dex_names()]
        for fetch_spec in fetch_specs:
            fetched = await self.cca.fetch_positions(**fetch_spec)
            if include_raw:
                raw_payloads.append({"fetch_spec": deepcopy(fetch_spec), "response": deepcopy(fetched)})
            for position in fetched:
                normalized = self._normalize_ccxt_position(position)
                if not self._get_hl_dex_for_symbol(normalized["symbol"]):
                    continue
                self._record_hl_live_margin_mode(
                    normalized["symbol"], normalized.get("margin_mode")
                )
                key = (normalized["symbol"], normalized["position_side"])
                positions_by_key[key] = normalized
        normalized_positions = list(positions_by_key.values())
        if include_raw:
            return raw_payloads, normalized_positions
        return normalized_positions

    def _filter_approved_symbols(self, pside: str, symbols: set[str]) -> set[str]:
        del pside
        return symbols

    def _hl_supports_hip3_live_trading(self) -> bool:
        return bool(getattr(self, "_hl_unified_enabled", False))

    def _assert_supported_live_state(self) -> None:
        if self.HIP3_ISOLATED_SUPPORTED or self._hl_supports_hip3_live_trading():
            return
        unsupported = []
        approved = set()
        for syms in getattr(self, "approved_coins_minus_ignored_coins", {}).values():
            approved.update(syms)
        approved_hip3 = sorted(symbol for symbol in approved if self._get_hl_dex_for_symbol(symbol))
        if approved_hip3:
            unsupported.append(
                "approved_coins="
                + ",".join(sorted({symbol.split("/")[0] if "/" in symbol else symbol for symbol in approved_hip3}))
            )
        for symbol in sorted(
            set(getattr(self, "positions", {})) | set(getattr(self, "open_orders", {}))
        ):
            if not self._get_hl_dex_for_symbol(symbol):
                continue
            has_pos = False
            pos = getattr(self, "positions", {}).get(symbol, {})
            for pside in ("long", "short"):
                if abs(float(pos.get(pside, {}).get("size", 0.0) or 0.0)) > 0.0:
                    has_pos = True
                    break
            has_orders = bool(getattr(self, "open_orders", {}).get(symbol))
            if not (has_pos or has_orders):
                continue
            isolated_live_mode = getattr(self, "_hl_live_margin_modes", {}).get(symbol) == "isolated"
            isolated_only = self._requires_isolated_margin(symbol)
            reasons = []
            if isolated_only:
                reasons.append("isolated-only market")
            if isolated_live_mode:
                reasons.append("live isolated margin state")
            if not reasons:
                reasons.append("hip3 live state")
            state_bits = []
            if has_pos:
                state_bits.append("position")
            if has_orders:
                state_bits.append("open_orders")
            unsupported.append(f"{symbol} ({'/'.join(state_bits)}; {', '.join(reasons)})")
        if unsupported:
            raise FatalBotException(
                "Hyperliquid HIP-3/non-standard perps require unifiedAccount mode in Passivbot. "
                f"Current abstraction={getattr(self, '_hl_user_abstraction', 'unknown')}. "
                f"Unsupported HIP-3 state detected: {'; '.join(unsupported)}. "
                "Upgrade the Hyperliquid account to unifiedAccount or remove all HIP-3 "
                "symbols, positions, and open orders before running the bot."
            )

    async def watch_orders(self):
        res = None
        _ws_consecutive_rate_limits = 0
        while True:
            try:
                if self.stop_websocket:
                    break
                res = await self.ccp.watch_orders()
                _ws_consecutive_rate_limits = 0  # 成功时重置
                for i in range(len(res)):
                    res[i]["position_side"] = self.determine_pos_side(res[i])
                    res[i]["qty"] = res[i]["amount"]
                self.handle_order_update(res)
            except RateLimitExceeded:
                self._health_ws_reconnects += 1
                self._health_rate_limits += 1
                _ws_consecutive_rate_limits += 1
                backoff = min(30, 2 ** _ws_consecutive_rate_limits) + random.uniform(0, 1)
                logging.warning(
                    "[ws] %s: rate limited (reconnect #%d), backing off %.0fs...",
                    self.exchange,
                    self._health_ws_reconnects,
                    backoff,
                )
                await asyncio.sleep(backoff)
                logging.info("[ws] %s: reconnecting after rate limit...", self.exchange)
            except Exception as e:
                self._health_ws_reconnects += 1
                _ws_consecutive_rate_limits = 0
                logging.warning(
                    "[ws] %s: connection lost (reconnect #%d), retrying in 1s: %s",
                    self.exchange,
                    self._health_ws_reconnects,
                    type(e).__name__,
                )
                logging.debug("[ws] %s: full exception: %s", self.exchange, e)
                logging.debug("".join(traceback.format_exc()))
                await asyncio.sleep(1)
                logging.info("[ws] %s: reconnecting...", self.exchange)

    def determine_pos_side(self, order):
        # hyperliquid 不是双向持仓模式
        if order["symbol"] in self.positions:
            if self.positions[order["symbol"]]["long"]["size"] != 0.0:
                return "long"
            elif self.positions[order["symbol"]]["short"]["size"] != 0.0:
                return "short"
            else:
                return "long" if order["side"] == "buy" else "short"
        else:
            if "reduceOnly" in order:
                if order["side"] == "buy":
                    return "short" if order["reduceOnly"] else "long"
                if order["side"] == "sell":
                    return "long" if order["reduceOnly"] else "short"
            return "long" if order["side"] == "buy" else "short"

    def _get_position_side_for_order(self, order: dict) -> str:
        """钩子：为 Hyperliquid（单向模式）从订单数据推导 position_side。"""
        return self.determine_pos_side(order)

    async def _do_fetch_open_orders(self, symbol: str = None):
        fetched = []
        seen_ids = set()
        query_symbols = [symbol] if symbol is not None else []
        query_dexes = self._get_hl_hip3_dex_names() if symbol is None else []

        # 默认路由覆盖核心永续合约；HIP-3 品种需要 dex 作用域的查询。
        if symbol is None or not self._get_hl_dex_for_symbol(symbol):
            for order in await self.cca.fetch_open_orders(symbol=symbol):
                if order["id"] in seen_ids:
                    continue
                seen_ids.add(order["id"])
                fetched.append(order)

        if symbol is not None and self._get_hl_dex_for_symbol(symbol):
            hip3_symbols = query_symbols
        else:
            hip3_symbols = []

        for hip3_symbol in hip3_symbols:
            for order in await self.cca.fetch_open_orders(symbol=hip3_symbol):
                if order["id"] in seen_ids:
                    continue
                seen_ids.add(order["id"])
                fetched.append(order)

        for dex_name in query_dexes:
            for order in await self.cca.fetch_open_orders(params={"dex": dex_name}):
                if order["id"] in seen_ids:
                    continue
                seen_ids.add(order["id"])
                fetched.append(order)
        return fetched

    def _normalize_open_orders(self, fetched: list) -> list:
        for elm in fetched:
            elm["position_side"] = self.determine_pos_side(elm)
            elm["qty"] = elm["amount"]
        return sorted(fetched, key=lambda x: x["timestamp"])

    async def fetch_open_orders(self, symbol: str = None):
        fetched = await self._do_fetch_open_orders(symbol=symbol)
        return self._normalize_open_orders(fetched)

    async def _fetch_positions_and_balance(self):
        info = await self.cca.fetch_balance()
        positions = {}
        for x in info["info"]["assetPositions"]:
            symbol = self.coin_to_symbol(x["position"]["coin"])
            leverage = x["position"].get("leverage", {})
            if isinstance(leverage, dict):
                self._record_hl_live_margin_mode(symbol, leverage.get("type"))
            size = float(x["position"]["szi"])
            elm = {
                "symbol": symbol,
                "position_side": ("long" if size > 0.0 else "short"),
                "size": size,
                "price": float(x["position"]["entryPx"]),
                "margin_mode": (
                    str(leverage.get("type")).lower()
                    if isinstance(leverage, dict) and leverage.get("type")
                    else None
                ),
                "margin_used": float(x["position"].get("marginUsed") or 0.0),
            }
            positions[(elm["symbol"], elm["position_side"])] = elm
        hip3_raw, hip3_positions = await self._fetch_hip3_positions(include_raw=True)
        for position in hip3_positions:
            positions[(position["symbol"], position["position_side"])] = position
        balance = float(info["info"]["marginSummary"]["accountValue"]) - sum(
            [float(x["position"]["unrealizedPnl"]) for x in info["info"]["assetPositions"]]
        )
        raw_snapshot = {
            "balance": deepcopy(info),
            "positions": {
                "core": deepcopy(info["info"].get("assetPositions", [])),
                "hip3": hip3_raw,
            },
        }
        return raw_snapshot, list(positions.values()), balance

    async def _get_positions_and_balance_cached(self, my_gen: int = 0):
        """获取持仓+余额，带去重：并发调用者共享一次 API 调用。

        my_gen 是调用者在获取锁*之前*拍摄的 _hl_cache_generation 快照。
        如果其他调用者在此期间完成了获取（cache_generation 已推进），
        则返回缓存结果（或如果获取失败则重新引发缓存的异常）。
        """
        async with self._hl_fetch_lock:
            cached_gen = self._hl_cache_generation
            if cached_gen > my_gen and hasattr(self, "_hl_cached_result"):
                if isinstance(self._hl_cached_result, Exception):
                    raise self._hl_cached_result
                return self._hl_cached_result
            try:
                result = await self._fetch_positions_and_balance()
            except Exception as e:
                self._hl_cached_result = e
                self._hl_cache_generation = cached_gen + 1
                raise
            self._hl_cached_result = result
            self._hl_cache_generation = cached_gen + 1
            return result

    async def fetch_positions(self):
        # 在锁定*之前*快照生成，以便每个调用者跟踪自己的视图。
        my_gen = self._hl_cache_generation
        _, positions, balance = await self._get_positions_and_balance_cached(my_gen)
        self._last_hl_balance = balance
        self._hl_balance_consumed = False
        return positions

    async def capture_positions_snapshot(self) -> tuple[list, list]:
        my_gen = self._hl_cache_generation
        raw_snapshot, positions, balance = await self._get_positions_and_balance_cached(my_gen)
        self._last_hl_balance = balance
        self._hl_balance_consumed = False
        return deepcopy(raw_snapshot["positions"]), deepcopy(positions)

    async def fetch_balance(self):
        # 检查 fetch_positions 是否已经获取了新的余额
        if getattr(self, "_last_hl_balance", None) is not None and not getattr(
            self, "_hl_balance_consumed", True
        ):
            self._hl_balance_consumed = True
            return self._last_hl_balance
        # 在锁定*之前*快照生成，以便每个调用者跟踪自己的视图。
        my_gen = self._hl_cache_generation
        _, positions, balance = await self._get_positions_and_balance_cached(my_gen)
        return balance

    async def capture_balance_snapshot(self) -> tuple[dict, float]:
        my_gen = self._hl_cache_generation
        raw_snapshot, positions, balance = await self._get_positions_and_balance_cached(my_gen)
        return deepcopy(raw_snapshot["balance"]), float(balance)

    def _symbol_is_cross_hip3(self, symbol: str) -> bool:
        if not symbol or not self._get_hl_dex_for_symbol(symbol):
            return False
        if self._requires_isolated_margin(symbol):
            return False
        return self._get_margin_mode_for_symbol(symbol) == "cross"

    def _has_active_position_on_symbol(self, symbol: str) -> bool:
        for position in getattr(self, "fetched_positions", []):
            if position.get("symbol") == symbol and abs(float(position.get("size") or 0.0)) > 0.0:
                return True
        return False

    def _position_margin_to_restore(self) -> float:
        reserve = 0.0
        for position in getattr(self, "fetched_positions", []):
            symbol = str(position.get("symbol") or "")
            if not self._symbol_is_cross_hip3(symbol):
                continue
            if abs(float(position.get("size") or 0.0)) <= 0.0:
                continue
            reserve += max(0.0, float(position.get("margin_used") or 0.0))
        return reserve

    def _reserved_margin_for_resting_order(self, order: dict) -> float:
        symbol = str(order.get("symbol") or "")
        if not symbol:
            return 0.0
        if not self._is_passivbot_managed_open_order(order):
            return 0.0
        if not self._symbol_is_cross_hip3(symbol) and self._has_active_position_on_symbol(symbol):
            return 0.0
        if self._requires_isolated_margin(symbol):
            return 0.0
        if self._get_margin_mode_for_symbol(symbol) != "cross":
            return 0.0
        if bool(order.get("reduceOnly") or order.get("reduce_only")):
            return 0.0
        qty = order.get("qty", order.get("amount"))
        price = order.get("price")
        if qty is None or price is None:
            return 0.0
        qty = abs(float(qty))
        price = float(price)
        if qty <= 0.0 or price <= 0.0:
            return 0.0
        leverage = max(1.0, float(self._calc_leverage_for_symbol(symbol)))
        c_mult = float(self.c_mults.get(symbol, 1.0) or 1.0)
        notional = qty * price * c_mult
        return (notional / leverage) * self.HIP3_ORDER_MARGIN_BUFFER

    def _is_passivbot_managed_open_order(self, order: dict) -> bool:
        for cid in (
            order.get("custom_id"),
            order.get("customId"),
            order.get("client_order_id"),
            order.get("clientOrderId"),
            order.get("client_oid"),
            order.get("clientOid"),
            order.get("order_link_id"),
            order.get("orderLinkId"),
        ):
            if cid and _PASSIVBOT_CUSTOM_ID_MARKER_RE.search(str(cid)):
                return True
        return False

    def _reconcile_balance_from_exchange_state(self, *, include_open_orders: bool) -> bool:
        if getattr(self, "balance_override", None) is not None:
            return False
        exchange_reported = float(
            getattr(self, "_exchange_reported_balance_raw", self.get_raw_balance()) or 0.0
        )
        reserve = self._position_margin_to_restore()
        if include_open_orders:
            # 不要将机器人管理的挂单保证金储备反馈到已发布的余额中。
            # 该储备会随着机器人取消/重建入场单而变化，
            # 可能导致自引用的 REST/REST+open_orders 余额波动。
            pass
        corrected_raw = exchange_reported + reserve
        current_raw = self.get_raw_balance()
        if abs(corrected_raw - current_raw) <= 1e-12:
            return False
        balance_snapped = corrected_raw
        if getattr(self, "balance_override", None) is None:
            if getattr(self, "previous_hysteresis_balance", None) is None:
                self.previous_hysteresis_balance = corrected_raw
            balance_snapped = pbr.hysteresis(
                corrected_raw,
                self.previous_hysteresis_balance,
                self.balance_hysteresis_snap_pct,
            )
            self.previous_hysteresis_balance = balance_snapped
        self.balance_raw = corrected_raw
        self.balance = balance_snapped
        return True

    def _reconcile_balance_after_open_orders_refresh(self) -> bool:
        return self._reconcile_balance_from_exchange_state(include_open_orders=True)

    def _reconcile_balance_after_positions_and_balance_refresh(self) -> bool:
        return self._reconcile_balance_from_exchange_state(include_open_orders=False)

    async def fetch_tickers(self):
        fetched = await self.cca.fetch(
            self._hl_info_url(),
            method="POST",
            headers={"Content-Type": "application/json"},
            body=json.dumps({"type": "allMids"}),
        )
        return {
            self.coin_to_symbol(coin): {
                "bid": float(fetched[coin]),
                "ask": float(fetched[coin]),
                "last": float(fetched[coin]),
            }
            for coin in fetched
        }

    async def fetch_ohlcv(self, symbol: str, timeframe="1m"):
        # 时间间隔：1,3,5,15,30,60,120,240,360,720,D,M,W
        # 获取最新的 OHLCV
        str2int = {"1m": 1, "5m": 5, "15m": 15, "1h": 60, "4h": 60 * 4}
        n_candles = 480
        since = int(utc_ms() - 1000 * 60 * str2int[timeframe] * n_candles)
        return await self.cca.fetch_ohlcv(symbol, timeframe=timeframe, since=since)

    async def fetch_ohlcvs_1m(self, symbol: str, since: float = None, limit=None):
        n_candles_limit = 5000 if limit is None else limit
        result = await self.cca.fetch_ohlcv(
            symbol,
            timeframe="1m",
            limit=n_candles_limit,
            since=int(self.get_exchange_time() - 1000 * 60 * n_candles_limit * 0.95),
        )
        return result

    async def fetch_pnls(
        self,
        start_time: int = None,
        end_time: int = None,
        limit=None,
    ):
        # hyperliquid 从过去到未来获取
        if limit is None:
            limit = 2000
        if start_time is None:
            # 如果未传入时间范围，hyperliquid 返回最新交易
            return await self.fetch_pnl(limit=limit)
        all_fetched = {}
        prev_hash = ""
        while True:
            fetched = await self.fetch_pnl(start_time=start_time, limit=limit)
            if fetched == []:
                break
            for elm in fetched:
                all_fetched[elm["id"]] = elm
            if len(fetched) < limit:
                break
            if end_time and fetched[-1]["timestamp"] >= end_time:
                break
            new_hash = calc_hash(fetched)
            if prev_hash == new_hash:
                logging.debug(f"pnls hash unchanged: {prev_hash}")
                break
            prev_hash = new_hash
            logging.info(
                f"debug fetching pnls {ts_to_date(fetched[-1]['timestamp'])} len {len(fetched)}"
            )
            start_time = fetched[-1]["timestamp"] - 1000
            limit = 2000
        return sorted(all_fetched.values(), key=lambda x: x["timestamp"])

    async def gather_fill_events(self, start_time=None, end_time=None, limit=None):
        """返回 Hyperliquid 的标准成交事件（草稿占位符）。"""
        events = []
        fills = await self.fetch_pnls(start_time=start_time, end_time=end_time, limit=limit)
        for fill in fills:
            events.append(
                {
                    "id": fill.get("id"),
                    "timestamp": fill.get("timestamp"),
                    "symbol": fill.get("symbol"),
                    "side": fill.get("side"),
                    "position_side": fill.get("position_side"),
                    "qty": fill.get("amount"),
                    "price": fill.get("price"),
                    "pnl": fill.get("pnl"),
                    "fee": fill.get("fee"),
                    "info": fill.get("info"),
                }
            )
        return events

    async def fetch_pnl(
        self,
        start_time: int = None,
        limit=None,
    ):
        if start_time is None:
            fetched = await self.cca.fetch_my_trades(limit=limit)
        else:
            fetched = await self.cca.fetch_my_trades(since=max(1, int(start_time)), limit=limit)
        for elm in fetched:
            elm["pnl"] = float(elm["info"]["closedPnl"])
            elm["position_side"] = "long" if "long" in elm["info"]["dir"].lower() else "short"
        return sorted(fetched, key=lambda x: x["timestamp"])

    async def execute_cancellation(self, order: dict) -> dict:
        """Hyperliquid：取消订单，支持 vault。"""
        params = (
            {"vaultAddress": self.user_info["wallet_address"]} if self.user_info["is_vault"] else {}
        )

        def _is_already_gone(payload) -> bool:
            try:
                text = str(payload)
            except Exception:
                text = ""
            text_l = text.lower()
            if (
                "order was never placed" in text_l
                or "already canceled" in text_l
                or "already cancelled" in text_l
            ):
                return True
            return False

        try:
            res = await self.cca.cancel_order(order["id"], symbol=order["symbol"], params=params)
            # 有时 hyperliquid 返回带有嵌入错误的 "ok" 包装器；视为非致命错误。
            if _is_already_gone(res):
                logging.info("Order already canceled/filled on exchange; treating as success.")
                return {"status": "success"}
            return res
        except Exception as e:
            if _is_already_gone(e):
                logging.info("订单已在交易所取消/成交；视为成功。")
                return {"status": "success"}
            raise

    def did_cancel_order(self, executed, order=None) -> bool:
        if isinstance(executed, list) and len(executed) == 1:
            return self.did_cancel_order(executed[0], order)
        try:
            return "status" in executed and executed["status"] == "success"
        except (TypeError, KeyError):
            return False

    def _build_order_params(self, order: dict) -> dict:
        params = {
            "reduceOnly": order["reduce_only"],
            "timeInForce": (
                "Alo" if require_live_value(self.config, "time_in_force") == "post_only" else "Gtc"
            ),
            "clientOrderId": order["custom_id"],
        }
        if self.user_info["is_vault"]:
            params["vaultAddress"] = self.user_info["wallet_address"]
        return params

    async def execute_order(self, order: dict) -> dict:
        """Hyperliquid：执行订单，在特定错误时自动调整 min_cost。"""
        try:
            return await super().execute_order(order)
        except Exception as e:
            # 尝试通过调整 min_cost 从 Hyperliquid 的 "$10 最低" 错误中恢复
            try:
                if self.adjust_min_cost_on_error(e, order):
                    logging.info(f"Adjusted min_cost for order, will retry: {order['symbol']}")
                    return {}
            except Exception as e0:
                logging.error(f"error with adjust_min_cost_on_error {e0}")
            # 无法恢复 - 重新引发以触发 restart_bot_on_too_many_errors
            raise

    async def execute_orders(self, orders: [dict]) -> [dict]:
        return await self.execute_multiple(orders, "execute_order")

    def did_create_order(self, executed) -> bool:
        did_create = super().did_create_order(executed)
        try:
            return did_create and (
                "info" in executed and ("filled" in executed["info"] or "resting" in executed["info"])
            )
        except (TypeError, KeyError):
            return False

    def adjust_min_cost_on_error(self, error, order=None):
        any_adjusted = False
        successful_orders = []
        str_e = str(error)
        brace_idx = str_e.find("{")
        if brace_idx == -1:
            return False
        try:
            error_json = json.loads(str_e[brace_idx:])
        except json.JSONDecodeError:
            return False
        if (
            "response" in error_json
            and "data" in error_json["response"]
            and "statuses" in error_json["response"]["data"]
        ):
            for elm in error_json["response"]["data"]["statuses"]:
                if "error" in elm:
                    if "Order must have minimum value of $10" in elm["error"]:
                        asset_id = int(elm["error"][elm["error"].find("asset=") + 6 :])
                        for symbol in self.markets_dict:
                            if (
                                "baseId" in self.markets_dict[symbol]["info"]
                                and self.markets_dict[symbol]["info"]["baseId"] == asset_id
                            ):
                                break
                        else:
                            raise Exception(f"No symbol match for asset_id={asset_id}")
                        new_min_cost = pbr.round_(self.min_costs[symbol] * 1.1, 0.1)
                        logging.info(
                            f"caught {elm['error']} {symbol}. Upping min_cost from {self.min_costs[symbol]} to {new_min_cost}. Order: {order}"
                        )
                        self.min_costs[symbol] = new_min_cost
                        any_adjusted = True
        return any_adjusted

    def symbol_is_eligible(self, symbol):
        """检查品种是否有资格进行交易。

        HIP-3 股票永续合约仍可被发现，但仅限逐仓的实盘交易
        目前通过品种过滤/启动验证在其他地方被禁用。
        """
        try:
            market_info = self.markets_dict[symbol]["info"]

            # 零持仓量表示市场不活跃
            if float(market_info.get("openInterest", 0)) == 0.0:
                return False
        except Exception as e:
            logging.error(f"error with symbol_is_eligible {e} {symbol}")
            return False
        return True

    async def update_exchange_config_by_symbols(self, symbols):
        """设置 Hyperliquid 品种的杠杆和保证金模式。

        使用基类方法进行逐仓检测和杠杆计算。
        添加 Hyperliquid 专用的 vault 地址处理。
        顺序执行调用，间隔小延迟以避免速率限制突发。
        """
        for symbol in symbols:
            to_print = ""
            try:
                leverage = self._calc_leverage_for_symbol(symbol)
                margin_mode = self._get_margin_mode_for_symbol(symbol)

                params = {"leverage": leverage}
                if self.user_info["is_vault"]:
                    params["vaultAddress"] = self.user_info["wallet_address"]

                try:
                    res = await self.cca.set_margin_mode(
                        margin_mode, symbol=symbol, params=params
                    )
                    to_print = (
                        f"margin={format_exchange_config_response(res)} ({margin_mode})"
                    )
                except Exception as e:
                    if '"code":"59107"' in str(e):
                        to_print = f"margin=ok (unchanged, {margin_mode})"
                    else:
                        logging.error(f"{symbol} error setting {margin_mode} mode {e}")
            except Exception as e:
                logging.error(f"{symbol}: error setting margin mode and leverage {e}")
            if to_print:
                logging.debug(f"{symbol}: {to_print}")
            # 保证金模式 API 调用之间的小延迟，以避免速率限制突发
            await asyncio.sleep(0.2)

    async def update_exchange_config(self):
        pass

    async def calc_ideal_orders(self):
        # hyperliquid 需要自定义价格舍入
        ideal_orders = await super().calc_ideal_orders()
        for sym in ideal_orders:
            for i in range(len(ideal_orders[sym])):
                if ideal_orders[sym][i]["side"] == "sell":
                    ideal_orders[sym][i]["price"] = round_dynamic_up(
                        round(ideal_orders[sym][i]["price"], self.n_decimal_places),
                        self.n_significant_figures,
                    )
                elif ideal_orders[sym][i]["side"] == "buy":
                    ideal_orders[sym][i]["price"] = round_dynamic_dn(
                        round(ideal_orders[sym][i]["price"], self.n_decimal_places),
                        self.n_significant_figures,
                    )
                else:
                    ideal_orders[sym][i]["price"] = round_dynamic(
                        round(ideal_orders[sym][i]["price"], self.n_decimal_places),
                        self.n_significant_figures,
                    )
                ideal_orders[sym][i]["price"] = round_(
                    ideal_orders[sym][i]["price"], self.price_steps[sym]
                )
        return ideal_orders

    def format_custom_id_single(self, order_type_id: int) -> str:
        formatted = super().format_custom_id_single(order_type_id)
        return (formatted)[: self.custom_id_max_length]
