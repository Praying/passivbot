"""
CCXTBot: 使用 CCXT 统一 API 的通用交易所连接器。

这是快速接入新交易所的基类。继承此类并仅覆盖需要交易所特定行为的方法。

设计原理详见 docs/plans/2026-01-02-ccxtbot-design.md。

钩子分类
========
CCXTBot 使用统一的命名约定来定义扩展点：

    can_*        - 能力检查（返回 bool）
                   示例：can_watch_orders() -> 如果支持 WebSocket 则返回 True

    _do_*        - 调用交易所 API 的异步操作
                   示例：_do_fetch_balance() -> 来自 CCXT 的 dict

    _get_*       - 从 API 响应中提取值
                   示例：_get_balance(fetched) -> float

    _normalize_* - 数据转换为 passivbot 格式
                   示例：_normalize_positions(fetched) -> list[dict]

    _build_*     - 配置/参数构建
                   示例：_build_order_params(order) -> CCXT 所需的 dict

为新交易所自定义行为：
1. 继承 CCXTBot
2. 仅覆盖需要交易所特定逻辑的钩子
3. 模板方法（fetch_balance、fetch_positions 等）负责编排钩子
"""

import asyncio
import math
import time
import traceback
from copy import deepcopy

from passivbot import Passivbot, logging, custom_id_to_snake
import ccxt.pro as ccxt_pro
import ccxt.async_support as ccxt_async
from procedures import assert_correct_ccxt_version
from config.access import get_optional_live_value, require_live_value

assert_correct_ccxt_version(ccxt=ccxt_async)


def format_exchange_config_response(res: dict) -> str:
    """简洁格式化交易所配置 API 响应（杠杆、保证金模式）。

    不记录完整 JSON，例如：
        {'symbol': 'ADAUSDT', 'leverage': '10', 'maxNotionalValue': '10000000'}
    返回：
        'ok' 或 'leverage=10x' 或错误信息
    """
    if not isinstance(res, dict):
        return str(res)[:50]

    # 检查成功指示器
    code = res.get("code") or res.get("retCode")
    msg = res.get("msg") or res.get("retMsg") or res.get("message", "")
    status = res.get("status", "")

    # 成功情况
    if code in (0, "0", "200000", 200000):
        return "ok"
    if status == "ok":
        return "ok"

    # "无需更改"视为成功
    if "no need" in str(msg).lower():
        return "ok (unchanged)"

    # 提取有用信息
    leverage = res.get("leverage") or res.get("lever")
    if leverage:
        return f"leverage={leverage}x"

    # 错误情况 - 显示代码和消息
    if code and msg:
        return f"code={code}: {msg[:40]}"
    if msg:
        return msg[:50]

    # 回退：截断字符串
    s = str(res)
    return s[:60] + "..." if len(s) > 60 else s


class CCXTBot(Passivbot):
    """使用 CCXT 统一 API 的通用交易所机器人。

    钩子分类和扩展模式详见模块文档字符串。
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.quote = self.user_info.get("quote", "USDT")
        self._live_margin_modes = {}
        self._blocked_margin_symbols_warned = set()

    # ═══════════════════ 订单监听钩子 ═══════════════════

    def can_watch_orders(self) -> bool:
        """钩子：此交易所是否支持实时监听订单？

        默认：检查 CCXT 的 has['watchOrders']
        覆盖：如果实现了原生 WebSocket 则返回 True
        """
        if self.ccp is None:
            return False
        return bool(self.ccp.has.get("watchOrders"))

    async def _do_watch_orders(self) -> list:
        """钩子：获取下一批订单更新。

        默认：使用 CCXT 的 watchOrders()
        覆盖：实现原生 WebSocket
        """
        return await self.ccp.watch_orders()

    def _normalize_order_update(self, order: dict) -> dict:
        """钩子：将原始订单转换为 passivbot 格式。

        默认：处理 CCXT 统一格式
        覆盖：处理交易所特定格式
        """
        order["position_side"] = self._get_position_side_for_order(order)
        order["qty"] = order["amount"]
        return order

    def _get_position_side_for_order(self, order: dict) -> str:
        """钩子：从订单数据推导 position_side。

        默认：使用 CCXT 统一字段，回退到 custom_id 推导。
        覆盖：当两个来源都不可用时使用交易所特定逻辑。
        """
        info = order.get("info", {})

        # 1. 交易所直接提供 positionSide
        pos_side = info.get("positionSide", "")
        if pos_side:
            return pos_side.lower()

        # 2. 从 CCXT 统一 clientOrderId 推导
        custom_id = order.get("clientOrderId", "")
        if custom_id:
            order_type = custom_id_to_snake(custom_id)
            if order_type.endswith("_long"):
                return "long"
            if order_type.endswith("_short"):
                return "short"

        return "both"

    # ═══════════════════ PnL 获取钩子 ═══════════════════

    def _get_pnl_from_trade(self, trade: dict) -> float:
        """钩子：从交易中提取已实现 PnL。

        默认：查找常见的 CCXT info 字段
        覆盖：交易所特定的字段名
        """
        info = trade.get("info", {})
        for field in ["realized_pnl", "realizedPnl", "pnl", "profit"]:
            if field in info:
                return float(info[field])
        return 0.0

    def _get_position_side_from_trade(self, trade: dict) -> str:
        """钩子：从交易中判断持仓方向。

        默认：根据 side + PnL 推断（开仓 vs 平仓）
        覆盖：交易所特定逻辑

        逻辑：PnL=0 表示开仓，PnL!=0 表示平仓
        - buy + 开仓 = long
        - buy + 平仓 = short（平空仓）
        - sell + 开仓 = short
        - sell + 平仓 = long（平多仓）
        """
        pnl = self._get_pnl_from_trade(trade)
        if trade["side"] == "buy":
            return "long" if pnl == 0.0 else "short"
        else:
            return "short" if pnl == 0.0 else "long"

    def _build_ccxt_config(self) -> dict:
        """通过透传所有 user_info 字段来构建 CCXT 配置。

        CCXT 会忽略未知字段，因此我们传递除 passivbot 特定字段以外的所有内容。
        用户可以在 api-keys.json 中直接使用任何 CCXT 支持的凭据字段
        （apiKey、secret、password、walletAddress、privateKey 等）。
        """
        # passivbot 使用的字段，非 CCXT 字段
        passivbot_fields = {"exchange", "options", "quote"}

        config = {k: v for k, v in self.user_info.items() if k not in passivbot_fields}
        config["enableRateLimit"] = True
        config.setdefault("timeout", 30000)  # 30 秒 - CCXT 默认约 10 秒在冷启动时过于紧张

        # 为向后兼容，将旧版凭据字段名映射为 CCXT 原生名称
        legacy_mappings = {
            "key": "apiKey",
            "api_key": "apiKey",
            "wallet": "walletAddress",
            "private_key": "privateKey",
            "passphrase": "password",
            "wallet_address": "walletAddress",
        }
        deprecated_fields = []  # 收集用于聚合日志记录
        for old_name, new_name in legacy_mappings.items():
            if old_name in config and new_name not in config:
                deprecated_fields.append(f"{old_name}->{new_name}")
                config[new_name] = config.pop(old_name)

        # 在单条消息中记录所有已弃用字段（每个交易所仅记录一次）
        if deprecated_fields:
            cache_key = f"_deprecated_keys_warned_{self.exchange}"
            if not getattr(self, cache_key, False):
                setattr(self, cache_key, True)
                logging.info(
                    "[config] %s: deprecated api-keys.json fields remapped: %s (use CCXT-native names)",
                    self.exchange,
                    ", ".join(deprecated_fields),
                )

        return config

    def create_ccxt_sessions(self):
        """初始化 REST 和 WebSocket CCXT 客户端。

        REST 客户端（cca）始终创建。WebSocket 客户端（ccp）
        仅在 ws_enabled=True 时创建；否则 ccp 设为 None，
        机器人回退到 REST 轮询来获取订单更新。
        """
        ccxt_config = self._build_ccxt_config()
        user_options = self.user_info.get("options", {})

        # REST 客户端 - 优先使用期货特定 id（如 binanceusdm）而非通用名称
        ccxt_id = getattr(self, "exchange_ccxt_id", self.exchange)
        exchange_class = getattr(ccxt_async, ccxt_id)
        self.cca = exchange_class(ccxt_config)
        self.cca.options.update(self._build_ccxt_options())
        self.cca.options.update(user_options)
        self.cca.options["defaultType"] = "swap"
        self._apply_endpoint_override(self.cca)

        # WebSocket 客户端 - 可选，启用更快的订单更新
        if self.ws_enabled:
            ws_class = getattr(ccxt_pro, ccxt_id)
            self.ccp = ws_class(ccxt_config)
            self.ccp.options.update(self._build_ccxt_options())
            self.ccp.options.update(user_options)
            self.ccp.options["defaultType"] = "swap"
            self._apply_endpoint_override(self.ccp)
        else:
            self.ccp = None
            logging.info(f"{self.exchange}: WebSocket disabled, using REST polling")

    async def validate_websocket_support(self):
        """检查 WebSocket 能力（信息性，非致命）。

        记录 watchOrders 是否可用。不会抛出异常。
        子类可以覆盖此方法，在实现原生 WebSocket 时设置 ws_orders_supported=True。
        """
        if self.ccp is None:
            logging.info(f"{self.exchange}: WebSocket client not initialized")
            return

        if self.ccp.has.get("watchOrders"):
            logging.info(f"{self.exchange}: watchOrders support confirmed")
        else:
            logging.info(f"{self.exchange}: watchOrders not supported in CCXT, using REST polling")

    async def fetch_balance(self) -> float:
        """模板方法：获取报价货币的账户余额。

        使用钩子：
        - _do_fetch_balance()：调用交易所 API
        - _get_balance()：提取余额值

        返回：
            float：报价货币的总余额。

        异常：
            Exception：API 错误或缺少必需的余额字段时
                （调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        fetched = await self._do_fetch_balance()
        return self._get_balance(fetched)

    async def capture_balance_snapshot(self) -> tuple[dict, float]:
        """获取一次余额并从同一数据中派生标准化值。"""
        fetched = await self._do_fetch_balance()
        return fetched, self._get_balance(deepcopy(fetched))

    async def _do_fetch_balance(self) -> dict:
        """钩子：调用交易所 API 获取余额。

        默认：使用 CCXT 的 fetch_balance()
        覆盖：自定义 API 调用或不同端点
        """
        logging.debug(f"{self.exchange}: fetching balance via CCXT fetch_balance()")
        t0 = time.time()
        result = await self.cca.fetch_balance()
        elapsed_ms = (time.time() - t0) * 1000
        logging.debug(f"{self.exchange}: fetch_balance completed in {elapsed_ms:.1f}ms")
        return result

    def _get_balance(self, fetched: dict) -> float:
        """钩子：从响应中提取余额值。

        默认：CCXT 统一格式 total[quote]
        覆盖：交易所特定的字段路径（如 info.totalCrossWalletBalance）
        """
        total = fetched.get("total")
        if not isinstance(total, dict):
            raise KeyError(
                f"{self.exchange}: fetch_balance response missing 'total' mapping for quote {self.quote}"
            )
        if self.quote not in total:
            raise KeyError(
                f"{self.exchange}: fetch_balance response missing total[{self.quote!r}]"
            )
        return float(total[self.quote])

    async def fetch_positions(self) -> list:
        """模板方法：获取所有持仓。

        使用钩子：
        - _do_fetch_positions()：调用交易所 API
        - _normalize_positions()：转换为 passivbot 格式
        - _get_position_side()：为每个持仓推导 position_side

        返回：
            list：标准化字段的持仓字典列表。

        异常：
            Exception：API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        fetched = await self._do_fetch_positions()
        return self._normalize_positions(fetched)

    async def capture_positions_snapshot(self) -> tuple[list, list]:
        """获取一次持仓并从同一数据中派生标准化持仓。"""
        fetched = await self._do_fetch_positions()
        return fetched, self._normalize_positions(deepcopy(fetched))

    async def _do_fetch_positions(self) -> list:
        """钩子：调用交易所 API 获取持仓。

        默认：使用 CCXT 的 fetch_positions()
        覆盖：自定义 API 调用
        """
        logging.debug(f"{self.exchange}: fetching positions via CCXT fetch_positions()")
        t0 = time.time()
        result = await self.cca.fetch_positions()
        elapsed_ms = (time.time() - t0) * 1000
        logging.debug(
            f"{self.exchange}: fetch_positions completed in {elapsed_ms:.1f}ms, {len(result)} raw positions"
        )
        return result

    def _normalize_positions(self, fetched: list) -> list:
        """钩子：将原始持仓转换为 passivbot 格式。

        默认：使用 CCXT 统一字段（contracts、entryPrice、side）
        覆盖：交易所特定的字段映射
        """
        positions = []
        for elm in fetched:
            contracts = float(elm.get("contracts", 0))
            if contracts != 0:
                normalized = {
                    "symbol": elm["symbol"],
                    "position_side": self._get_position_side(elm),
                    "size": contracts,
                    "price": float(elm.get("entryPrice", 0)),
                }
                margin_mode = self._extract_live_margin_mode(elm)
                if margin_mode is not None:
                    normalized["margin_mode"] = margin_mode
                    self._record_live_margin_mode(normalized["symbol"], margin_mode)
                positions.append(normalized)
        return positions

    def _get_position_side(self, elm: dict) -> str:
        """钩子：从持仓数据推导 position_side。

        默认：CCXT 统一的 'side' 字段
        覆盖：交易所特定逻辑（如 info.positionSide）
        """
        return elm.get("side", "long").lower()

    async def _do_fetch_open_orders(self, symbol: str = None) -> list:
        """钩子：调用交易所 API 获取未成交订单。"""
        exchange = getattr(self, "exchange", "unknown")
        sym_str = symbol if symbol else "all symbols"
        logging.debug(f"{exchange}: fetching open orders for {sym_str}")
        t0 = time.time()
        fetched = await self.cca.fetch_open_orders(symbol=symbol)
        elapsed_ms = (time.time() - t0) * 1000
        logging.debug(
            f"{exchange}: fetch_open_orders completed in {elapsed_ms:.1f}ms, {len(fetched)} orders"
        )
        return fetched

    def _normalize_open_orders(self, fetched: list) -> list:
        """钩子：将原始未成交订单转换为 passivbot 格式。"""
        for elm in fetched:
            elm["position_side"] = self._get_position_side_for_order(elm)
            elm["qty"] = elm["amount"]
            self._record_live_margin_mode_from_payload(elm)
        return sorted(fetched, key=lambda x: x["timestamp"])

    async def fetch_open_orders(self, symbol: str = None) -> list:
        """获取未成交订单，可按交易对筛选。

        参数：
            symbol：可选的交易对筛选条件。

        返回：
            list：按时间戳排序的标准化字段订单列表。

        异常：
            Exception：API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        fetched = await self._do_fetch_open_orders(symbol=symbol)
        return self._normalize_open_orders(fetched)

    async def capture_open_orders_snapshot(self, symbol: str = None) -> tuple[list, list]:
        """获取一次未成交订单并从同一数据中派生标准化订单。"""
        fetched = await self._do_fetch_open_orders(symbol=symbol)
        return fetched, self._normalize_open_orders(deepcopy(fetched))

    async def watch_orders(self):
        """模板方法：监听订单更新。

        使用钩子进行自定义：
        - can_watch_orders()：检查是否可监听
        - _do_watch_orders()：获取原始订单更新
        - _normalize_order_update()：转换为 passivbot 格式

        如果不可监听，优雅退出（轮询处理更新）。
        """
        if not self.can_watch_orders():
            logging.info(f"[ws] {self.exchange}: watch_orders not available, using REST polling")
            return

        logging.info(f"[ws] {self.exchange}: starting order watch")
        while True:
            try:
                if self.stop_websocket:
                    break
                raw_orders = await self._do_watch_orders()
                normalized = [self._normalize_order_update(o) for o in raw_orders]
                self.handle_order_update(normalized)
            except Exception as e:
                self._health_ws_reconnects += 1
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

    async def update_exchange_config(self):
        """如果支持，将交易所设置为对冲模式。

        使用能力检查来确定交易所是否支持持仓模式设置。
        如果不支持则优雅跳过。

        异常：
            Exception：API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        if not self.cca.has.get("setPositionMode"):
            logging.debug("[config] %s does not support setPositionMode, skipping", self.exchange)
            return

        logging.debug(
            "[config] %s: setting position mode to hedge via CCXT set_position_mode(True)",
            self.exchange,
        )
        t0 = time.time()
        res = await self.cca.set_position_mode(True)
        elapsed_ms = (time.time() - t0) * 1000
        logging.debug("[config] %s: set_position_mode completed in %.1fms", self.exchange, elapsed_ms)
        logging.debug("[config] set hedge mode response: %s", res)

    def _should_set_margin_mode(self, symbol: str) -> bool:
        """钩子：是否应为此交易对调用 set_margin_mode？

        默认：检查 CCXT 的 has['setMarginMode']
        覆盖：如果交易所不支持/不需要则返回 False
        """
        return self.cca.has.get("setMarginMode", False)

    def _get_margin_mode_preference(self) -> str:
        """返回标准化的 live.margin_mode_preference。

        逐仓保证金开仓支持目前有意禁用。现有的实时逐仓
        持仓/订单在重启后仍被保留和可管理。
        """
        config = getattr(self, "config", {}) or {}
        raw = get_optional_live_value(config, "margin_mode_preference", "cross")
        pref = str(raw).strip().lower() if raw is not None else "cross"
        aliases = {
            "auto": "cross",
            "auto_cross": "cross",
            "auto_cross_preferred": "cross",
            "cross": "cross",
            "auto_isolated": "isolated_disabled",
            "auto_isolated_preferred": "isolated_disabled",
            "isolated": "isolated_disabled",
        }
        if pref in aliases:
            normalized = aliases[pref]
        else:
            normalized = "cross"
        if normalized == "isolated_disabled":
            if not getattr(self, "_margin_mode_preference_warned", False):
                logging.warning(
                    "live.margin_mode_preference=%r requested isolated margin, but isolated entry support is currently disabled; using cross-only behavior",
                    raw,
                )
                self._margin_mode_preference_warned = True
            return "cross"
        if normalized == "cross":
            return "cross"
        if not getattr(self, "_margin_mode_preference_warned", False):
            logging.warning(
                "Invalid live.margin_mode_preference=%r; using cross-only behavior",
                raw,
            )
            self._margin_mode_preference_warned = True
        return "cross"

    def _normalize_margin_mode(self, value) -> str | None:
        """将各种保证金模式表示标准化为 'cross' 或 'isolated'。

        处理布尔值、数字字符串（"0"/"1"）以及常见变体拼写。
        无法识别的值返回 None。
        """
        if value is None:
            return None
        if isinstance(value, bool):
            return "isolated" if value else "cross"
        text = str(value).strip().lower()
        if not text:
            return None
        if text in {"0", "cross", "crossed", "cross_margin", "cross margin"}:
            return "cross"
        if text in {"1", "isolated", "isolate", "isolated_margin", "isolated margin"}:
            return "isolated"
        if "isol" in text:
            return "isolated"
        if "cross" in text:
            return "cross"
        return None

    def _extract_live_margin_mode(self, payload: dict | None) -> str | None:
        """从交易所响应载荷中提取实时保证金模式。

        按优先级检查多个已知字段名（margin_mode、marginMode、marginType、
        tradeMode、tdMode、mgnMode），包括嵌套的 info 结构和 leverage.type。
        """
        if not isinstance(payload, dict):
            return None
        candidates = [
            payload.get("margin_mode"),
            payload.get("marginMode"),
            payload.get("marginType"),
            payload.get("tradeMode"),
            payload.get("tdMode"),
            payload.get("mgnMode"),
        ]
        info = payload.get("info")
        if isinstance(info, dict):
            candidates.extend(
                [
                    info.get("margin_mode"),
                    info.get("marginMode"),
                    info.get("marginType"),
                    info.get("tradeMode"),
                    info.get("tdMode"),
                    info.get("mgnMode"),
                ]
            )
            position = info.get("position")
            if isinstance(position, dict):
                leverage = position.get("leverage")
                if isinstance(leverage, dict):
                    candidates.append(leverage.get("type"))
        for candidate in candidates:
            normalized = self._normalize_margin_mode(candidate)
            if normalized is not None:
                return normalized
        if payload.get("isolated") is not None:
            return self._normalize_margin_mode(bool(payload.get("isolated")))
        if isinstance(info, dict) and info.get("isolated") is not None:
            return self._normalize_margin_mode(bool(info.get("isolated")))
        return None

    def _record_live_margin_mode(self, symbol: str | None, margin_mode: str | None) -> None:
        if not symbol or not margin_mode:
            return
        normalized = self._normalize_margin_mode(margin_mode)
        if normalized in {"cross", "isolated"}:
            self._live_margin_modes[str(symbol)] = normalized

    def _record_live_margin_mode_from_payload(
        self, payload: dict | None, symbol: str | None = None
    ) -> None:
        if not isinstance(payload, dict):
            return
        target_symbol = symbol or payload.get("symbol")
        self._record_live_margin_mode(target_symbol, self._extract_live_margin_mode(payload))

    def _has_live_symbol_state(self, symbol: str) -> bool:
        pos = getattr(self, "positions", {}).get(symbol, {})
        if abs(float(pos.get("long", {}).get("size", 0.0) or 0.0)) > 0.0:
            return True
        if abs(float(pos.get("short", {}).get("size", 0.0) or 0.0)) > 0.0:
            return True
        if symbol in getattr(self, "open_orders", {}) and self.open_orders.get(symbol):
            return True
        return False

    def _get_margin_capability(self, symbol: str) -> str:
        """返回以下之一：both、cross_only、isolated_only。"""
        if self._requires_isolated_margin(symbol):
            return "isolated_only"

        market = getattr(self, "markets_dict", {}).get(symbol, {})
        margin_modes = market.get("marginModes", {})
        if isinstance(margin_modes, dict):
            cross = margin_modes.get("cross")
            isolated = margin_modes.get("isolated")
            if cross is True and isolated is True:
                return "both"
            if cross is True and isolated is False:
                return "cross_only"
            if cross is False and isolated is True:
                return "isolated_only"
        return "both"

    def _resolve_margin_policy_for_symbol(self, symbol: str) -> dict:
        """解析实际要应用的模式以及是否必须阻止新开仓。"""
        live_margin_mode = getattr(self, "_live_margin_modes", {}).get(symbol)
        if self._has_live_symbol_state(symbol) and live_margin_mode in {"cross", "isolated"}:
            capability = self._get_margin_capability(symbol)
            return {
                "mode": live_margin_mode,
                "blocked": False,
                "capability": capability,
                "live_margin_mode": live_margin_mode,
            }
        capability = self._get_margin_capability(symbol)
        preference = self._get_margin_mode_preference()
        if capability == "isolated_only":
            return {
                "mode": "isolated",
                "blocked": preference == "cross",
                "capability": capability,
            }
        return {"mode": "cross", "blocked": False, "capability": capability}

    def _requires_isolated_margin(self, symbol: str) -> bool:
        """检查交易对是否需要逐仓保证金模式。

        在子类中覆盖以检测交易所特定的仅逐仓市场。
        示例：Hyperliquid 上的 HIP-3 股票永续合约、某些杠杆代币。

        参数：
            symbol：CCXT 格式的交易对

        返回：
            如果此交易对需要逐仓保证金模式则返回 True
        """
        # 默认：检查市场信息中常见的仅逐仓标志
        market = getattr(self, "markets_dict", {}).get(symbol, {})
        info = market.get("info", {})

        # 检查各交易所的通用标志
        if info.get("onlyIsolated", False):
            return True
        if info.get("marginMode") == "isolated":
            return True
        if info.get("isolatedOnly", False):
            return True

        return False

    def _get_margin_mode_for_symbol(self, symbol: str) -> str:
        """获取交易对的适当保证金模式。

        参数：
            symbol：CCXT 格式的交易对

        返回：
            "isolated" 或 "cross"
        """
        return self._resolve_margin_policy_for_symbol(symbol)["mode"]

    def _calc_min_isolated_leverage(self) -> int:
        """计算逐仓持仓所需的最小杠杆。

        对于逐仓保证金，margin_required = exposure / leverage。
        为确保保证金需求不超过余额：
            margin_required <= balance
            (TWEL * balance) / leverage <= balance
            leverage >= TWEL

        返回：
            最小杠杆（两侧最大 TWEL 的向上取整）
        """
        long_twel = float(self.bot_value("long", "total_wallet_exposure_limit") or 0.0)
        short_twel = float(self.bot_value("short", "total_wallet_exposure_limit") or 0.0)
        max_twel = max(long_twel, short_twel)

        if max_twel <= 0:
            return 1

        # 向上取整确保始终有足够的保证金
        return max(1, math.ceil(max_twel))

    def _calc_leverage_for_symbol(self, symbol: str) -> int:
        """计算交易对的适当杠杆。

        对于逐仓交易对，确保杠杆足够高以满足
        给定配置 TWEL 的保证金需求。

        参数：
            symbol：CCXT 格式的交易对

        返回：
            使用的杠杆（受交易对的 max_leverage 限制）
        """
        configured = int(self.config_get(["live", "leverage"], symbol=symbol))
        max_lev = getattr(self, "max_leverage", {}).get(symbol, configured)

        if self._get_margin_mode_for_symbol(symbol) == "isolated":
            min_lev = self._calc_min_isolated_leverage()
            leverage = max(configured, min_lev)

            if min_lev > max_lev:
                logging.warning(
                    f"{symbol}: TWEL requires {min_lev}x leverage for isolated margin, "
                    f"but max is {max_lev}x. Risk of insufficient margin errors."
                )

            leverage = min(leverage, max_lev)
            if leverage != configured:
                logging.info(
                    f"{symbol}: isolated margin requires min {min_lev}x leverage "
                    f"(configured: {configured}x, using: {leverage}x)"
                )
            return leverage

        return min(configured, max_lev)

    def _filter_approved_symbols(self, pside: str, symbols: set[str]) -> set[str]:
        """过滤掉因保证金模式不兼容而被阻止新开仓的交易对。

        受阻止的交易对会记录一次警告（每 pside/symbol/capability 组合仅记录一次），
        但现有的持仓和订单不受影响。
        """
        symbols = super()._filter_approved_symbols(pside, symbols)
        kept = set()
        for symbol in symbols:
            policy = self._resolve_margin_policy_for_symbol(symbol)
            if not policy["blocked"]:
                kept.add(symbol)
                continue
            warn_key = (pside, symbol, policy["capability"])
            if warn_key in self._blocked_margin_symbols_warned:
                continue
            self._blocked_margin_symbols_warned.add(warn_key)
            blocked_reason = (
                "isolated-only" if policy["capability"] == "isolated_only" else "cross-only"
            )
            logging.warning(
                "[margin] disabling %s %s for new entries: isolated margin support is currently disabled and this symbol is %s on %s. Existing positions/orders remain manageable.",
                pside,
                symbol,
                blocked_reason,
                getattr(self, "exchange", "this exchange"),
            )
        return kept

    async def update_exchange_config_by_symbols(self, symbols: list):
        """为每个交易对设置杠杆和保证金模式。

        对于逐仓交易对，杠杆会自动调整以确保
        给定配置 TWEL 的保证金需求可以满足。

        参数：
            symbols：要配置的交易对列表。

        异常：
            Exception：API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        can_set_leverage = self.cca.has.get("setLeverage", False)

        for symbol in symbols:
            if can_set_leverage:
                leverage = self._calc_leverage_for_symbol(symbol)
                logging.debug(f"{self.exchange}: setting leverage for {symbol} to {leverage}x")
                t0 = time.time()
                await self.cca.set_leverage(leverage, symbol=symbol)
                elapsed_ms = (time.time() - t0) * 1000
                logging.debug(f"{self.exchange}: set_leverage completed in {elapsed_ms:.1f}ms")
                logging.info(f"{symbol}: set leverage to {leverage}x")

            if self._should_set_margin_mode(symbol):
                margin_mode = self._get_margin_mode_for_symbol(symbol)
                logging.debug(f"{self.exchange}: setting margin mode for {symbol} to {margin_mode}")
                t0 = time.time()
                await self.cca.set_margin_mode(margin_mode, symbol=symbol)
                elapsed_ms = (time.time() - t0) * 1000
                logging.debug(f"{self.exchange}: set_margin_mode completed in {elapsed_ms:.1f}ms")
                logging.info(f"{symbol}: set {margin_mode} margin mode")

    def set_market_specific_settings(self):
        """从 CCXT 市场信息中提取市场特定设置。

        从 CCXT 的统一市场结构中填充 symbol_ids、min_costs、min_qtys、
        qty_steps、price_steps 和 c_mults。
        """
        super().set_market_specific_settings()
        for symbol, market in self.markets_dict.items():
            self.symbol_ids[symbol] = market["id"]
            self.min_costs[symbol] = market["limits"]["cost"]["min"] or 0.1
            raw_min_qty = (
                market["precision"]["amount"]
                if market["limits"]["amount"]["min"] is None
                else market["limits"]["amount"]["min"]
            )
            qty_step = market["precision"]["amount"]
            if raw_min_qty <= 0.0 and qty_step is not None and qty_step > 0.0:
                raw_min_qty = qty_step
            self.min_qtys[symbol] = raw_min_qty
            self.qty_steps[symbol] = qty_step
            self.price_steps[symbol] = market["precision"]["price"]
            self.c_mults[symbol] = market.get("contractSize", 1)

    async def fetch_tickers(self) -> dict:
        """模板方法：获取所有市场的当前行情数据。

        使用钩子：
        - _do_fetch_tickers()：调用交易所 API
        - _normalize_tickers()：转换为 {symbol: {bid, ask, last}}

        返回：
            dict：按交易对键控的行情数据，包含 bid/ask/last 价格。

        异常：
            Exception：API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        fetched = await self._do_fetch_tickers()
        return self._normalize_tickers(fetched)

    async def _do_fetch_tickers(self) -> dict:
        """钩子：调用交易所 API 获取行情数据。

        默认：使用 CCXT 的 fetch_tickers()
        覆盖：自定义 API 调用或不同端点
        """
        logging.debug(f"{self.exchange}: fetching tickers via CCXT fetch_tickers()")
        t0 = time.time()
        result = await self.cca.fetch_tickers()
        elapsed_ms = (time.time() - t0) * 1000
        logging.debug(
            f"{self.exchange}: fetch_tickers completed in {elapsed_ms:.1f}ms, {len(result)} tickers"
        )
        return result

    def _normalize_tickers(self, fetched: dict) -> dict:
        """钩子：转换为 {symbol: {bid, ask, last}} 格式。

        默认：使用 CCXT 统一字段，过滤到 markets_dict
        覆盖：交易所特定的字段映射
        """
        tickers = {}
        for symbol, data in fetched.items():
            if symbol in self.markets_dict:
                tickers[symbol] = {
                    "bid": float(data.get("bid") or 0),
                    "ask": float(data.get("ask") or 0),
                    "last": float(data.get("last") or data.get("bid") or 0),
                }
        return tickers

    async def fetch_ohlcv(self, symbol: str, timeframe: str = "1m") -> list:
        """获取 OHLCV K 线数据。

        参数：
            symbol：交易对符号。
            timeframe：K 线时间周期（默认 "1m"）。

        返回：
            list：OHLCV 数据。

        异常：
            Exception：API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        logging.debug(f"{self.exchange}: fetching OHLCV for {symbol} ({timeframe})")
        t0 = time.time()
        result = await self.cca.fetch_ohlcv(symbol, timeframe=timeframe, limit=1000)
        elapsed_ms = (time.time() - t0) * 1000
        logging.debug(
            f"{self.exchange}: fetch_ohlcv completed in {elapsed_ms:.1f}ms, {len(result)} candles"
        )
        return result

    async def fetch_ohlcvs_1m(self, symbol: str, since: float = None, limit: int = None) -> list:
        """获取 1 分钟 OHLCV 数据，支持分页。

        参数：
            symbol：交易对符号。
            since：起始时间戳（毫秒）。
            limit：最大 K 线数量。

        返回：
            list：按时间戳排序的 OHLCV K 线数据。

        异常：
            Exception：API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        n_limit = limit or 1000
        logging.debug(
            f"{self.exchange}: fetching 1m OHLCV for {symbol}, since={since}, limit={n_limit}"
        )
        t0 = time.time()

        if since is None:
            result = await self.cca.fetch_ohlcv(symbol, timeframe="1m", limit=n_limit)
            elapsed_ms = (time.time() - t0) * 1000
            logging.debug(
                f"{self.exchange}: fetch_ohlcvs_1m completed in {elapsed_ms:.1f}ms, {len(result)} candles"
            )
            return result

        since = int(since // 60000 * 60000)  # 取整到分钟
        all_candles = {}
        page_count = 0
        for _ in range(5):  # 最多 5 次分页请求
            fetched = await self.cca.fetch_ohlcv(symbol, timeframe="1m", since=since, limit=n_limit)
            page_count += 1
            if not fetched:
                break
            for candle in fetched:
                all_candles[candle[0]] = candle
            if len(fetched) < n_limit:
                break
            since = fetched[-1][0]

        elapsed_ms = (time.time() - t0) * 1000
        logging.debug(
            f"{self.exchange}: fetch_ohlcvs_1m completed in {elapsed_ms:.1f}ms, {len(all_candles)} candles ({page_count} pages)"
        )
        return sorted(all_candles.values(), key=lambda x: x[0])

    async def fetch_pnls(self, start_time=None, end_time=None, limit=None) -> list:
        """模板方法：获取交易历史用于 PnL 追踪。

        使用钩子：
        - _do_fetch_pnls()：调用交易所 API
        - _normalize_pnls()：为每笔交易添加 pnl、position_side、qty
        - _get_pnl_from_trade()：提取 PnL 值
        - _get_position_side_from_trade()：推导 position_side

        参数：
            start_time：起始时间戳（毫秒）。
            end_time：结束时间戳（毫秒）。
            limit：最大获取交易数量。

        返回：
            list：按时间戳排序的交易列表，包含 pnl、position_side、qty 字段。

        异常：
            Exception：API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        trades = await self._do_fetch_pnls(start_time, end_time, limit)
        return self._normalize_pnls(trades)

    async def _do_fetch_pnls(self, start_time, end_time, limit) -> list:
        """钩子：调用交易所 API 获取交易记录。

        默认：使用 CCXT 的 fetch_my_trades()
        覆盖：自定义 API 调用或不同端点
        """
        logging.debug(
            f"{self.exchange}: fetching PnLs via CCXT fetch_my_trades(), "
            f"since={start_time}, end_time={end_time}, limit={limit}"
        )
        t0 = time.time()
        params = {}
        if end_time:
            params["until"] = int(end_time)
        result = await self.cca.fetch_my_trades(
            symbol=None,
            since=int(start_time) if start_time else None,
            limit=limit,
            params=params,
        )
        elapsed_ms = (time.time() - t0) * 1000
        logging.debug(
            f"{self.exchange}: fetch_my_trades completed in {elapsed_ms:.1f}ms, {len(result)} trades"
        )
        return result

    def _normalize_pnls(self, trades: list) -> list:
        """钩子：为每笔交易添加 pnl、position_side、qty。

        默认：使用 _get_pnl_from_trade 和 _get_position_side_from_trade
        覆盖：交易所特定的标准化
        """
        for trade in trades:
            trade["qty"] = trade["amount"]
            trade["pnl"] = self._get_pnl_from_trade(trade)
            trade["position_side"] = self._get_position_side_from_trade(trade)
        return sorted(trades, key=lambda x: x["timestamp"])

    def _build_order_params(self, order: dict) -> dict:
        """钩子：为 CCXT 订单创建构建执行参数。

        默认：处理 positionSide、clientOrderId、postOnly/timeInForce
        覆盖：交易所特定的参数需求

        参数：
            order：包含 type、position_side、custom_id 等的订单字典。

        返回：
            dict：CCXT create_order 的参数。
        """
        params = {}

        if order.get("position_side"):
            params["positionSide"] = order["position_side"].upper()

        if order.get("custom_id"):
            params["clientOrderId"] = order["custom_id"]

        if order.get("type") == "limit":
            tif = require_live_value(self.config, "time_in_force")
            if tif == "post_only":
                params["postOnly"] = True
            else:
                params["timeInForce"] = "GTC"

        return params

    async def execute_orders(self, orders: list[dict]) -> list[dict]:
        """使用 asyncio.gather 并行执行订单创建。

        与基类的顺序方式不同，此方法同时发送所有订单，
        在具有良好限速的交易所上实现更低的延迟。
        """
        if not orders:
            return []

        tasks = [self.execute_order(order) for order in orders]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 检查异常并按需触发错误处理
        any_exceptions = any(isinstance(r, Exception) for r in results)
        if any_exceptions:
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logging.error(f"error executing order {orders[i]}: {result}")
            await self.restart_bot_on_too_many_errors()

        return results

    async def execute_cancellations(self, orders: list[dict]) -> list[dict]:
        """使用 asyncio.gather 并行执行订单取消。"""
        if not orders:
            return []

        tasks = [self.execute_cancellation(order) for order in orders]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        any_exceptions = any(isinstance(r, Exception) for r in results)
        if any_exceptions:
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logging.error(f"error cancelling order {orders[i]}: {result}")
            await self.restart_bot_on_too_many_errors()

        return results
