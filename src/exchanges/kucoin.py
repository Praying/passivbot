from __future__ import annotations
from exchanges.ccxt_bot import CCXTBot, format_exchange_config_response
from passivbot import logging
import ccxt.pro as ccxt_pro
import ccxt.async_support as ccxt_async
import asyncio
import passivbot_rust as pbr
from utils import ts_to_date, utc_ms
from procedures import assert_correct_ccxt_version
from collections import defaultdict
import hmac
import hashlib
import base64

calc_order_price_diff = pbr.calc_order_price_diff

# ---------------------------------------------------------------------------
# 经纪人 mixin 类，用于在 KuCoin 期货请求中注入 KC-BROKER-NAME。
#
# 当通过 ``options['partner']`` 字典（见下方 ``create_ccxt_sessions``）
# 提供经纪人配置时，CCXT 会自动向私有 API 调用添加 ``KC-API-PARTNER``、
# ``KC-API-PARTNER-SIGN`` 和 ``KC-API-PARTNER-VERIFY`` 头部。
# 然而，人类可读的经纪人名称（``KC-BROKER-NAME``）仅附加在
# 经纪人专用端点上。为确保 KuCoin 将期货交易归因于经纪人，
# 我们重写 ``sign`` 方法，在 private 和 futuresPrivate 请求上
# 附加经纪人名称。当定义了经纪人代码时，应使用这些类
# 代替原始的 CCXT 交易所类。


def _add_kucoin_broker_name_header(signed: dict, options: dict) -> dict:
    partner_root = options.get("partner")
    if not isinstance(partner_root, dict):
        raise TypeError("KuCoin broker partner config must be a mapping")
    partner_cfg = partner_root.get("future")
    if not isinstance(partner_cfg, dict):
        raise KeyError("KuCoin futures broker partner config missing 'future' section")
    broker_name = partner_cfg.get("name")
    if not isinstance(broker_name, str) or not broker_name:
        raise ValueError("KuCoin futures broker-name must be a non-empty string")

    headers = signed.get("headers")
    if not isinstance(headers, dict):
        raise TypeError("KuCoin signed request missing headers mapping")
    headers = dict(headers)
    headers["KC-BROKER-NAME"] = broker_name
    signed["headers"] = headers
    return signed


class AsyncKucoinBrokerFutures(ccxt_async.kucoinfutures):
    """支持经纪人标记的异步 KuCoin 期货交易所。"""

    def __init__(self, config=None):
        super().__init__(config)

    @property
    def checkConflictingProxies(self):
        """确保 camelCase 版本始终指向 snake_case"""
        return self.check_conflicting_proxies

    def sign(self, path, api="public", method="GET", params=None, headers=None, body=None):
        signed = super().sign(path, api, method, params or {}, headers, body)
        if api in {"private", "futuresPrivate", "broker"}:
            return _add_kucoin_broker_name_header(signed, self.options)
        return signed


class ProKucoinBrokerFutures(ccxt_pro.kucoinfutures):
    """支持经纪人标记的 WebSocket KuCoin 期货交易所。"""

    def __init__(self, config=None):
        super().__init__(config)

    @property
    def checkConflictingProxies(self):
        """确保 camelCase 版本始终指向 snake_case"""
        return self.check_conflicting_proxies

    def sign(self, path, api="public", method="GET", params=None, headers=None, body=None):
        signed = super().sign(path, api, method, params or {}, headers, body)
        if api in {"private", "futuresPrivate", "broker"}:
            return _add_kucoin_broker_name_header(signed, self.options)
        return signed


assert_correct_ccxt_version(ccxt=ccxt_async)


class KucoinBot(CCXTBot):
    MAX_OPEN_ORDERS = 150

    def __init__(self, config: dict):
        super().__init__(config)
        self.custom_id_max_length = 40
        self.quote = "USDT"
        self.hedge_mode = True

    def _get_partner_signature(self, timestamp: str) -> str:
        prehash = f"{timestamp}{self.partner}{self.api_key}"
        digest = hmac.new(self.broker_key.encode(), prehash.encode(), hashlib.sha256).digest()
        return base64.b64encode(digest).decode()

    def create_ccxt_sessions(self) -> None:
        """初始化支持经纪人的 KuCoin 期货 CCXT 会话。

        如果在 ``self.broker_code['futures']`` 下定义了经纪人代码，
        则使用这些值配置合作伙伴签名，以便 private/futures
        请求包含正确的经纪人元数据。
        """
        if not isinstance(self.broker_code, dict):
            raise TypeError("KuCoin broker code must be an object with a futures section")
        if "futures" not in self.broker_code:
            raise KeyError("KuCoin broker code missing 'futures' section")
        broker_cfg = self.broker_code["futures"]
        if not isinstance(broker_cfg, dict):
            raise TypeError("KuCoin futures broker code must be an object")
        required = {
            "partner": broker_cfg.get("partner"),
            "broker-key": broker_cfg.get("broker-key"),
            "broker-name": broker_cfg.get("broker-name"),
        }
        missing = [key for key, value in required.items() if not isinstance(value, str) or not value]
        if missing:
            raise ValueError(f"KuCoin futures broker code missing required fields: {missing}")

        options = {
            "partner": {
                "future": {
                    "id": required["partner"],
                    "secret": required["broker-key"],
                    "name": required["broker-name"],
                }
            }
        }
        base_kwargs = {
            "apiKey": self.user_info["key"],
            "secret": self.user_info["secret"],
            "password": self.user_info["passphrase"],
            "enableRateLimit": True,
        }
        base_kwargs["options"] = options

        async_cls = AsyncKucoinBrokerFutures
        pro_cls = ProKucoinBrokerFutures

        self.cca = async_cls(dict(base_kwargs))
        self.cca.options.update(self._build_ccxt_options())
        self.cca.options["defaultType"] = "swap"
        self._apply_endpoint_override(self.cca)

        if self.ws_enabled:
            self.ccp = pro_cls(dict(base_kwargs))
            self.ccp.options.update(self._build_ccxt_options())
            self.ccp.options["defaultType"] = "swap"
            self._apply_endpoint_override(self.ccp)
        elif self.endpoint_override:
            logging.info("由于自定义端点覆盖，跳过 Kucoin websocket 会话。")

    async def watch_ohlcvs_1m(self):
        """KuCoin：空操作 - 不使用 OHLCV websocket。"""
        return

    async def watch_ohlcv_1m_single(self, symbol):
        """KuCoin：空操作 - 不使用 OHLCV websocket。"""
        return

    def _get_position_side_for_order(self, order: dict) -> str:
        """KuCoin：从持仓状态推导 position_side。"""
        return self.determine_pos_side(order)

    def determine_pos_side(self, order):
        # 非双向持仓模式
        if self.has_position("long", order["symbol"]):
            return "long"
        elif self.has_position("short", order["symbol"]):
            return "short"
        elif order["side"] == "buy":
            return "long"
        elif order["side"] == "sell":
            return "short"
        raise Exception(f"unknown side {order['side']}")

    async def _do_fetch_open_orders(self, symbol: str = None) -> list:
        """KuCoin：分页获取未成交订单。

        Returns:
            list: 跨页的原始未成交订单。

        Raises:
            Exception: API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        open_orders = []
        page_size = 100
        current_page = 1
        while True:
            params = {"pageSize": page_size, "currentPage": current_page}
            fetched = await self.cca.fetch_open_orders(symbol=symbol, params=params)
            if not fetched:
                break
            for order in fetched:
                order["position_side"] = self.determine_pos_side(order)
                order["qty"] = order["amount"]
                self._record_live_margin_mode_from_payload(order)
            open_orders.extend(fetched)
            if len(fetched) < page_size:
                break
            if len(open_orders) >= self.MAX_OPEN_ORDERS:
                break
            current_page += 1
        return open_orders

    def _normalize_open_orders(self, fetched: list) -> list:
        for order in fetched:
            order["position_side"] = self.determine_pos_side(order)
            order["qty"] = order["amount"]
            self._record_live_margin_mode_from_payload(order)
        return sorted(fetched, key=lambda x: x["timestamp"])

    async def fetch_open_orders(self, symbol: str = None) -> list:
        fetched = await self._do_fetch_open_orders(symbol=symbol)
        return self._normalize_open_orders(fetched)

    def _get_balance(self, fetched: dict) -> float:
        """KuCoin 使用 info.data 中的 marginBalance。"""
        return float(fetched["info"]["data"]["marginBalance"])

    async def calc_ideal_orders(self):
        # KuCoin 强制 150 个未成交订单上限；仅保留最接近价格目标的订单。
        ideal_orders = await super().calc_ideal_orders()
        flattened = []
        for symbol, orders in ideal_orders.items():
            if not orders:
                continue
            market_price = await self.cm.get_current_close(symbol, max_age_ms=10_000)
            for order in orders:
                price_diff = calc_order_price_diff(order["side"], order["price"], market_price)
                flattened.append((price_diff, symbol, order))
        limit = getattr(self, "MAX_OPEN_ORDERS", 150)
        flattened.sort(key=lambda x: x[0])
        trimmed = flattened[:limit] if limit and limit > 0 else flattened
        filtered: dict[str, list] = {symbol: [] for symbol in self.active_symbols}
        for _, symbol, order in trimmed:
            filtered.setdefault(symbol, []).append(order)
        for symbol in ideal_orders:
            filtered.setdefault(symbol, ideal_orders[symbol])
        return filtered

    async def fetch_fills(self, start_time=None, end_time=None, limit=None):
        if start_time is None:
            logging.warning(
                "fetch_fills called without start_time; "
                "consider setting pnls_max_lookback_days in config to limit fetch"
            )
        all_fills = []
        params = {}
        if end_time:
            params["until"] = int(end_time)
        day = 1000 * 60 * 60 * 24
        now_ms = self.get_exchange_time()
        while True:
            fills = await self.cca.fetch_my_trades(params=params)
            if fills:
                new_until = fills[0]["timestamp"]
                if "until" in params and new_until == params["until"]:
                    new_until -= day
            else:
                if "until" in params:
                    new_until = params["until"] - day
                else:
                    new_until = now_ms - day
            params["until"] = new_until
            all_fills = fills + all_fills
            if start_time is not None and new_until <= start_time + day:
                break
            logging.info(
                f"fetched {len(fills)} fill{'' if len(fills) == 1 else 's'}"
                f" {ts_to_date(new_until)[:19]}"
            )
        for i in range(len(all_fills)):
            all_fills[i]["qty"] = all_fills[i]["amount"]
            all_fills[i]["pnl"] = 0.0
            if all_fills[i]["side"] == "buy":
                all_fills[i]["position_side"] = (
                    "long" if float(all_fills[i]["info"]["closeFeePay"]) == 0.0 else "short"
                )
            elif all_fills[i]["side"] == "sell":
                all_fills[i]["position_side"] = (
                    "short" if float(all_fills[i]["info"]["closeFeePay"]) == 0.0 else "long"
                )
            else:
                raise Exception(f"invalid side {all_fills[i]}")
        deduped = {x["id"]: x for x in all_fills}
        if start_time:
            deduped = {k: v for k, v in deduped.items() if v["timestamp"] >= start_time}
        if end_time:
            deduped = {k: v for k, v in deduped.items() if v["timestamp"] <= end_time}
        return sorted(deduped.values(), key=lambda x: x["timestamp"])

    async def fetch_positions_history(self, start_time=None, end_time=None, limit=None):
        if start_time is None:
            logging.warning(
                "fetch_positions_history called without start_time; "
                "consider setting pnls_max_lookback_days in config to limit fetch"
            )
        all_ph = []
        params = {}
        if end_time:
            params["until"] = int(end_time)
        day = 1000 * 60 * 60 * 24
        now_ms = self.get_exchange_time()
        while True:
            ph = await self.cca.fetch_positions_history(params=params)
            ph = sorted(ph, key=lambda x: x["lastUpdateTimestamp"])
            if ph:
                new_until = ph[0]["lastUpdateTimestamp"]
                if "until" in params and new_until == params["until"]:
                    new_until -= day
            else:
                if "until" in params:
                    new_until = params["until"] - day
                else:
                    new_until = now_ms - day
            params["until"] = new_until
            all_ph = ph + all_ph
            if start_time is not None and new_until <= start_time + day:
                break
            logging.info(
                f"fetched {len(ph)} pos histor{'y' if len(ph) == 1 else 'ies'}"
                f" {ts_to_date(new_until)[:19]}"
            )
        deduped = {x["info"]["closeId"]: x for x in all_ph}
        if start_time:
            deduped = {k: v for k, v in deduped.items() if v["lastUpdateTimestamp"] >= start_time}
        if end_time:
            deduped = {k: v for k, v in deduped.items() if v["lastUpdateTimestamp"] <= end_time}
        return sorted(deduped.values(), key=lambda x: x["lastUpdateTimestamp"])

    async def fetch_pnls(self, start_time=None, end_time=None, limit=None):
        # 获取成交记录...
        mt = await self.fetch_fills(start_time=start_time, end_time=end_time)
        closes = [
            x
            for x in mt
            if (x["side"] == "sell" and x["position_side"] == "long")
            or (x["side"] == "buy" and x["position_side"] == "short")
        ]
        if not closes:
            return mt
        # 获取持仓历史以计算 PnL
        ph = await self.fetch_positions_history(
            start_time=closes[0]["timestamp"] - 60000, end_time=closes[-1]["timestamp"] + 60000
        )

        # 匹配...
        cld, phd = defaultdict(list), defaultdict(list)
        for x in closes:
            cld[x["symbol"]].append(x)
        for x in ph:
            phd[x["symbol"]].append(x)
        matches = []
        seen_trade_id = set()
        for symbol in phd:
            if symbol not in cld:
                logging.debug(f"no fills for pos close {symbol} {phd[symbol]}")
                continue
            for p in phd[symbol]:
                with_td = sorted(
                    [x for x in cld[symbol] if x["id"] not in seen_trade_id],
                    key=lambda x: abs(p["lastUpdateTimestamp"] - x["timestamp"]),
                )
                if not with_td:
                    logging.debug(f"no matching fill for {p}")
                    continue
                best_match = with_td[0]
                matches.append((p, best_match))
                timedelta = best_match["timestamp"] - p["lastUpdateTimestamp"]
                if timedelta > 1000:
                    logging.debug(
                        f"best match fill and pos close {symbol} timedelta>1000ms: {best_match['timestamp'] - p['lastUpdateTimestamp']}ms"
                    )
                seen_trade_id.add(best_match["id"])
            if len(phd[symbol]) != len(cld[symbol]):
                logging.debug(
                    f"len mismatch between closes and positions_history for {symbol}: {len(cld[symbol])} {len(phd[symbol])}"
                )
        # 添加 PnL，去重并返回
        deduped = {}
        for p, c in matches:
            c["pnl"] = p["realizedPnl"]
            if c["id"] in deduped:
                logging.debug(f"unexpected duplicate {c}")
                continue
            deduped[c["id"]] = c
        for t in mt:
            if t["id"] not in deduped:
                deduped[t["id"]] = t

        return sorted(deduped.values(), key=lambda x: x["timestamp"])

    async def gather_fill_events(self, start_time=None, end_time=None, limit=None):
        """返回 KuCoin 的标准成交事件。

        Returns:
            list: 规范化字段的成交事件。

        Raises:
            Exception: API 错误时（调用者通过 restart_bot_on_too_many_errors 处理）。
        """
        fills = await self.fetch_pnls(start_time=start_time, end_time=end_time, limit=limit)
        events = []
        for fill in fills:
            events.append(
                {
                    "id": fill.get("id") or fill.get("orderId"),
                    "timestamp": fill.get("timestamp"),
                    "symbol": fill.get("symbol"),
                    "side": fill.get("side"),
                    "position_side": fill.get("position_side"),
                    "qty": fill.get("qty") or fill.get("amount"),
                    "price": fill.get("price"),
                    "pnl": fill.get("pnl"),
                    "fee": fill.get("fee"),
                    "info": fill.get("info"),
                }
            )
        return events

    def _build_order_params(self, order: dict) -> dict:
        margin_mode = self._get_margin_mode_for_symbol(order["symbol"])
        return {
            "timeInForce": "GTC",
            "reduceOnly": order.get("reduce_only", False),
            "marginMode": margin_mode.upper(),
            "clientOid": order.get("custom_id", None),
            "positionSide": order.get("position_side", "").upper(),
        }

    def did_cancel_order(self, executed, order=None) -> bool:
        if isinstance(executed, list) and len(executed) == 1:
            return self.did_cancel_order(executed[0], order)
        try:
            return order is not None and order["id"] in executed.get("info", {}).get("data", {}).get(
                "cancelledOrderIds", []
            )
        except (KeyError, TypeError, AttributeError):
            return False

    async def update_exchange_config(self):
        """确保账户级别设置（双向持仓模式）已应用。"""
        try:
            # 启用双向持仓模式，使多/空头寸可以共存。
            if hasattr(self.cca, "set_position_mode"):
                res = await self.cca.set_position_mode(True)
                logging.info(f"set_position_mode hedged=True {res}")
            else:
                logging.info("set_position_mode not supported by current KuCoin client; continuing")
        except Exception as e:
            logging.warning(f"set_position_mode hedged=True not applied: {e}")

    async def update_exchange_config_by_symbols(self, symbols):
        coros_to_call = []
        for symbol in symbols:
            try:
                margin_mode = self._get_margin_mode_for_symbol(symbol)
                params = {
                    "marginMode": margin_mode,
                    "symbol": symbol,
                }
                coros_to_call.append(
                    (
                        symbol,
                        "set_margin_mode",
                        asyncio.create_task(self.cca.set_margin_mode(**params)),
                    )
                )
            except Exception as e:
                logging.warning(f"{symbol}: error set_margin_mode {e}")
        for symbol, task_name, task in coros_to_call:
            res = None
            to_print = ""
            try:
                res = await task
                to_print += f"{task_name}={format_exchange_config_response(res)}"
            except Exception as e:
                logging.warning(f"{symbol} error {task_name} {e}")
            if to_print:
                logging.info(f"{symbol}: {to_print}")

        coros_to_call = []
        for symbol in symbols:
            try:
                margin_mode = self._get_margin_mode_for_symbol(symbol)
                params = {
                    "leverage": self._calc_leverage_for_symbol(symbol),
                    "symbol": symbol,
                    "params": {"marginMode": margin_mode},
                }
                coros_to_call.append(
                    (symbol, "set_leverage", asyncio.create_task(self.cca.set_leverage(**params)))
                )
            except Exception as e:
                logging.warning(f"{symbol}: error set_leverage {e}")
        for symbol, task_name, task in coros_to_call:
            res = None
            to_print = ""
            try:
                res = await task
                to_print += f"{task_name}={format_exchange_config_response(res)}"
            except Exception as e:
                logging.warning(f"{symbol} error {task_name} {e}")
            if to_print:
                logging.info(f"{symbol}: {to_print}")
