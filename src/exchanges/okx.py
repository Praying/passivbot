from exchanges.ccxt_bot import CCXTBot, format_exchange_config_response
from passivbot import logging
import passivbot_rust as pbr

import asyncio
from utils import ts_to_date, utc_ms
from config.access import require_live_value

calc_order_price_diff = pbr.calc_order_price_diff


class OKXBot(CCXTBot):
    def __init__(self, config: dict):
        super().__init__(config)
        self.order_side_map = {
            "buy": {"long": "open_long", "short": "close_short"},
            "sell": {"long": "close_long", "short": "open_short"},
        }
        self.custom_id_max_length = 32
        # 跟踪是否可用双向/对冲模式；默认为 True。
        self.okx_dual_side = True
        self.okx_pm_account = False

    async def _detect_account_config(self):
        """
        检查账户配置以检测组合保证金（PM）和持仓模式。
        如果端点不可用则静默回退。
        """
        try:
            cfg = await self.cca.private_get_account_config()
            data = cfg.get("data", [{}])
            data0 = data[0] if data else {}
            pos_mode = str(data0.get("posMode", "")).lower()  # "long_short_mode" 或 "net_mode"
            acct_lv = str(data0.get("acctLv", "")).lower()  # "pm" 表示组合保证金账户
            if pos_mode == "net_mode":
                self.okx_dual_side = False
                self.hedge_mode = False
            elif pos_mode == "long_short_mode":
                self.okx_dual_side = True
            # 如果未知，保持默认 True，让后续失败将其关闭。
            self.okx_pm_account = acct_lv == "pm"
            if self.okx_pm_account:
                logging.info(
                    "OKX 账户检测为组合保证金（PM）；模式/杠杆变更可能受限。"
                )
            if not self.okx_dual_side:
                logging.info("OKX 账户为净额（单向）模式；不使用 posSide/对冲运行。")
        except Exception as e:
            logging.warning(f"无法检测 OKX 账户配置：{e}")

    # ═══════════════════ 钩子重写 ═══════════════════

    def _get_position_side_for_order(self, order: dict) -> str:
        """OKX 在 info 中提供 posSide。"""
        return order.get("info", {}).get("posSide", "long").lower()

    def _normalize_positions(self, fetched: list) -> list:
        """OKX：在全仓和逐仓模式下保留实盘持仓。"""
        positions = []
        for elm in fetched:
            contracts = float(elm.get("contracts", 0))
            if contracts != 0:
                normalized = {
                    "symbol": elm["symbol"],
                    "position_side": elm.get("side", "long").lower(),
                    "size": contracts,
                    "price": float(elm.get("entryPrice", 0)),
                }
                margin_mode = self._extract_live_margin_mode(elm)
                if margin_mode is not None:
                    normalized["margin_mode"] = margin_mode
                    self._record_live_margin_mode(normalized["symbol"], margin_mode)
                positions.append(normalized)
        return positions

    def _get_pnl_from_trade(self, trade: dict) -> float:
        """OKX 使用 info 中的 fillPnl。"""
        return float(trade.get("info", {}).get("fillPnl", 0))

    def _get_position_side_from_trade(self, trade: dict) -> str:
        """OKX 在 info 中提供 posSide。"""
        return trade.get("info", {}).get("posSide", "long").lower()

    # ═══════════════════ OKX 专用方法 ═══════════════════

    async def fetch_balance(self) -> float:
        """OKX：复杂的多资产模式余额计算。

        OKX 拥有独特的余额结构，需要对多种资产的抵押品求和，
        并将每种资产转换为报价货币。
        """
        fetched_balance = await self.cca.fetch_balance()
        balance = 0.0

        is_multi_asset_mode = True
        if len(fetched_balance["info"]["data"]) == 1:
            if len(fetched_balance["info"]["data"][0]["details"]) == 1:
                if fetched_balance["info"]["data"][0]["details"][0]["ccy"] == self.quote:
                    if not fetched_balance["info"]["data"][0]["details"][0]["collateralEnabled"]:
                        is_multi_asset_mode = False

        if is_multi_asset_mode:
            for elm in fetched_balance["info"]["data"]:
                for elm2 in elm["details"]:
                    if elm2["collateralEnabled"]:
                        balance += float(elm2["cashBal"]) * (
                            (
                                await self.cm.get_current_close(
                                    self.coin_to_symbol(elm2["ccy"]), max_age_ms=10_000
                                )
                            )
                            if elm2["ccy"] != self.quote
                            else 1.0
                        )
        else:
            balance = float(fetched_balance["info"]["data"][0]["details"][0]["cashBal"])
        return balance

    async def fetch_pnls(self, start_time: int = None, end_time: int = None, limit=None):
        if limit is None:
            limit = 100
        if start_time is None and end_time is None:
            return await self.fetch_pnl()
        all_fetched = {}
        while True:
            fetched = await self.fetch_pnl(start_time=start_time, end_time=end_time)
            if fetched == []:
                break
            for elm in fetched:
                all_fetched[elm["id"]] = elm
            if len(fetched) < limit:
                break
            logging.debug(f"fetching income {ts_to_date(fetched[-1]['timestamp'])}")
            end_time = fetched[0]["timestamp"]
        return sorted(all_fetched.values(), key=lambda x: x["timestamp"])

    async def gather_fill_events(self, start_time=None, end_time=None, limit=None):
        """返回 OKX 的标准成交事件。"""
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
        end_time: int = None,
    ):
        """从 OKX 获取交易。如果超过 100 笔成交，则获取最新的。"""
        if end_time is None:
            end_time = utc_ms() + 1000 * 60 * 60 * 24
        if start_time is None:
            start_time = end_time - 1000 * 60 * 60 * 24 * 7
        fetched = await self.cca.fetch_my_trades(
            since=int(start_time), params={"until": int(end_time)}
        )
        for i in range(len(fetched)):
            fetched[i]["pnl"] = float(fetched[i]["info"]["fillPnl"])
            fetched[i]["position_side"] = fetched[i]["info"]["posSide"]
        return sorted(fetched, key=lambda x: x["timestamp"])

    async def execute_cancellation(self, order: dict) -> dict:
        """OKX：取消订单，特殊处理 51400（已取消/已成交）。"""
        try:
            return await self.cca.cancel_order(order["id"], symbol=order["symbol"])
        except Exception as e:
            # 51400 = 订单已取消或已成交 - 非错误
            if '"sCode":"51400"' in str(e):
                logging.info(f"订单已取消/已成交：{e}")
                return {}
            raise

    def _build_order_params(self, order: dict) -> dict:
        margin_mode = self._get_margin_mode_for_symbol(order["symbol"])
        params = {
            "postOnly": require_live_value(self.config, "time_in_force") == "post_only",
            "reduceOnly": order["reduce_only"],
            "hedged": True,
            "tag": self.broker_code,
            "clOrdId": order["custom_id"],
            "marginMode": margin_mode,
        }
        # 仅在确认双向模式时发送 positionSide。
        if self.okx_dual_side:
            params["positionSide"] = order["position_side"]
        return params

    async def update_exchange_config_by_symbols(self, symbols: [str]):
        coros_to_call_margin_mode = {}
        for symbol in symbols:
            margin_mode = self._get_margin_mode_for_symbol(symbol)
            try:
                leverage = self._calc_leverage_for_symbol(symbol)
                coros_to_call_margin_mode[symbol] = asyncio.create_task(
                    self.cca.set_margin_mode(
                        margin_mode,
                        symbol=symbol,
                        params={"lever": leverage},
                    )
                )
            except Exception as e:
                logging.error(f"{symbol}: error setting {margin_mode} mode and leverage {e}")
        for symbol in symbols:
            res = None
            to_print = ""
            try:
                res = await coros_to_call_margin_mode[symbol]
                to_print += f"margin={format_exchange_config_response(res)}"
            except Exception as e:
                err_str = str(e)
                if '"code":"59107"' in err_str:
                    to_print += f"margin=ok (unchanged)"
                elif '"code":"51039"' in err_str:
                    logging.warning(
                        f"{symbol}: unable to adjust margin mode/leverage (possibly PM or open positions)"
                    )
                    continue
                else:
                    logging.error(f"{symbol} error setting cross mode {e}")
            if to_print:
                logging.info(f"{symbol}: {to_print}")

    async def update_exchange_config(self):
        # 检测当前账户模式；在尝试变更前调整预期。
        await self._detect_account_config()
        if not self.okx_dual_side:
            # 单向模式：跳过设置双向持仓模式的尝试；订单将省略 posSide。
            return
        try:
            res = await self.cca.set_position_mode(True)
            logging.debug("[config] set hedge mode response: %s", res)
        except Exception as e:
            err_str = str(e)
            if '"code":"59000"' in err_str:
                logging.info("[config] 双向持仓模式更新已跳过：%s", e)
            elif '"code":"51039"' in err_str or '"code":"51000"' in err_str:
                # 无法切换到双向/对冲模式（通常由于 PM 或未成交订单/持仓）。
                self.okx_dual_side = False
                self.hedge_mode = False
                logging.warning(
                    "[config] OKX 拒绝双向/对冲切换（51039/51000）。以净额模式继续运行，不使用 posSide。"
                )
            else:
                logging.error("[config] 设置双向持仓模式出错：%s", e)

    async def calc_ideal_orders(self):
        # okx 最多 100 个未成交订单。丢弃价格差异最大的订单。
        ideal_orders = await super().calc_ideal_orders()
        ideal_orders_tmp = []
        for s in ideal_orders:
            for x in ideal_orders[s]:
                ideal_orders_tmp.append(
                    (
                        calc_order_price_diff(
                            x["side"],
                            x["price"],
                            await self.cm.get_current_close(s, max_age_ms=10_000),
                        ),
                        {**x, **{"symbol": s}},
                    )
                )
        ideal_orders_tmp = [x[1] for x in sorted(ideal_orders_tmp, key=lambda x: x[0])][:100]
        ideal_orders = {symbol: [] for symbol in self.active_symbols}
        for x in ideal_orders_tmp:
            ideal_orders[x["symbol"]].append(x)
        return ideal_orders

    def format_custom_id_single(self, order_type_id: int) -> str:
        formatted = super().format_custom_id_single(order_type_id)
        return (self.broker_code + formatted)[: self.custom_id_max_length]
