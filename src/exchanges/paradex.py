"""
ParadexBot：Paradex 专用交易所连接器。

通过 Paradex 专用的入驻逻辑扩展 CCXTBot。
Paradex 要求用户在任何已认证的 API 调用生效之前
先调用一次 /onboarding 端点。

参见：https://docs.paradex.trade/api-reference/general-information/authentication
"""

import json

import ccxt
import websockets
from websockets.exceptions import ConnectionClosed

from exchanges.ccxt_bot import CCXTBot
from passivbot import logging


class ParadexBot(CCXTBot):
    """支持自动入驻的 Paradex 交易所机器人。"""

    # 不应触发重试的认证错误
    WS_AUTH_ERROR_CODES = {40110, 40111, 40112}

    def __init__(self, config: dict):
        super().__init__(config)
        # Paradex 仅支持 USDC 作为报价货币 - 必须在 super() 之后设置
        # 以覆盖 CCXTBot 的默认 USDT
        self.quote = "USDC"
        self.hedge_mode = False  # Paradex 不支持双向持仓模式
        self._ws = None

    def _build_ccxt_config(self) -> dict:
        """将 wallet_address/private_key 转换为 CCXT 的 paradexAccount 格式。"""
        config = {"enableRateLimit": True}

        # 构建 CCXT 为 Paradex L2 身份验证所需的嵌套结构
        if self.user_info.get("wallet_address") and self.user_info.get("private_key"):
            config["options"] = {
                "paradexAccount": {
                    "address": self.user_info["wallet_address"],
                    "privateKey": self.user_info["private_key"],
                }
            }

        return config

    async def init_markets(self, verbose=True):
        """重写以确保在市场初始化前完成开通。

        Paradex 要求账户在任何经过身份验证的 API 调用生效前完成开通。
        CCXT 会抛出：
        - 如果已完成，抛出带有 'ETHEREUM_ADDRESS_ALREADY_ONBOARDED' 的 BadRequest
        - 如果未完成，抛出带有 'NOT_ONBOARDED' 的 BadRequest（但我们先调用开通）

        使用仅限 L2 的身份验证（wallet_address/private_key）时，跳过开通，
        因为账户已通过 Paradex UI 完成开通。
        """
        # 如果使用仅限 L2 的身份验证（wallet_address/private_key），跳过开通
        if self.user_info.get("wallet_address"):
            logging.info("paradex：使用仅限 L2 的身份验证，跳过开通")
        else:
            try:
                await self.cca.onboarding()
                logging.info("paradex：开通成功")
            except ccxt.BadRequest as e:
                if "ALREADY_ONBOARDED" not in str(e):
                    raise
                logging.info("paradex：账户已开通")

        await super().init_markets(verbose)

    def _should_set_margin_mode(self, symbol: str) -> bool:
        """Paradex 仅使用全仓模式 — 没有 API 来设置它。"""
        return False

    def can_watch_orders(self) -> bool:
        """重写：始终返回 True - 我们实现了原生 WebSocket。"""
        return True

    async def _do_watch_orders(self) -> list:
        """钩子：按需连接，然后接收订单更新。"""
        if self._ws is None or self._ws.closed:
            await self._ws_connect()
        return await self._ws_receive_orders()

    def _get_ws_url(self) -> str:
        """返回 WebSocket URL，遵循测试网设置。"""
        api_url = self.cca.urls.get("api", {}).get("public", "")
        if "testnet" in api_url:
            return "wss://ws.api.testnet.paradex.trade/v1"
        return "wss://ws.api.prod.paradex.trade/v1"

    async def _ws_send_and_expect(self, method: str, params: dict, msg_id: int, success_log: str):
        """发送 JSON-RPC 消息并验证响应。"""
        msg = {"jsonrpc": "2.0", "method": method, "params": params, "id": msg_id}
        try:
            await self._ws.send(json.dumps(msg))
            raw = await self._ws.recv()
        except ConnectionClosed as e:
            self._ws = None
            raise ConnectionError(f"paradex WS closed during {method}: {e}")
        except Exception as e:
            raise ConnectionError(f"paradex WS {method} error: {e}")

        response = json.loads(raw)
        if "error" in response:
            code = response["error"].get("code", 0)
            message = response["error"].get("message", "unknown")
            if code in self.WS_AUTH_ERROR_CODES:
                raise ccxt.AuthenticationError(f"paradex WS {method}: [{code}] {message}")
            raise Exception(f"paradex WS {method}: [{code}] {message}")

        logging.info(success_log)
        return response

    async def _ws_connect(self):
        """建立 WebSocket 连接、进行身份验证并订阅。"""
        url = self._get_ws_url()
        try:
            self._ws = await websockets.connect(url)
        except Exception as e:
            raise ConnectionError(f"paradex WS connect failed: {e}")
        logging.info("paradex：WebSocket 已连接")

        jwt = await self.cca.authenticate_rest()
        await self._ws_send_and_expect("auth", {"bearer": jwt}, 1, "paradex：WS 已认证")
        await self._ws_send_and_expect(
            "subscribe", {"channel": "orders.ALL"}, 2, "paradex：已订阅 orders.ALL"
        )

    async def _ws_receive_orders(self) -> list:
        """接收下一条消息并提取订单更新。"""
        try:
            raw = await self._ws.recv()
        except ConnectionClosed as e:
            self._ws = None
            raise ConnectionError(f"paradex WS closed: {e}")

        msg = json.loads(raw)
        if msg.get("method") != "subscription":
            return []

        params = msg.get("params", {})
        if not params.get("channel", "").startswith("orders."):
            return []

        data = params.get("data")
        return [data] if data else []

    def _normalize_order_update(self, order: dict) -> dict:
        """将 Paradex 订单格式转换为 passivbot 格式。"""
        custom_id = order.get("client_id") or ""
        normalized = {
            "id": order.get("id"),
            "symbol": self._paradex_market_to_symbol(order.get("market")),
            "side": (order.get("side") or "").lower(),
            "type": (order.get("type") or "").lower(),
            "price": float(order.get("price") or 0),
            "amount": float(order.get("size") or 0),
            "qty": float(order.get("size") or 0),
            "status": self._normalize_status(order.get("status")),
            "timestamp": order.get("created_at"),
            "clientOrderId": custom_id,
            "custom_id": custom_id,
            "info": order,
        }
        normalized["position_side"] = self._get_position_side_for_order(normalized)
        return normalized

    def _normalize_status(self, status: str) -> str:
        """将 Paradex 状态映射为 CCXT 风格的状态。"""
        mapping = {"NEW": "open", "UNTRIGGERED": "open", "OPEN": "open", "CLOSED": "closed"}
        return mapping.get(status, (status or "").lower())

    def _paradex_market_to_symbol(self, market: str) -> str:
        """将 Paradex 市场转换为 CCXT 品种名。"""
        if market in self.markets_dict:
            return market
        for symbol, info in self.markets_dict.items():
            if info.get("id") == market:
                return symbol
        return market

    def did_cancel_order(self, executed, order=None) -> bool:
        """Paradex 在成功取消时返回 204 No Content。

        CCXT 将其转换为所有值均为 None 的订单结构。
        我们通过检查是否收到预期结构（具有 'id' 键）而非空错误字典 {}
        来检测成功。
        """
        if isinstance(executed, list) and len(executed) == 1:
            return self.did_cancel_order(executed[0], order)
        # 成功：CCXT 返回 {'id': None, 'status': None, ...}（完整结构）
        # 错误：execute_cancellation 返回 {}（空字典）
        return "id" in executed
