#!/usr/bin/env python3
"""
使用 ccxt 获取并打印指定用户的余额。

用法：
  python src/tools/fetch_balance.py --user USER

此脚本从 api-keys.json（默认从仓库根目录）读取 API 密钥。
预期格式灵活；它会在顶层键或 "users" 映射中查找指定用户。
用户条目应包含交易所 ID（如 "binance"）和凭据（apiKey/key 和 secret）。示例：

{
  "tester": {
    "exchange": "binance",
    "apiKey": "...",
    "secret": "..."
  }
}
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import ccxt


def load_api_keys(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"api-keys file not found: {path}")
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def get_user_info(api_keys: Dict[str, Any], user: str) -> Dict[str, Any]:
    # 常见格式：{ "user": {...} } 或 { "users": { "user": {...} } }
    if user in api_keys and isinstance(api_keys[user], dict):
        return api_keys[user]
    if isinstance(api_keys, dict) and "users" in api_keys and user in api_keys["users"]:
        return api_keys["users"][user]
    raise KeyError(f"user '{user}' not found in api-keys.json")


def build_exchange(user_info: Dict[str, Any]) -> ccxt.Exchange:
    # 为方便起见接受多种可能的键名
    exchange_id = (
        user_info.get("exchange") or user_info.get("exchange_id") or user_info.get("exchangeId")
    )
    if not exchange_id:
        raise KeyError("missing 'exchange' in user info")

    # ccxt 将交易所作为 ccxt 模块上的属性暴露
    exchange_cls = getattr(ccxt, exchange_id, None) or getattr(ccxt, exchange_id.lower(), None)
    if exchange_cls is None:
        raise Exception(f"exchange '{exchange_id}' not found in ccxt")

    # 支持时优先使用永续合约（swap）
    try:
        if not hasattr(exchange_cls, "options") or not isinstance(exchange_cls.options, dict):
            exchange_cls.options = {"defaultType": "swap"}
        else:
            exchange_cls.options["defaultType"] = "swap"
    except Exception:
        # 尽力而为：忽略设置类级别选项的失败
        pass

    api_key = user_info.get("apiKey") or user_info.get("key") or user_info.get("apikey")
    secret = user_info.get("secret") or user_info.get("apiSecret") or user_info.get("apisecret")
    password = user_info.get("password") or user_info.get("pwd") or user_info.get("passphrase")

    params = {"enableRateLimit": True}
    # 如果需要，允许通过 user_info 传递额外的 ccxt 参数
    extra = user_info.get("ccxt", {})
    if isinstance(extra, dict):
        params.update(extra)

    # 为 ccxt 交易所构造函数构建 kwargs，标准化常见键名
    exchange_kwargs = dict(params)  # start with params (e.g. enableRateLimit, etc.)
    if api_key:
        exchange_kwargs["apiKey"] = api_key
    if secret:
        exchange_kwargs["secret"] = secret
    if password:
        exchange_kwargs["password"] = password

    # 包含 user_info 中的其他有用字段（如 wallet_address, private_key），
    # 但避免重复复制控制字段或凭据别名。
    for k, v in user_info.items():
        if k in ("exchange", "exchange_id", "exchangeId", "ccxt"):
            continue
        if k in (
            "key",
            "apiKey",
            "apikey",
            "secret",
            "apiSecret",
            "apisecret",
            "password",
            "pwd",
            "passphrase",
        ):
            continue
        # 不覆盖已设置的标准化键
        if k not in exchange_kwargs:
            exchange_kwargs[k] = v

    exchange = exchange_cls(exchange_kwargs)
    return exchange


def pretty_print_balance(bal: Dict[str, Any]) -> None:
    # 打印稳定且可读的 JSON
    print(json.dumps(bal, indent=2, sort_keys=True, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="获取并打印用户的交易所余额")
    parser.add_argument("--user", required=True, help="api-keys.json 中的用户键")
    parser.add_argument(
        "--api-keys",
        default="api-keys.json",
        help="api-keys.json 的路径（默认：仓库根目录的 api-keys.json）",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        keys_path = Path(args.api_keys)
        api_keys = load_api_keys(keys_path)
        user_info = get_user_info(api_keys, args.user)
        exchange = build_exchange(user_info)
        logging.info("Using exchange: %s", getattr(exchange, "id", type(exchange).__name__))
        logging.info("Fetching balance...")
        balance = exchange.fetch_balance()
        pretty_print_balance(balance)
    except Exception:
        logging.exception("Failed to fetch balance")
        sys.exit(1)


if __name__ == "__main__":
    main()
