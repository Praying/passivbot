#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from tools.hyperliquid_probe_common import (
    add_probe_identity_args,
    create_hyperliquid_probe_session,
    extract_balance_summary,
    load_hyperliquid_wallet,
    mask_secret,
)


async def _main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "只读 Hyperliquid 余额冒烟测试。"
            "获取一个钱包余额并打印标准化摘要。"
        )
    )
    add_probe_identity_args(parser)
    parser.add_argument(
        "--dump-raw",
        action="store_true",
        help="在摘要后打印完整的原始 fetch_balance 负载",
    )
    args = parser.parse_args()

    user_info, wallet_address, private_key = load_hyperliquid_wallet(
        args.user, api_keys_path=args.api_keys
    )
    session = create_hyperliquid_probe_session(wallet_address, private_key)

    try:
        balance = await session.fetch_balance()
        summary = {
            "user": args.user,
            "exchange": "hyperliquid",
            "wallet_address": mask_secret(wallet_address),
            "is_vault": bool(user_info.get("is_vault")),
            "balance_summary": extract_balance_summary(balance),
        }
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
        if args.dump_raw:
            print(json.dumps(balance, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        await session.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
