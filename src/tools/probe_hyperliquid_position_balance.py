#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
from tools.hyperliquid_probe_common import (
    add_live_mutation_confirmation_arg,
    add_probe_identity_args,
    create_hyperliquid_probe_session,
    extract_balance_summary,
    extract_position_summary,
    hyperliquid_probe_vault_params,
    load_hyperliquid_wallet,
    mask_secret,
    require_live_mutation_confirmation,
    round_to_step,
)


async def _main() -> int:
    """在 Hyperliquid 上开微小仓位，快照余额/仓位，可选挂单后平仓。"""
    parser = argparse.ArgumentParser(
        description=(
            "修改性 Hyperliquid 诊断。开一个微小仓位，可选地添加挂单的"
            "开仓/平仓订单，快照余额/仓位，并可在退出前平仓。"
        )
    )
    add_probe_identity_args(parser)
    add_live_mutation_confirmation_arg(parser)
    parser.add_argument("--symbol", default="XYZ-SP500/USDC:USDC", help="要探测的交易对")
    parser.add_argument("--side", default="buy", choices=("buy", "sell"), help="开仓方向")
    parser.add_argument(
        "--set-margin-mode",
        choices=("cross", "isolated"),
        default="cross",
        help="探测前设置的保证金模式",
    )
    parser.add_argument("--leverage", type=int, default=5, help="请求的杠杆倍数")
    parser.add_argument(
        "--notional-usdc",
        type=float,
        default=11.5,
        help="以 USDC 计的目标开仓名义价值",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=2.0,
        help="创建/平仓后重新获取状态前的等待时间",
    )
    parser.add_argument(
        "--place-reduce-only-close-order",
        action="store_true",
        help="开仓后，在平仓前挂一个 reduce-only 平仓单",
    )
    parser.add_argument(
        "--place-resting-entry-order",
        action="store_true",
        help="开仓后，额外挂一个非 reduce-only 的入场单",
    )
    parser.add_argument(
        "--leave-open-after-entry",
        action="store_true",
        help="开仓后退出，不追加订单也不平仓",
    )
    parser.add_argument(
        "--flatten-only",
        action="store_true",
        help="如果该交易对已有仓位，直接平仓后退出，不开新仓",
    )
    parser.add_argument(
        "--close-order-distance-pct",
        type=float,
        default=0.25,
        help="可选的 reduce-only 平仓单的距离",
    )
    parser.add_argument(
        "--dump-raw-info",
        action="store_true",
        help="在输出中包含原始余额载荷",
    )
    args = parser.parse_args()
    require_live_mutation_confirmation(
        parser, args, action_description="probe_hyperliquid_position_balance"
    )

    user_info, wallet_address, private_key = load_hyperliquid_wallet(
        args.user, api_keys_path=args.api_keys
    )
    session = create_hyperliquid_probe_session(wallet_address, private_key)
    vault_params = hyperliquid_probe_vault_params(user_info)

    reduce_only_order_id = None
    extra_entry_order_id = None
    try:
        markets = await session.load_markets()
        market = markets[args.symbol]
        qty_step = float(market.get("precision", {}).get("amount") or 0.0)
        min_cost = float((market.get("limits", {}).get("cost", {}) or {}).get("min") or 10.0)
        target_notional = max(float(args.notional_usdc), min_cost * 1.05)

        set_margin_mode_response = await session.set_margin_mode(
            args.set_margin_mode,
            symbol=args.symbol,
            params={**vault_params, "leverage": int(args.leverage)},
        )

        before_balance = await session.fetch_balance()
        before_positions = await session.fetch_positions(symbols=[args.symbol])
        ticker = await session.fetch_ticker(args.symbol)
        last_price = float(ticker.get("last") or ticker.get("bid") or ticker.get("ask"))
        if not math.isfinite(last_price) or last_price <= 0.0:
            raise ValueError(f"invalid market price for {args.symbol}: {last_price}")
        amount = round_to_step(target_notional / last_price, qty_step, mode="up")

        if args.flatten_only:
            existing_positions = [p for p in before_positions if abs(float(p.get("contracts") or 0.0)) > 0.0]
            close_trade = None
            if existing_positions:
                position = existing_positions[0]
                close_side = "sell" if str(position.get("side")).lower() == "long" else "buy"
                close_amount = float(position.get("contracts") or 0.0)
                close_trade = await session.create_order(
                    args.symbol,
                    "market",
                    close_side,
                    close_amount,
                    last_price,
                    params={**vault_params, "reduceOnly": True},
                )
                await asyncio.sleep(float(args.settle_seconds))
            after_flat_balance = await session.fetch_balance()
            after_flat_positions = await session.fetch_positions(symbols=[args.symbol])
            output = {
                "user": args.user,
                "wallet_address": mask_secret(wallet_address),
                "symbol": args.symbol,
                "flatten_only": True,
                "before": extract_balance_summary(before_balance),
                "after_flat": extract_balance_summary(after_flat_balance),
                "positions_before": extract_position_summary(before_positions),
                "positions_after_flat": extract_position_summary(after_flat_positions),
                "close_trade": close_trade,
            }
            if args.dump_raw_info:
                output["raw_info"] = {
                    "before": before_balance.get("info"),
                    "after_flat": after_flat_balance.get("info"),
                }
            print(json.dumps(output, indent=2, sort_keys=True, default=str))
            return 0

        entry = await session.create_order(
            args.symbol,
            "market",
            args.side,
            amount,
            last_price,
            params=vault_params,
        )
        await asyncio.sleep(float(args.settle_seconds))
        after_entry_balance = await session.fetch_balance()
        after_entry_positions = await session.fetch_positions(symbols=[args.symbol])

        extra_entry_order = None
        if args.place_resting_entry_order:
            if args.side == "buy":
                entry_price = last_price * (1.0 - abs(float(args.close_order_distance_pct)))
            else:
                entry_price = last_price * (1.0 + abs(float(args.close_order_distance_pct)))
            extra_entry_order = await session.create_order(
                args.symbol,
                "limit",
                args.side,
                amount,
                entry_price,
                {**vault_params, "timeInForce": "Alo"},
            )
            extra_entry_order_id = extra_entry_order.get("id")
            await asyncio.sleep(float(args.settle_seconds))
        after_extra_entry_order_balance = await session.fetch_balance()
        after_extra_entry_order_positions = await session.fetch_positions(symbols=[args.symbol])
        open_orders_after_extra_entry_order = await session.fetch_open_orders(symbol=args.symbol)

        if args.leave_open_after_entry:
            output = {
                "user": args.user,
                "wallet_address": mask_secret(wallet_address),
                "symbol": args.symbol,
                "entry_side": args.side,
                "leave_open_after_entry": True,
                "last_price": last_price,
                "amount": amount,
                "target_notional": target_notional,
                "set_margin_mode": {
                    "requested": args.set_margin_mode,
                    "leverage": int(args.leverage),
                    "response": set_margin_mode_response,
                },
                "before": extract_balance_summary(before_balance),
                "after_entry": extract_balance_summary(after_entry_balance),
                "after_resting_entry_order": extract_balance_summary(after_extra_entry_order_balance),
                "positions_before": extract_position_summary(before_positions),
                "positions_after_entry": extract_position_summary(after_entry_positions),
                "positions_after_resting_entry_order": extract_position_summary(
                    after_extra_entry_order_positions
                ),
                "entry_order": entry,
                "resting_entry_order": extra_entry_order,
                "open_orders_after_resting_entry_order": open_orders_after_extra_entry_order,
            }
            if args.dump_raw_info:
                output["raw_info"] = {
                    "before": before_balance.get("info"),
                    "after_entry": after_entry_balance.get("info"),
                    "after_resting_entry_order": after_extra_entry_order_balance.get("info"),
                }
            print(json.dumps(output, indent=2, sort_keys=True, default=str))
            return 0

        close_order = None
        if args.place_reduce_only_close_order:
            close_side = "sell" if args.side == "buy" else "buy"
            if close_side == "sell":
                close_price = last_price * (1.0 + abs(float(args.close_order_distance_pct)))
            else:
                close_price = last_price * (1.0 - abs(float(args.close_order_distance_pct)))
            close_order = await session.create_order(
                args.symbol,
                "limit",
                close_side,
                amount,
                close_price,
                {**vault_params, "timeInForce": "Alo", "reduceOnly": True},
            )
            reduce_only_order_id = close_order.get("id")
            await asyncio.sleep(float(args.settle_seconds))
        after_close_order_balance = await session.fetch_balance()
        after_close_order_positions = await session.fetch_positions(symbols=[args.symbol])
        open_orders_after_close_order = await session.fetch_open_orders(symbol=args.symbol)

        if reduce_only_order_id is not None:
            await session.cancel_order(
                reduce_only_order_id, symbol=args.symbol, params=vault_params
            )
            await asyncio.sleep(float(args.settle_seconds))
        if extra_entry_order_id is not None:
            await session.cancel_order(
                extra_entry_order_id, symbol=args.symbol, params=vault_params
            )
            await asyncio.sleep(float(args.settle_seconds))

        close_side = "sell" if args.side == "buy" else "buy"
        close_trade = await session.create_order(
            args.symbol,
            "market",
            close_side,
            amount,
            last_price,
            params={**vault_params, "reduceOnly": True},
        )
        await asyncio.sleep(float(args.settle_seconds))
        after_flat_balance = await session.fetch_balance()
        after_flat_positions = await session.fetch_positions(symbols=[args.symbol])

        output = {
            "user": args.user,
            "wallet_address": mask_secret(wallet_address),
            "symbol": args.symbol,
            "entry_side": args.side,
            "last_price": last_price,
            "amount": amount,
            "target_notional": target_notional,
            "set_margin_mode": {
                "requested": args.set_margin_mode,
                "leverage": int(args.leverage),
                "response": set_margin_mode_response,
            },
            "before": extract_balance_summary(before_balance),
            "after_entry": extract_balance_summary(after_entry_balance),
            "after_resting_entry_order": extract_balance_summary(after_extra_entry_order_balance),
            "after_reduce_only_close_order": extract_balance_summary(after_close_order_balance),
            "after_flat": extract_balance_summary(after_flat_balance),
            "positions_before": extract_position_summary(before_positions),
            "positions_after_entry": extract_position_summary(after_entry_positions),
            "positions_after_resting_entry_order": extract_position_summary(
                after_extra_entry_order_positions
            ),
            "positions_after_reduce_only_close_order": extract_position_summary(
                after_close_order_positions
            ),
            "positions_after_flat": extract_position_summary(after_flat_positions),
            "entry_order": entry,
            "resting_entry_order": extra_entry_order,
            "reduce_only_close_order": close_order,
            "open_orders_after_resting_entry_order": open_orders_after_extra_entry_order,
            "open_orders_after_reduce_only_close_order": open_orders_after_close_order,
            "close_trade": close_trade,
        }
        if args.dump_raw_info:
            output["raw_info"] = {
                "before": before_balance.get("info"),
                "after_entry": after_entry_balance.get("info"),
                "after_resting_entry_order": after_extra_entry_order_balance.get("info"),
                "after_reduce_only_close_order": after_close_order_balance.get("info"),
                "after_flat": after_flat_balance.get("info"),
            }
        print(json.dumps(output, indent=2, sort_keys=True, default=str))
        return 0
    finally:
        try:
            if reduce_only_order_id is not None:
                try:
                    await session.cancel_order(
                        reduce_only_order_id, symbol=args.symbol, params=vault_params
                    )
                except Exception:
                    pass
            if extra_entry_order_id is not None:
                try:
                    await session.cancel_order(
                        extra_entry_order_id, symbol=args.symbol, params=vault_params
                    )
                except Exception:
                    pass
            await session.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
