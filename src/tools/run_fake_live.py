from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from candlestick_manager import CANDLE_DTYPE
from config import load_prepared_config
from exchanges.fake import FakeCCXTClient, load_fake_scenario
from fill_events_manager import FillEvent, FillEventCache
from logging_setup import configure_logging
import passivbot as passivbot_mod
from passivbot import setup_bot, shutdown_bot
from procedures import ensure_parent_directory


def _build_output_dir(root: str | None, scenario: dict) -> Path:
    """构建输出目录路径，格式为 {root}/{timestamp}_{scenario_name}。"""
    base = Path(root) if root else Path("artifacts") / "fake_live"
    stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    scenario_name = str(
        scenario.get("name")
        or Path(str(scenario.get("_scenario_path", "scenario"))).stem
    )
    return base / f"{stamp}_{scenario_name}"


def _dump_json(path: Path, data: Any) -> None:
    """将数据以 JSON 格式写入文件，自动创建父目录。"""
    ensure_parent_directory(path)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")


def _summarize_remote_calls(call_log: List[dict]) -> dict:
    """汇总远程调用日志，按方法和步骤统计调用次数。"""
    by_method: Dict[str, int] = {}
    by_step: Dict[str, Dict[str, int]] = {}
    ohlcv_calls: List[dict] = []
    for entry in call_log:
        method = str(entry.get("method") or "unknown")
        step_key = str(entry.get("step_index") if entry.get("step_index") is not None else "unknown")
        by_method[method] = by_method.get(method, 0) + 1
        step_bucket = by_step.setdefault(step_key, {})
        step_bucket[method] = step_bucket.get(method, 0) + 1
        if method == "fetch_ohlcv":
            ohlcv_calls.append(
                {
                    "step_index": entry.get("step_index"),
                    "symbol": entry.get("symbol"),
                    "timeframe": entry.get("timeframe"),
                    "since": entry.get("since"),
                    "until": entry.get("until"),
                    "limit": entry.get("limit"),
                    "rows": entry.get("rows"),
                }
            )
    return {
        "total_calls": len(call_log),
        "by_method": dict(sorted(by_method.items())),
        "by_step": {key: dict(sorted(value.items())) for key, value in sorted(by_step.items())},
        "ohlcv_calls": ohlcv_calls,
    }


def _install_candle_remote_fetch_trace(bot) -> tuple[List[dict], callable]:
    """安装 K 线远程获取回调追踪，返回事件列表和恢复函数。"""
    if not hasattr(bot, "cm"):
        return [], lambda: None
    existing_cb = getattr(bot.cm, "_remote_fetch_callback", None)
    events: List[dict] = []

    def traced(payload: Dict[str, Any]) -> None:
        item = dict(payload)
        item["event_index"] = len(events)
        events.append(item)
        if existing_cb is not None:
            existing_cb(payload)

    bot.cm._remote_fetch_callback = traced
    return events, lambda: setattr(bot.cm, "_remote_fetch_callback", existing_cb)


def _attach_file_logging(path: Path) -> logging.Handler:
    """附加文件日志处理器到根日志器，返回处理器以便后续清理。"""
    ensure_parent_directory(path)
    root = logging.getLogger()
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    if root.handlers and root.handlers[0].formatter is not None:
        handler.setFormatter(root.handlers[0].formatter)
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(handler)
    return handler


def _extract_hsl_trace(bot) -> Dict[str, dict]:
    """提取硬止损（HSL）状态追踪信息。"""
    trace: Dict[str, dict] = {}
    for pside in ("long", "short"):
        if not hasattr(bot, "_hsl_state"):
            break
        state = bot._hsl_state(pside)
        trace[pside] = {
            "halted": bool(state.get("halted", False)),
            "no_restart_latched": bool(state.get("no_restart_latched", False)),
            "cooldown_until_ms": state.get("cooldown_until_ms"),
            "pending_red_since_ms": state.get("pending_red_since_ms"),
            "red_flat_confirmations": state.get("red_flat_confirmations"),
            "cooldown_intervention_active": bool(state.get("cooldown_intervention_active", False)),
            "cooldown_repanic_reset_pending": bool(
                state.get("cooldown_repanic_reset_pending", False)
            ),
            "last_metrics": state.get("last_metrics"),
            "last_stop_event": state.get("last_stop_event"),
        }
    return trace


def _coerce_numeric_assertion(spec: Any) -> Dict[str, float]:
    """将断言规格转换为标准化的数值断言字典。"""
    if isinstance(spec, (int, float)):
        return {"eq": float(spec)}
    if not isinstance(spec, dict):
        raise TypeError(f"Unsupported numeric assertion spec: {spec!r}")
    result: Dict[str, float] = {}
    for key in ("eq", "min", "max", "approx", "tolerance"):
        if key in spec:
            result[key] = float(spec[key])
    return result


def _assert_numeric(name: str, actual: float, spec: Any) -> None:
    """对数值执行断言检查（eq/min/max/approx）。"""
    parsed = _coerce_numeric_assertion(spec)
    if "eq" in parsed and actual != parsed["eq"]:
        raise AssertionError(f"{name}: expected {parsed['eq']} got {actual}")
    if "min" in parsed and actual < parsed["min"]:
        raise AssertionError(f"{name}: expected >= {parsed['min']} got {actual}")
    if "max" in parsed and actual > parsed["max"]:
        raise AssertionError(f"{name}: expected <= {parsed['max']} got {actual}")
    if "approx" in parsed:
        tolerance = parsed.get("tolerance", 1e-9)
        if abs(actual - parsed["approx"]) > tolerance:
            raise AssertionError(
                f"{name}: expected {parsed['approx']} +/- {tolerance} got {actual}"
            )


def _assert_value(name: str, actual: Any, expected: Any) -> None:
    """对任意值执行断言检查，支持数值断言和包含断言。"""
    if isinstance(expected, dict) and any(
        key in expected for key in ("eq", "min", "max", "approx", "tolerance")
    ):
        _assert_numeric(name, float(actual), expected)
        return
    if isinstance(expected, dict) and "contains" in expected:
        needle = str(expected["contains"])
        if needle not in str(actual):
            raise AssertionError(f"{name}: expected to contain {needle!r}, got {actual!r}")
        return
    if actual != expected:
        raise AssertionError(f"{name}: expected {expected!r} got {actual!r}")


def _get_path_value(root: Any, path: str) -> Any:
    """根据点分隔路径从嵌套结构中取值。"""
    current = root
    for segment in [part for part in str(path).split(".") if part]:
        if isinstance(current, list):
            current = current[int(segment)]
        elif isinstance(current, dict):
            current = current[segment]
        else:
            raise KeyError(f"Cannot descend into {segment!r} on non-container value {current!r}")
    return current


def _apply_path_assertions(group: str, root: Any, specs: Dict[str, Any]) -> None:
    """对嵌套结构的多个路径执行断言检查。"""
    for path, expected in specs.items():
        actual = _get_path_value(root, path)
        _assert_value(f"{group}[{path}]", actual, expected)


def _positions_map(fake_client: FakeCCXTClient) -> Dict[str, float]:
    """构建仓位映射：symbol|position_side -> size。"""
    result: Dict[str, float] = {}
    for row in fake_client.export_positions():
        result[f"{row['symbol']}|{row['position_side']}"] = float(row["size"])
    return result


def _apply_assertions(
    bot,
    fake_client: FakeCCXTClient,
    scenario: dict,
    *,
    step_summaries: List[dict] | None = None,
    log_text: str = "",
) -> None:
    """对场景定义的断言进行校验，包括仓位、余额、价格和日志等。"""
    assertions = scenario.get("assertions") or {}
    if not assertions:
        return
    state = fake_client.export_state()
    hsl_trace = _extract_hsl_trace(bot)

    if "fill_count" in assertions:
        _assert_numeric("fill_count", float(len(fake_client.fills)), assertions["fill_count"])
    if "final_balance" in assertions:
        _assert_numeric(
            "final_balance",
            float(fake_client.balance_total),
            assertions["final_balance"],
        )
    if "last_prices" in assertions:
        current_prices = fake_client.get_current_step()["prices"]
        for symbol, expected in assertions["last_prices"].items():
            if symbol not in current_prices:
                raise AssertionError(f"last_prices: missing symbol {symbol}")
            _assert_numeric(f"last_price[{symbol}]", float(current_prices[symbol]), expected)
    if "final_positions" in assertions:
        actual_positions = _positions_map(fake_client)
        for key, expected in assertions["final_positions"].items():
            actual = float(actual_positions.get(key, 0.0))
            _assert_numeric(f"final_position[{key}]", actual, expected)
    if "halted_psides" in assertions:
        for pside, expected in assertions["halted_psides"].items():
            actual = bool(bot._hsl_state(pside)["halted"])
            if actual != bool(expected):
                raise AssertionError(f"halted_psides[{pside}]: expected {expected} got {actual}")
    if "state_paths" in assertions:
        _apply_path_assertions("state_paths", state, assertions["state_paths"])
    if "hsl_paths" in assertions:
        _apply_path_assertions("hsl_paths", hsl_trace, assertions["hsl_paths"])
    if "summary_paths" in assertions:
        summary_root = {
            "step_count": len(step_summaries or []),
            "last": (step_summaries or [None])[-1],
            "steps": step_summaries or [],
        }
        _apply_path_assertions("summary_paths", summary_root, assertions["summary_paths"])
    if "log_contains" in assertions:
        for fragment in assertions["log_contains"]:
            if str(fragment) not in log_text:
                raise AssertionError(f"log_contains missing fragment: {fragment!r}")


def _install_fake_user_override(config: dict, scenario_path: str, user: str | None) -> tuple[str, callable]:
    """安装 fake 用户信息覆盖，使 load_user_info 返回 fake 场景信息。返回用户名和恢复函数。"""
    config.setdefault("live", {})
    fake_user = user or str(config["live"].get("user") or "fake_runner")
    config["live"]["user"] = fake_user
    fake_user_info = {
        "exchange": "fake",
        "quote": str(config["live"].get("quote") or "USDT"),
        "fake_scenario_path": scenario_path,
    }
    original = passivbot_mod.load_user_info

    def patched(requested_user: str):
        if requested_user == fake_user:
            return dict(fake_user_info)
        return original(requested_user)

    passivbot_mod.load_user_info = patched
    return fake_user, lambda: setattr(passivbot_mod, "load_user_info", original)


def _prime_fake_fill_cache(bot, fake_client: FakeCCXTClient, cache_root: Path | None = None) -> Path:
    """用 fake 客户端的成交事件预填充 FillEventCache。"""
    root = cache_root or Path("caches") / "fill_events"
    cache_path = root / str(bot.exchange) / str(bot.user)
    cache_path.mkdir(parents=True, exist_ok=True)
    for path in cache_path.glob("*.json"):
        path.unlink()
    metadata_path = cache_path / "metadata.json"
    if metadata_path.exists():
        metadata_path.unlink()
    cache = FillEventCache(cache_path)
    events = [FillEvent.from_dict(event) for event in fake_client.get_fill_events(None, None)]
    cache.save(events)
    return cache_path


def _prime_fake_candles(bot, fake_client: FakeCCXTClient) -> None:
    """用 fake 客户端的 K 线数据预填充 CandleManager 缓存。"""
    if not hasattr(bot, "cm"):
        return
    for symbol in fake_client.symbols:
        rows = fake_client._candles_by_symbol.get(symbol, [])[: fake_client.current_index + 1]
        arr = np.zeros(len(rows), dtype=CANDLE_DTYPE)
        for idx, row in enumerate(rows):
            arr[idx]["ts"] = int(row[0])
            arr[idx]["o"] = float(row[1])
            arr[idx]["h"] = float(row[2])
            arr[idx]["l"] = float(row[3])
            arr[idx]["c"] = float(row[4])
            arr[idx]["bv"] = float(row[5])
        bot.cm._cache[symbol] = arr
        bot.cm._ema_cache.pop(symbol, None)
        bot.cm._current_close_cache.pop(symbol, None)
        bot.cm._tf_range_cache.pop(symbol, None)


def _install_runtime_overrides(bot, scenario: dict) -> None:
    """安装运行时覆盖，如用 fake 客户端时间替换交易所时间。"""
    if hasattr(bot, "cca") and isinstance(bot.cca, FakeCCXTClient):
        bot.get_exchange_time = lambda: int(bot.cca.now_ms)


def _fake_active_red_psides(bot) -> List[str]:
    """返回当前处于 red 锁定但尚未 halt 的侧列表。"""
    return [
        pside
        for pside in bot._hsl_psides()
        if bot._equity_hard_stop_enabled(pside)
        and bot._equity_hard_stop_runtime_red_latched(pside)
        and not bot._hsl_state(pside)["halted"]
    ]


async def _run_fake_red_supervisor_step(bot) -> dict:
    """执行 fake 环境下的 red supervisor 步骤，检查平仓确认并完成止损。"""
    active_red_psides = _fake_active_red_psides(bot)
    if not active_red_psides:
        return {"red_supervisor": False}

    for pside in list(active_red_psides):
        state = bot._hsl_state(pside)
        n_positions = bot._equity_hard_stop_count_open_positions(pside)
        entry_orders, nonpanic_close_orders = bot._equity_hard_stop_count_blocking_open_orders(pside)
        if n_positions == 0 and entry_orders == 0 and nonpanic_close_orders == 0:
            if state["red_flat_confirmations"] == 0:
                state["pending_stop_event"] = await bot._equity_hard_stop_compute_stop_event(
                    pside, int(bot.get_exchange_time())
                )
            state["red_flat_confirmations"] += 1
        else:
            state["red_flat_confirmations"] = 0
            state["pending_stop_event"] = None
        bot._equity_hard_stop_log_red_progress(
            pside,
            n_positions,
            entry_orders,
            nonpanic_close_orders,
            state["red_flat_confirmations"],
        )
        if state["red_flat_confirmations"] >= 2:
            await bot._equity_hard_stop_finalize_red_stop(pside, state["pending_stop_event"])

    active_red_psides = _fake_active_red_psides(bot)
    if not active_red_psides:
        return {"red_supervisor": True, "finalized": True}

    for pside in active_red_psides:
        bot._equity_hard_stop_set_red_runtime_forced_modes(pside)
    bot._equity_hard_stop_refresh_halted_runtime_forced_modes()
    await bot.execute_to_exchange()
    return {"red_supervisor": True, "finalized": False}


async def _run_fake_bot(
    bot,
    fake_client: FakeCCXTClient,
    max_steps: int | None,
    *,
    snapshot_dir: Path | None = None,
    run_initial_cycle: bool = True,
) -> List[dict]:
    """运行 fake 机器人，逐步推进时间并收集每步摘要。"""
    summaries: List[dict] = []
    steps_run = 0

    if run_initial_cycle:
        # 运行初始启动周期
        _prime_fake_candles(bot, fake_client)
        result = await _run_fake_cycle(bot)
        summaries.append(
            {
                "step_index": int(fake_client.current_index),
                "timestamp": int(fake_client.now_ms),
                "result": str(result),
                "fills": len(fake_client.fills),
                "open_orders": len(fake_client.open_orders),
                "positions": fake_client.export_positions(),
            }
        )
        if snapshot_dir is not None:
            _dump_json(
                snapshot_dir / f"step_{int(fake_client.current_index):04d}.json",
                {
                    "summary": summaries[-1],
                    "state": fake_client.export_state(),
                    "hsl_trace": _extract_hsl_trace(bot),
                },
            )
        steps_run += 1

    while fake_client.has_next_step():
        if max_steps is not None and steps_run >= max_steps:
            break
        # 推进时间并执行下一个周期
        fake_client.advance_time()
        _prime_fake_candles(bot, fake_client)
        result = await _run_fake_cycle(bot)
        summaries.append(
            {
                "step_index": int(fake_client.current_index),
                "timestamp": int(fake_client.now_ms),
                "result": str(result),
                "fills": len(fake_client.fills),
                "open_orders": len(fake_client.open_orders),
                "positions": fake_client.export_positions(),
            }
        )
        if snapshot_dir is not None:
            _dump_json(
                snapshot_dir / f"step_{int(fake_client.current_index):04d}.json",
                {
                    "summary": summaries[-1],
                    "state": fake_client.export_state(),
                    "hsl_trace": _extract_hsl_trace(bot),
                },
            )
        steps_run += 1

    return summaries


async def _run_fake_cycle(bot):
    """执行一个 fake 交易周期：更新状态、检查硬止损、执行订单。"""
    if not await bot.update_pos_oos_pnls_ohlcvs():
        return {"updated": False}
    if bot._equity_hard_stop_enabled():
        if any(
            bot._equity_hard_stop_runtime_red_latched(pside) and not bot._hsl_state(pside)["halted"]
            for pside in bot._hsl_psides()
            if bot._equity_hard_stop_enabled(pside)
        ):
            if getattr(bot, "exchange", "").lower() == "fake":
                return await _run_fake_red_supervisor_step(bot)
            await bot._equity_hard_stop_run_red_supervisor()
            return {"red_supervisor": True}
        await bot._equity_hard_stop_check()
        if any(
            bot._equity_hard_stop_runtime_red_latched(pside) and not bot._hsl_state(pside)["halted"]
            for pside in bot._hsl_psides()
            if bot._equity_hard_stop_enabled(pside)
        ):
            if getattr(bot, "exchange", "").lower() == "fake":
                return await _run_fake_red_supervisor_step(bot)
            await bot._equity_hard_stop_run_red_supervisor()
            return {"red_supervisor": True}
    return await bot.execute_to_exchange()


async def _async_main(args: argparse.Namespace) -> int:
    """fake live 主流程：加载配置、初始化机器人、运行场景并收集输出。"""
    configure_logging(debug=args.log_level)
    config = load_prepared_config(
        args.config,
        verbose=False,
        target="live",
        runtime="live",
    )
    config.setdefault("live", {})
    config["live"]["fake_scenario_path"] = args.scenario

    scenario = load_fake_scenario(args.scenario)
    output_dir = _build_output_dir(args.output_dir, scenario)
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "fake_live.log"
    log_handler = _attach_file_logging(log_path)
    _, restore_user_override = _install_fake_user_override(config, args.scenario, args.user)
    bot = None
    restore_candle_trace = lambda: None

    try:
        bot = setup_bot(config)
        if bot.exchange != "fake":
            raise ValueError(
                f"Config user resolved to exchange '{bot.exchange}', expected 'fake' for fake harness"
            )
        bot.debug_mode = True
        if not isinstance(bot.cca, FakeCCXTClient):
            raise TypeError("Fake harness expected bot.cca to be FakeCCXTClient")
        # 安装追踪和预填充
        candle_remote_fetches, restore_candle_trace = _install_candle_remote_fetch_trace(bot)
        _prime_fake_fill_cache(bot, bot.cca)
        _prime_fake_candles(bot, bot.cca)
        _install_runtime_overrides(bot, scenario)
        await bot.start_bot()
        bot.debug_mode = False
        snapshot_dir = (output_dir / "snapshots") if args.snapshot_each_step else None
        if snapshot_dir is not None:
            snapshot_dir.mkdir(parents=True, exist_ok=True)
        step_summaries = await _run_fake_bot(
            bot,
            bot.cca,
            args.max_steps,
            snapshot_dir=snapshot_dir,
            run_initial_cycle=bool(scenario.get("run_initial_cycle", True)),
        )
        log_text = log_path.read_text(encoding="utf-8") if log_path.exists() else ""
        # 执行场景断言校验
        _apply_assertions(
            bot,
            bot.cca,
            scenario,
            step_summaries=step_summaries,
            log_text=log_text,
        )

        # 输出运行结果到 JSON 文件
        _dump_json(output_dir / "step_summaries.json", step_summaries)
        _dump_json(output_dir / "fake_exchange_state.json", bot.cca.export_state())
        _dump_json(output_dir / "fills.json", bot.cca.fills)
        _dump_json(output_dir / "positions.json", bot.cca.export_positions())
        _dump_json(output_dir / "hsl_trace.json", _extract_hsl_trace(bot))
        remote_calls = bot.cca.export_request_log()
        _dump_json(output_dir / "remote_calls.json", remote_calls)
        _dump_json(output_dir / "remote_call_summary.json", _summarize_remote_calls(remote_calls))
        _dump_json(output_dir / "candle_remote_fetches.json", candle_remote_fetches)
        print(str(output_dir))
        return 0
    finally:
        try:
            if bot is not None:
                await shutdown_bot(bot)
        finally:
            try:
                restore_candle_trace()
            except Exception:
                pass
            restore_user_override()
            logging.getLogger().removeHandler(log_handler)
            log_handler.close()


def main() -> int:
    """run-fake-live 工具入口。"""
    parser = argparse.ArgumentParser(description="使用 fake 交易所模拟运行 passivbot")
    parser.add_argument("config", help="Passivbot 配置路径")
    parser.add_argument("scenario", help="Fake 场景路径（HJSON 或 JSON）")
    parser.add_argument("--user", default=None, help="覆盖配置中的 live.user")
    parser.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="最大执行周期数（含初始启动周期）",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="输出目录根路径（默认: artifacts/fake_live）",
    )
    parser.add_argument(
        "--log-level",
        type=int,
        default=1,
        help="日志级别 0-3（warning/info/debug/trace）",
    )
    parser.add_argument(
        "--snapshot-each-step",
        action="store_true",
        help="每个执行周期后写入 JSON 快照",
    )
    args = parser.parse_args()
    return asyncio.run(_async_main(args))


if __name__ == "__main__":
    raise SystemExit(main())
