from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) in sys.path:
    sys.path.remove(str(SCRIPT_DIR))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from logging_setup import configure_logging
from monitor_tui import MonitorTuiClient


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Passivbot 监控中继的最小终端仪表板。"
    )
    parser.add_argument(
        "--relay-url",
        type=str,
        default="http://127.0.0.1:8765",
        help="监控中继的基础 URL。",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default=None,
        help="从多机器人中继选择一个机器人时的交易所名称。",
    )
    parser.add_argument(
        "--user",
        type=str,
        default=None,
        help="从多机器人中继选择一个机器人时的用户/账户名。",
    )
    parser.add_argument(
        "--focus-symbol",
        type=str,
        default=None,
        help="可选的在 TUI 面板中优先显示的交易对。",
    )
    parser.add_argument(
        "--snapshot-refresh-seconds",
        type=float,
        default=2.0,
        help="当前状态面板刷新 /snapshot 的频率。",
    )
    parser.add_argument(
        "--render-interval-ms",
        type=int,
        default=250,
        help="终端重绘频率。",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="WARNING",
        help="中继连接诊断的日志级别。",
    )
    return parser.parse_args()


async def _run_async(args: argparse.Namespace) -> None:
    client = MonitorTuiClient(
        relay_url=args.relay_url,
        exchange=args.exchange,
        user=args.user,
        focus_symbol=args.focus_symbol,
        snapshot_refresh_seconds=args.snapshot_refresh_seconds,
        render_interval_ms=args.render_interval_ms,
    )
    await client.run()


def main() -> None:
    args = _parse_args()
    configure_logging(args.log_level.upper())
    logging.info(
        "[monitor-tui] connecting relay_url=%s exchange=%s user=%s focus_symbol=%s",
        args.relay_url,
        args.exchange,
        args.user,
        args.focus_symbol,
    )
    try:
        asyncio.run(_run_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
