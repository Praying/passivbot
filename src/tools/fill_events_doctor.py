"""审计和修复缓存的成交事件异常。"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# 确保可以从 src/ 导入模块
SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

from fill_events_manager import BaseFetcher, FillEventsManager, _parse_log_level
from logging_setup import configure_logging


class _NoopFetcher(BaseFetcher):
    async def fetch(self, since_ms, until_ms, detail_cache, on_batch=None):
        if on_batch:
            on_batch([])
        return []


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="审计和修复成交事件缓存异常")
    parser.add_argument("--exchange", required=True, help="交易所 ID（如 bybit）")
    parser.add_argument("--user", required=True, help="用户/账户名")
    parser.add_argument(
        "--cache-root",
        default="caches/fill_events",
        help="成交事件缓存根目录（默认：caches/fill_events）",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="自动修复缓存文件中检测到的异常",
    )
    parser.add_argument(
        "--logging-level",
        default="info",
        help="日志级别名称或整数（warning=0, info=1, debug=2, trace=3）",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    manager = FillEventsManager(
        exchange=str(args.exchange).lower(),
        user=str(args.user),
        fetcher=_NoopFetcher(),
        cache_path=Path(args.cache_root) / str(args.exchange).lower() / str(args.user),
    )
    report = await manager.run_doctor(auto_repair=bool(args.repair))
    print(json.dumps(report, indent=2, sort_keys=True))
    if report.get("anomaly_events", 0) and not args.repair:
        return 2
    if report.get("anomaly_events_after", 0):
        return 1
    return 0


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    configure_logging(_parse_log_level(args.logging_level))
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
