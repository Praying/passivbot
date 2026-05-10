from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from aiohttp import web

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_ROOT = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) in sys.path:
    sys.path.remove(str(SCRIPT_DIR))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from logging_setup import configure_logging
from monitor_relay import create_monitor_relay_app


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="提供只读的 Passivbot 监控快照和实时流服务。"
    )
    parser.add_argument(
        "--monitor-root",
        type=str,
        default="monitor",
        help="包含 {exchange}/{user}/ 清单和快照的基础监控根目录。",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="中继服务器绑定主机。",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="中继服务器绑定端口。",
    )
    parser.add_argument(
        "--poll-interval-ms",
        type=int,
        default=250,
        help="当前事件/历史文件的轮询间隔。",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=1000,
        help="每个订阅者的出站队列大小，超出后需重新同步。",
    )
    parser.add_argument(
        "--ws-replay-limit",
        type=int,
        default=50,
        help="websocket 连接时每个当前事件/历史文件回放的最近行数。",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="中继进程的日志级别。",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    configure_logging(args.log_level.upper())
    app = create_monitor_relay_app(
        monitor_root=args.monitor_root,
        poll_interval_ms=args.poll_interval_ms,
        subscriber_queue_size=args.queue_size,
        ws_replay_limit=args.ws_replay_limit,
    )
    logging.info(
        "[monitor-relay] serving monitor_root=%s host=%s port=%s poll_interval_ms=%s ws_replay_limit=%s",
        args.monitor_root,
        args.host,
        args.port,
        args.poll_interval_ms,
        args.ws_replay_limit,
    )
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
