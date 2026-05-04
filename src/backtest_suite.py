#!/usr/bin/env python3
"""回测套件运行器的兼容性包装器。"""

from __future__ import annotations

import argparse
from pathlib import Path

from suite_runner import cli_entrypoint


def main() -> None:
    parser = argparse.ArgumentParser(description="运行回测套件。")
    parser.add_argument(
        "config_path",
        type=Path,
        nargs="?",
        default=None,
        help="基础 Passivbot 配置路径。默认使用代码中的 schema 默认值。",
    )
    parser.add_argument(
        "--suite-config",
        type=Path,
        default=None,
        help="包含 backtest.suite 覆盖的文件的可选路径。",
    )
    args = parser.parse_args()
    cli_entrypoint(
        str(args.config_path) if args.config_path else "",
        str(args.suite_config) if args.suite_config else None,
    )


if __name__ == "__main__":
    main()
