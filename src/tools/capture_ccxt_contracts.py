import argparse
import asyncio

from ccxt_contracts import (
    DEFAULT_CAPTURE_SECTIONS,
    capture_contract_snapshot,
    default_snapshot_path,
    dump_snapshot,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="捕获实时 CCXT 合约快照，用于 Passivbot 升级检查。"
    )
    parser.add_argument("--user", required=True, help="用于捕获的 api-keys.json 用户名")
    parser.add_argument(
        "--label",
        default=None,
        help="用于元数据和默认输出文件名的友好标签（默认：user）",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="输出文件路径。默认为 artifacts/ccxt_contracts/{exchange}/{label}.json",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/ccxt_contracts",
        help="默认输出路径的基础目录",
    )
    parser.add_argument(
        "--sections",
        default=",".join(DEFAULT_CAPTURE_SECTIONS),
        help=f"要捕获的逗号分隔部分（默认：{','.join(DEFAULT_CAPTURE_SECTIONS)}）",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="可选的逗号分隔交易对，用于限定订单/成交捕获范围",
    )
    parser.add_argument(
        "--trades-limit",
        type=int,
        default=25,
        help="启用成交部分时的原始成交捕获限制",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    sections = [x.strip() for x in args.sections.split(",") if x.strip()]
    symbols = [x.strip() for x in args.symbols.split(",") if x.strip()]
    snapshot = await capture_contract_snapshot(
        user=args.user,
        label=args.label,
        sections=sections,
        symbols=symbols,
        trades_limit=args.trades_limit,
    )
    output_path = args.output
    if output_path is None:
        output_path = default_snapshot_path(
            args.output_dir,
            snapshot["meta"]["exchange"],
            args.label or args.user,
        )
    dumped = dump_snapshot(snapshot, output_path)
    print(dumped)


if __name__ == "__main__":
    asyncio.run(main())
