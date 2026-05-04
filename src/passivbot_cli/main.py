from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import inspect
import os
import runpy
import sys
from dataclasses import dataclass
from pathlib import Path

from cli_utils import help_requested


@dataclass(frozen=True)
class CommandSpec:
    module: str
    summary: str
    requires_full: bool = False


CORE_COMMANDS: dict[str, CommandSpec] = {
    "live": CommandSpec("main", "运行实盘交易机器人"),
    "backtest": CommandSpec(
        "backtest",
        "运行历史回测（需要完整安装）",
        requires_full=True,
    ),
    "optimize": CommandSpec(
        "optimize",
        "运行优化器（需要完整安装）",
        requires_full=True,
    ),
    "download": CommandSpec(
        "ohlcv_download",
        "下载 OHLCV 数据（需要完整安装）",
        requires_full=True,
    ),
}

TOOL_COMMANDS: dict[str, CommandSpec] = {
    "candle-doctor": CommandSpec(
        "tools.candle_doctor",
        "审计 K 线缓存（需要完整安装）",
        requires_full=True,
    ),
    "fetch-balance": CommandSpec("tools.fetch_balance", "获取交易所余额"),
    "hyperliquid-balance-probe": CommandSpec(
        "tools.probe_hyperliquid_balance",
        "只读 Hyperliquid 余额冒烟测试",
    ),
    "hyperliquid-order-margin-probe": CommandSpec(
        "tools.probe_hyperliquid_order_margin",
        "修改性 Hyperliquid 订单保证金诊断",
    ),
    "hyperliquid-position-probe": CommandSpec(
        "tools.probe_hyperliquid_position_balance",
        "修改性 Hyperliquid 仓位/余额诊断",
    ),
    "fill-events-dash": CommandSpec(
        "tools.fill_events_dash",
        "启动成交事件仪表板（需要完整安装）",
        requires_full=True,
    ),
    "fill-events-doctor": CommandSpec(
        "tools.fill_events_doctor",
        "审计成交事件缓存（需要完整安装）",
        requires_full=True,
    ),
    "generate-mcap-list": CommandSpec(
        "tools.generate_mcap_list",
        "按市值生成批准币种列表（需要完整安装）",
        requires_full=True,
    ),
    "iterative-backtester": CommandSpec(
        "tools.iterative_backtester",
        "启动交互式迭代回测器（需要完整安装）",
        requires_full=True,
    ),
    "iterative-history-plot": CommandSpec(
        "tools.iterative_history_plot",
        "绘制迭代历史文件（需要完整安装）",
        requires_full=True,
    ),
    "inspect-ohlcvs": CommandSpec(
        "tools.inspect_ohlcvs",
        "检查 v2 OHLCV 缓存元数据和间隙（需要完整安装）",
        requires_full=True,
    ),
    "migrate-historical-data": CommandSpec(
        "tools.migrate_historical_data",
        "迁移历史数据布局（需要完整安装）",
        requires_full=True,
    ),
    "merge-paretos": CommandSpec(
        "tools.merge_paretos",
        "将 Pareto 前沿合并为起始配置（需要完整安装）",
        requires_full=True,
    ),
    "monitor-relay": CommandSpec(
        "tools.monitor_relay",
        "提供监控快照和实时流服务（需要完整安装）",
        requires_full=True,
    ),
    "monitor-dev": CommandSpec(
        "tools.monitor_dev",
        "根据需要启动中继并附加终端监控器（需要完整安装）",
        requires_full=True,
    ),
    "monitor-web": CommandSpec(
        "tools.monitor_web",
        "根据需要启动中继并保持 Web 仪表板可用（需要完整安装）",
        requires_full=True,
    ),
    "monitor-tui": CommandSpec(
        "tools.monitor_tui",
        "启动终端监控读取器（需要完整安装）",
        requires_full=True,
    ),
    "pad-historical-daily": CommandSpec(
        "tools.pad_historical_daily",
        "填充缺失的每日历史数据（需要完整安装）",
        requires_full=True,
    ),
    "pareto": CommandSpec(
        "tools.pareto_explorer",
        "从 Pareto 前沿选择单个候选（需要完整安装）",
        requires_full=True,
    ),
    "pareto-dash": CommandSpec(
        "tools.pareto_dash",
        "启动 Pareto 仪表板（需要完整安装）",
        requires_full=True,
    ),
    "pareto-explorer": CommandSpec(
        "tools.pareto_explorer",
        "从 Pareto 前沿选择单个候选（需要完整安装）",
        requires_full=True,
    ),
    "pareto-transform": CommandSpec(
        "tools.pareto_transform",
        "转换 Pareto 结果数据（需要完整安装）",
        requires_full=True,
    ),
    "streamline-json": CommandSpec("tools.streamline_json", "重新格式化配置或结果 JSON"),
    "verify-hlcvs-data": CommandSpec(
        "tools.verify_hlcvs_data",
        "验证缓存的 OHLCV 数据集（需要完整安装）",
        requires_full=True,
    ),
}

FULL_INSTALL_MODULE_HINTS = {
    "aiohttp",
    "colorama",
    "dash",
    "dash_bootstrap_components",
    "deap",
    "dictdiffer",
    "matplotlib",
    "msgpack",
    "plotly",
    "pymoo",
    "psutil",
    "pyecharts",
    "requests",
}


FULL_INSTALL_MARKER_MODULES = tuple(sorted(FULL_INSTALL_MODULE_HINTS | {"websockets"}))
ENV_MISMATCH_IGNORE_ENV = "PASSIVBOT_IGNORE_ENV_MISMATCH"
ENV_REEXEC_GUARD_ENV = "PASSIVBOT_ENV_REEXEC"


def _build_root_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="passivbot",
        description="Passivbot 统一 CLI",
        epilog=(
            "使用 'passivbot <command> -h' 获取特定命令的帮助。\n"
            "基础安装支持实盘交易。使用 "
            "'python3 -m pip install -e \".[full]\"' 安装 Passivbot 以获取回测、优化、下载器"
            "和高级工具。"
        ),
    )
    subparsers = parser.add_subparsers(dest="command", metavar="command")
    for name, spec in CORE_COMMANDS.items():
        subparsers.add_parser(name, help=spec.summary)
    subparsers.add_parser("tool", help="运行辅助工具（部分需要完整安装）")
    return parser


def _build_tool_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="passivbot tool",
        description="运行 Passivbot 辅助工具",
        epilog=(
            "使用 'passivbot tool <tool> -h' 获取特定工具的帮助。\n"
            "使用 'python3 -m pip install -e \".[full]\"' 安装 Passivbot 以使用标记为"
            "需要完整安装的工具。"
        ),
    )
    parser.add_argument("tool_name", nargs="?", help="要运行的工具")
    return parser


def _restore_env_var(name: str, previous: str | None) -> None:
    if previous is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = previous


def _active_env_prefix() -> Path | None:
    for name in ("VIRTUAL_ENV", "CONDA_PREFIX"):
        raw = os.environ.get(name)
        if raw:
            return _resolve_path(raw)
    return None


def _resolve_path(value: str | os.PathLike[str]) -> Path:
    expanded = os.path.abspath(os.path.expanduser(os.fspath(value)))
    return Path(os.path.realpath(expanded))


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _current_interpreter_prefixes() -> tuple[Path, ...]:
    prefixes: list[Path] = []
    for value in (getattr(sys, "prefix", None), getattr(sys, "exec_prefix", None)):
        if not value:
            continue
        prefixes.append(_resolve_path(value))
    return tuple(dict.fromkeys(prefixes))


def _env_bin_dir(prefix: Path) -> Path:
    return prefix / ("Scripts" if os.name == "nt" else "bin")


def _expected_console_script(prefix: Path) -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    return _env_bin_dir(prefix) / f"passivbot{suffix}"


def _expected_python(prefix: Path) -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    return _env_bin_dir(prefix) / f"python{suffix}"


def _install_command_line(command: str, prefix: Path | None = None) -> str:
    if prefix is not None:
        return f"{_expected_python(prefix)} -m pip install -e {command}"
    return f'python3 -m pip install -e {command}'


def _install_guidance(prefix: Path | None = None) -> str:
    full = '".[full]"'
    dev = '".[dev]"'
    return (
        "Install Passivbot into the active environment with one of:\n"
        f"  {_install_command_line('.', prefix)}\n"
        f"  {_install_command_line(full, prefix)}\n"
        f"  {_install_command_line(dev, prefix)}\n"
    )


def _environment_mismatch_message(prefix: Path, actual_python: Path) -> str:
    script = _resolve_path(sys.argv[0]) if sys.argv and sys.argv[0] else None
    expected_script = _expected_console_script(prefix)
    return (
        "passivbot 检测到活动环境不匹配。\n"
        f"  活动环境: {prefix}\n"
        f"  运行的 python:     {actual_python}\n"
        f"  运行的脚本:     {script}\n"
        f"  期望的脚本:    {expected_script}\n"
        "这通常意味着你的 shell 解析了过期的 shim 或不同的安装。\n\n"
        f"{_install_guidance(prefix)}"
        "安装后，重新激活环境并刷新 shell 命令查找"
        "（例如：'hash -r'；使用 zsh 时还需运行 'rehash'）。\n"
        f"设置 {ENV_MISMATCH_IGNORE_ENV}=1 以有意绕过此检查。\n"
    )


def _ensure_expected_environment() -> None:
    if os.environ.get(ENV_MISMATCH_IGNORE_ENV):
        return

    prefix = _active_env_prefix()
    if prefix is None:
        return

    actual_python = _resolve_path(sys.executable)
    if _path_is_within(actual_python, prefix):
        return
    if any(current_prefix == prefix for current_prefix in _current_interpreter_prefixes()):
        return

    script = _resolve_path(sys.argv[0]) if sys.argv and sys.argv[0] else None
    expected_script = _expected_console_script(prefix)
    if script is not None and script == expected_script:
        return

    expected_python = _expected_python(prefix)
    if expected_script.exists() and expected_python.exists() and not os.environ.get(ENV_REEXEC_GUARD_ENV):
        os.environ[ENV_REEXEC_GUARD_ENV] = "1"
        os.execv(
            str(expected_python),
            [str(expected_python), str(expected_script), *sys.argv[1:]],
        )

    raise SystemExit(_environment_mismatch_message(prefix, actual_python))


def _full_install_message(prog_name: str, missing_module: str | None = None) -> str:
    detail = f" Missing dependency: {missing_module}." if missing_module else ""
    return (
        f"{prog_name} requires the full Passivbot install.{detail}\n"
        "Install it with:\n"
        '  python3 -m pip install -e ".[full]"\n'
    )


def _missing_full_install_markers() -> list[str]:
    return [name for name in FULL_INSTALL_MARKER_MODULES if importlib.util.find_spec(name) is None]


def _is_help_request(argv: list[str]) -> bool:
    return help_requested(argv)


def _invoke_module_main(module_name: str) -> tuple[bool, int]:
    module = importlib.import_module(module_name)
    main_fn = getattr(module, "main", None)
    if not callable(main_fn):
        return False, 0

    result = main_fn()
    if inspect.isawaitable(result):
        result = asyncio.run(result)

    if result is None:
        return True, 0
    if isinstance(result, int):
        return True, result
    return True, 0


def _run_module(module_name: str, prog_name: str, argv: list[str], requires_full: bool = False) -> int:
    if requires_full and not _is_help_request(argv):
        if _missing_full_install_markers():
            print(_full_install_message(prog_name), file=sys.stderr)
            return 2

    previous_argv = sys.argv[:]
    previous_prog = os.environ.get("PASSIVBOT_CLI_PROG")
    sys.argv = [prog_name, *argv]
    os.environ["PASSIVBOT_CLI_PROG"] = prog_name
    try:
        ran_main, exit_code = _invoke_module_main(module_name)
        if ran_main:
            return exit_code
        runpy.run_module(module_name, run_name="__main__")
    except ModuleNotFoundError as exc:
        if requires_full and exc.name and exc.name.split(".", 1)[0] in FULL_INSTALL_MODULE_HINTS:
            print(_full_install_message(prog_name, exc.name), file=sys.stderr)
            return 2
        raise
    except SystemExit as exc:
        if exc.code is None:
            return 0
        if isinstance(exc.code, int):
            return exc.code
        raise
    finally:
        sys.argv = previous_argv
        _restore_env_var("PASSIVBOT_CLI_PROG", previous_prog)
    return 0


def _dispatch_tool(argv: list[str]) -> int:
    parser = _build_tool_parser()
    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        print("\nAvailable tools:")
        for name in sorted(TOOL_COMMANDS):
            print(f"  {name:<24} {TOOL_COMMANDS[name].summary}")
        return 0

    tool_name = argv[0]
    spec = TOOL_COMMANDS.get(tool_name)
    if spec is None:
        parser.exit(2, f"passivbot tool: unknown tool {tool_name!r}\n")
    return _run_module(
        spec.module,
        f"passivbot tool {tool_name}",
        argv[1:],
        requires_full=spec.requires_full,
    )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = _build_root_parser()
    if not argv or argv[0] in {"-h", "--help"}:
        parser.print_help()
        return 0

    if argv[0] == "help":
        if len(argv) == 1:
            parser.print_help()
            return 0
        return main([argv[1], "-h", *argv[2:]])

    command = argv[0]
    if command == "tool":
        return _dispatch_tool(argv[1:])

    spec = CORE_COMMANDS.get(command)
    if spec is None:
        parser.exit(2, f"passivbot: unknown command {command!r}\n")
    return _run_module(
        spec.module,
        f"passivbot {command}",
        argv[1:],
        requires_full=spec.requires_full,
    )


def console_main() -> None:
    _ensure_expected_environment()
    raise SystemExit(main())
