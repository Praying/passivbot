import argparse
import os


def get_cli_prog(default: str) -> str:
    """获取 CLI 程序名，优先使用环境变量 PASSIVBOT_CLI_PROG 覆盖。"""
    override = os.environ.get("PASSIVBOT_CLI_PROG")
    if not override:
        return default
    override = override.strip()
    return override or default


def help_requested(argv: list[str]) -> bool:
    """判断命令行参数是否请求了帮助信息。"""
    return any(arg in {"-h", "--help", "--help-all"} for arg in argv)


def help_all_requested(argv: list[str]) -> bool:
    """判断是否请求了全部帮助信息（含高级选项）。"""
    return "--help-all" in argv


def expand_help_all_argv(argv: list[str]) -> list[str]:
    """将 --help-all 扩展为同时包含 --help 的参数列表。"""
    if "--help-all" not in argv:
        return argv
    if any(arg in {"-h", "--help"} for arg in argv):
        return argv
    return [*argv, "--help"]


def build_command_parser(
    *,
    prog: str,
    description: str,
    usage: str,
    epilog: str,
) -> argparse.ArgumentParser:
    """构建命令解析器，使用 RawDescriptionHelpFormatter 保留说明格式。"""
    return argparse.ArgumentParser(
        prog=prog,
        description=description,
        usage=usage,
        epilog=epilog,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )


def add_help_all_argument(
    parser: argparse.ArgumentParser,
    *,
    help_all: bool,
    help_text: str = "显示所有配置覆盖标志，包括高级选项。",
) -> None:
    """添加 --help-all 参数，当 help_all 为 True 时隐藏该选项。"""
    parser.add_argument(
        "--help-all",
        action="store_true",
        default=False,
        help=argparse.SUPPRESS if help_all else help_text,
    )
