import re
import json
import ccxt.async_support as ccxt
import os
import datetime
import dateutil.parser
import asyncio
import hjson
import inspect
import time
from collections import defaultdict
from typing import Dict, Any, List, Union, Optional
import re
import logging
from copy import deepcopy
from pathlib import Path
import portalocker  # type: ignore
from custom_endpoint_overrides import (
    apply_rest_overrides_to_ccxt,
    resolve_custom_endpoint_override,
)

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# 带磁盘变更检测的符号/币种映射的内存缓存
_COIN_TO_SYMBOL_CACHE = {}  # {exchange: {"map": dict, "mtime_ns": int, "size": int}}
_SYMBOL_TO_COIN_CACHE = {"map": None, "mtime_ns": None, "size": None}
_SYMBOL_TO_COIN_WARNINGS: set[str] = set()
_COIN_TO_SYMBOL_FALLBACKS: set[tuple[str, str]] = set()

# 符号/币种映射文件的锁常量
_SYMBOL_MAP_LOCK_STALE_SECONDS = 180  # 移除超过 3 分钟的锁
_SYMBOL_MAP_LOCK_TIMEOUT = 5  # 等待获取锁的超时秒数
_SYMBOL_MAP_STALE_CLEANUP_DONE = False  # 跟踪本次会话是否已执行过清理
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LEGACY_COINS_FILE_ALIASES = {
    "approved_coins_topmcap.json": Path("configs/approved_coins.json"),
    "approved_coins_topmcap.txt": Path("configs/approved_coins.json"),
}


def _atomic_write_json(path: str, data: dict, indent=None, sort_keys=False) -> None:
    """原子写入 JSON：先写入 .tmp 文件再用 os.replace() 保证崩溃安全。"""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=indent, sort_keys=sort_keys)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def _cleanup_stale_symbol_map_locks() -> None:
    """
    移除明显过期的符号/币种映射遗留 .lock 文件。
    每个会话在首次访问时运行一次，防止累积。
    """
    global _SYMBOL_MAP_STALE_CLEANUP_DONE
    if _SYMBOL_MAP_STALE_CLEANUP_DONE:
        return
    _SYMBOL_MAP_STALE_CLEANUP_DONE = True

    cache_dir = Path("caches")
    if not cache_dir.exists():
        return

    now = time.time()
    threshold = _SYMBOL_MAP_LOCK_STALE_SECONDS

    # 清理 caches/ 和 caches/{exchange}/ 中的锁文件
    lock_patterns = [
        "*.lock",  # 顶层锁 (symbol_to_coin_map.json.lock)
        "*/*.lock",  # 每个交易所的锁 (caches/{exchange}/coin_to_symbol_map.json.lock)
    ]

    for pattern in lock_patterns:
        for lock_path in cache_dir.glob(pattern):
            # 仅清理符号/币种映射相关的锁
            if "symbol" not in lock_path.name and "coin" not in lock_path.name:
                continue
            try:
                stat = lock_path.stat()
                age = now - stat.st_mtime
                if age > threshold:
                    lock_path.unlink()
                    logging.info("removed stale symbol map lock %s (age %.1fs)", lock_path, age)
            except FileNotFoundError:
                continue
            except Exception as exc:
                logging.debug("failed to remove stale lock %s: %s", lock_path, exc)


def _resolve_coins_file_path(value: str) -> Optional[Path]:
    if not isinstance(value, str) or not value.strip():
        return None
    raw_path = Path(value.strip())
    candidates: List[Path] = []

    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        candidates.extend(
            [
                PROJECT_ROOT / raw_path,
                Path.cwd() / raw_path,
            ]
        )

    alias = LEGACY_COINS_FILE_ALIASES.get(raw_path.name)
    if alias is not None:
        if not alias.is_absolute():
            candidates.append(PROJECT_ROOT / alias)
        else:
            candidates.append(alias)

    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists():
            if candidate.name != raw_path.name and raw_path.name in LEGACY_COINS_FILE_ALIASES:
                try:
                    rel = candidate.relative_to(PROJECT_ROOT)
                except ValueError:
                    rel = candidate
                logging.warning(
                    "Resolved legacy coins file '%s' to '%s'. Update your config to the new path.",
                    raw_path,
                    rel,
                )
            return candidate
    return None


def _require_live_value(config: Dict[str, Any], key: str):
    if "live" not in config or not isinstance(config["live"], dict):
        raise KeyError("config missing required key 'live'")
    live = config["live"]
    if key not in live:
        raise KeyError(f"config missing required key 'live.{key}'")
    return live[key]


def ts_to_date(timestamp: Union[float, str, int]) -> str:
    """
    将时间戳转换为 ISO 格式的 UTC 日期字符串。

    Args:
        timestamp: 时间戳，可以是 float、str 或 int - 可能是秒、毫秒或纳秒

    Returns:
        ISO 格式的 UTC 日期字符串（如 "2025-03-12T12:43:22.123"）
    """
    # 如果是字符串或整数，转换为浮点数
    if isinstance(timestamp, (str, int)):
        timestamp = float(timestamp)

    # 检测时间戳精度并转换为秒
    if timestamp > 1e15:  # 可能是纳秒（> ~2033 年的毫秒值）
        # 纳秒
        timestamp_seconds = timestamp / 1_000_000_000
    elif timestamp > 1e10:  # 可能是毫秒（> ~2001 年的秒值）
        # 毫秒
        timestamp_seconds = timestamp / 1000
    else:
        # 秒
        timestamp_seconds = timestamp

    # 转换为 UTC datetime
    dt = datetime.datetime.fromtimestamp(timestamp_seconds, tz=datetime.timezone.utc)

    # 返回不带时区后缀的 ISO 格式
    return dt.isoformat().replace("+00:00", "")


def date_to_ts(date_str: str) -> float:
    """
    将灵活的日期字符串转换为毫秒级 UTC 时间戳。

    Args:
        date_str: 各种格式的日期字符串：
                 - "2020" -> "2020-01-01T00:00:00"
                 - "2024-04" -> "2024-04-01T00:00:00"
                 - "2022-04-23" -> "2022-04-23T00:00:00"
                 - "2021-11-13T03:23:12"（完整 ISO 格式）
                 - 以及其他常见变体

    Returns:
        毫秒级 UTC 时间戳（float）
    """
    date_str = date_str.strip()

    # 使用 dateutil.parser，缺失部分的默认日期为 2000 年 1 月 1 日
    default_date = datetime.datetime(2000, 1, 1)

    try:
        dt = dateutil.parser.parse(date_str, default=default_date)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Unable to parse date string '{date_str}': {e}")

    # 如果 datetime 是朴素的（无时区信息），视为 UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)

    # 转换为毫秒级 UTC 时间戳
    return dt.timestamp() * 1000


def get_file_mod_ms(filepath):
    """
    获取文件最后修改的 UTC 时间戳。
    Args:
        filepath (str): 文件路径。
    Returns:
        float: 文件最后修改的毫秒级 UTC 时间戳。
    """
    # 获取自纪元以来的最后修改时间（秒，已是 UTC 基准）
    mod_time_epoch = os.path.getmtime(filepath)
    # 转换为毫秒
    return mod_time_epoch * 1000


def format_end_date(end_date) -> str:
    if end_date in ["today", "now", "", None]:
        ms2day = 1000 * 60 * 60 * 24
        end_date = ts_to_date((utc_ms() - ms2day * 2) // ms2day * ms2day)
    else:
        end_date = ts_to_date(date_to_ts(end_date))
    return end_date[:10]


def make_get_filepath(filepath: str) -> str:
    """
    确保文件路径的目录存在并返回该路径。
    """
    dirpath = os.path.dirname(filepath) if not filepath.endswith("/") else filepath
    if dirpath and not os.path.isdir(dirpath):
        os.makedirs(dirpath, exist_ok=True)
    return filepath


def utc_ms() -> float:
    return time.time() * 1000


def _inline_simple_containers(text: str, max_inline: int) -> str:
    """将长度不超过 ``max_inline`` 字符的扁平列表/字典块折叠为单行。"""

    result: list[str] = []
    i = 0
    length = len(text)

    while i < length:
        char = text[i]
        if char in "[{":
            closing = "]" if char == "[" else "}"
            j = i + 1
            depth = 1
            nested = False
            while j < length and depth > 0:
                if text[j] == char:
                    depth += 1
                    nested = True
                elif text[j] == closing:
                    depth -= 1
                j += 1
            segment = text[i:j]
            if (
                depth == 0
                and not nested
                and "\n" in segment
                and len("".join(segment.split())) <= max_inline
            ):
                inner = "".join(line.strip() for line in segment.splitlines()[1:-1])
                result.append(f"{char}{inner}{closing}")
            else:
                result.append(segment)
            i = j
        else:
            result.append(char)
            i += 1
    return "".join(result)


def dump_json_streamlined(
    data: Any,
    fp,
    *,
    indent: int = 4,
    max_inline: int = 60,
    separators: tuple[str, str] = (",", ":"),
    sort_keys: bool = False,
) -> None:
    """
    写入 JSON，短列表/字典保持在一行，较大块保持正常缩进。

    Args:
        data: 要序列化的对象。
        fp: 带有 ``write`` 方法的文件对象。
        indent: 基础缩进级别（类似 ``json.dump``）。
        max_inline: 允许内联容器的最大字符数（包括括号/花括号）。
        separators: 传递给 ``json.dumps`` 用于间距控制。
        sort_keys: 是否对字典键排序。
    """

    fp.write(
        json_dumps_streamlined(
            data,
            indent=indent,
            max_inline=max_inline,
            separators=separators,
            sort_keys=sort_keys,
        )
    )


def json_dumps_streamlined(
    data: Any,
    *,
    indent: int = 4,
    max_inline: int = 60,
    separators: tuple[str, str] = (",", ":"),
    sort_keys: bool = False,
) -> str:
    """返回精简的 JSON 字符串（类似 ``dump_json_streamlined`` 但在内存中操作）。"""

    compact_separators = separators

    def _inline_repr(value: Any) -> Optional[str]:
        try:
            return json.dumps(value, separators=compact_separators, sort_keys=sort_keys)
        except TypeError:
            return None

    def _render(value: Any, level: int) -> str:
        inline = _inline_repr(value)
        if inline is not None and len(inline) <= max_inline:
            return inline

        indent_str = " " * (indent * level)
        child_indent = " " * (indent * (level + 1))

        if isinstance(value, dict):
            items = list(value.items())
            if sort_keys:
                items = sorted(items)
            parts = ["{"]
            total = len(items)
            for idx, (key, val) in enumerate(items):
                rendered = _render(val, level + 1)
                comma = "," if idx < total - 1 else ""
                parts.append(f"{child_indent}{json.dumps(key)}: {rendered}{comma}")
            parts.append(f"{indent_str}}}")
            return "\n".join(parts)

        if isinstance(value, (list, tuple)):
            total = len(value)
            parts = ["["]
            for idx, item in enumerate(value):
                rendered = _render(item, level + 1)
                comma = "," if idx < total - 1 else ""
                parts.append(f"{child_indent}{rendered}{comma}")
            parts.append(f"{indent_str}]")
            return "\n".join(parts)

        return json.dumps(value, separators=compact_separators)

    return _render(data, 0)


def trim_analysis_aliases(analysis: dict) -> dict:
    """返回 ``analysis`` 的副本，移除冗余的别名指标。

    应用两条清理规则：

    1. 如果某个键以 ``"_usd"`` 结尾且其值与基础指标（去掉后缀的同名键）相同，
       则删除基础条目，保留显式的 ``*_usd`` 键。
    2. 在剩余条目中，如果多个键是相同下划线分隔 token 的排列且值完全相同
       （如 ``"drawdown_btc_worst"`` vs ``"drawdown_worst_btc"``），
       仅保留一个键。优先保留尾部 token 为货币标签（``usd``/``btc``）的键；
       平局时按键长度和字典序回退。

    原始 ``analysis`` 映射不会被修改。
    """

    trimmed = dict(analysis)

    # 步骤 1：当 *_usd 携带相同值时移除基础键。
    for key, value in list(trimmed.items()):
        if key.endswith("_usd"):
            base_key = key[:-4]
            if base_key in trimmed and trimmed[base_key] == value:
                trimmed.pop(base_key)

    # 步骤 2：移除共享相同值的重复排列。
    groups = {}
    for key in trimmed:
        canon = tuple(sorted(key.split("_")))
        groups.setdefault(canon, []).append(key)

    def _score(alias: str) -> tuple:
        tokens = alias.split("_")
        tail_currency = 1 if tokens and tokens[-1] in {"usd", "btc"} else 0
        return (tail_currency, -len(alias), alias)

    for keys in groups.values():
        if len(keys) < 2:
            continue
        values = {}
        for key in keys:
            values.setdefault(trimmed[key], []).append(key)
        for aliases in values.values():
            if len(aliases) < 2:
                continue
            keep = max(aliases, key=_score)
            for alias in aliases:
                if alias != keep:
                    trimmed.pop(alias, None)

    return trimmed


def filter_markets(markets: dict, exchange: str, quote=None, verbose=False) -> (dict, dict, dict):
    """
    返回 (eligible, ineligible, reasons)
    """
    eligible = {}
    ineligible = {}
    reasons = {}
    quote = get_quote(to_ccxt_exchange_id(exchange), quote)
    for k, v in markets.items():
        if not v["active"]:
            ineligible[k] = v
            reasons[k] = "not active"
        elif not v["swap"]:
            ineligible[k] = v
            reasons[k] = "not swap"
        elif not v["linear"]:
            ineligible[k] = v
            reasons[k] = "not linear"
        elif not k.endswith(f"/{quote}:{quote}"):
            ineligible[k] = v
            reasons[k] = "wrong quote"
        elif exchange == "hyperliquid" and float(v.get("info", {}).get("openInterest", 0)) == 0.0:
            # 零持仓量意味着市场不活跃
            # 注意：HIP-3 股票永续合约允许 onlyIsolated=True
            ineligible[k] = v
            reasons[k] = f"ineligible on {exchange}"
        else:
            eligible[k] = v

    if verbose:
        for line in sorted(set(reasons.values())):
            syms = [k for k in reasons if reasons[k] == line]
            log = (
                logging.debug
                if line in {"not active", "wrong quote", "not swap", "not linear"}
                else logging.info
            )
            if len(syms) > 12:
                log(f"{line}: {len(syms)} symbols")
            elif len(syms) > 0:
                log(f"{line}: {','.join(sorted(set([s for s in syms])))}")

    return eligible, ineligible, reasons


async def load_markets(
    exchange: str,
    max_age_ms: int = 1000 * 60 * 60 * 24,
    verbose=True,
    cc=None,
    quote=None,
) -> dict:
    """
    加载并缓存给定交易所 CCXT markets 的独立辅助函数。

    - 如果缓存新鲜则从 caches/{exchange}/markets.json 读取
    - 否则通过 ccxt 获取、写入缓存并返回 markets 字典

    返回 ccxt 提供的 markets 字典。

    注意：使用交易所的原始名称（如 "binance" 而非 "binanceusdm"）以保持
    与其他缓存路径（pnls, ohlcv, fill_events）的一致性。
    """
    # 优先使用 cc.id（如果提供了 ccxt 实例），否则使用提供的交易所字符串。
    # 反规范化以使用规范形式作为缓存路径（如 "binance" 而非 "binanceusdm"）
    ex = to_standard_exchange_name(getattr(cc, "id", None) or exchange or "")
    markets_path = os.path.join("caches", ex, "markets.json")

    # 先尝试缓存
    try:
        if os.path.exists(markets_path):
            if utc_ms() - get_file_mod_ms(markets_path) < max_age_ms:
                with open(markets_path, "r") as f:
                    markets = json.load(f)
                if verbose:
                    logging.info(f"{ex} Loaded markets from cache")
                create_coin_symbol_map_cache(ex, markets, quote=quote, verbose=verbose)
                return markets
    except Exception as e:
        logging.error("Error loading %s: %s", markets_path, e)

    # 通过 ccxt 从交易所获取
    owned_cc = cc is None
    if owned_cc:
        cc = load_ccxt_instance(ex, enable_rate_limit=True)
    try:
        markets = await cc.load_markets(True)
    except Exception as e:
        logging.error(f"Error loading markets from {ex}: {e}")
        raise
    finally:
        # 仅在此处创建的 ccxt 客户端才关闭。
        if owned_cc:
            try:
                await cc.close()
            except Exception:
                pass

    # 写入缓存
    try:
        path = make_get_filepath(markets_path)
        with open(path, "w") as f:
            json.dump(markets, f)
        if verbose:
            logging.info(f"{ex} Dumped markets to cache")
    except Exception as e:
        logging.error("Error dumping markets to cache at %s: %s", markets_path, e)
    create_coin_symbol_map_cache(ex, markets, quote=quote, verbose=verbose)
    return markets


def to_ccxt_exchange_id(exchange: str) -> str:
    """
    将简短交易所名称转换为 ccxt 的 USDT 保证金永续期货 id。

    示例：
    - "binance" -> "binanceusdm"
    - "kucoin"  -> "kucoinfutures"
    - "kraken"  -> "krakenfutures"

    如果没有特定的期货 id（如 "okx"、"bybit"、"mexc"），输入原样返回。
    此函数使用 ccxt.exchanges 检测可用 id，因此会自动识别遵循
    常见后缀模式（如 'usdm' 或 'futures'）的新交易所。
    """
    ex = (exchange or "").lower()
    valid = set(getattr(ccxt, "exchanges", []))

    # 已知特殊情况的显式映射
    if ex == "binance":
        return "binanceusdm"

    # 如果已经是期货/永续 id，保持不变
    if ex.endswith("usdm") or ex.endswith("futures"):
        return ex

    # 启发式：优先尝试 '{exchange}usdm'，然后尝试 '{exchange}futures'（如果在 ccxt 中可用）
    for suffix in ("usdm", "futures"):
        cand = f"{ex}{suffix}"
        if cand in valid:
            return cand

    return ex


def to_standard_exchange_name(exchange: str) -> str:
    """
    将 ccxt 交易所 id 转换为配置、缓存和日志中使用的规范简短形式。

    示例：
    - "binanceusdm" -> "binance"
    - "kucoinfutures" -> "kucoin"
    - "krakenfutures" -> "kraken"

    如果交易所没有已知后缀，原样返回。
    """
    ex = (exchange or "").lower()

    # 移除已知的期货后缀
    for suffix in ("usdm", "futures"):
        if ex.endswith(suffix):
            return ex[: -len(suffix)]

    return ex


# 已弃用的别名，用于向后兼容 - 将在未来版本中移除
def normalize_exchange_name(exchange: str) -> str:
    """已弃用：请改用 to_ccxt_exchange_id()。"""
    import warnings

    warnings.warn(
        "normalize_exchange_name() is deprecated, use to_ccxt_exchange_id() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return to_ccxt_exchange_id(exchange)


def denormalize_exchange_name(exchange: str) -> str:
    """已弃用：请改用 to_standard_exchange_name()。"""
    import warnings

    warnings.warn(
        "denormalize_exchange_name() is deprecated, use to_standard_exchange_name() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return to_standard_exchange_name(exchange)


def load_ccxt_instance(exchange_id: str, enable_rate_limit: bool = True, timeout_ms: int = 60_000):
    """
    返回给定交易所 id 的 ccxt async-support 交易所实例。

    调用者应使用 await cc.close() 关闭返回的实例。
    """
    ex = to_ccxt_exchange_id(exchange_id)
    try:
        cc = getattr(ccxt, ex)(
            {
                "enableRateLimit": bool(enable_rate_limit),
                # ccxt 默认超时对于长回看周期可能太低；提高以增强容错性。
                "timeout": int(timeout_ms),
            }
        )
    except Exception:
        raise RuntimeError(f"ccxt exchange '{ex}' not available")
    try:
        cc.options["defaultType"] = "swap"
        if ex == "hyperliquid":
            # 包含来自 TradeXYZ 的 HIP-3 股票永续合约
            cc.options["fetchMarkets"] = {
                "types": ["swap", "hip3"],
                "hip3": {
                    "dex": ["xyz"],  # TradeXYZ DEX，用于股票永续合约
                },
            }
    except Exception:
        pass
    try:
        override = resolve_custom_endpoint_override(ex)
        apply_rest_overrides_to_ccxt(cc, override)
    except Exception as exc:
        logging.warning("Failed to apply custom endpoint override for %s: %s", ex, exc)
    return cc


def get_quote(exchange, quote=None):
    """返回交易所的报价货币。

    Args:
        exchange: 交易所名称
        quote: 显式报价覆盖（来自 api-keys.json）。
               如果提供，直接返回此值。

    Returns:
        报价货币字符串（如 "USDT"、"USDC"）
    """
    if quote is not None:
        return quote
    # 向后兼容的旧版硬编码默认值
    exchange = to_ccxt_exchange_id(exchange)
    return "USDC" if exchange in ["hyperliquid", "defx", "paradex"] else "USDT"


def remove_powers_of_ten(text):
    """
    从字符串中移除 "10"、"100"、"1000"、"10000" 等各种变体。
    通过前瞻/后顾断言处理 "1000SHIB" 等情况。
    """
    # 匹配 1 后跟一个或多个零，带单词边界或字符串起止
    pattern = r"(?<!\d)1(?:0+)(?!\d)"
    return re.sub(pattern, "", text)


def _load_coin_to_symbol_map(exchange: str) -> dict:
    """
    惰性加载并缓存 caches/{exchange}/coin_to_symbol_map.json 到内存。
    当磁盘文件变更（mtime 或 size）时重新加载。
    使用共享锁防止并发写入时的读取问题。
    """
    # 首次访问时执行过期锁清理
    _cleanup_stale_symbol_map_locks()

    path = os.path.join("caches", exchange, "coin_to_symbol_map.json")
    try:
        st = os.stat(path)
        mtime_ns, size = st.st_mtime_ns, st.st_size
    except Exception:
        return {}
    entry = _COIN_TO_SYMBOL_CACHE.get(exchange)
    if entry and entry.get("mtime_ns") == mtime_ns and entry.get("size") == size:
        return entry.get("map", {})
    lock_path = path + ".lock"
    try:
        with portalocker.Lock(lock_path, timeout=_SYMBOL_MAP_LOCK_TIMEOUT, flags=portalocker.LOCK_SH):
            with open(path) as f:
                data = json.load(f)
        _COIN_TO_SYMBOL_CACHE[exchange] = {"map": data, "mtime_ns": mtime_ns, "size": size}
        return data
    except portalocker.LockException:
        logging.warning("Could not acquire shared lock for %s, returning cached data", path)
        return entry.get("map", {}) if entry else {}
    except Exception as e:
        logging.error(f"failed to load coin_to_symbol_map for {exchange}: {e}")
        return {}


def _load_symbol_to_coin_map() -> dict:
    """
    惰性加载并缓存 caches/symbol_to_coin_map.json 到内存。
    当磁盘文件变更（mtime 或 size）时重新加载。
    使用共享锁防止并发写入时的读取问题。
    """
    # 首次访问时执行过期锁清理
    _cleanup_stale_symbol_map_locks()

    path = os.path.join("caches", "symbol_to_coin_map.json")
    try:
        st = os.stat(path)
        mtime_ns, size = st.st_mtime_ns, st.st_size
    except Exception:
        return {}
    entry = _SYMBOL_TO_COIN_CACHE
    if (
        entry.get("map") is not None
        and entry.get("mtime_ns") == mtime_ns
        and entry.get("size") == size
    ):
        return entry.get("map", {})
    lock_path = path + ".lock"
    try:
        with portalocker.Lock(lock_path, timeout=_SYMBOL_MAP_LOCK_TIMEOUT, flags=portalocker.LOCK_SH):
            with open(path) as f:
                data = json.load(f)
        _SYMBOL_TO_COIN_CACHE["map"] = data
        _SYMBOL_TO_COIN_CACHE["mtime_ns"] = mtime_ns
        _SYMBOL_TO_COIN_CACHE["size"] = size
        return data
    except portalocker.LockException:
        logging.warning("Could not acquire shared lock for %s, returning cached data", path)
        return entry.get("map") if entry.get("map") is not None else {}
    except Exception as e:
        logging.error(f"failed to load symbol_to_coin_map: {e}")
        return {}


def _build_coin_symbol_maps(markets, quote):
    """
    从 markets 数据构建 coin_to_symbol_map（字典的列表形式）和 symbol_to_coin_map。
    此函数是纯函数，不执行磁盘 I/O。
    """

    def _namespaced_aliases(base: str, market: dict) -> set[str]:
        aliases = set()
        if not isinstance(base, str) or not base:
            return aliases
        is_namespaced_hip3 = bool((market.get("info") or {}).get("hip3")) or base.startswith(
            ("XYZ-", "xyz:")
        )
        if not is_namespaced_hip3:
            return aliases
        if ":" in base:
            prefix, ticker = base.split(":", 1)
            if prefix and ticker:
                aliases.add(ticker)
                aliases.add(f"{prefix.upper()}-{ticker}")
        elif "-" in base:
            prefix, ticker = base.split("-", 1)
            if prefix and ticker:
                aliases.add(ticker)
                aliases.add(f"{prefix.lower()}:{ticker}")
        return aliases

    coin_to_symbol_map = defaultdict(set)
    symbol_to_coin_map = {}
    for k, v in markets.items():
        try:
            # 仅包含具有正确报价的永续市场。
            if not v.get("swap"):
                continue
            # 如果 "linear" 显式为 False，跳过；否则将缺失视为可接受。
            if v.get("linear") is False:
                continue
            if not k.endswith(f":{quote}"):
                continue
            coin = ""
            variants = set()
            for k0 in ["baseName", "base"]:
                if base := v.get(k0):
                    variants.add(base)
                    variants.add(base.replace("k", ""))
                    variants.add(remove_powers_of_ten(base))
                    cleaned = remove_powers_of_ten(base.replace("k", ""))
                    variants.add(cleaned)
                    if not coin:
                        coin = cleaned
                    variants.update(_namespaced_aliases(base, v))
            symbol_to_coin_map[k] = coin
            for variant in variants:
                existing = symbol_to_coin_map.get(variant)
                if existing and existing != coin:
                    continue
                symbol_to_coin_map[variant] = coin
                coin_to_symbol_map[variant].add(k)
            if symbol_id := v.get("id"):
                symbol_to_coin_map[symbol_id] = coin
        except Exception:
            # 跳过格式错误的市场条目，继续处理其他
            continue

    # 将集合转换为列表以支持 JSON 序列化/磁盘存储
    coin_to_symbol_map = {k: list(v) for k, v in coin_to_symbol_map.items()}
    return coin_to_symbol_map, symbol_to_coin_map


def _write_coin_symbol_maps(
    exchange: str, coin_to_symbol_map: dict, symbol_to_coin_map: dict, verbose=True
):
    """
    使用文件锁和原子写入将 coin/symbol 映射写入磁盘。
    使用 portalocker 防止多个机器人同时启动时的竞态条件。
    """
    # 首次访问时执行过期锁清理
    _cleanup_stale_symbol_map_locks()

    coin_to_symbol_map_path = make_get_filepath(
        os.path.join("caches", exchange, "coin_to_symbol_map.json")
    )
    symbol_to_coin_map_path = make_get_filepath(os.path.join("caches", "symbol_to_coin_map.json"))

    # 写入 coin_to_symbol_map（每交易所），带锁
    c2s_lock_path = coin_to_symbol_map_path + ".lock"
    try:
        with portalocker.Lock(c2s_lock_path, timeout=_SYMBOL_MAP_LOCK_TIMEOUT):
            if verbose:
                logging.debug("dumping coin_to_symbol_map %s", coin_to_symbol_map_path)
            _atomic_write_json(coin_to_symbol_map_path, coin_to_symbol_map, indent=4, sort_keys=True)
    except portalocker.LockException:
        logging.warning("Could not acquire lock for %s, skipping write", coin_to_symbol_map_path)

    # 写入 symbol_to_coin_map（全局），带锁
    s2c_lock_path = symbol_to_coin_map_path + ".lock"
    try:
        with portalocker.Lock(s2c_lock_path, timeout=_SYMBOL_MAP_LOCK_TIMEOUT):
            if verbose:
                logging.debug("dumping symbol_to_coin_map %s", symbol_to_coin_map_path)
            _atomic_write_json(symbol_to_coin_map_path, symbol_to_coin_map)
    except portalocker.LockException:
        logging.warning("Could not acquire lock for %s, skipping write", symbol_to_coin_map_path)

    # 更新内存缓存以避免过期读取
    try:
        st = os.stat(coin_to_symbol_map_path)
        _COIN_TO_SYMBOL_CACHE[exchange] = {
            "map": coin_to_symbol_map,
            "mtime_ns": st.st_mtime_ns,
            "size": st.st_size,
        }
    except Exception:
        pass

    try:
        st2 = os.stat(symbol_to_coin_map_path)
        _SYMBOL_TO_COIN_CACHE["map"] = symbol_to_coin_map
        _SYMBOL_TO_COIN_CACHE["mtime_ns"] = st2.st_mtime_ns
        _SYMBOL_TO_COIN_CACHE["size"] = st2.st_size
    except Exception:
        pass


def create_coin_symbol_map_cache(exchange: str, markets, quote=None, verbose=True):
    """
    高级函数，协调加载现有的 symbol_to_coin_map、从 markets 构建新映射、
    合并（新数据覆盖旧数据）并将结果写入磁盘。I/O 在此处执行；
    转换逻辑位于 _build_coin_symbol_maps() 中。

    使用文件锁使读-修改-写循环原子化，防止多个机器人同时启动时的竞态条件。

    注意：使用交易所的原始名称（如 "binance" 而非 "binanceusdm"）以保持
    与其他缓存路径的一致性。
    """
    # 首次访问时执行过期锁清理
    _cleanup_stale_symbol_map_locks()

    try:
        exchange = (exchange or "").lower()
        quote = get_quote(exchange, quote)

        symbol_to_coin_map_path = make_get_filepath(os.path.join("caches", "symbol_to_coin_map.json"))
        s2c_lock_path = symbol_to_coin_map_path + ".lock"

        # 在整个读-修改-写循环中锁定 symbol_to_coin_map
        try:
            with portalocker.Lock(s2c_lock_path, timeout=_SYMBOL_MAP_LOCK_TIMEOUT):
                # 持锁期间读取现有的 symbol->coin 映射
                symbol_to_coin_map = {}
                try:
                    if os.path.exists(symbol_to_coin_map_path):
                        with open(symbol_to_coin_map_path, "r") as f:
                            symbol_to_coin_map = json.load(f)
                except Exception as e:
                    logging.error("failed to load symbol_to_coin_map %s", e)

                # 从提供的 markets 构建新映射（纯逻辑）
                coin_to_symbol_map, new_symbol_to_coin_map = _build_coin_symbol_maps(markets, quote)

                # 合并：优先使用新发现的映射，同时保留其他
                symbol_to_coin_map.update(new_symbol_to_coin_map)

                # 仍持锁期间原子写入 symbol_to_coin_map
                if verbose:
                    logging.debug("dumping symbol_to_coin_map %s", symbol_to_coin_map_path)
                _atomic_write_json(symbol_to_coin_map_path, symbol_to_coin_map)

                # 更新内存缓存
                try:
                    st2 = os.stat(symbol_to_coin_map_path)
                    _SYMBOL_TO_COIN_CACHE["map"] = symbol_to_coin_map
                    _SYMBOL_TO_COIN_CACHE["mtime_ns"] = st2.st_mtime_ns
                    _SYMBOL_TO_COIN_CACHE["size"] = st2.st_size
                except Exception:
                    pass

            # 单独写入 coin_to_symbol_map（每交易所，使用自己的锁）
            coin_to_symbol_map_path = make_get_filepath(
                os.path.join("caches", exchange, "coin_to_symbol_map.json")
            )
            c2s_lock_path = coin_to_symbol_map_path + ".lock"
            try:
                with portalocker.Lock(c2s_lock_path, timeout=_SYMBOL_MAP_LOCK_TIMEOUT):
                    if verbose:
                        logging.debug("dumping coin_to_symbol_map %s", coin_to_symbol_map_path)
                    _atomic_write_json(
                        coin_to_symbol_map_path, coin_to_symbol_map, indent=4, sort_keys=True
                    )
                    # 更新内存缓存
                    try:
                        st = os.stat(coin_to_symbol_map_path)
                        _COIN_TO_SYMBOL_CACHE[exchange] = {
                            "map": coin_to_symbol_map,
                            "mtime_ns": st.st_mtime_ns,
                            "size": st.st_size,
                        }
                    except Exception:
                        pass
            except portalocker.LockException:
                logging.info(
                    "[mapping] could not acquire lock for %s, skipping write", coin_to_symbol_map_path
                )

        except portalocker.LockException:
            logging.info("[mapping] could not acquire lock for symbol map cache update, skipping")
            return False

        return True
    except Exception as e:
        logging.error("error with create_coin_symbol_map_cache %s: %s", exchange, e)
        return False


def coin_to_symbol(coin, exchange, quote=None, verbose=True):
    # 将 coin_to_symbol_map 缓存到内存，文件变更时重新加载
    if coin == "":
        return ""
    # 反规范化以使用规范形式作为缓存路径（如 "binance" 而非 "binanceusdm"）
    ex = to_standard_exchange_name(exchange or "")
    quote = get_quote(ex, quote)
    coin_sanitized = symbol_to_coin(coin, verbose=verbose)
    fallback = f"{coin_sanitized}/{quote}:{quote}"
    try:
        loaded = _load_coin_to_symbol_map(ex)
        candidates = loaded.get(coin_sanitized, []) if loaded else []
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            if verbose:
                logging.info(
                    "Multiple candidates for %s (raw=%s): %s",
                    coin_sanitized,
                    coin,
                    candidates,
                )
            return candidates[0]
        if loaded:
            # 映射存在但币种缺失
            warn_key = (ex, coin_sanitized)
            if warn_key not in _COIN_TO_SYMBOL_FALLBACKS:
                if verbose:
                    logging.warning(
                        "No mapping for %s (raw=%s) on %s; using fallback %s",
                        coin_sanitized,
                        coin,
                        ex,
                        fallback,
                    )
                _COIN_TO_SYMBOL_FALLBACKS.add(warn_key)
        else:
            warn_key = (ex, coin_sanitized)
            if warn_key not in _COIN_TO_SYMBOL_FALLBACKS:
                if verbose:
                    logging.warning(
                        "coin_to_symbol map for %s missing; using fallback for %s (raw=%s) -> %s",
                        ex,
                        coin_sanitized,
                        coin,
                        fallback,
                    )
                _COIN_TO_SYMBOL_FALLBACKS.add(warn_key)
    except Exception as e:
        if verbose:
            logging.error(
                "error with coin_to_symbol %s (raw=%s) %s: %s", coin_sanitized, coin, exchange, e
            )
    return fallback


def get_caller_name():
    return inspect.currentframe().f_back.f_back.f_code.co_name


def symbol_to_coin(symbol, verbose=True):
    # 将 symbol_to_coin_map 缓存到内存，文件变更时重新加载
    try:
        loaded = _load_symbol_to_coin_map()
        if symbol in loaded:
            return loaded[symbol]
        msg = f"failed to convert {symbol} to its coin with symbol_to_coin_map. Caller: {get_caller_name()}"
    except Exception:
        msg = f"failed to convert {symbol} to its coin with symbol_to_coin_map. Caller: {get_caller_name()}"

    if symbol == "":
        return ""
    if "/" in symbol:
        coin = symbol[: symbol.find("/")]
    else:
        coin = symbol
    for x in ["USDT", "USDC", "BUSD", "USD", "/:"]:
        coin = coin.replace(x, "")
    if "1000" in coin:
        istart = coin.find("1000")
        iend = istart + 1
        while True:
            if iend >= len(coin):
                break
            if coin[iend] != "0":
                break
            iend += 1
        coin = coin[:istart] + coin[iend:]
    if coin.startswith("k") and coin[1:].isupper():
        # hyperliquid 使用 kSHIB 代替 1000SHIB
        coin = coin[1:]
    if coin:
        msg += f". Using heuristics to guess coin: {coin}"
    if verbose:
        warn_key = str(symbol)
        if warn_key not in _SYMBOL_TO_COIN_WARNINGS:
            logging.warning(msg)
            _SYMBOL_TO_COIN_WARNINGS.add(warn_key)
    return coin


def coin_symbol_warning_counts() -> dict[str, int]:
    """返回回退转换的计数，用于摘要日志。"""
    return {
        "coin_to_symbol_fallbacks": len(_COIN_TO_SYMBOL_FALLBACKS),
        "symbol_to_coin_fallbacks": len(_SYMBOL_TO_COIN_WARNINGS),
    }


def _snapshot(value):
    return deepcopy(value) if isinstance(value, (dict, list)) else value


def _diff_snapshot(before, after):
    if before == after:
        return None
    return {"old": _snapshot(before), "new": _snapshot(after)}


def _resolve_fake_scenario_path(config) -> Optional[str]:
    live = config.get("live", {}) if isinstance(config, dict) else {}
    scenario_path = live.get("fake_scenario_path")
    if scenario_path:
        return scenario_path
    user = live.get("user")
    if not user:
        return None
    from procedures import load_user_info

    user_info = load_user_info(user)
    return user_info.get("fake_scenario_path")


def _load_fake_approved_coins(config, *, quote=None):
    live = config.get("live", {}) if isinstance(config, dict) else {}
    scenario_path = _resolve_fake_scenario_path(config)
    if not scenario_path:
        raise ValueError(
            "fake exchange approved_coins='all' requires live.fake_scenario_path "
            "or api-keys fake_scenario_path during startup"
        )
    from exchanges.fake import load_fake_scenario

    scenario = load_fake_scenario(scenario_path)
    symbols_config = scenario.get("symbols")
    if not isinstance(symbols_config, dict) or not symbols_config:
        raise ValueError("Fake scenario must define symbols to expand approved_coins='all'")
    approved_coins = []
    for symbol in symbols_config:
        if quote is not None:
            symbol_quote = str(symbol).split("/", 1)[1].split(":", 1)[0]
            if symbol_quote != str(quote):
                continue
        coin = symbol_to_coin(symbol)
        if coin:
            approved_coins.append(coin)
    return sorted(set(approved_coins))


async def format_approved_ignored_coins(config, exchanges: [str], quote=None, verbose=True):
    if isinstance(exchanges, str):
        exchanges = [exchanges]
    before_approved = deepcopy(config.get("live", {}).get("approved_coins"))
    before_ignored = deepcopy(config.get("live", {}).get("ignored_coins"))
    before_sources = deepcopy(config.get("_coins_sources", {}))
    coin_sources = config.setdefault("_coins_sources", {})
    approved_source = coin_sources.get("approved_coins", config.get("live", {}).get("approved_coins"))
    if approved_source is None:
        approved_source = _require_live_value(config, "approved_coins")
    coin_sources["approved_coins"] = deepcopy(approved_source)
    ac = normalize_coins_source(approved_source, allow_all=True)
    needs_market_expansion = any(
        _coins_source_side_is_all(ac[pside]) for pside in ("long", "short")
    )

    approved_coins_sorted = None
    if needs_market_expansion:
        approved_coins = set()
        standard_exchanges = []
        for ex in exchanges:
            if str(ex).lower() == "fake":
                approved_coins.update(_load_fake_approved_coins(config, quote=quote))
            else:
                standard_exchanges.append(ex)
        if standard_exchanges:
            marketss = await asyncio.gather(
                *[load_markets(ex, verbose=False, quote=quote) for ex in standard_exchanges]
            )
            marketss = [
                filter_markets(m, ex, quote=quote)[0] for m, ex in zip(marketss, standard_exchanges)
            ]
            for markets in marketss:
                for symbol in markets:
                    approved_coins.add(symbol_to_coin(symbol, verbose=verbose))
        approved_coins_sorted = sorted([x for x in approved_coins if x])

    config["live"]["approved_coins"] = {}
    for pside in ("long", "short"):
        if _coins_source_side_is_all(ac[pside]):
            config["live"]["approved_coins"][pside] = list(approved_coins_sorted or [])
        else:
            config["live"]["approved_coins"][pside] = [
                cf for x in ac[pside] if (cf := symbol_to_coin(x))
            ]

    ignored_source = coin_sources.get("ignored_coins", config.get("live", {}).get("ignored_coins"))
    if ignored_source is None:
        ignored_source = _require_live_value(config, "ignored_coins")
    coin_sources["ignored_coins"] = deepcopy(ignored_source)
    ic = normalize_coins_source(ignored_source, allow_all=False)
    config["live"]["ignored_coins"] = {
        pside: [cf for x in ic[pside] if (cf := symbol_to_coin(x))] for pside in ic
    }

    approved_diff = _diff_snapshot(before_approved, config["live"]["approved_coins"])
    ignored_diff = _diff_snapshot(before_ignored, config["live"]["ignored_coins"])
    sources_diff = _diff_snapshot(before_sources, config.get("_coins_sources", {}))
    if approved_diff or ignored_diff or sources_diff:
        from config_transform import record_transform

        details = {"exchanges": list(exchanges)}
        if approved_diff:
            details["approved_coins"] = approved_diff
        if ignored_diff:
            details["ignored_coins"] = ignored_diff
        if sources_diff:
            details["coin_sources"] = sources_diff
        record_transform(config, "format_approved_ignored_coins", details)


def _coins_source_side_is_all(value) -> bool:
    return isinstance(value, list) and len(value) == 1 and str(value[0]).strip().lower() == "all"


def normalize_coins_source(src, *, allow_all: bool = True):
    """
    始终返回：{'long': [symbols…], 'short': [symbols…]}
    - 处理：
        • 直接的币种列表或逗号分隔的字符串
        • 包含路径或字符串的列表/元组
        • 带 'long' / 'short' 键的字典，其值本身可以是
          字符串、列表或外部列表的路径
        • 用于 approved_coins 的显式 'all' 哨兵值
    """

    # --------------------------------------------------------------------- #
    #  辅助函数                                                              #
    # --------------------------------------------------------------------- #
    def _expand(seq):
        """展平 seq 并分割其中包含的逗号分隔字符串。"""
        out = []
        for item in seq:
            if isinstance(item, (list, tuple, set)):
                out.extend(_expand(item))  # 递归
            elif isinstance(item, str):
                out.extend(x.strip() for x in item.split(",") if x.strip())
            elif item is not None:
                out.append(str(item).strip())
        return out

    def _parse_jsonish(raw: str):
        raw = raw.strip()
        if not raw:
            return None
        if raw[0] not in "[{" or raw[-1] not in "]}":
            return None
        parsed = None
        try:
            import hjson

            parsed = hjson.loads(raw)
        except Exception:
            parsed = None
        if parsed is None:
            try:
                import json

                parsed = json.loads(raw)
            except Exception:
                parsed = None
        return parsed

    def _maybe_parse_jsonish(val):
        if isinstance(val, str):
            parsed = _parse_jsonish(val)
            return parsed if parsed is not None else val
        if isinstance(val, (list, tuple)) and val and all(isinstance(x, str) for x in val):
            joined = ",".join(x.strip() for x in val if x.strip())
            parsed = _parse_jsonish(joined)
            return parsed if parsed is not None else val
        return val

    def _load_if_file(x):
        """
        如果 *x*（或当 x 是单元素列表/元组时 *x[0]*）是可读的文件路径，
        则使用 `read_external_coins_lists` 加载它。否则原样返回 *x*。
        """

        def _maybe_read(path_candidate):
            resolved = _resolve_coins_file_path(path_candidate)
            if resolved is not None:
                return read_external_coins_lists(str(resolved))
            return None

        if isinstance(x, str):
            loaded = _maybe_read(x)
            if loaded is not None:
                return loaded
        if isinstance(x, (list, tuple)) and len(x) == 1 and isinstance(x[0], str):
            loaded = _maybe_read(x[0])
            if loaded is not None:
                return loaded
        return x

    def _normalize_side(value, side):
        """
        解析一个 *long*/*short* 条目：
        1. 必要时从文件加载。
        2. 如果加载器返回了字典，提取正确的方向。
        3. 使用 _expand 展平并分割，得到干净的列表。
        """
        value = _load_if_file(value)
        value = _maybe_parse_jsonish(value)

        if isinstance(value, dict) and set(value).issubset({"long", "short"}):
            value = value.get(side, [])

        if value in (None, "", [], (), {}, {"long": [], "short": []}):
            return []

        # 确保 _expand 有一个合理的序列
        if not isinstance(value, (list, tuple)):
            value = [value]

        expanded = _expand(value)
        if not expanded:
            return []
        if allow_all and len(expanded) == 1 and expanded[0].strip().lower() == "all":
            return ["all"]
        return expanded

    # --------------------------------------------------------------------- #
    #  主逻辑                                                                #
    # --------------------------------------------------------------------- #
    src = _load_if_file(src)  # 尝试加载 *src* 本身
    src = _maybe_parse_jsonish(src)

    # 情况 1 – 已经是带 'long' 和 'short' 键的字典
    if isinstance(src, dict):
        if not src:
            return {"long": [], "short": []}
        if set(src).issubset({"long", "short"}):
            return {
                "long": _normalize_side(src.get("long", []), "long"),
                "short": _normalize_side(src.get("short", []), "short"),
            }

    if src in (None, "", [], (), {}):
        return {"long": [], "short": []}

    if allow_all:
        global_tokens = _normalize_side(src, "long")
        if _coins_source_side_is_all(global_tokens):
            return {"long": ["all"], "short": ["all"]}

    # 情况 1 – 已经是带 'long' / 'short' 键的字典（包括部分键）
    if isinstance(src, dict) and set(src).issubset({"long", "short"}):
        return {
            "long": _normalize_side(src.get("long", []), "long"),
            "short": _normalize_side(src.get("short", []), "short"),
        }

    # 情况 2 – 其他情况对两个方向同等处理
    return {
        "long": global_tokens if allow_all else _normalize_side(src, "long"),
        "short": global_tokens if allow_all else _normalize_side(src, "short"),
    }


def read_external_coins_lists(filepath) -> dict:
    """
    读取文件路径并返回字典 {'long': [str], 'short': [str]}
    """
    try:
        with open(filepath, "r") as f:
            content = hjson.load(f)
        if isinstance(content, list) and all(isinstance(x, str) for x in content):
            return {"long": content, "short": content}
        if isinstance(content, dict) and all(
            pside in content
            and isinstance(content[pside], list)
            and all(isinstance(x, str) for x in content[pside])
            for pside in ["long", "short"]
        ):
            return content
    except Exception:
        # 回退到下方的纯文本读取
        pass
    with open(filepath, "r") as file:
        content = file.read().strip()
    # 检查内容是否为列表格式
    if content.startswith("[") and content.endswith("]"):
        # 移除方括号并按逗号分割
        items = content[1:-1].split(",")
        # 移除引号和空白
        items = [item.strip().strip("\"'") for item in items if item.strip()]
    elif all(
        line.strip().startswith('"') and line.strip().endswith('"')
        for line in content.split("\n")
        if line.strip()
    ):
        # 按换行分割，移除引号和空白
        items = [line.strip().strip("\"'") for line in content.split("\n") if line.strip()]
    else:
        # 按换行、逗号和/或空格分割，过滤掉空字符串
        items = [item.strip() for item in content.replace(",", " ").split() if item.strip()]
    return {"long": items, "short": items}


async def get_first_ohlcv_iteratively(cc, symbol):
    """返回 Bitget 市场最早的 OHLCV K 线。

    Bitget 不接受常规的 ``since`` 参数用于永续 OHLCV 查询。
    改为使用 ``params={"until": ms}`` 向后分页，空响应表示 ``until`` 早于
    该工具的上市时间。我们利用此行为对月线进行二分搜索，然后用日线获取
    来细化结果。返回值是第一个完整的 K 线 ``[timestamp, open, high, low, close, volume]``
    （如果可用），否则返回 ``None``。"""

    DAY_MS = 86_400_000
    MONTH_MS = 30 * DAY_MS

    async def fetch_month(until: Optional[int] = None):
        params = {"limit": 200}
        if until is not None:
            params["until"] = int(until)
        return await cc.fetch_ohlcv(symbol, timeframe="1M", params=params)

    async def fetch_day(until: int):
        return await cc.fetch_ohlcv(
            symbol, timeframe="1d", params={"until": int(until), "limit": 200}
        )

    month_chunk = await fetch_month()
    if not month_chunk:
        return None

    best_candle = month_chunk[0]
    first_month_ts = int(best_candle[0])

    # 二分搜索的初始边界：从接近零开始，将上界限制为当前时间。
    now_ms = int(getattr(cc, "milliseconds")())
    lo = 0
    hi = max(now_ms, int(month_chunk[-1][0]) + MONTH_MS)

    while hi - lo > MONTH_MS:
        mid = (lo + hi) // 2
        candles = await fetch_month(mid)
        if candles:
            new_first = int(candles[0][0])
            if new_first >= hi:
                break
            best_candle = candles[0]
            hi = new_first
            first_month_ts = new_first
        else:
            lo = mid

    # 顺序回退，以防月线分页被限制截断。
    while True:
        prev_until = max(0, first_month_ts - 1)
        if prev_until <= 0:
            break
        prev_chunk = await fetch_month(prev_until)
        if not prev_chunk:
            break
        prev_first = int(prev_chunk[0][0])
        if prev_first >= first_month_ts:
            break
        first_month_ts = prev_first
        best_candle = prev_chunk[0]

    # 在发现的月边界附近用日线细化。
    daily_chunk = await fetch_day(first_month_ts + 32 * DAY_MS)
    if daily_chunk:
        return daily_chunk[0]

    return best_candle


def deep_get(d, key_path, *args):
    """
    使用点表示法从嵌套字典中检索值。
    通过贪婪匹配处理可能包含点的键。
    """
    # 检查是否通过 *args 提供了默认值
    has_default = len(args) > 0
    default = args[0] if has_default else None

    segments = key_path.split(".")
    current = d

    i = 0
    while i < len(segments):
        found = False

        # 贪婪前瞻：尝试找到最长的匹配键
        for j in range(i + 1, len(segments) + 1):
            candidate_key = ".".join(segments[i:j])

            if isinstance(current, dict) and candidate_key in current:
                current = current[candidate_key]
                i = j  # 将指针向前跳转
                found = True
                break

        if not found:
            if has_default:
                return default
            raise KeyError(f"Path segment '{segments[i]}' not found in '{key_path}'")

    return current
