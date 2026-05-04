import glob
import json
import logging
import os
import asyncio
from datetime import datetime, timezone
from time import time
import numpy as np
import pprint
from copy import deepcopy
import argparse
import re
from collections import defaultdict
from collections.abc import Sized
from utils import (
    coin_to_symbol,
    symbol_to_coin,
    make_get_filepath,
    load_markets,
    get_file_mod_ms,
    date_to_ts,
    get_first_ohlcv_iteratively,
    load_ccxt_instance,
)
import sys
import passivbot_rust as pbr
from typing import Union, Optional, Set, Any, List
from pathlib import Path
import ccxt.async_support as ccxta

try:
    import hjson
except:
    print("hjson not found, trying without...")
    pass
try:
    import pandas as pd
except:
    print("pandas not found, trying without...")
    pass

from pure_funcs import (
    numpyize,
    ts_to_date,
    config_pretty_str,
    sort_dict_keys,
    flatten,
)


def get_all_eligible_symbols(exchange="binance"):
    exchange_map = {
        "bybit": "bybit",
        "binance": "binanceusdm",
        "gateio": "gateio",
        # "bitget": "bitget", TODO
        # "hyperliquid": "hyperliquid", TODO
    }
    quote_map = {k: "USDT" for k in exchange_map}
    quote_map["hyperliquid"] = "USDC"
    if exchange not in exchange_map:
        raise Exception(f"only exchanges {list(exchange_map.values())} are supported for backtesting")
    filepath = make_get_filepath(f"caches/{exchange}/eligible_symbols.json")
    loaded_json = None
    try:
        loaded_json = json.load(open(filepath))
        if utc_ms() - get_file_mod_ms(filepath) > 1000 * 60 * 60 * 24:
            print(f"Eligible_symbols cache more than 24h old. Fetching new.")
        else:
            return loaded_json
    except Exception as e:
        print(f"failed to load {filepath}. Fetching from {exchange}")
        pass
    try:
        quote = quote_map[exchange]
        import ccxt

        cc = getattr(ccxt, exchange_map[exchange])()
        markets = cc.fetch_markets()
        symbols = [
            x["symbol"] for x in markets if "symbol" in x and x["symbol"].endswith(f":{quote}")
        ]
        eligible_symbols = sorted(set([x.replace(f"/{quote}:", "") for x in symbols]))
        eligible_symbols = [x for x in eligible_symbols if x]
        json.dump(eligible_symbols, open(filepath, "w"))
        return eligible_symbols
    except Exception as e:
        print(f"error fetching eligible symbols {e}")
        if loaded_json:
            print(f"using cached data")
            return loaded_json
        raise Exception("unable to fetch or load from cache")


def dump_pretty_json(data: dict, filepath: str):
    try:
        with open(filepath, "w") as f:
            f.write(config_pretty_str(sort_dict_keys(data)) + "\n")
    except Exception as e:
        raise Exception(f"failed to dump data {filepath}: {e}")


def ensure_parent_directory(
    filepath: Union[str, Path], mode: int = 0o755, exist_ok: bool = True
) -> Path:
    """
    为给定文件路径创建目录和子目录（如果不存在），然后以 Path 对象返回该路径。

    Args:
        filepath: 表示文件或目录路径的字符串或 Path 对象
        mode: 目录权限（默认：0o755）
        exist_ok: 如果为 False，目录已存在时抛出 FileExistsError（默认：True）

    Returns:
        表示输入路径的 Path 对象

    Raises:
        TypeError: filepath 既不是 str 也不是 Path
        PermissionError: 用户没有创建目录的权限
        FileExistsError: 目录已存在且 exist_ok 为 False
    """
    try:
        # 转换为 Path 对象
        path = Path(filepath)

        # 判断路径是否指向目录
        # （以路径分隔符结尾或明确是一个目录）
        if str(path).endswith(os.path.sep) or (path.exists() and path.is_dir()):
            dirpath = path
        else:
            dirpath = path.parent

        # 如果目录不存在则创建
        if not dirpath.exists():
            dirpath.mkdir(parents=True, mode=mode, exist_ok=exist_ok)
        elif not exist_ok:
            raise FileExistsError(f"Directory already exists: {dirpath}")

        return path

    except TypeError as e:
        raise TypeError(f"filepath must be str or Path, not {type(filepath)}") from e
    except PermissionError as e:
        raise PermissionError(f"Permission denied creating directory: {dirpath}") from e
    except Exception as e:
        raise RuntimeError(f"Error processing filepath: {str(e)}") from e


def load_user_info(user: str, api_keys_path="api-keys.json") -> dict:
    """从 api-keys.json 加载用户凭据。

    返回用户条目的所有字段，加上旧版字段的空字符串默认值，
    以保持与现有机器人的向后兼容性。
    """
    if api_keys_path is None:
        api_keys_path = "api-keys.json"
    try:
        api_keys = json.load(open(api_keys_path))
    except Exception as e:
        raise Exception(f"error loading api keys file {api_keys_path} {e}")
    if user not in api_keys:
        raise Exception(f"user {user} not found in {api_keys_path}")

    # 旧版字段使用空字符串默认值（向后兼容）
    legacy_fields = [
        "exchange",
        "key",
        "secret",
        "passphrase",
        "wallet_address",
        "private_key",
        "is_vault",
    ]
    result = {k: "" for k in legacy_fields}

    # 覆盖用户条目的所有字段（CCXTBot 直接透传）
    result.update(api_keys[user])

    return result


def load_exchange_key_secret_passphrase(
    user: str, api_keys_path="api-keys.json"
) -> (str, str, str, str):
    if api_keys_path is None:
        api_keys_path = "api-keys.json"
    try:
        keyfile = json.load(open(api_keys_path))
        if user in keyfile:
            return (
                keyfile[user]["exchange"],
                keyfile[user]["key"],
                keyfile[user]["secret"],
                keyfile[user]["passphrase"] if "passphrase" in keyfile[user] else "",
            )
        else:
            print("Looks like the keys aren't configured yet, or you entered the wrong username!")
        raise Exception("API KeyFile Missing!")
    except FileNotFoundError:
        print("File Not Found!")
        raise Exception("API KeyFile Missing!")


def _broker_codes_path() -> Path:
    env_path = os.environ.get("PASSIVBOT_BROKER_CODES_PATH")
    if env_path:
        return Path(env_path).expanduser()

    repo_path = Path(__file__).resolve().parents[1] / "broker_codes.hjson"
    if repo_path.exists():
        return repo_path

    # 打包/容器化部署可能将 broker_codes.hjson 放在
    # 进程工作目录旁而非源码目录旁。
    return Path.cwd() / "broker_codes.hjson"


def load_broker_codes() -> dict[str, Any]:
    path = _broker_codes_path()
    try:
        with path.open() as f:
            codes = hjson.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"broker code registry not found at {path}. "
            "Run from the Passivbot checkout, include broker_codes.hjson in the deployment, "
            "or set PASSIVBOT_BROKER_CODES_PATH."
        ) from e
    except (OSError, ValueError) as e:
        raise RuntimeError(f"failed to load broker code registry from {path}: {e}") from e

    if not isinstance(codes, dict):
        raise TypeError(f"broker code registry {path} must contain a top-level object")
    return codes


def load_broker_code(exchange: str) -> Any:
    codes = load_broker_codes()
    if exchange not in codes:
        raise KeyError(
            f"broker code registry has no entry for exchange {exchange!r}. "
            "Add a broker code, or add an explicit null entry for supported exchanges "
            f"without broker-code attribution. Known entries: {sorted(codes)}"
        )
    code = codes[exchange]
    if code is None:
        return ""
    if not isinstance(code, (str, dict)):
        raise TypeError(
            f"broker code registry entry for {exchange!r} must be a string, object, or null"
        )
    return code


def print_(args, r=False, n=False):
    line = ts_to_date(utc_ms())[:19] + "  "
    # line = ts_to_date(local_time())[:19] + '  '  # 使用本地时间替代 UTC
    str_args = "{} " * len(args)
    line += str_args.format(*args)
    if n:
        print("\n" + line, end=" ")
    elif r:
        print("\r" + line, end=" ")
    else:
        print(line)
    return line


def local_time() -> float:
    return datetime.now().astimezone().timestamp() * 1000


def print_async_exception(coro):
    if isinstance(coro, list):
        for elm in coro:
            print_async_exception(elm)
    try:
        print(f"result: {coro.result()}")
    except:
        pass
    try:
        print(f"exception: {coro.exception()}")
    except:
        pass
    try:
        print(f"returned: {coro}")
    except:
        pass


async def get_first_timestamps_unified(coins: List[str], exchange: str = None):
    """
    默认返回每个币种在所有交易所上最早出现的时间戳。
    如果指定了 'exchange'，则仅返回该交易所的最早时间戳。

    每次以 10 个币种为一批发送请求，每批处理完后立即将结果写入磁盘。

    :param coins: 需要获取首次时间戳数据的币种符号列表。
    :param exchange: 可选字符串，指定单个交易所（如 'binanceusdm'）。
                     设置后仅返回该交易所的最早时间戳。
    :return: 币种 -> 最早时间戳（毫秒）的字典。如果提供了 `exchange`，
             则仅返回指定交易所的条目。
    """

    # cheap_exchanges = {"binanceusdm", "bybit", "okx", "gateio", "hyperliquid"}  # 低成本交易所
    cheap_exchanges = {"binanceusdm", "bybit", "okx"}

    async def fetch_ohlcv_with_start(exchange_name, symbol, cc):
        """
        获取 `exchange_name` 上 `symbol` 的 OHLCV 数据，根据交易所已知的数据可用性
        从特定日期范围开始获取。返回 K 线数据列表。
        """
        if exchange_name == "binanceusdm":
            # 数据实际上从很久以前就开始
            return await cc.fetch_ohlcv(symbol, since=1, timeframe="1d")

        elif exchange_name in ["bybit", "gateio"]:
            # 数据从 2018 年开始
            return await cc.fetch_ohlcv(symbol, since=int(date_to_ts("2018-01-01")), timeframe="1d")

        elif exchange_name == "okx":
            # 月线时间框架；数据从 2018 年开始
            return await cc.fetch_ohlcv(symbol, since=int(date_to_ts("2018-01-01")), timeframe="1M")

        elif exchange_name == "bitget":
            first_candle = await get_first_ohlcv_iteratively(cc, symbol)
            return [first_candle] if first_candle else []

        else:  # 如 'hyperliquid'
            # 周线时间框架；数据从 2021 年开始
            return await cc.fetch_ohlcv(symbol, since=int(date_to_ts("2021-01-01")), timeframe="1w")

    # 去重并排序输入币种以保持一致性
    coins = sorted(set(symbol_to_coin(coin) for coin in coins))

    # 缓存文件路径
    cache_fpath = make_get_filepath("caches/first_ohlcv_timestamps_unified.json")
    cache_fpath_exchange_specific = "caches/first_ohlcv_timestamps_unified_exchange_specific.json"

    # 用于存储时间戳的内存字典
    ftss = {}  # 币种 -> 跨所有交易所的最早时间戳
    ftss_exchange_specific = {}  # 币种 -> {交易所 -> 最早时间戳}

    # 加载主缓存（如果存在）
    if os.path.exists(cache_fpath):
        try:
            with open(cache_fpath, "r") as f:
                ftss = json.load(f)
            logging.debug("loaded first_ohlcv_timestamps from %s (%d coins)", cache_fpath, len(ftss))
        except Exception as e:
            logging.warning("error reading %s: %s", cache_fpath, e)

    # 加载交易所特定缓存（如果存在）
    if os.path.exists(cache_fpath_exchange_specific):
        try:
            with open(cache_fpath_exchange_specific, "r") as f:
                ftss_exchange_specific = json.load(f)
            logging.debug(
                "loaded first_ohlcv_timestamps (exchange-specific) from %s (%d coins)",
                cache_fpath_exchange_specific,
                len(ftss_exchange_specific),
            )
        except Exception as e:
            logging.warning("error reading %s: %s", cache_fpath_exchange_specific, e)

    # 如果指定了交易所，处理 "binance" 别名
    if exchange == "binance":
        exchange = "binanceusdm"

    def _valid_first_timestamp(value: Any) -> bool:
        try:
            return float(value) > 0.0
        except Exception:
            return False

    # 1) 如果未指定交易所且所有币种都有有效的缓存时间戳，直接返回 ftss
    if exchange is None:
        if all(_valid_first_timestamp(ftss.get(coin)) for coin in coins):
            return ftss

    # 2) 如果请求了特定交易所：
    else:
        # 如果所有币种都存在于该交易所的特定缓存中，返回它们
        if all(_valid_first_timestamp(ftss_exchange_specific.get(coin, {}).get(exchange)) for coin in coins):
            return {c: ftss_exchange_specific[c][exchange] for c in coins}

    # 查找缓存中缺失或时间戳无效的币种
    if exchange is None:
        missing_coins = {c for c in coins if not _valid_first_timestamp(ftss.get(c))}
    else:
        missing_coins = {
            c for c in coins if not _valid_first_timestamp(ftss_exchange_specific.get(c, {}).get(exchange))
        }
    if not missing_coins:
        if exchange is not None:
            return {c: ftss_exchange_specific.get(c, {}).get(exchange, 0.0) for c in coins}
        return ftss

    print("Missing coins:", sorted(missing_coins))

    # 交易所 -> 报价货币映射
    exchange_map = {
        "okx": "USDT",
        "binanceusdm": "USDT",
        "bybit": "USDT",
        "gateio": "USDT",
        "bitget": "USDT",
        "hyperliquid": "USDC",
    }

    # 为每个交易所初始化 ccxt 客户端
    ccxt_clients = {}
    for ex_name in sorted(exchange_map):
        try:
            ccxt_clients[ex_name] = load_ccxt_instance(ex_name)
        except Exception as e:
            print(f"Error loading {ex_name} from ccxt. Skipping. {e}")
            del exchange_map[ex_name]
            if ex_name in ccxt_clients:
                del ccxt_clients[ex_name]
    try:
        print("Loading markets for each exchange...")
        load_tasks = {}
        for ex_name in sorted(ccxt_clients):
            try:
                load_tasks[ex_name] = load_markets(ex_name)
            except Exception as e:
                print(f"Error creating task for {ex_name}: {e}")
                del ccxt_clients[ex_name]
                if ex_name in exchange_map:
                    del exchange_map[ex_name]
        all_markets = {}
        for ex_name, task in load_tasks.items():
            try:
                res = await task
                all_markets[ex_name] = res
            except Exception as e:
                print(f"Warning: failed to load markets for {ex_name}: {e}")
                del ccxt_clients[ex_name]
                if ex_name in exchange_map:
                    del exchange_map[ex_name]
        # 以每批 10 个币种的方式获取缺失的币种，避免过载
        BATCH_SIZE = 10
        missing_coins = sorted(missing_coins)

        for i in range(0, len(missing_coins), BATCH_SIZE):
            batch = missing_coins[i : i + BATCH_SIZE]
            print(f"\nProcessing batch: {batch}")

            # 为此批次中每个币种/交易所对创建任务
            tasks = {}
            bitget_symbols = {}
            for coin in batch:
                tasks[coin] = {}
                for ex_name, quote in exchange_map.items():
                    # 将币种转换为交易所识别的符号，如 "BTC/USDT:USDT"
                    symbol = coin_to_symbol(coin, ex_name)
                    if not symbol:
                        continue
                    if ex_name == "bitget":
                        bitget_symbols[coin] = symbol
                        continue
                    tasks[coin][ex_name] = asyncio.create_task(
                        fetch_ohlcv_with_start(ex_name, symbol, ccxt_clients[ex_name])
                    )

            # 收集此批次的所有结果
            batch_results = {}
            fast_exchanges = [ex for ex in exchange_map if ex != "bitget"]
            for coin in batch:
                batch_results[coin] = {}
                for ex_name in fast_exchanges:
                    if ex_name in tasks[coin]:
                        try:
                            data = await tasks[coin][ex_name]
                            if data:
                                batch_results[coin][ex_name] = data
                                print(
                                    f"Fetched {ex_name} {coin} => first candle: {data[0] if data else 'no data'}"
                                )
                        except Exception as e:
                            print(f"Warning: failed to fetch OHLCV for {coin} on {ex_name}: {e}")

            # 第二轮：仅为未解决的币种发起开销较大的 Bitget 请求。
            for coin in batch:
                symbol = bitget_symbols.get(coin)
                if not symbol:
                    continue
                has_valid = False
                for ex_name, arr in batch_results[coin].items():
                    if ex_name not in cheap_exchanges:
                        continue
                    if arr and arr[0][0] > 1262304000000.0:
                        has_valid = True
                        break
                if has_valid:
                    continue
                try:
                    data = await fetch_ohlcv_with_start("bitget", symbol, ccxt_clients["bitget"])
                    if data:
                        batch_results[coin]["bitget"] = data
                        print(
                            f"Fetched bitget {coin} => first candle: {data[0] if data else 'no data'}"
                        )
                except Exception as e:
                    print(f"Warning: failed to fetch OHLCV for {coin} on bitget: {e}")

            # 处理此批次中每个币种的结果
            for coin in batch:
                exchange_data = batch_results.get(coin, {})
                fts_for_this_coin = {ex: 0.0 for ex in exchange_map}  # 所有交易所默认 0.0
                earliest_candidates = []

                for ex_name, arr in exchange_data.items():
                    if arr and len(arr) > 0:
                        # arr[0][0] 是毫秒时间戳
                        # 仅考虑 2010 年之后的"合理"时间戳
                        if arr[0][0] > 1262304000000.0:
                            earliest_candidates.append(arr[0][0])
                            fts_for_this_coin[ex_name] = arr[0][0]

                # 如果找到有效时间戳，保留最早的
                if earliest_candidates:
                    ftss[coin] = min(earliest_candidates)
                else:
                    print(f"No valid first timestamp for coin {coin}")
                    ftss[coin] = 0.0

                # 更新交易所特定字典
                ftss_exchange_specific[coin] = fts_for_this_coin

            # 每批处理后立即将更新的字典写入磁盘
            with open(cache_fpath, "w") as f:
                json.dump(ftss, f, indent=4, sort_keys=True)

            with open(cache_fpath_exchange_specific, "w") as f:
                json.dump(ftss_exchange_specific, f, indent=4, sort_keys=True)

            print(f"Finished batch {batch}. Caches updated.")

        # 关闭所有 ccxt 客户端会话

        # 如果请求了单个交易所，仅返回该交易所特定的时间戳。
        if exchange is not None:
            return {coin: ftss_exchange_specific.get(coin, {}).get(exchange, 0.0) for coin in coins}

        # 否则，返回跨交易所的最早时间戳
        return ftss
    finally:
        await asyncio.gather(
            *(ccxt_clients[e].close() for e in ccxt_clients if hasattr(ccxt_clients[e], "close"))
        )


def assert_correct_ccxt_version(version=None, ccxt=None):
    if os.environ.get("SKIP_CCXT_ASSERT", "").lower() in ("1", "true", "yes"):
        return
    if version is None:
        version = load_ccxt_version()
    if ccxt is None:
        import ccxt

    assert (
        ccxt.__version__ == version
    ), f"Currently ccxt {ccxt.__version__} is installed. Please pip reinstall requirements.txt or install ccxt v{version} manually"


def load_ccxt_version():
    try:
        # 获取当前脚本的目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # 构建 requirements.txt 文件的路径
        requirements_path = os.path.join(script_dir, "..", "requirements-live.txt")

        # 打开并读取 requirements.txt 文件
        with open(requirements_path, "r") as f:
            lines = f.readlines()

        # 查找包含 'ccxt' 的行并提取版本号
        ccxt_line = [line for line in lines if "ccxt" in line][0].strip()
        return ccxt_line[ccxt_line.find("==") + 2 :]
    except Exception as e:
        print(f"failed to load ccxt version {e}")
        return None


def get_size(obj: Any, seen: Set = None) -> int:
    """
    递归计算对象及其内容的字节大小。

    Args:
        obj: 要计算大小的对象
        seen: 已见过的对象 id 集合（用于处理循环引用）

    Returns:
        总字节大小
    """
    # 如果是顶层调用，初始化已见对象集合
    if seen is None:
        seen = set()

    # 获取对象 id 以处理循环引用
    obj_id = id(obj)

    # 如果对象已见过，不再重复计算
    if obj_id in seen:
        return 0

    # 将此对象标记为已见
    seen.add(obj_id)

    # 获取对象的基本大小
    size = sys.getsizeof(obj)

    # 处理不同类型的容器
    if isinstance(obj, (str, bytes, bytearray)):
        pass  # 基本大小已包含内容

    elif isinstance(obj, (tuple, list, set, frozenset)):
        size += sum(get_size(item, seen) for item in obj)

    elif isinstance(obj, dict):
        size += sum(get_size(k, seen) + get_size(v, seen) for k, v in obj.items())

    elif hasattr(obj, "__dict__"):
        # 添加自定义对象的所有属性大小
        size += get_size(obj.__dict__, seen)

    elif hasattr(obj, "__slots__"):
        # 处理使用 __slots__ 的对象
        size += sum(
            get_size(getattr(obj, attr), seen) for attr in obj.__slots__ if hasattr(obj, attr)
        )

    return size


def format_size(size_bytes: int) -> str:
    """
    将字节大小格式化为人类可读的字符串。

    Args:
        size_bytes: 字节大小

    Returns:
        格式化后的字符串，如 '1.23 MB'
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def compare_dicts_table(dict1, dict2, dict1_name="Dict 1", dict2_name="Dict 2"):
    """
    以整齐的表格格式比较两个具有相同键的字典。

    Args:
        dict1: 第一个字典
        dict2: 第二个字典
        dict1_name: 第一个字典列的名称
        dict2_name: 第二个字典列的名称
    """
    # 获取所有键（假设键相同）
    keys = list(dict1.keys())

    # 计算列宽
    key_width = max(len("Key"), max(len(str(k)) for k in keys))
    val1_width = max(len(dict1_name), max(len(str(dict1[k])) for k in keys))
    val2_width = max(len(dict2_name), max(len(str(dict2[k])) for k in keys))

    # 创建分隔线
    separator = (
        "+"
        + "-" * (key_width + 2)
        + "+"
        + "-" * (val1_width + 2)
        + "+"
        + "-" * (val2_width + 2)
        + "+"
    )

    # 打印表格
    print(separator)
    print(f"| {'Key':<{key_width}} | {dict1_name:<{val1_width}} | {dict2_name:<{val2_width}} |")
    print(separator)

    for key in sorted(keys):
        val1 = str(pbr.round_dynamic(dict1[key], 4))
        val2 = str(pbr.round_dynamic(dict2[key], 4))
        print(f"| {str(key):<{key_width}} | {val1:<{val1_width}} | {val2:<{val2_width}} |")

    print(separator)


def main():
    pass


if __name__ == "__main__":
    main()
