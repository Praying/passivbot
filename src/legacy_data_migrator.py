"""
旧版数据迁移工具。

本模块提供以下功能：
1. 标准化缓存目录名称（如 binanceusdm -> binance）
2. 将旧版数据从 historical_data/ 迁移到 caches/ohlcv/
3. 合并因路径清理不一致而产生的重复符号目录

迁移是非破坏性的：旧版数据被复制（非移动），historical_data/ 目录保持不变，由用户手动删除。
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import sys

# Windows 兼容性检查（与 candlestick_manager.py 相同逻辑）
# 参见：https://github.com/enarjord/passivbot/issues/547
windows_compatibility = (
    sys.platform.startswith("win") or os.environ.get("WINDOWS_COMPATIBILITY") == "1"
)


def _sanitize_symbol(symbol: str) -> str:
    """
    将符号转换为文件系统安全的路径组件。

    与 candlestick_manager._sanitize_symbol() 使用相同逻辑以确保一致性。
    非 Windows：LINK/USDT:USDT -> LINK_USDT:USDT（保留冒号）
    Windows：LINK/USDT:USDT -> LINK_USDT_USDT（替换冒号）
    """
    sanitized = symbol.replace("/", "_")
    if windows_compatibility:
        sanitized = sanitized.replace(":", "_")
    return sanitized


# ccxt 交易所 ID 到标准（简短）名称的映射。
# 仅包含 ccxt ID 与标准名称不同的条目。
# 重要：绝不要添加恒等映射（如 "gateio": "gateio"），否则会导致合并逻辑
# 在合并到自身后删除该目录！
CCXT_ID_TO_STANDARD = {
    "binanceusdm": "binance",
    "kucoinfutures": "kucoin",
    "krakenfutures": "kraken",
    # gateio、bybit、okx、hyperliquid 在 ccxt 和标准中使用相同名称
}

# 反向映射
STANDARD_TO_CCXT_ID = {v: k for k, v in CCXT_ID_TO_STANDARD.items()}

# 在 historical_data/ 中搜索的旧版目录名称模式
LEGACY_DIR_PATTERNS = [
    "ohlcvs_binanceusdm",
    "ohlcvs_binance",
    "ohlcvs_futures",  # Old Binance futures path
    "ohlcvs_bybit",
    "ohlcvs_kucoinfutures",
    "ohlcvs_kucoin",
    "ohlcvs_okx",
    "ohlcvs_gateio",
    "ohlcvs_hyperliquid",
]


def standardize_cache_directories(cache_base: str = "caches/ohlcv", dry_run: bool = False) -> int:
    """
    将缓存目录从 ccxt ID 重命名为标准名称。

    例如：
    - caches/ohlcv/binanceusdm/ -> caches/ohlcv/binance/
    - caches/ohlcv/kucoinfutures/ -> caches/ohlcv/kucoin/

    同时移除作为变通方案创建的符号链接。

    Args:
        cache_base: OHLCV 缓存的基础目录（默认："caches/ohlcv"）
        dry_run: 如果为 True，仅记录将执行的操作而不实际更改

    Returns:
        重命名/清理的目录数量
    """
    base_path = Path(cache_base)
    if not base_path.exists():
        return 0

    changes = 0

    # 首先移除符号链接
    for item in base_path.iterdir():
        if item.is_symlink():
            target = os.readlink(str(item))
            if dry_run:
                logging.info("[dry-run] Would remove symlink %s -> %s", item, target)
            else:
                logging.info("Removing cache symlink %s -> %s", item, target)
                item.unlink()
            changes += 1

    # 然后将目录从 ccxt ID 重命名为标准名称
    for ccxt_id, standard_name in CCXT_ID_TO_STANDARD.items():
        # 安全检查：跳过恒等映射以避免将目录合并到自身
        # 然后删除它（导致灾难性数据丢失）
        if ccxt_id == standard_name:
            continue

        ccxt_path = base_path / ccxt_id
        standard_path = base_path / standard_name

        if not ccxt_path.exists() or ccxt_path.is_symlink():
            continue

        if standard_path.exists() and not standard_path.is_symlink():
            # 两者都存在 - 需要合并
            if dry_run:
                logging.info("[dry-run] Would merge %s into %s", ccxt_path, standard_path)
            else:
                logging.info("Merging cache directory %s into %s", ccxt_path, standard_path)
                _merge_cache_directories(ccxt_path, standard_path)
                shutil.rmtree(ccxt_path)
            changes += 1
        else:
            # 简单重命名
            if dry_run:
                logging.info("[dry-run] Would rename %s to %s", ccxt_path, standard_path)
            else:
                logging.info("Renaming cache directory %s to %s", ccxt_path, standard_path)
                ccxt_path.rename(standard_path)
            changes += 1

    return changes


def _merge_cache_directories(source: Path, dest: Path) -> None:
    """
    将源缓存目录合并到目标目录，保留较新的文件。

    冲突时（两边都有同一文件），保留修改时间较新的文件。
    """
    for item in source.rglob("*"):
        if not item.is_file():
            continue

        rel_path = item.relative_to(source)
        dest_file = dest / rel_path

        if dest_file.exists():
            # 保留较新的文件
            if item.stat().st_mtime > dest_file.stat().st_mtime:
                dest_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(item, dest_file)
        else:
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dest_file)


def merge_duplicate_symbol_directories(
    cache_base: str = "caches/ohlcv",
    dry_run: bool = False,
) -> int:
    """
    合并因路径清理不一致而产生的重复符号目录。

    问题：旧版迁移器使用 `symbol.replace("/", "_").replace(":", "_")`
    但 CandlestickManager 使用 `_sanitize_symbol()`，后者仅在 Windows 上替换 ":"。

    这导致了重复目录，如：
    - LINK_USDT_USDT（错误 - 来自旧版迁移器）
    - LINK_USDT:USDT（正确 - 来自 CandlestickManager）

    此函数查找并合并这些重复目录。

    Returns:
        合并/移除的目录数量
    """
    base_path = Path(cache_base)
    if not base_path.exists():
        return 0

    merged_count = 0

    # 遍历交易所目录
    for exchange_dir in base_path.iterdir():
        if not exchange_dir.is_dir():
            continue

        # 遍历时间周期目录（如 1m、5m）
        for tf_dir in exchange_dir.iterdir():
            if not tf_dir.is_dir():
                continue

            # 查找所有符号目录
            symbol_dirs = [d for d in tf_dir.iterdir() if d.is_dir()]

            # 按规范符号分组（即 _sanitize_symbol 的输出）
            # 我们需要检测仅在 ":" 与 "_" 上有差异的目录
            groups: Dict[str, List[Path]] = {}

            for sym_dir in symbol_dirs:
                dir_name = sym_dir.name

                # Try to extract the original symbol and compute canonical path
                # Pattern: COIN_QUOTE_QUOTE or COIN_QUOTE:QUOTE
                # e.g., LINK_USDT_USDT or LINK_USDT:USDT

                # The canonical form depends on windows_compatibility
                # On non-Windows: LINK_USDT:USDT
                # On Windows: LINK_USDT_USDT

                # 规范化为将两种变体归为一组的键
                # 将所有 : 替换为 _ 以用于分组
                group_key = dir_name.replace(":", "_")

                if group_key not in groups:
                    groups[group_key] = []
                groups[group_key].append(sym_dir)

            # 处理含重复项的分组
            for group_key, dirs in groups.items():
                if len(dirs) <= 1:
                    continue

                # 确定规范目录名称
                # 从纯下划线形式重建符号
                # 例如 LINK_USDT_USDT -> LINK/USDT:USDT -> _sanitize_symbol -> 规范形式

                # 查找"正确"目录（匹配 _sanitize_symbol 输出的那个）
                correct_dir = None
                wrong_dirs = []

                for d in dirs:
                    # 检查是否匹配 _sanitize_symbol 的输出
                    # 正确的目录在非 windows_compatibility 时包含 ":"
                    if windows_compatibility:
                        # 在 Windows 上，纯下划线形式是正确的
                        if ":" not in d.name:
                            correct_dir = d
                        else:
                            wrong_dirs.append(d)
                    else:
                        # 在非 Windows 上，带冒号的版本是正确的
                        if ":" in d.name:
                            correct_dir = d
                        else:
                            wrong_dirs.append(d)

                if correct_dir is None:
                    # 所有目录都使用了错误格式 - 选择一个作为目标
                    # 并将其名称转换为正确格式
                    source_dir = dirs[0]
                    wrong_dirs = dirs[1:]

                    # 重建正确名称：将 USDT/USDC 前的最后一个 _ 替换为 :
                    # 例如 LINK_USDT_USDT -> LINK_USDT:USDT
                    correct_name = _convert_to_canonical_symbol_path(source_dir.name)
                    correct_dir = source_dir.parent / correct_name

                    if dry_run:
                        logging.info("[dry-run] Would rename %s to %s", source_dir, correct_dir)
                    else:
                        logging.info(
                            "[boot] Renaming symbol directory %s to %s", source_dir.name, correct_name
                        )
                        source_dir.rename(correct_dir)
                    merged_count += 1

                # 将错误目录合并到正确目录
                for wrong_dir in wrong_dirs:
                    if not wrong_dir.exists():
                        continue

                    if dry_run:
                        logging.info(
                            "[dry-run] Would merge %s into %s and delete", wrong_dir, correct_dir
                        )
                    else:
                        logging.info(
                            "[boot] Merging duplicate symbol directory %s into %s",
                            wrong_dir.name,
                            correct_dir.name,
                        )
                        _merge_cache_directories(wrong_dir, correct_dir)
                        shutil.rmtree(wrong_dir)
                    merged_count += 1

    return merged_count


def normalize_ccxt_volume_to_base(exchange_id: str, close: float, volume: float) -> float:
    """
    将 ccxt OHLCV 成交量标准化为基础成交量。

    部分交易所（特别是 gateio swap）在 ccxt OHLCV 的 "volume" 字段中报告报价成交量。
    对于这些交易所，需除以收盘价来获得基础成交量。
    """
    exid = str(exchange_id).lower()
    if exid == "gateio" and close > 0:
        return float(volume) / float(close)
    return float(volume)


def _convert_to_canonical_symbol_path(dir_name: str) -> str:
    """
    将符号目录名称转换为规范格式。

    非 Windows：LINK_USDT_USDT -> LINK_USDT:USDT
    Windows：保持不变（仅下划线）

    处理以下模式：
    - COIN_QUOTE_QUOTE（如 LINK_USDT_USDT）
    - 1000COIN_QUOTE_QUOTE（如 1000PEPE_USDT_USDT）
    """
    if windows_compatibility:
        return dir_name

    # 模式：取到最后一个 _USDT 或 _USDC 为止的所有内容，
    # 然后将最终报价货币前的下划线替换为 :
    # 例如 LINK_USDT_USDT -> LINK_USDT:USDT
    #     BTC_USDC_USDC -> BTC_USDC:USDC

    for quote in ["USDT", "USDC"]:
        suffix = f"_{quote}_{quote}"
        if dir_name.endswith(suffix):
            base = dir_name[: -len(suffix)]
            return f"{base}_{quote}:{quote}"

    # 回退：如果模式不匹配则不做更改
    return dir_name


def get_legacy_exchange_name(dir_name: str) -> Optional[str]:
    """
    从旧版目录名称中提取交易所名称。

    示例：
    - "ohlcvs_binanceusdm" -> "binance"
    - "ohlcvs_bybit" -> "bybit"
    - "ohlcvs_futures" -> "binance"（旧版 Binance 合约路径）
    """
    if not dir_name.startswith("ohlcvs_"):
        return None

    suffix = dir_name[7:]  # 去掉 "ohlcvs_" 前缀

    # 旧版 Binance 合约路径的特殊情况
    if suffix == "futures":
        return "binance"

    # 检查是否是需要标准化的 ccxt ID
    if suffix in CCXT_ID_TO_STANDARD:
        return CCXT_ID_TO_STANDARD[suffix]

    return suffix


def scan_legacy_data(
    historical_data_path: str = "historical_data",
) -> Dict[str, Dict[str, List[str]]]:
    """
    扫描 historical_data/ 中的旧版 OHLCV 分片。

    Returns:
        字典映射 exchange -> coin -> 日期字符串列表（YYYY-MM-DD）
    """
    result: Dict[str, Dict[str, List[str]]] = {}
    base = Path(historical_data_path)

    if not base.exists():
        return result

    for legacy_dir in base.iterdir():
        if not legacy_dir.is_dir():
            continue

        exchange = get_legacy_exchange_name(legacy_dir.name)
        if exchange is None:
            continue

        if exchange not in result:
            result[exchange] = {}

        # 扫描币种目录
        for coin_dir in legacy_dir.iterdir():
            if not coin_dir.is_dir():
                continue

            coin = coin_dir.name
            dates = []

            # 查找所有 .npy 分片文件
            for shard_file in coin_dir.glob("*.npy"):
                # 从文件名提取日期（YYYY-MM-DD.npy）
                date_str = shard_file.stem
                if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
                    dates.append(date_str)

            if dates:
                result[exchange][coin] = sorted(dates)

    return result


def migrate_legacy_data_for_exchange(
    exchange: str,
    cache_base: str = "caches/ohlcv",
    historical_data_path: str = "historical_data",
    dry_run: bool = False,
    quote: str = "USDT",
) -> Tuple[int, int]:
    """
    迁移特定交易所的旧版数据。

    Args:
        exchange: 标准交易所名称（如 "binance"）
        cache_base: OHLCV 缓存的基础目录
        historical_data_path: 旧版 historical_data 目录路径
        dry_run: 如果为 True，仅记录将执行的操作
        quote: 用于构建符号路径的报价货币

    Returns:
        (已迁移文件数, 已跳过文件数) 元组
    """
    # 在此处导入 CANDLE_DTYPE 以避免循环导入
    from candlestick_manager import CANDLE_DTYPE
    import time

    migrated = 0
    skipped = 0

    legacy_data = scan_legacy_data(historical_data_path)

    if exchange not in legacy_data:
        return migrated, skipped

    # 统计总分片数用于进度报告
    total_shards = sum(len(dates) for dates in legacy_data[exchange].values())
    processed = 0
    last_log_time = time.monotonic()
    log_interval_seconds = 10.0  # 每 10 秒记录一次进度

    for coin, dates in legacy_data[exchange].items():
        symbol = f"{coin}/{quote}:{quote}"
        safe_symbol = _sanitize_symbol(symbol)

        for date_str in dates:
            processed += 1

            # 定期记录进度
            now = time.monotonic()
            if now - last_log_time >= log_interval_seconds:
                pct = int(100 * processed / total_shards) if total_shards > 0 else 0
                logging.info(
                    "[boot] Migration progress for %s: %d%% (%d/%d shards, %d migrated, %d skipped)",
                    exchange,
                    pct,
                    processed,
                    total_shards,
                    migrated,
                    skipped,
                )
                last_log_time = now
            # 构建目标路径
            target_path = Path(cache_base) / exchange / "1m" / safe_symbol / f"{date_str}.npy"

            if target_path.exists():
                skipped += 1
                continue

            # 查找源文件
            source_paths = _find_legacy_source_paths(exchange, coin, date_str, historical_data_path)

            if not source_paths:
                continue

            # 使用第一个有效源
            source_data = None
            for source_path in source_paths:
                if not os.path.exists(source_path):
                    continue
                try:
                    source_data = _load_and_convert_legacy_shard(source_path, CANDLE_DTYPE)
                    if source_data is not None and len(source_data) > 0:
                        break
                except Exception as e:
                    logging.debug("Failed to load %s: %s", source_path, e)
                    continue

            if source_data is None or len(source_data) == 0:
                continue

            if dry_run:
                logging.info(
                    "[dry-run] Would migrate %s/%s/%s (%d candles)",
                    exchange,
                    coin,
                    date_str,
                    len(source_data),
                )
            else:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                # 原子写入：先写入临时文件，然后重命名
                # 使用 .tmp.npy 后缀以避免 numpy 再次添加 .npy 扩展名
                tmp_path = target_path.with_suffix(".tmp.npy")
                np.save(tmp_path, source_data)
                tmp_path.rename(target_path)
                logging.debug(
                    "Migrated %s/%s/%s (%d candles)", exchange, coin, date_str, len(source_data)
                )

            migrated += 1

    return migrated, skipped


def _find_legacy_source_paths(
    exchange: str, coin: str, date_str: str, historical_data_path: str
) -> List[str]:
    """
    查找旧版分片的潜在源路径。

    Returns:
        候选路径列表，按优先级排序。
    """
    paths = []
    base = Path(historical_data_path)

    # 尝试各种旧版路径模式
    patterns = [
        f"ohlcvs_{exchange}",
        f"ohlcvs_{STANDARD_TO_CCXT_ID.get(exchange, exchange)}",
    ]

    # Binance 的特殊情况
    if exchange == "binance":
        patterns.extend(["ohlcvs_binanceusdm", "ohlcvs_futures"])

    for pattern in patterns:
        candidate = base / pattern / coin / f"{date_str}.npy"
        if candidate.exists():
            paths.append(str(candidate))

    return paths


def _load_and_convert_legacy_shard(path: str, candle_dtype) -> Optional[np.ndarray]:
    """
    加载旧版 .npy 分片并转换为 CANDLE_DTYPE。

    旧版格式：非结构化数组，列为 [ts, o, h, l, c, volume]
    新格式：使用 CANDLE_DTYPE 的结构化数组
    """
    try:
        arr = np.load(path, allow_pickle=False)
    except Exception:
        return None

    if arr is None or arr.size == 0:
        return None

    # 检查是否已经是正确的 dtype
    if arr.dtype == candle_dtype:
        return arr

    # 从旧版格式转换
    if arr.ndim == 2 and arr.shape[1] >= 6:
        result = np.empty(arr.shape[0], dtype=candle_dtype)
        result["ts"] = arr[:, 0].astype(np.int64)
        result["o"] = arr[:, 1].astype(np.float32)
        result["h"] = arr[:, 2].astype(np.float32)
        result["l"] = arr[:, 3].astype(np.float32)
        result["c"] = arr[:, 4].astype(np.float32)
        result["bv"] = arr[:, 5].astype(np.float32)
        return result

    return None


# 跟踪本次会话中是否已记录迁移消息
# 包含 cache_base/historical_data_path 以便隔离的缓存可以独立迁移。
_MIGRATION_LOGGED: Set[Tuple[str, str, str]] = set()


def migrate_legacy_data_all_on_init(
    cache_base: str = "caches/ohlcv",
    historical_data_path: str = "historical_data",
    quote: str = "USDT",
    *,
    audit_gateio_volume: bool = True,
) -> int:
    """
    每个进程仅迁移一次所有交易所的旧版数据。

    设计为全局调用一次（如首次 CandlestickManager 初始化时），
    将迁移在 historical_data/ 下发现的所有交易所。

    Args:
        cache_base: OHLCV 缓存的基础目录
        historical_data_path: 旧版 historical_data 目录路径
        quote: 用于构建符号路径的报价货币
        audit_gateio_volume: 如果为 True，因成交量差异跳过 gateio 迁移

    Returns:
        所有交易所迁移的文件总数
    """
    legacy_data = scan_legacy_data(historical_data_path)
    if not legacy_data:
        return 0

    total_exchanges = len(legacy_data)
    total_coins = sum(len(coins) for coins in legacy_data.values())
    total_shards = sum(len(dates) for coins in legacy_data.values() for dates in coins.values())

    logging.info(
        "[boot] Legacy data found in %s/ (%d exchanges, %d coins, %d shards). "
        "Migrating missing files to %s/",
        historical_data_path,
        total_exchanges,
        total_coins,
        total_shards,
        cache_base,
    )

    migrated_total = 0

    for exchange in sorted(legacy_data.keys()):
        key = (exchange, cache_base, historical_data_path)
        if key in _MIGRATION_LOGGED:
            continue
        _MIGRATION_LOGGED.add(key)

        if audit_gateio_volume and exchange == "gateio":
            logging.info(
                "[boot] skipping gateio legacy migration audit; gateio cache should be refreshed from remote data"
            )
            continue

        migrated, skipped = migrate_legacy_data_for_exchange(
            exchange=exchange,
            cache_base=cache_base,
            historical_data_path=historical_data_path,
            dry_run=False,
            quote=quote,
        )
        migrated_total += migrated

        if migrated > 0:
            logging.info(
                "[boot] Migrated %d legacy shards for %s (%d already existed). "
                "You may safely delete %s/ohlcvs_%s/ to save disk space.",
                migrated,
                exchange,
                skipped,
                historical_data_path,
                exchange,
            )
        elif skipped > 0:
            logging.info(
                "[boot] Legacy data for %s already migrated (%d shards). "
                "You may safely delete %s/ohlcvs_%s/ to save disk space.",
                exchange,
                skipped,
                historical_data_path,
                exchange,
            )

    return migrated_total


def migrate_legacy_data_on_init(
    exchange: str,
    cache_base: str = "caches/ohlcv",
    historical_data_path: str = "historical_data",
    quote: str = "USDT",
    *,
    audit_gateio_volume: bool = True,
) -> int:
    """
    在 CandlestickManager 初始化时检查并迁移旧版数据。

    每个交易所每会话调用一次。功能：
    1. 如果存在旧版数据则记录消息
    2. 将缺失的数据复制到新缓存位置
    3. 保持 historical_data/ 不变

    Args:
        exchange: 标准交易所名称
        cache_base: OHLCV 缓存的基础目录
        historical_data_path: 旧版 historical_data 目录路径
        quote: 报价货币

    Returns:
        迁移的文件数量
    """
    global _MIGRATION_LOGGED

    key = (exchange, cache_base, historical_data_path)
    if key in _MIGRATION_LOGGED:
        return 0

    legacy_data = scan_legacy_data(historical_data_path)

    if exchange not in legacy_data:
        return 0

    total_coins = len(legacy_data[exchange])
    total_shards = sum(len(dates) for dates in legacy_data[exchange].values())

    if total_shards == 0:
        return 0

    # 每会话仅记录一次
    _MIGRATION_LOGGED.add(key)
    logging.info(
        "[boot] Legacy data found in %s/ohlcvs_%s/ (%d coins, %d shards). "
        "Migrating missing files to %s/",
        historical_data_path,
        exchange,
        total_coins,
        total_shards,
        cache_base,
    )

    migrated, skipped = migrate_legacy_data_for_exchange(
        exchange=exchange,
        cache_base=cache_base,
        historical_data_path=historical_data_path,
        dry_run=False,
        quote=quote,
    )

    if migrated > 0:
        logging.info(
            "[boot] Migrated %d legacy shards for %s (%d already existed). "
            "You may safely delete %s/ohlcvs_%s/ to save disk space.",
            migrated,
            exchange,
            skipped,
            historical_data_path,
            exchange,
        )
    elif skipped > 0:
        logging.info(
            "[boot] Legacy data for %s already migrated (%d shards). "
            "You may safely delete %s/ohlcvs_%s/ to save disk space.",
            exchange,
            skipped,
            historical_data_path,
            exchange,
        )

    if audit_gateio_volume and exchange == "gateio":
        logging.info(
            "[boot] skipping gateio legacy migration audit; gateio cache should be refreshed from remote data"
        )
    return migrated
