from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ohlcv_catalog import GapRecord, OhlcvCatalog
from ohlcv_legacy_import import LegacyRangeInspection, inspect_legacy_range


@dataclass(frozen=True)
class SymbolRangePlan:
    """单个交易对在给定时间范围内的本地数据可用性评估结果。"""
    exchange: str
    timeframe: str
    symbol: str
    start_ts: int
    end_ts: int
    status: str
    bounds: tuple[int | None, int | None]
    legacy_inspection: LegacyRangeInspection | None
    persistent_gaps: tuple[GapRecord, ...]

    @property
    def local_store_complete(self) -> bool:
        """本地存储已完整覆盖请求范围。"""
        return self.status == "store_complete"

    @property
    def should_try_legacy_import(self) -> bool:
        """数据可从旧版格式导入。"""
        return self.status == "legacy_importable"

    @property
    def blocked_by_persistent_gap(self) -> bool:
        """存在持久化缺口，阻塞数据获取。"""
        return self.status == "blocked_by_persistent_gap"

    @property
    def requires_remote_fetch(self) -> bool:
        """本地数据缺失，需要从远程抓取。"""
        return self.status == "missing_local"


def plan_local_symbol_range(
    *,
    catalog: OhlcvCatalog,
    legacy_root: str | Path | None,
    exchange: str,
    timeframe: str,
    symbol: str,
    start_ts: int,
    end_ts: int,
) -> SymbolRangePlan:
    """评估交易对在本地数据存储和旧版数据中的可用性，返回获取计划。"""
    if end_ts < start_ts:
        raise ValueError("end_ts must be >= start_ts")
    bounds = catalog.get_symbol_bounds(exchange, timeframe, symbol)
    # 检查本地存储是否已完整覆盖请求范围
    store_complete = (
        bounds[0] is not None
        and bounds[1] is not None
        and int(bounds[0]) <= int(start_ts)
        and int(bounds[1]) >= int(end_ts)
    )
    persistent_gaps = tuple(
        catalog.get_persistent_gaps(exchange, timeframe, symbol, start_ts, end_ts)
    )
    # 当本地不完整或存在缺口时，尝试检查旧版数据
    legacy_inspection = None
    if (
        (not store_complete or persistent_gaps)
        and legacy_root is not None
        and Path(legacy_root).exists()
    ):
        legacy_inspection = inspect_legacy_range(
            legacy_root=legacy_root,
            exchange=exchange,
            timeframe=timeframe,
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
        )
    # 确定数据可用性状态：持久化缺口优先级最高
    if persistent_gaps and not (
        legacy_inspection is not None and legacy_inspection.all_days_present
    ):
        status = "blocked_by_persistent_gap"
    # 旧版数据可完全覆盖则标记为可导入
    elif legacy_inspection is not None and legacy_inspection.all_days_present:
        status = "legacy_importable"
    elif store_complete:
        status = "store_complete"
    else:
        status = "missing_local"
    return SymbolRangePlan(
        exchange=str(exchange),
        timeframe=str(timeframe),
        symbol=str(symbol),
        start_ts=int(start_ts),
        end_ts=int(end_ts),
        status=status,
        bounds=bounds,
        legacy_inspection=legacy_inspection,
        persistent_gaps=persistent_gaps,
    )
