from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from config.metrics import (
    ANALYSIS_SHARED_KEYS,
    CURRENCY_METRICS,
    SHARED_METRICS,
    canonical_metric_name,
    metric_aliases,
)
from config.scoring import extract_objective_specs
from utils import trim_analysis_aliases


@dataclass(frozen=True)
class VisibleAnalysis:
    """可见分析结果，包含过滤后的分析数据、显示计数和解析的指标名。"""
    analysis: dict
    shown_count: int
    total_count: int
    resolved_metric_names: tuple[str, ...]


def collect_visible_metric_requests(config: dict) -> tuple[list[str], list[str], object]:
    """收集可见指标请求：从优化目标和显式配置中提取。返回 (派生指标, 显式指标, 原始配置)。"""
    optimize_cfg = (config or {}).get("optimize", {}) or {}
    derived = []
    for spec in extract_objective_specs(optimize_cfg.get("scoring", []) or []):
        metric = spec.metric
        if metric.endswith(("_usd", "_btc")) and metric.rsplit("_", 1)[0] in CURRENCY_METRICS:
            metric = metric.rsplit("_", 1)[0]
        derived.append(metric)
    derived.extend(
        canonical_metric_name(limit["metric"])
        for limit in (optimize_cfg.get("limits", []) or [])
        if isinstance(limit, dict) and limit.get("metric")
    )
    visible_cfg = (config or {}).get("backtest", {}).get("visible_metrics")
    explicit = [] if visible_cfg is None else _normalize_visible_metrics_config(visible_cfg)
    return derived, explicit, visible_cfg


def resolve_visible_metric_names(
    config: dict,
    analysis_keys: Iterable[str],
) -> list[str]:
    """将配置中的可见指标请求解析为实际的分析键名列表。"""
    ordered_keys = list(analysis_keys)
    key_set = set(ordered_keys)
    derived, explicit, visible_cfg = collect_visible_metric_requests(config)
    if visible_cfg == []:
        return ordered_keys

    resolved = []
    resolved_seen = set()
    unresolved_explicit = []
    for metric in [*derived, *explicit]:
        matches = _expand_metric_name(metric, ordered_keys, key_set)
        if matches:
            for match in matches:
                if match not in resolved_seen:
                    resolved.append(match)
                    resolved_seen.add(match)
        elif metric in explicit:
            unresolved_explicit.append(metric)
    if unresolved_explicit:
        available = ", ".join(sorted(ordered_keys))
        raise ValueError(
            "unknown backtest.visible_metrics entries: "
            + ", ".join(unresolved_explicit)
            + f" | available metrics: {available}"
        )
    return resolved


def filter_analysis_for_visibility(analysis: dict, config: dict) -> VisibleAnalysis:
    """根据可见性配置过滤分析数据，返回 VisibleAnalysis 结果。"""
    trimmed = trim_analysis_aliases(analysis)
    visible_names = resolve_visible_metric_names(config, trimmed.keys())
    filtered = {key: trimmed[key] for key in visible_names}
    return VisibleAnalysis(
        analysis=filtered,
        shown_count=len(filtered),
        total_count=len(trimmed),
        resolved_metric_names=tuple(visible_names),
    )


def validate_visible_metrics_config(config: dict) -> None:
    """验证 visible_metrics 配置中的指标名是否可识别，不可识别时抛出 ValueError。"""
    _derived, explicit, visible_cfg = collect_visible_metric_requests(config)
    if visible_cfg in (None, []):
        return
    known_metrics = _known_visible_metric_names()
    unresolved_explicit = [
        metric for metric in explicit if not _metric_name_is_known(metric, known_metrics)
    ]
    if unresolved_explicit:
        available = ", ".join(sorted(known_metrics))
        raise ValueError(
            "unknown backtest.visible_metrics entries: "
            + ", ".join(unresolved_explicit)
            + f" | available metrics: {available}"
        )


def _normalize_visible_metrics_config(value) -> list[str]:
    """标准化 visible_metrics 配置值为指标名字符串列表。"""
    if value == []:
        return []
    if not isinstance(value, (list, tuple, set)):
        raise ValueError(
            "backtest.visible_metrics must be null, [], or a list/tuple/set of metric names"
        )
    normalized = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("backtest.visible_metrics entries must be non-empty strings")
        normalized.append(canonical_metric_name(item.strip()))
    return normalized


def _expand_metric_name(metric: str, ordered_keys: Sequence[str], key_set: set[str]) -> list[str]:
    """展开指标名为匹配的分析键，支持别名、货币后缀和前缀匹配。"""
    metric = canonical_metric_name(metric)
    if metric in key_set:
        return [metric]
    for alias in metric_aliases(metric):
        if alias in key_set:
            return [alias]
    if metric in SHARED_METRICS:
        return [metric] if metric in key_set else []
    if metric in CURRENCY_METRICS:
        matches = [key for key in ordered_keys if key in {f"{metric}_usd", f"{metric}_btc"}]
        if matches:
            return matches
    prefixes = metric_aliases(metric)
    prefixed = [
        key
        for key in ordered_keys
        if any(key.startswith(f"{prefix}_") for prefix in prefixes)
    ]
    if prefixed:
        return prefixed
    return []


def _known_visible_metric_names() -> set[str]:
    """返回所有已知可见指标名的集合，含货币后缀变体。"""
    known = set(CURRENCY_METRICS) | set(ANALYSIS_SHARED_KEYS)
    known |= {
        f"{metric}_{suffix}"
        for metric in CURRENCY_METRICS
        for suffix in ("usd", "btc")
    }
    return known


def _metric_name_is_known(metric: str, known_metrics: set[str]) -> bool:
    """判断指标名是否已知，支持别名和前缀匹配。"""
    metric = canonical_metric_name(metric)
    if metric in known_metrics:
        return True
    if metric.endswith(("_usd", "_btc")) and metric.rsplit("_", 1)[0] in CURRENCY_METRICS:
        return True
    return any(known_metric.startswith(f"{metric}_") for known_metric in known_metrics)
