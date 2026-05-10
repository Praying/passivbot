from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from optimization.bounds import Bound
from optimization.config_adapter import extract_bounds_tuple_list_from_config, get_optimization_key_paths


@dataclass(frozen=True)
class OptimizationShape:
    """优化问题的形状描述：边界、键路径和有效位数。"""

    bounds: Tuple[Bound, ...]
    key_paths: Tuple[tuple[str, tuple[str, ...]], ...]
    sig_digits: int | None


def build_optimization_shape(config: dict) -> OptimizationShape:
    """从配置构建优化形状对象。"""
    return OptimizationShape(
        bounds=tuple(extract_bounds_tuple_list_from_config(config)),
        key_paths=tuple(get_optimization_key_paths(config)),
        sig_digits=config.get("optimize", {}).get("round_to_n_significant_digits", 6),
    )
