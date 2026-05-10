from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np

from config.scoring import ObjectiveSpec, dominates_objectives, extract_objective_specs, from_engine_value
from config.metrics import resolve_metric_value


@dataclass(frozen=True)
class ParetoPoint:
    """Pareto 前沿上的一个点：哈希标识、目标值向量和约束违反度。"""

    hash_id: str
    objectives: Tuple[float, ...]
    violation: float = 0.0


def detect_latest_pareto_dir(root: str | Path = "optimize_results") -> Optional[Path]:
    """检测最新的包含 Pareto JSON 文件的优化结果目录。"""
    base = Path(root).expanduser()
    if not base.is_dir():
        return None
    runs = [
        path
        for path in base.iterdir()
        if path.is_dir() and (path / "pareto").is_dir() and any((path / "pareto").glob("*.json"))
    ]
    if not runs:
        return None
    return (sorted(runs, key=lambda path: path.name)[-1] / "pareto").resolve()


def extract_objectives(
    entry: Dict[str, Any], scoring_keys: Optional[Sequence[str]] = None
) -> Tuple[Tuple[float, ...], List[str]]:
    """
    从结果条目中提取原始目标值。

    若提供了 scoring_keys/specs 则按其顺序，否则按排序后的命名键。
    旧式 w_i 引擎空间载荷在有评分元数据时被转换回原始值。
    """
    metrics_block = entry.get("metrics") or {}
    objectives_map = metrics_block.get("objectives", metrics_block) or {}
    specs = extract_objective_specs(scoring_keys or entry.get("optimize", {}).get("scoring", []))
    if specs:
        values: list[float] = []
        keys: list[str] = []
        for idx, spec in enumerate(specs):
            value = resolve_metric_value(objectives_map, spec.metric)
            if value is None:
                value = objectives_map.get(f"w_{idx}")
                if value is not None:
                    value = from_engine_value(spec, float(value))
            values.append(value)
            keys.append(spec.metric)
        return tuple(values), keys

    keys = sorted(k for k in objectives_map if not str(k).startswith("w_"))
    if not keys:
        keys = sorted(k for k in objectives_map if str(k).startswith("w_"))
    objectives = tuple(objectives_map.get(key) for key in keys)
    return objectives, keys


def extract_violation(entry: Dict[str, Any]) -> float:
    metrics_block = entry.get("metrics") or {}
    try:
        return float(metrics_block.get("constraint_violation") or 0.0)
    except Exception:
        return 0.0


def dominates_with_violation(
    obj_a: Sequence[float],
    viol_a: float,
    obj_b: Sequence[float],
    viol_b: float,
    objective_specs: Optional[Sequence[ObjectiveSpec]] = None,
    tol: float = 1e-12,
) -> bool:
    """
    带约束的 Pareto 支配判断：违反度低者优先；否则按目标方向执行标准支配判断。
    """
    if np.isclose(viol_a, viol_b, atol=tol, rtol=0.0):
        if objective_specs:
            return dominates_objectives(obj_a, obj_b, objective_specs)
        better_in_one = False
        for a, b in zip(obj_a, obj_b):
            if a < b:
                better_in_one = True
            elif a > b:
                return False
        return better_in_one
    return viol_a < viol_b


def crowding_distances(values: np.ndarray) -> np.ndarray:
    """
    计算目标向量数组的拥挤距离（值越低表示越拥挤）。
    """
    if values.ndim != 2:
        return np.zeros(len(values))
    n, m = values.shape
    if n == 0:
        return np.array([])
    if n <= 2:
        return np.full(n, np.inf)
    distances = np.zeros(n)
    for col in range(m):
        order = np.argsort(values[:, col])
        distances[order[0]] = distances[order[-1]] = np.inf
        column = values[order, col]
        min_v = column[0]
        max_v = column[-1]
        denom = max_v - min_v
        if denom == 0:
            continue
        normalized = (column[2:] - column[:-2]) / denom
        distances[order[1:-1]] += normalized
    return distances


def prune_front_with_extremes(
    front_hashes: Sequence[str],
    objectives_map: Dict[str, Tuple[float, ...]],
    violations_map: Dict[str, float],
    max_size: int,
) -> List[str]:
    """
    确定需要移除的成员以满足 max_size 限制，
    同时始终保留每个目标轴的极值（最小/最大）点。
    返回需要移除的 hash_id 列表。
    """
    if max_size <= 0 or len(front_hashes) <= max_size:
        return []
    objs = [objectives_map[idx] for idx in front_hashes]
    arr = np.asarray(objs, dtype=float)
    required: set[str] = set()
    for dim in range(arr.shape[1]):
        min_idx = int(np.argmin(arr[:, dim]))
        max_idx = int(np.argmax(arr[:, dim]))
        required.add(front_hashes[min_idx])
        required.add(front_hashes[max_idx])

    crowding = crowding_distances(arr)
    scored = list(zip(front_hashes, crowding))
    scored.sort(key=lambda item: item[1])  # 优先移除拥挤度最低的

    to_remove: List[str] = []
    for hash_id, _cd in scored:
        if hash_id in required:
            continue
        to_remove.append(hash_id)
        if len(to_remove) >= len(front_hashes) - max_size:
            break
    return to_remove


def compute_ideal(
    values_matrix: np.ndarray,
    mode: str = "min",
    weights=None,
    eps: float = 1e-3,
    pct: float = 10,
    objective_specs: Optional[Sequence[ObjectiveSpec]] = None,
):
    """根据指定模式计算理想点（支持 min/weighted/utopian/percentile/midrange/geomedian）。"""
    def _require_specs() -> Sequence[ObjectiveSpec]:
        if not objective_specs:
            raise ValueError("目标感知理想点计算需要 objective_specs")
        if len(objective_specs) != values_matrix.shape[1]:
            raise ValueError(
                "objective_specs 长度必须与目标列数匹配 "
                f"({len(objective_specs)} != {values_matrix.shape[1]})"
            )
        return objective_specs

    if objective_specs:
        specs = _require_specs()
        mins = values_matrix.min(axis=0)
        maxs = values_matrix.max(axis=0)
        if mode in ["m", "min"]:  # 按目标方向取各轴最优
            return np.array(
                [
                    maxs[i] if specs[i].goal == "max" else mins[i]
                    for i in range(values_matrix.shape[1])
                ]
            )
        if mode in ["w", "weighted"]:  # 加权偏移
            if weights is None:
                raise ValueError("需要 weights")
            ideal = np.array(
                [
                    maxs[i] if specs[i].goal == "max" else mins[i]
                    for i in range(values_matrix.shape[1])
                ]
            )
            anti_ideal = np.array(
                [
                    mins[i] if specs[i].goal == "max" else maxs[i]
                    for i in range(values_matrix.shape[1])
                ]
            )
            return ideal + weights * (anti_ideal - ideal)
        if mode in ["u", "utopian"]:  # utopian 点：理想点偏移 eps
            ranges = maxs - mins
            return np.array(
                [
                    maxs[i] + eps * ranges[i] if specs[i].goal == "max" else mins[i] - eps * ranges[i]
                    for i in range(values_matrix.shape[1])
                ]
            )
        if mode in ["p", "percentile"]:  # 百分位理想点
            return np.array(
                [
                    np.percentile(values_matrix[:, i], 100.0 - pct)
                    if specs[i].goal == "max"
                    else np.percentile(values_matrix[:, i], pct)
                    for i in range(values_matrix.shape[1])
                ]
            )
        if mode in ["mi", "midrange"]:  # 中点
            return 0.5 * (mins + maxs)

    # 无 objective_specs 时的简易分支
    if mode in ["m", "min"]:
        return values_matrix.min(axis=0)

    if mode in ["w", "weighted"]:  # 加权偏移（无 specs 时）
        if weights is None:
            raise ValueError("需要 weights")
        vmin = values_matrix.min(axis=0)
        vmax = values_matrix.max(axis=0)
        return vmin + weights * (vmax - vmin)

    if mode in ["u", "utopian"]:  # utopian 点偏移
        mins = values_matrix.min(axis=0)
        ranges = values_matrix.ptp(axis=0)
        return mins - eps * ranges

    if mode in ["p", "percentile"]:  # 百分位（无 specs 时）
        return np.percentile(values_matrix, pct, axis=0)

    if mode in ["mi", "midrange"]:  # 中点（无 specs 时）
        return 0.5 * (values_matrix.min(axis=0) + values_matrix.max(axis=0))

    if mode in ["g", "geomedian"]:  # 几何中位数迭代
        z = values_matrix.mean(axis=0)
        for _ in range(10):
            d = np.linalg.norm(values_matrix - z, axis=1)
            w = np.where(d > 0, 1.0 / d, 0.0)
            z_new = (values_matrix * w[:, None]).sum(axis=0) / w.sum()
            if np.allclose(z, z_new, atol=1e-9):
                break
            z = z_new
        return z

    raise ValueError(f"未知模式 {mode}")
