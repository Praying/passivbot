"""
优化的 DEAP 适配器函数。

本模块包含包装器和辅助函数，将领域特定的 Bound 逻辑
适配到 DEAP 进化算法库。
"""

from typing import List, Sequence, Tuple
import numpy as np

try:
    from deap import tools as deap_tools
except ImportError:  # pragma: no cover
    deap_tools = None

from optimization.bounds import Bound

# DEAP 算子中临时调整相等边界时使用的 epsilon
DEAP_EQUAL_BOUNDS_EPSILON = 1e-6


# === 遗传算子的索引空间转换辅助函数 ==================


def to_index_space(
    values: List[float],
    bounds: Sequence[Bound],
) -> Tuple[List[float], List[float], List[float]]:
    """
    将值转换为阶梯参数的索引空间。

    对于阶梯参数，值被转换为离散网格中的索引。
    对于连续参数，值和边界原样传递。

    Args:
        values: 待转换的参数值
        bounds: Bound 实例列表

    Returns:
        (index_values, index_low, index_up) 元组
    """
    index_values = []
    index_low = []
    index_up = []

    for i, val in enumerate(values):
        bound = bounds[i]
        if bound.is_stepped:
            index_values.append(bound.value_to_index(val))
            idx_low, idx_up = bound.get_index_bounds()
            index_low.append(idx_low)
            index_up.append(idx_up)
        else:
            index_values.append(val)
            index_low.append(bound.low)
            index_up.append(bound.high)

    return index_values, index_low, index_up


def prepare_bounds_for_deap(
    index_low: List[float],
    index_up: List[float],
) -> Tuple[List[float], List[float], np.ndarray]:
    """
    为 DEAP 遗传算子准备边界，处理相等边界的情况。

    DEAP 算子在 low == high 时会失败，因此用一个小 epsilon 临时调整。
    返回 equal_bounds_mask 以便调用者在操作后重置这些值。

    Args:
        index_low: 索引空间的下界
        index_up: 索引空间的上界

    Returns:
        (temp_low, temp_up, equal_bounds_mask) 元组
    """
    low_array = np.array(index_low)
    up_array = np.array(index_up)
    equal_bounds_mask = low_array == up_array
    temp_low = np.where(equal_bounds_mask, low_array - DEAP_EQUAL_BOUNDS_EPSILON, low_array)
    temp_up = np.where(equal_bounds_mask, up_array + DEAP_EQUAL_BOUNDS_EPSILON, up_array)
    return list(temp_low), list(temp_up), equal_bounds_mask


def from_index_space(
    index_values: List[float],
    bounds: Sequence[Bound],
    equal_mask: np.ndarray,
) -> List[float]:
    """
    将索引空间值转换回参数空间。

    对于阶梯参数，索引被转换回网格上的值。
    对于连续参数，值直接复制。
    边界相等的参数重置为其 low 值。

    Args:
        index_values: 索引空间中的值（被 DEAP 算子修改）
        bounds: Bound 实例列表
        equal_mask: 布尔掩码，指示哪些参数具有相等边界

    Returns:
        List[float]: 转换回参数空间的值
    """
    result = []
    for i in range(len(index_values)):
        bound = bounds[i]
        if equal_mask[i]:
            result.append(bound.low)
        elif bound.is_stepped:
            result.append(bound.index_to_value(index_values[i]))
        else:
            result.append(index_values[i])
    return result


# === DEAP 遗传算子包装器 =========================================


def mutPolynomialBoundedWrapper(individual, eta, indpb, bounds: Sequence[Bound]):
    """
    DEAP mutPolynomialBounded 函数的包装器，预处理边界
    并处理上下界相等的情况。

    对于阶梯参数，变异在索引空间中执行以确保
    后代值保持在网格上。

    Args:
        individual: 待变异的序列个体。
        eta: 变异的拥挤度。
        indpb: 每个属性独立变异的概率。
        bounds: 定义参数约束的 Bound 实例列表。

    Returns:
        包含一个个体的元组，已考虑相等上下界进行变异。
    """
    if deap_tools is None:  # pragma: no cover
        raise ModuleNotFoundError("deap is required for optimizer mutation operators")

    index_ind, index_low, index_up = to_index_space(individual, bounds)
    temp_low, temp_up, equal_mask = prepare_bounds_for_deap(index_low, index_up)

    deap_tools.mutPolynomialBounded(index_ind, eta, temp_low, temp_up, indpb)

    individual[:] = from_index_space(index_ind, bounds, equal_mask)
    return (individual,)


def cxSimulatedBinaryBoundedWrapper(ind1, ind2, eta, bounds: Sequence[Bound]):
    """
    DEAP cxSimulatedBinaryBounded 函数的包装器，预处理边界
    并处理上下界相等的情况。

    对于阶梯参数，交叉在索引空间中执行以确保
    后代值保持在网格上。

    Args:
        ind1: 参与交叉的第一个个体。
        ind2: 参与交叉的第二个个体。
        eta: 交叉的拥挤度。
        bounds: 定义参数约束的 Bound 实例列表。

    Returns:
        交叉操作后的两个个体元组。
    """
    if deap_tools is None:  # pragma: no cover
        raise ModuleNotFoundError("deap is required for optimizer crossover operators")

    index_ind1, index_low, index_up = to_index_space(ind1, bounds)
    index_ind2, _, _ = to_index_space(ind2, bounds)
    temp_low, temp_up, equal_mask = prepare_bounds_for_deap(index_low, index_up)

    deap_tools.cxSimulatedBinaryBounded(index_ind1, index_ind2, eta, temp_low, temp_up)

    ind1[:] = from_index_space(index_ind1, bounds, equal_mask)
    ind2[:] = from_index_space(index_ind2, bounds, equal_mask)
    return ind1, ind2
