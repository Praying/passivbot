"""
参数边界和步长约束的工具模块。

本模块提供可复用的函数，用于将参数值量化到离散步长
并对配置字典实施边界约束。
"""

import logging
from dataclasses import dataclass
import math
from typing import List, Optional, Sequence, Tuple

import numpy as np

# 步长计算中浮点数比较的 epsilon 值
STEP_EPSILON = 1e-9


def round_to_sig_digits(value: float, sig_digits: int) -> float:
    """
    纯 Python 有效数字舍入。

    故意避免依赖 passivbot_rust，以便单元测试可以在
    原生扩展被桩化或不可用的环境中运行。
    """
    if sig_digits is None:
        return value
    if not isinstance(value, (int, float)):
        raise TypeError(f"value must be numeric, got {type(value).__name__}")
    value = float(value)
    if value == 0.0 or not math.isfinite(value):
        return value
    digits = sig_digits - 1 - int(math.floor(math.log10(abs(value))))
    return round(value, digits)


@dataclass(frozen=True, slots=True)
class Bound:
    """
    表示优化的参数边界。

    对于连续参数，step 为 None。
    对于阶梯（离散）参数，step 定义网格间距。

    Args:
        low: 下界
        high: 上界
        step: 离散参数的步长（连续参数为 None）
    """

    low: float
    high: float
    step: Optional[float] = None

    @property
    def is_stepped(self) -> bool:
        """检查是否为阶梯（离散）参数。"""
        return self.step is not None and self.step > 0

    @property
    def max_index(self) -> int:
        """
        获取阶梯参数的最大有效索引。

        返回满足 low + n*step <= high 的最大 n。
        若参数非阶梯类型则抛出 ValueError。
        """
        if not self.is_stepped:
            raise ValueError("max_index only valid for stepped parameters")
        return int((self.high - self.low + STEP_EPSILON) / self.step)

    def quantize(self, value: float) -> float:
        """
        将值量化到边界内最近的步长网格点。

        对于连续参数，仅钳位到 [low, high]。
        对于阶梯参数，对齐到最近的网格点。

        Args:
            value: 待量化的值

        Returns:
            钳位到有效边界内的量化值
        """
        if not self.is_stepped:
            return max(self.low, min(self.high, value))

        # 先钳位到边界
        clamped = max(self.low, min(self.high, value))

        # 找到最近的步长（使用 int + 0.5 实现正确四舍五入）
        n_steps_from_low = int((clamped - self.low) / self.step + 0.5)

        # 将索引钳位到有效范围
        clamped_index = max(0, min(self.max_index, n_steps_from_low))

        quantized = self.low + clamped_index * self.step

        # 按步长精度舍入以清除浮点误差
        # 例如 step=0.0002 -> 4 位小数，step=0.01 -> 2 位小数
        if self.step < 1:
            decimal_places = -int(math.floor(math.log10(self.step)))
            quantized = round(quantized, decimal_places)

        # 最终安全钳位
        return max(self.low, min(self.high, quantized))

    def random_on_grid(self) -> float:
        """
        生成遵守步长约束的随机值。

        对于连续参数，返回 [low, high] 上的均匀随机值。
        对于阶梯参数，返回网格上的随机值。

        Returns:
            随机值
        """
        if not self.is_stepped:
            return np.random.uniform(self.low, self.high)
        random_idx = np.random.randint(0, self.max_index + 1)
        return self.low + random_idx * self.step

    def value_to_index(self, value: float) -> float:
        """
        将参数值转换为索引空间。

        对于连续参数，原样返回值。
        对于阶梯参数，返回索引（从 0 开始）。

        Args:
            value: 参数值

        Returns:
            步长空间中的索引，连续参数则为原始值
        """
        if not self.is_stepped:
            return value
        return (value - self.low) / self.step

    def index_to_value(self, index: float) -> float:
        """
        将索引转换回参数值。

        对于连续参数，返回钳位到边界的索引值。
        对于阶梯参数，将索引转换为网格值。

        Args:
            index: 步长空间中的索引（连续参数则为值）

        Returns:
            参数值
        """
        if not self.is_stepped:
            return max(self.low, min(self.high, index))
        # 四舍五入到最近的整数索引并转换
        rounded_index = int(index + 0.5)
        clamped_index = max(0, min(self.max_index, rounded_index))
        return self.low + clamped_index * self.step

    def get_index_bounds(self) -> Tuple[float, float]:
        """
        获取索引空间中的边界。

        对于连续参数，返回 (low, high)。
        对于阶梯参数，返回 (0, max_index)。

        Returns:
            (low_index, high_index)
        """
        if not self.is_stepped:
            return (self.low, self.high)
        return (0.0, float(self.max_index))

    @classmethod
    def from_config(cls, key: str, val) -> "Bound":
        """
        从配置值中提取并验证 Bound。

        支持的格式：
        - 单个值：固定参数（low=high）
        - [low, high]：连续优化
        - [low, high, step]：带步长的离散优化
        - [low, high, 0] 或 [low, high, null]：视为连续

        Args:
            key: 参数键名（用于错误信息）
            val: 配置值（数字、列表或元组）

        Returns:
            经过验证的 Bound 实例

        Raises:
            Exception: 边界规格格式错误时抛出
        """
        if isinstance(val, (float, int)):
            return cls(float(val), float(val), None)

        if isinstance(val, (tuple, list)):
            if len(val) == 0:
                raise Exception(f"malformed bound {key}: empty array")
            if len(val) == 1:
                return cls(float(val[0]), float(val[0]), None)
            if len(val) == 2:
                low, high = sorted([float(val[0]), float(val[1])])
                return cls(low, high, None)
            if len(val) >= 3:
                low, high = sorted([float(val[0]), float(val[1])])
                if len(val) > 3:
                    logging.warning(
                        "Bound %s has %d elements; expected 1, 2, or 3. Ignoring step and using sig_digits.",
                        key,
                        len(val),
                    )
                    return cls(low, high, None)

                step_raw = val[2]
                if step_raw is None:
                    logging.warning(
                        "Bound %s step is null; treating as continuous and using sig_digits.", key
                    )
                    return cls(low, high, None)

                try:
                    step = float(step_raw)
                except Exception:
                    logging.warning(
                        "Bound %s step is not a number (%r); treating as continuous and using sig_digits.",
                        key,
                        step_raw,
                    )
                    return cls(low, high, None)

                if step <= 0:
                    logging.warning(
                        "Bound %s step must be > 0 (got %s); treating as continuous and using sig_digits.",
                        key,
                        step,
                    )
                    return cls(low, high, None)

                if high != low and step > (high - low):
                    logging.warning(
                        "Bound %s step=%s is larger than range [%s, %s]; treating as continuous and using sig_digits.",
                        key,
                        step,
                        low,
                        high,
                    )
                    return cls(low, high, None)

                return cls(low, high, step)

        raise Exception(f"malformed bound {key}: {val}")


def enforce_bounds(
    values: Sequence[float], bounds: Sequence[Bound], sig_digits: int = None
) -> List[float]:
    """
    将每个值钳位到对应的 [low, high] 区间，若有步长则量化。
    同时按有效数字舍入（可选）。

    Args:
        values : 浮点数可迭代对象（长度 == len(bounds)）
        bounds : Bound 实例的可迭代对象
        sig_digits: 有效数字位数

    Returns:
        List[float] – 钳位和量化后的副本（原值不被修改）
    """
    if len(values) != len(bounds):
        raise ValueError(
            f"values/bounds length mismatch: got {len(values)} values but {len(bounds)} bounds"
        )
    result = []
    for v, bound in zip(values, bounds):
        if bound.is_stepped:
            result.append(bound.quantize(v))
        else:
            rounded = v if sig_digits is None else round_to_sig_digits(v, sig_digits)
            result.append(
                bound.high if rounded > bound.high else bound.low if rounded < bound.low else rounded
            )
    return result
