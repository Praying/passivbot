from __future__ import annotations

import numpy as np
from pymoo.core.repair import Repair

from optimization.bounds import Bound, enforce_bounds


class BoundsRepair(Repair):
    """将超出边界的个体修复到合法范围内。"""
    def __init__(self, bounds: list[Bound], sig_digits: int | None):
        super().__init__()
        self.bounds = list(bounds)
        self.sig_digits = sig_digits

    def _do(self, problem, X, **kwargs):
        """对种群矩阵逐行执行边界修复。"""
        repaired = [
            enforce_bounds(row.tolist(), self.bounds, self.sig_digits)
            for row in np.asarray(X, dtype=np.float64)
        ]
        return np.asarray(repaired, dtype=np.float64)
