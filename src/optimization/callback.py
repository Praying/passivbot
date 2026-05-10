from __future__ import annotations

from typing import Sequence

import numpy as np
from pymoo.core.callback import Callback

from config_utils import strip_config_metadata


# 回调写入前需要剥离的元数据键
CALLBACK_METADATA_KEYS = (
    "_raw",
    "_raw_effective",
    "_transform_log",
    "_coins_sources",
    "_config_path",
)


def build_pymoo_record_entry(
    *,
    vector,
    metrics,
    template: dict,
    build_config_fn,
    overrides_fn,
    overrides_list: Sequence[str] | None = None,
) -> dict:
    """构建一条 pymoo 优化记录条目，包含完整配置与指标。"""
    if isinstance(vector, np.ndarray):
        vector = vector.tolist()
    metrics = dict(metrics or {})
    suite_metrics = metrics.pop("suite_metrics", None)
    config = build_config_fn(
        vector,
        overrides_fn,
        list(overrides_list or []),
        template,
    )
    entry = dict(config)
    if suite_metrics is not None:
        entry["suite_metrics"] = suite_metrics
        backtest = entry.get("backtest")
        if isinstance(backtest, dict):
            backtest.pop("coins", None)
    if metrics:
        entry["metrics"] = metrics
    return strip_config_metadata(entry, keys=CALLBACK_METADATA_KEYS)


class PymooRecorderCallback(Callback):
    """pymoo 回调：将每个批次个体记入优化记录器。"""
    def __init__(
        self,
        *,
        recorder,
        template: dict,
        build_config_fn,
        overrides_fn,
        overrides_list: Sequence[str] | None = None,
    ):
        super().__init__()
        self.recorder = recorder
        self.template = template
        self.build_config_fn = build_config_fn
        self.overrides_fn = overrides_fn
        self.overrides_list = list(overrides_list or [])

    def _build_entry(self, individual) -> dict:
        """从个体中提取向量和指标，构建记录条目。"""
        vector = None
        if hasattr(individual, "data") and isinstance(individual.data, dict):
            vector = individual.data.get("evaluation_vector")
        if vector is None:
            vector = individual.X
        metrics = {}
        if hasattr(individual, "data") and isinstance(individual.data, dict):
            metrics = individual.data.get("metrics") or {}
        return build_pymoo_record_entry(
            vector=vector,
            metrics=metrics,
            template=self.template,
            build_config_fn=self.build_config_fn,
            overrides_fn=self.overrides_fn,
            overrides_list=self.overrides_list,
        )

    def notify(self, algorithm):
        """当算法生成新批次时，记录每个个体。"""
        batch = getattr(algorithm, "off", None)
        if batch is None:
            batch = getattr(algorithm, "pop", None)
        if batch is None:
            batch = []
        for individual in batch:
            self.recorder.record(self._build_entry(individual))
