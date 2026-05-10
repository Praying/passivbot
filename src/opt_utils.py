import json
import logging
import math
import msgpack
from typing import Any
import passivbot_rust as pbr


def dominates(p0, p1):
    """判断 p0 是否 Pareto 支配 p1（所有目标越小越好）。"""
    better_in_one = False
    for a, b in zip(p0, p1):
        if a < b:
            better_in_one = True
        elif a > b:
            return False
    return better_in_one


def dominates_d(x, y, higher_is_better):
    """带方向标记的 Pareto 支配判断。"""
    better_in_one = False
    for xi, yi, hib in zip(x, y, higher_is_better):
        if hib:
            if xi > yi:
                better_in_one = True
            elif xi < yi:
                return False
        else:
            if xi < yi:
                better_in_one = True
            elif xi > yi:
                return False
    return better_in_one


def update_pareto_front(new_index, new_obj, current_front, objectives_dict, higher_is_better):
    """将新候选加入 Pareto 前沿，移除被支配的成员。"""
    for idx in current_front:
        if dominates_d(objectives_dict[idx], new_obj, higher_is_better):
            return current_front
    new_front = [
        idx
        for idx in current_front
        if not dominates_d(new_obj, objectives_dict[idx], higher_is_better)
    ]
    new_front.append(new_index)
    return new_front


def calc_dist(p0, p1):
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(p0, p1)))


def calc_normalized_dist(point, ideal, w0_min, w0_max, w1_min, w1_max):
    norm_point = [
        (p - min_v) / (max_v - min_v) if max_v > min_v else p
        for p, min_v, max_v in zip(point, mins, maxs)
    ]
    norm_ideal = [
        (i - min_v) / (max_v - min_v) if max_v > min_v else i
        for i, min_v, max_v in zip(ideal, mins, maxs)
    ]
    return math.sqrt(sum((p - i) ** 2 for p, i in zip(norm_point, norm_ideal)))


def format_distance(dist: float) -> str:
    """将距离格式化为固定宽度字符串，用于字典序排序。"""
    return f"{dist:08.4f}"


def make_json_serializable(obj):
    if isinstance(obj, dict):
        return {k: make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, tuple):
        return [make_json_serializable(e) for e in obj]
    elif isinstance(obj, list):
        return [make_json_serializable(e) for e in obj]
    else:
        return obj


def gprint(verbose):
    return print if verbose else (lambda *args, **kwargs: None)


def generate_diffs(dictlist):
    """生成相邻字典之间的增量差异，支持嵌套字典。"""

    def dict_diff(d1, d2):
        diff = {}
        for k in d2:
            if k not in d1:
                diff[k] = d2[k]
            elif isinstance(d2[k], dict) and isinstance(d1.get(k), dict):
                nested = dict_diff(d1[k], d2[k])
                if nested:
                    diff[k] = nested
            elif d1[k] != d2[k]:
                diff[k] = d2[k]
        return diff

    prev = {}
    for d in dictlist:
        if not prev:
            yield d
        else:
            yield dict_diff(prev, d)
        prev = d


def deep_updated(base, diff):
    out = {}  # 构建新字典
    keys = base.keys() | diff.keys()
    for k in keys:
        if k in diff:
            v2 = diff[k]
            if isinstance(v2, dict) and isinstance(base.get(k), dict):
                out[k] = deep_updated(base[k], v2)
            else:
                out[k] = v2
        else:
            out[k] = base[k]
    return out


def generate_incremental_diff(prev, current):
    """返回两个字典之间的增量差异。"""

    def dict_diff(d1, d2):
        diff = {}
        for k in d2:
            if k not in d1:
                diff[k] = d2[k]
            elif isinstance(d2[k], dict) and isinstance(d1.get(k), dict):
                nested = dict_diff(d1[k], d2[k])
                if nested:
                    diff[k] = nested
            elif d1[k] != d2[k]:
                diff[k] = d2[k]
        return diff

    return dict_diff(prev or {}, current)


def apply_diffs(difflist, base=None):
    """依次应用增量差异，还原完整字典，支持嵌套字典。"""
    current = base or {}
    for d in difflist:
        current = deep_updated(current, d)
        yield current


def load_results(filepath):
    """
    生成器：通过应用增量差异还原每条完整配置。
    无需区分完整配置和增量差异。
    """
    with open(filepath, "rb") as f:
        unpacker = msgpack.Unpacker(f, raw=False)
        current = {}
        for entry in unpacker:
            for full_config in apply_diffs([entry], base=current):
                current = full_config
            yield current


def round_floats(obj: Any, sig_digits: int = 6) -> Any:
    if isinstance(obj, float):
        return pbr.round_dynamic(obj, sig_digits)
    elif isinstance(obj, dict):
        return {k: round_floats(v, sig_digits) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [round_floats(v, sig_digits) for v in obj]
    else:
        return obj


def round_floats_sig_digits(obj: Any, sig_digits: int) -> Any:
    if isinstance(obj, float):
        return pbr.round_dynamic(obj, sig_digits)
    elif isinstance(obj, dict):
        return {k: round_floats_sig_digits(v, sig_digits) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [round_floats_sig_digits(v, sig_digits) for v in obj]
    elif isinstance(obj, tuple):
        return tuple([round_floats_sig_digits(v, sig_digits) for v in obj])
    else:
        return obj


def round_floats_step(obj: Any, step: float) -> Any:
    if isinstance(obj, float):
        return pbr.round_(obj, step)
    elif isinstance(obj, dict):
        return {k: round_floats_step(v, step) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [round_floats_step(v, step) for v in obj]
    elif isinstance(obj, tuple):
        return tuple([round_floats_step(v, step) for v in obj])
    else:
        return obj


def quantize_floats(obj: Any, sig_digits: int = None, step: float = None) -> Any:
    """
    量化浮点数：若指定 step 则按步长取整，否则按有效位数取整。
    """
    if step is None:
        if sig_digits is None:
            raise Exception("必须提供 sig_digits 或 step")
        return round_floats_sig_digits(obj, sig_digits)
    else:
        return round_floats_step(obj, step)


def enforce_bounds_v2(obj: Any, bounds: Any = None, sig_digits: int = None):
    """
    对 obj 中每个元素施加上下限截断和取整。

    obj 可以为 bot 配置：
        - 从 config.optimize.bounds 取边界
        - 应用到 config.bot
    obj 可以为浮点数列表：
        - 要求 len(obj) == len(bounds)
        - obj 格式为 [float]
        - bounds 格式为 [[float]]
        - 每个 bounds 元素长度为 2 或 3
        - bound[0] 为下界，bound[1] 为上界
        - 若长度为 3，bound[2] 为步长
        - 若长度为 2，使用 sig_digits（缺失则报错）
    """
    pass
