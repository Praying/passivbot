import os
import sys
import argparse
from cli_utils import help_requested

if sys.platform.startswith("win"):
    # ==== Windows 平台 fcntl 桩模块 ====
    try:
        import fcntl
    except ImportError:
        # 创建一个伪模块，使后续 `import fcntl` 不会报错
        class _FcntlStub:
            LOCK_EX = None
            LOCK_SH = None
            LOCK_UN = None

            def lockf(self, *args, **kwargs):
                pass

            def ioctl(self, *args, **kwargs):
                pass

        sys.modules["fcntl"] = _FcntlStub()
        fcntl = sys.modules["fcntl"]
    # ==== Windows 平台 fcntl 桩模块结束 ====

# 导入编译模块前的 Rust 扩展检查
from rust_utils import check_and_maybe_compile, verify_loaded_runtime_extension

_rust_parser = argparse.ArgumentParser(add_help=False)
_rust_parser.add_argument("--skip-rust-compile", action="store_true", help="Skip Rust build check.")
_rust_parser.add_argument(
    "--force-rust-compile", action="store_true", help="Force rebuild of Rust extension."
)
_rust_parser.add_argument(
    "--fail-on-stale-rust",
    action="store_true",
    help="Abort if Rust extension appears stale instead of attempting rebuild.",
)
_rust_known, _rust_remaining = _rust_parser.parse_known_args()
_help_only = help_requested(_rust_remaining)
try:
    check_and_maybe_compile(
        skip=_help_only
        or _rust_known.skip_rust_compile
        or os.environ.get("SKIP_RUST_COMPILE", "").lower() in ("1", "true", "yes"),
        force=_rust_known.force_rust_compile,
        fail_on_stale=_rust_known.fail_on_stale_rust,
    )
except Exception as exc:
    print(f"Rust extension check failed: {exc}")
    sys.exit(1)
sys.argv = [sys.argv[0]] + _rust_remaining

import passivbot_rust as pbr
verify_loaded_runtime_extension()
from backtest import (
    prepare_hlcvs_mss,
    build_backtest_payload,
    execute_backtest,
)
import asyncio
import argparse
import multiprocessing
import time
from collections import defaultdict
from cli_utils import (
    add_help_all_argument,
    build_command_parser,
    expand_help_all_argv,
    get_cli_prog,
    help_all_requested,
)
from config import load_input_config, load_prepared_config, prepare_config
from config.access import get_optional_config_value, require_config_value
from config.limits import normalize_limit_entries, parse_limit_cli_entries
from config.metrics import resolve_metric_value
from config.scoring import (
    ObjectiveSpec,
    default_scoring_weights,
    extract_objective_specs,
    objective_index_map,
    objective_metric_names,
    to_engine_value,
)
from config.parse import load_raw_config as load_hjson_config
from config.schema import get_template_config
from warmup_utils import compute_backtest_warmup_minutes
from config_utils import (
    format_bot_config,
    add_config_arguments,
    project_template_config_for_cli,
    update_config_with_args,
    recursive_config_update,
    merge_negative_cli_values,
    clean_config,
    strip_config_metadata,
)
from pure_funcs import (
    denumpyize,
    sort_dict_keys,
    calc_hash,
    str2bool,
)
from utils import date_to_ts, ts_to_date, utc_ms, make_get_filepath, format_approved_ignored_coins
from logging_setup import configure_logging, resolve_log_level
from copy import deepcopy
import gc
import numpy as np
from uuid import uuid4
import logging
import traceback
import json
import pprint

try:
    from deap import base, creator, tools, algorithms
except ImportError:  # pragma: no cover - 允许在最小化测试环境中导入

    class _DummyFitness:
        weights = ()

        def __init__(self, values=()):
            self.values = values

        def wvalues(self):
            return self.values

    class _DummyBase:
        Fitness = _DummyFitness

    class _DummyCreator:
        def create(self, *args, **kwargs):
            return None

        def __getattr__(self, name):
            raise AttributeError

    base = _DummyBase()
    creator = _DummyCreator()
    tools = algorithms = None
import math
import fcntl
from optimizer_overrides import optimizer_overrides
from opt_utils import (
    make_json_serializable,
    generate_incremental_diff,
    round_floats,
    quantize_floats,
    deep_updated,
)
from limit_utils import expand_limit_checks, compute_limit_violation
from pareto_store import ParetoStore
import msgpack
from typing import Sequence, Tuple, List, Dict, Any, Optional
from shared_arrays import SharedArrayManager, attach_shared_array
from ohlcv_utils import align_and_aggregate_hlcvs
from optimize_suite import (
    ScenarioEvalContext,
    prepare_suite_contexts,
)
from suite_runner import (
    SuiteScenario,
    ScenarioResult,
    extract_suite_config,
    filter_scenarios_by_label,
    aggregate_metrics,
    build_suite_metrics_payload,
)
from metrics_schema import build_scenario_metrics, flatten_metric_stats
from optimization.bounds import (
    Bound,
    enforce_bounds,
)
from optimization.backend_shared import cancel_pending_async_results, drain_async_results
from optimization.config_adapter import extract_bounds_tuple_list_from_config
from optimization.backends import get_backend_runner
from optimization.config_adapter import get_optimization_key_paths, OPTIMIZABLE_BOT_KEY_PATHS
from optimization.warmup import (
    build_optimizer_vector_config,
    compute_optimizer_per_coin_warmup_minutes,
    stamp_warmup_metadata,
)
from optimization.shape import OptimizationShape, build_optimization_shape
from optimization.deap_adapters import (
    mutPolynomialBoundedWrapper,
    cxSimulatedBinaryBoundedWrapper,
)
from multiprocessing_utils import ignore_sigint_in_worker


class ConstraintAwareFitness(base.Fitness):
    constraint_violation: float = 0.0

    def dominates(self, other, obj=slice(None)):
        self_violation = getattr(self, "constraint_violation", 0.0)
        other_violation = getattr(other, "constraint_violation", 0.0)
        if math.isclose(self_violation, other_violation, rel_tol=0.0, abs_tol=1e-12):
            return super().dominates(other, obj)
        return self_violation < other_violation


def _apply_config_overrides(config: Dict[str, Any], overrides: Dict[str, Any]) -> None:
    if not overrides:
        return
    for dotted_path, value in overrides.items():
        if not isinstance(dotted_path, str):
            continue
        parts = dotted_path.split(".")
        if not parts:
            continue
        target = config
        for part in parts[:-1]:
            if part not in target or not isinstance(target[part], dict):
                target[part] = {}
            target = target[part]
        target[parts[-1]] = value


_BOOL_LITERALS = {"1", "0", "true", "false", "t", "f", "yes", "no", "y", "n"}


def _looks_like_bool_token(value: str) -> bool:
    return value.lower() in _BOOL_LITERALS


def _normalize_optional_bool_flag(argv: list[str], flag: str) -> list[str]:
    """对可选布尔参数进行规范化：当参数后跟非布尔非选项值时，自动插入 =true。"""
    result: list[str] = []
    i = 0
    while i < len(argv):
        token = argv[i]
        if token == flag:
            next_token = argv[i + 1] if i + 1 < len(argv) else None
            if (
                next_token
                and not next_token.startswith("-")
                and not _looks_like_bool_token(next_token)
            ):
                result.append(f"{flag}=true")
                i += 1
                continue
        result.append(token)
        i += 1
    return result


def _maybe_aggregate_backtest_data(hlcvs, timestamps, btc_usd_prices, mss, config):
    """当 candle_interval_minutes > 1 时，将 1m K线聚合到目标周期。"""
    candle_interval = int(config.get("backtest", {}).get("candle_interval_minutes", 1) or 1)
    if candle_interval <= 1:
        return hlcvs, timestamps, btc_usd_prices
    n_before = hlcvs.shape[0]
    hlcvs, timestamps, btc_usd_prices, offset_bars = align_and_aggregate_hlcvs(
        hlcvs, timestamps, btc_usd_prices, candle_interval
    )
    logging.debug(
        "[optimize] 聚合 %dm K线: %d 根 -> %d 根（对齐裁剪 %d 根）",
        candle_interval,
        n_before,
        hlcvs.shape[0],
        offset_bars,
    )
    meta = mss.setdefault("__meta__", {})
    meta["data_interval_minutes"] = candle_interval
    meta["candle_interval_offset_bars"] = int(offset_bars)
    if timestamps is not None and len(timestamps) > 0:
        meta["effective_start_ts"] = int(timestamps[0])
        meta["effective_start_date"] = ts_to_date(int(timestamps[0]))
    return hlcvs, timestamps, btc_usd_prices


def _stamp_optimizer_warmup(config: dict, mss: dict, coins: list[str]) -> None:
    """
    用优化器搜索空间实际能产生的最坏情况预热值覆盖
    ``mss[coin]["warmup_minutes"]`` 和 ``["trade_start_index"]``，
    从 ``optimize.bounds`` 计算，而非使用模板机器人的值。

    ``prepare_hlcvs_mss`` 从 ``compute_per_coin_warmup_minutes(config)``
    写入这些字段，该函数直接读取 ``bot.*`` 且不了解边界。
    当用户的模板机器人具有较大的装饰性值（例如 ``entry_volatility_ema_span_hours=1690``）
    但边界将这些字段限制得很低时，每次优化器回测都会在模板大小的窗口上交易，
    而非搜索空间大小的窗口。
    此辅助函数通过合成一个最大边界个体，通过 ``individual_to_config`` 处理，
    并从结果配置重新计算预热来纠正写入。

    必须在 ``prepare_hlcvs_mss`` 之后、Evaluator 读取 ``mss`` 之前调用。
    """
    warmup_map = compute_optimizer_per_coin_warmup_minutes(config)
    stamped = stamp_warmup_metadata(mss, coins, warmup_map)
    if stamped:
        summary = ", ".join(
            f"{count}x(warmup={w},start={s})" for (w, s), count in stamped.items()
        )
        logging.info(
            "优化器预热已从边界写入 | %d 个币种 | %s",
            sum(stamped.values()),
            summary,
        )


def _register_exchange_data(
    exchange: str,
    prepare_result: tuple,
    config: dict,
    *,
    msss: dict,
    hlcvs_specs: dict,
    btc_usd_specs: dict,
    timestamps_dict: dict,
    array_manager: SharedArrayManager,
) -> tuple[list[str], dict]:
    """
    将一个交易所的准备数据注册到优化器的共享内存池中。
    整合了之前组合模式和按交易所模式分支中重复的设置逻辑。
    与原始内联代码无行为差异；修复详见提交历史。
    """
    coins, hlcvs, mss, _results_path, _cache_dir, btc_usd_prices, timestamps = prepare_result
    hlcvs, timestamps, btc_usd_prices = _maybe_aggregate_backtest_data(
        hlcvs, timestamps, btc_usd_prices, mss, config
    )
    _stamp_optimizer_warmup(config, mss, coins)
    timestamps_dict[exchange] = timestamps
    config["backtest"]["coins"][exchange] = coins
    msss[exchange] = mss
    validate_array(hlcvs, "hlcvs")
    hlcvs_array = np.ascontiguousarray(hlcvs, dtype=np.float64)
    hlcvs_spec, _ = array_manager.create_from(hlcvs_array)
    hlcvs_specs[exchange] = hlcvs_spec
    btc_usd_array = np.ascontiguousarray(btc_usd_prices, dtype=np.float64)
    validate_array(btc_usd_array, f"btc_usd_data for {exchange}", allow_nan=False)
    btc_usd_spec, _ = array_manager.create_from(btc_usd_array)
    btc_usd_specs[exchange] = btc_usd_spec
    return coins, mss


class ResultRecorder:
    """记录优化结果：维护 Pareto 前沿存储，可选写入全量结果二进制文件。"""
    def __init__(
        self,
        *,
        results_dir: str,
        sig_digits: int,
        flush_interval: int,
        scoring_keys: Sequence[str],
        compress: bool,
        write_all_results: bool,
        pareto_max_size: int = 1000,
        bounds: Optional[Sequence[Bound]] = None,
    ):
        """初始化结果记录器，创建 Pareto 存储和可选的全量结果文件。"""
        self.store = ParetoStore(
            directory=results_dir,
            sig_digits=sig_digits,
            bounds=bounds,
            flush_interval=flush_interval,
            log_name="optimizer.pareto",
            max_size=pareto_max_size,
        )
        self.write_all = write_all_results
        self.compress = compress
        self.results_file = None
        self.packer = None
        if self.write_all:
            filename = os.path.join(results_dir, "all_results.bin")
            self.results_file = open(filename, "ab")
            self.packer = msgpack.Packer(use_bin_type=True)
        self.prev_data = None
        self.counter = 0
        self.scoring_specs = extract_objective_specs(scoring_keys)
        self.scoring_keys = [spec.metric for spec in self.scoring_specs]

    def record(self, data: dict) -> None:
        """记录一条评估结果：写入全量文件（可选）并更新 Pareto 前沿。"""
        if self.write_all and self.results_file:
            if self.compress:
                if self.prev_data is None or self.counter % 100 == 0:
                    output_data = make_json_serializable(data)
                else:
                    diff = generate_incremental_diff(self.prev_data, data)
                    output_data = make_json_serializable(diff)
                self.counter += 1
                self.prev_data = data
            else:
                output_data = data
            try:
                self.results_file.write(self.packer.pack(output_data))
                self.results_file.flush()
            except Exception as exc:
                logging.error(f"写入结果时出错: {exc}")
        metrics_block = data.get("metrics", {}) or {}
        violation = metrics_block.get("constraint_violation")
        try:
            updated = self.store.add_entry(data)
        except Exception as exc:
            logging.error(f"ParetoStore 错误: {exc}")
        else:
            if updated:
                objectives_block = metrics_block.get("objectives", {})
                violation_str = (
                    f" | constraint={pbr.round_dynamic(violation, 3)}"
                    if isinstance(violation, (int, float))
                    else ""
                )
                logging.info(
                    "Pareto 更新 | 评估=%d | 前沿=%d | 目标=%s%s",
                    self.store.n_iters,
                    len(self.store._front),
                    _format_objectives(objectives_block, scoring_keys=self.scoring_keys),
                    violation_str,
                )

    def flush(self) -> None:
        self.store.flush_now()

    def close(self) -> None:
        if self.results_file:
            self.results_file.close()


logging.basicConfig(
    format="%(asctime)s %(processName)-12s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S",
)


TEMPLATE_CONFIG_MODE = "v7"
INVALID_BACKTEST_CANDIDATE_PENALTY = 1e18
DEFAULT_PARETO_MAX_SIZE = 1000
_RECOVERABLE_BACKTEST_PANIC_PATTERNS = (
    "hard-stop evaluation failed",
    "equity must be finite and > 0",
    "peak_strategy_equity must be finite and > 0",
)


def _format_objectives(
    values: Sequence[float] | dict[str, float],
    *,
    scoring_keys: Sequence[str] | None = None,
) -> str:
    """将目标值格式化为紧凑的可读字符串，用于日志输出。"""
    if isinstance(values, dict):
        order = list(scoring_keys or values.keys())
        parts = []
        for key in order:
            value = values.get(key)
            if value is None:
                continue
            parts.append(f"{key}={float(value):.3g}")
        return "[" + ", ".join(parts) + "]" if parts else "[]"
    if isinstance(values, np.ndarray):
        values = values.tolist()
    if not values:
        return "[]"
    return "[" + ", ".join(f"{float(v):.3g}" for v in values) + "]"


def _is_recoverable_backtest_candidate_error(exc: BaseException) -> bool:
    name = exc.__class__.__name__
    if name != "PanicException":
        return False
    message = str(exc)
    return any(pattern in message for pattern in _RECOVERABLE_BACKTEST_PANIC_PATTERNS)


def _build_invalid_candidate_metrics(
    scoring_keys: Sequence[str],
    error: str,
    *,
    include_stats: bool = True,
    include_suite_metrics: bool = False,
) -> tuple[tuple[float, ...], float, dict]:
    """构造无效候选个体（回测失败）的惩罚性指标和目标值。"""
    specs = extract_objective_specs(scoring_keys)
    raw_objectives = {spec.metric: 0.0 for spec in specs}
    objectives = tuple(to_engine_value(spec, 0.0) for spec in specs)
    metrics_payload = {
        "objectives": raw_objectives,
        "constraint_violation": INVALID_BACKTEST_CANDIDATE_PENALTY,
        "error": error,
    }
    if include_stats:
        metrics_payload["stats"] = {}
    if include_suite_metrics:
        metrics_payload["suite_metrics"] = {}
    return objectives, INVALID_BACKTEST_CANDIDATE_PENALTY, metrics_payload


def _analysis_indicates_liquidation(analysis: dict | None, config: dict) -> bool:
    del config
    if not isinstance(analysis, dict):
        return False
    liquidated = analysis.get("liquidated")
    if not isinstance(liquidated, (bool, int)):
        return False
    return bool(liquidated)


def _set_candidate_metrics(individual, metrics_payload) -> None:
    if hasattr(individual, "__dict__"):
        individual.evaluation_metrics = metrics_payload


def _clear_candidate_metrics(individual) -> None:
    if hasattr(individual, "evaluation_metrics"):
        delattr(individual, "evaluation_metrics")


def _record_individual_result(individual, evaluator_config, overrides_list, recorder):
    """将个体的回测指标写入结果记录器，包含配置和约束违反信息。"""
    metrics = getattr(individual, "evaluation_metrics", {}) or {}
    suite_metrics = metrics.pop("suite_metrics", None)
    config = individual_to_config(individual, optimizer_overrides, overrides_list, evaluator_config)
    entry = clean_config(strip_config_metadata(config))
    if suite_metrics is not None:
        entry["suite_metrics"] = suite_metrics
        bt = entry.get("backtest")
        if isinstance(bt, dict):
            bt.pop("coins", None)
    if metrics:
        if "constraint_violation" not in metrics:
            violation = getattr(individual, "constraint_violation", None)
            if violation is not None:
                metrics["constraint_violation"] = violation
        entry["metrics"] = metrics
    recorder.record(entry)
    _clear_candidate_metrics(individual)


def ea_mu_plus_lambda_stream(
    population,
    toolbox,
    mu,
    lambda_,
    cxpb,
    mutpb,
    ngen,
    stats,
    halloffame,
    verbose,
    recorder,
    evaluator_config,
    overrides_list,
    pool,
    duplicate_counter,
    pool_state,
):
    """基于 (μ+λ) 策略的流式进化算法主循环，支持异步并行评估和重复检测。"""
    logbook = tools.Logbook()
    logbook.header = "gen", "evals", "min", "max"

    start_time = time.time()
    total_evals = 0
    liquidation_total = 0
    liquidation_prev_total = 0

    def evaluate_and_record(individuals):
        """异步评估一批个体并记录结果，返回已完成评估数。"""
        nonlocal total_evals, liquidation_total
        if not individuals:
            return 0
        logging.debug("正在评估 %d 个候选个体", len(individuals))
        pending = {}
        for idx, ind in enumerate(individuals):
            pending[pool.apply_async(toolbox.evaluate, (ind,))] = idx

        completed = {"count": 0}

        def _on_result(idx, payload):
            """异步评估结果回调：设置适应度、记录指标和破产计数。"""
            nonlocal liquidation_total
            fit_values, penalty, metrics = payload
            ind = individuals[idx]
            ind.fitness.values = fit_values
            ind.fitness.constraint_violation = penalty
            ind.constraint_violation = penalty
            if metrics and isinstance(metrics, dict):
                suite = metrics.get("suite_metrics", {}) or {}
                metric_map = suite.get("metrics", {}) or {}
                adg_entry = metric_map.get("adg_pnl", {}) or {}
                prh_entry = metric_map.get("peak_recovery_hours_pnl", {}) or {}
                logging.debug(
                    "评估指标 | 索引=%d adg_pnl=%s peak_recovery_hours_pnl=%s",
                    idx,
                    adg_entry.get("aggregated"),
                    prh_entry.get("aggregated"),
                )
                scenario_labels = suite.get("scenario_labels") or []
                if not scenario_labels and isinstance(adg_entry, dict):
                    scenario_labels = list((adg_entry.get("scenarios") or {}).keys())
                for label in scenario_labels:
                    adg_val = (adg_entry.get("scenarios") or {}).get(label)
                    prh_val = (prh_entry.get("scenarios") or {}).get(label)
                    logging.debug(
                        "评估指标场景 | 索引=%d 标签=%s adg_pnl=%s peak_recovery_hours_pnl=%s",
                        idx,
                        label,
                        adg_val,
                        prh_val,
                    )
            if metrics is not None:
                if bool(metrics.get("liquidated")):
                    liquidation_total += 1
                _set_candidate_metrics(ind, metrics)
                _record_individual_result(ind, evaluator_config, overrides_list, recorder)
            else:
                _clear_candidate_metrics(ind)
            completed["count"] += 1

        def _on_interrupt(still_pending):
            logging.info("评估中断；正在终止待处理任务...")
            cancel_pending_async_results(still_pending)
            if not pool_state["terminated"]:
                logging.info("由于中断，立即终止工作进程池...")
                pool.terminate()
                pool_state["terminated"] = True
        drain_async_results(
            pending,
            poll_interval_seconds=0.1,
            on_result=_on_result,
            on_interrupt=_on_interrupt,
        )

        total_evals += completed["count"]
        return completed["count"]

    dup_prev_total = 0
    dup_prev_resolved = 0
    dup_prev_reused = 0

    def log_generation(gen, nevals, record):
        """输出每代进化统计日志：评估数、前沿大小、重复率、破产数等。"""
        nonlocal dup_prev_total, dup_prev_resolved, dup_prev_reused, liquidation_prev_total
        best = record.get("min") if record else None
        front_size = len(halloffame) if halloffame is not None else 0
        dup_tot = duplicate_counter["total"]
        dup_res = duplicate_counter["resolved"]
        dup_reuse = duplicate_counter["reused"]
        dup_ratio = (dup_tot / total_evals) if total_evals else 0.0
        dup_delta = dup_tot - dup_prev_total
        dup_res_delta = dup_res - dup_prev_resolved
        dup_reuse_delta = dup_reuse - dup_prev_reused
        dup_gen_ratio = (dup_delta / nevals) if nevals else 0.0
        liquidation_delta = liquidation_total - liquidation_prev_total
        logging.info(
            (
                "第 %d 代完成 | 评估=%d | 总计=%d | 前沿=%d | 最佳=%s | "
                "重复=%d (已解决=%d 已重用=%d) | 重复增量=%d (解决=%d 重用=%d) | "
                "重复率=%.2f%% | 本代重复=%.2f%% | "
                "破产数=%d (增量=%d) | 耗时=%.1fs"
            ),
            gen,
            nevals,
            total_evals,
            front_size,
            _format_objectives(best),
            dup_tot,
            dup_res,
            dup_reuse,
            dup_delta,
            dup_res_delta,
            dup_reuse_delta,
            dup_ratio * 100.0,
            dup_gen_ratio * 100.0,
            liquidation_total,
            liquidation_delta,
            time.time() - start_time,
        )
        dup_prev_total = dup_tot
        dup_prev_resolved = dup_res
        dup_prev_reused = dup_reuse
        liquidation_prev_total = liquidation_total
        if verbose and record:
            logging.debug("日志记录: %s", " ".join(f"{k}={v}" for k, v in record.items()))

    # 评估种群中尚未计算适应度的个体
    invalid_ind = [ind for ind in population if not ind.fitness.valid]
    if invalid_ind:
        logging.info("正在评估初始种群（%d 个候选个体）...", len(invalid_ind))
    nevals = evaluate_and_record(invalid_ind)

    if halloffame is not None:
        halloffame.update(population)

    # 记录第 0 代统计
    record = stats.compile(population) if stats is not None else {}
    logbook.record(gen=0, nevals=nevals, **record)
    log_generation(0, nevals, record)

    # 种群太小无法交叉/变异则直接返回
    if len(population) < 2:
        logging.warning(
            "种群太小，无法进行交叉/变异（大小=%d）；跳过进化步骤",
            len(population),
        )
        return population, logbook

    for gen in range(1, ngen + 1):
        # 生成子代：通过交叉和变异
        offspring = algorithms.varOr(population, toolbox, lambda_, cxpb, mutpb)
        # 只评估未计算过适应度的个体
        invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
        nevals = evaluate_and_record(invalid_ind)

        # 环境选择：从父代+子代中选择最优的 mu 个
        population[:] = toolbox.select(population + offspring, mu)

        if halloffame is not None:
            halloffame.update(population)

        record = stats.compile(population) if stats is not None else {}
        logbook.record(gen=gen, nevals=nevals, **record)
        log_generation(gen, nevals, record)

    logging.info(
        "优化摘要 | 代数=%d | 总评估=%d | 前沿=%d | 耗时=%.1fs",
        ngen,
        total_evals,
        len(halloffame) if halloffame is not None else 0,
        time.time() - start_time,
    )
    return population, logbook


def individual_to_config(individual, optimizer_overrides, overrides_list, template, key_paths=None):
    """
    假设个体已经过边界约束处理（或即将处理）
    """
    return build_optimizer_vector_config(
        individual,
        template,
        key_paths=key_paths,
        overrides_list=overrides_list,
    )


def config_to_individual(
    config,
    bounds,
    sig_digits=None,
    key_paths=None,
    optimization_shape: OptimizationShape | None = None,
):
    """从配置字典提取参数值并转换为优化个体向量，同时施加边界约束。"""
    if optimization_shape is not None:
        bounds = optimization_shape.bounds
        if sig_digits is None:
            sig_digits = optimization_shape.sig_digits
        if key_paths is None:
            key_paths = optimization_shape.key_paths
    values = []
    if key_paths is None:
        key_paths = get_optimization_key_paths(config)
    for _, path in key_paths:
        target = config
        for part in path:
            target = target[part]
        values.append(target)
    return enforce_bounds(
        values,
        bounds,
        sig_digits,
    )


def validate_array(arr, name, allow_nan=True):
    if not allow_nan and np.isnan(arr).any():
        raise ValueError(f"{name} contains NaN values")
    if np.isinf(arr).any():
        raise ValueError(f"{name} contains inf values")
    if allow_nan and np.isnan(arr).all():
        raise ValueError(f"{name} is entirely NaN")


class Evaluator:
    """优化评估器：执行回测、计算适应度、管理重复检测和共享内存数据。"""
    def __init__(
        self,
        hlcvs_specs,
        btc_usd_specs,
        msss,
        config,
        seen_hashes=None,
        duplicate_counter=None,
        timestamps=None,
        shared_array_manager: SharedArrayManager | None = None,
    ):
        """初始化评估器：绑定共享内存数据、构建优化形状和约束检查。"""
        logging.debug("正在初始化 Evaluator...")
        self.hlcvs_specs = hlcvs_specs
        self.btc_usd_specs = btc_usd_specs
        self.msss = msss
        self.timestamps = timestamps or {}
        self.exchanges = list(hlcvs_specs.keys())
        self.shared_array_manager = shared_array_manager
        self.shared_hlcvs_np = {}
        self.shared_btc_np = {}
        self._attachments = {"hlcvs": {}, "btc": {}}

        for exchange in self.exchanges:
            logging.debug("正在为 %s 准备缓存参数...", exchange)
            if self.shared_array_manager is not None:
                self.shared_hlcvs_np[exchange] = self.shared_array_manager.view(
                    self.hlcvs_specs[exchange]
                )
                btc_spec = self.btc_usd_specs.get(exchange)
                if btc_spec is not None:
                    self.shared_btc_np[exchange] = self.shared_array_manager.view(btc_spec)

        self.config = config
        logging.debug("Evaluator 初始化完成。")
        logging.info("Evaluator 就绪 | 交易所=%d", len(self.exchanges))
        self.seen_hashes = seen_hashes if seen_hashes is not None else {}
        self.duplicate_counter = duplicate_counter if duplicate_counter is not None else {"count": 0}
        self.optimization_shape = build_optimization_shape(self.config)
        self.bounds = list(self.optimization_shape.bounds)
        self.key_paths = list(self.optimization_shape.key_paths)
        self.sig_digits = self.optimization_shape.sig_digits
        self.use_duplicate_guard = True
        self.scoring_specs = extract_objective_specs(self.config)
        self.scoring_weights = default_scoring_weights()

        self.build_limit_checks()

    def _ensure_attached(self, exchange: str) -> None:
        if exchange not in self.shared_hlcvs_np:
            spec = self.hlcvs_specs[exchange]
            attachment = attach_shared_array(spec)
            self._attachments["hlcvs"][exchange] = attachment
            self.shared_hlcvs_np[exchange] = attachment.array
        if exchange not in self.shared_btc_np:
            btc_spec = self.btc_usd_specs.get(exchange)
            if btc_spec is not None:
                attachment = attach_shared_array(btc_spec)
                self._attachments["btc"][exchange] = attachment
                self.shared_btc_np[exchange] = attachment.array

    def perturb_step_digits(self, individual, change_chance=0.5):
        """按有效数字步长扰动个体参数，保持精度一致性。"""
        perturbed = []
        for i, val in enumerate(individual):
            if np.random.random() < change_chance:  # x% 概率保持不变
                perturbed.append(val)
                continue
            bound = self.bounds[i]
            if bound.high == bound.low:
                perturbed.append(val)
                continue

            # 对于步进参数，按定义的步长移动
            if bound.is_stepped:
                step = bound.step
            elif val != 0.0:
                exponent = math.floor(math.log10(abs(val))) - (self.sig_digits - 1)
                step = 10**exponent
            else:
                step = (bound.high - bound.low) * 10 ** -(self.sig_digits - 1)

            direction = np.random.choice([-1.0, 1.0])
            new_val = val + step * direction
            # 对于步进参数，不进行 round_dynamic；量化将在 enforce_bounds 中进行
            if bound.is_stepped:
                perturbed.append(new_val)
            else:
                perturbed.append(pbr.round_dynamic(new_val, self.sig_digits))

        return perturbed

    def perturb_x_pct(self, individual, magnitude=0.01):
        perturbed = []
        for i, val in enumerate(individual):
            bound = self.bounds[i]
            if bound.high == bound.low:
                perturbed.append(val)
                continue
            new_val = val * (1 + np.random.uniform(-magnitude, magnitude))
            # 对于步进参数，不进行 round_dynamic；量化将在 enforce_bounds 中进行
            if bound.is_stepped:
                perturbed.append(new_val)
            else:
                perturbed.append(pbr.round_dynamic(new_val, self.sig_digits))
        return perturbed

    def perturb_random_subset(self, individual, frac=0.2):
        perturbed = list(individual)
        n = len(individual)
        indices = np.random.choice(n, max(1, int(frac * n)), replace=False)
        for i in indices:
            bound = self.bounds[i]
            if bound.low != bound.high:
                if bound.is_stepped:
                    # 对于步进参数，按 +/- 步长移动
                    direction = np.random.choice([-1.0, 1.0])
                    perturbed[i] = individual[i] + bound.step * direction
                else:
                    delta = (bound.high - bound.low) * 0.01
                    perturbed[i] = individual[i] + delta * np.random.uniform(-1.0, 1.0)
        return perturbed

    def perturb_sample_some(self, individual, frac=0.2):
        perturbed = list(individual)
        n = len(individual)
        indices = np.random.choice(n, max(1, int(frac * n)), replace=False)
        for i in indices:
            bound = self.bounds[i]
            if bound.low != bound.high:
                perturbed[i] = bound.random_on_grid()
        return perturbed

    def perturb_gaussian(self, individual, scale=0.01):
        """以高斯分布扰动参数，步进参数按步数偏移。"""
        perturbed = []
        for i, val in enumerate(individual):
            bound = self.bounds[i]
            if bound.high == bound.low:
                perturbed.append(val)
                continue
            if bound.is_stepped:
                # 对于步进参数，生成高斯分布的步进数
                max_steps = (bound.high - bound.low) / bound.step
                n_steps = int(np.random.normal(0, scale * max_steps) + 0.5)
                perturbed.append(val + n_steps * bound.step)
            else:
                noise = np.random.normal(0, scale * (bound.high - bound.low))
                perturbed.append(val + noise)
        return perturbed

    def perturb_large_uniform(self, individual):
        perturbed = []
        for i in range(len(individual)):
            bound = self.bounds[i]
            if bound.low == bound.high:
                perturbed.append(bound.low)
            else:
                perturbed.append(bound.random_on_grid())
        return perturbed

    def evaluate(self, individual, overrides_list):
        """评估单个个体：施加边界、检测重复、执行回测并计算适应度。"""
        individual[:] = enforce_bounds(individual, self.bounds, self.sig_digits)
        config = individual_to_config(
            individual,
            optimizer_overrides,
            overrides_list,
            self.config,
            key_paths=self.key_paths,
        )
        individual_hash = calc_hash(individual)
        # 重复检测：如果个体已评估过，尝试扰动产生新个体
        if self.use_duplicate_guard:
            if individual_hash in self.seen_hashes:
                existing_entry = self.seen_hashes[individual_hash]
                existing_score = None
                existing_penalty = 0.0
                if existing_entry is not None:
                    existing_score, existing_penalty = existing_entry
                self.duplicate_counter["total"] += 1
                perturbation_funcs = [
                    self.perturb_x_pct,
                    self.perturb_step_digits,
                    self.perturb_gaussian,
                    self.perturb_random_subset,
                    self.perturb_sample_some,
                    self.perturb_large_uniform,
                ]
                for perturb_fn in perturbation_funcs:
                    perturbed = perturb_fn(individual)
                    perturbed = enforce_bounds(perturbed, self.bounds, self.sig_digits)
                    new_hash = calc_hash(perturbed)
                    # 找到未见过的新个体，替换并继续评估
                    if new_hash not in self.seen_hashes:
                        individual[:] = perturbed
                        self.seen_hashes[new_hash] = None
                        config = individual_to_config(
                            perturbed,
                            optimizer_overrides,
                            overrides_list,
                            self.config,
                            key_paths=self.key_paths,
                        )
                        self.duplicate_counter["resolved"] += 1
                        break
                else:
                    # 所有扰动仍重复，复用已有结果
                    if existing_score is not None:
                        self.duplicate_counter["reused"] += 1
                        return tuple(existing_score), existing_penalty, None
            else:
                self.seen_hashes[individual_hash] = None
        analyses = {}
        liquidated = False
        # 对每个交易所执行回测
        for exchange in self.exchanges:
            self._ensure_attached(exchange)
            payload = build_backtest_payload(
                self.shared_hlcvs_np[exchange],
                self.msss[exchange],
                config,
                exchange,
                self.shared_btc_np[exchange],
                self.timestamps.get(exchange),
                metrics_only=True,
            )
            try:
                fills, equities_array, analysis = execute_backtest(payload, config)
            except BaseException as exc:
                # 可恢复的回测错误：标记为无效候选并返回惩罚值
                if not _is_recoverable_backtest_candidate_error(exc):
                    raise
                error = f"{exc.__class__.__name__}: {exc}"
                logging.debug(
                    "优化器候选因可恢复回测失败而无效 | hash=%s | 交易所=%s | 错误=%s",
                    individual_hash[:12],
                    exchange,
                    error,
                )
                objectives, total_penalty, metrics_payload = _build_invalid_candidate_metrics(
                    self.config["optimize"]["scoring"],
                    error,
                    include_stats=True,
                )
                _set_candidate_metrics(individual, metrics_payload)
                actual_hash = calc_hash(individual)
                self.seen_hashes[actual_hash] = (tuple(objectives), total_penalty)
                return tuple(objectives), total_penalty, metrics_payload
            analyses[exchange] = analysis
            liquidated = liquidated or _analysis_indicates_liquidation(analysis, config)

            # 显式释放大型中间数组以保持工作进程 RSS 内存较低。
            del fills
            del equities_array
        scenario_metrics = build_scenario_metrics(analyses)
        aggregate_stats = scenario_metrics.get("stats", {})
        flat_stats = flatten_metric_stats(aggregate_stats)
        objectives, total_penalty, raw_objectives = self.calc_fitness(
            flat_stats, return_raw_objectives=True
        )
        metrics_payload = {
            "stats": aggregate_stats,
            "objectives": raw_objectives,
            "constraint_violation": total_penalty,
            "liquidated": liquidated,
        }
        _set_candidate_metrics(individual, metrics_payload)
        actual_hash = calc_hash(individual)
        if self.use_duplicate_guard:
            self.seen_hashes[actual_hash] = (tuple(objectives), total_penalty)
        return tuple(objectives), total_penalty, metrics_payload

    def build_limit_checks(self, aggregate_cfg: Dict[str, Any] | None = None):
        limits = self.config["optimize"].get("limits", [])
        self.limit_checks = expand_limit_checks(
            limits,
            self.scoring_weights,
            penalty_weight=1e6,
            objective_index_map=objective_index_map(self.scoring_specs),
            aggregate_cfg=aggregate_cfg,
        )

    def calc_fitness(self, analyses_combined, *, return_raw_objectives: bool = False):
        """根据评分规格计算适应度值，应用约束违反惩罚。"""
        per_objective_modifier = [0.0] * len(self.scoring_specs)
        global_modifier = 0.0
        for check in self.limit_checks:
            val = resolve_metric_value(analyses_combined, check["metric_key"])
            penalty = compute_limit_violation(check, val)
            if not penalty:
                continue
            targets = check.get("objective_indexes") or []
            if targets:
                for idx in targets:
                    if 0 <= idx < len(per_objective_modifier):
                        per_objective_modifier[idx] += penalty
            else:
                global_modifier += penalty

        total_penalty = global_modifier + sum(per_objective_modifier)
        engine_scores = []
        raw_objectives: Dict[str, float] = {}
        for idx, spec in enumerate(self.scoring_specs):
            val = resolve_metric_value(analyses_combined, f"{spec.metric}_mean")
            if val is None and spec.metric.endswith(("_usd", "_btc")):
                val = resolve_metric_value(analyses_combined, f"{spec.metric.rsplit('_', 1)[0]}_mean")

            if val is None:
                val = 0
            raw_value = float(val)
            raw_objectives[spec.metric] = raw_value
            penalty_total = global_modifier + per_objective_modifier[idx]
            if penalty_total:
                engine_scores.append(penalty_total)
            else:
                engine_scores.append(to_engine_value(spec, raw_value))
        if return_raw_objectives:
            return tuple(engine_scores), total_penalty, raw_objectives
        return tuple(engine_scores), total_penalty

    def __del__(self):
        for attachment_map in self._attachments.values():
            for attachment in attachment_map.values():
                attachment.close()

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("shared_hlcvs_np", None)
        state.pop("shared_btc_np", None)
        state.pop("_attachments", None)
        state.pop("shared_array_manager", None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.shared_array_manager = None
        self.shared_hlcvs_np = {}
        self.shared_btc_np = {}
        self._attachments = {"hlcvs": {}, "btc": {}}
        for exchange in self.exchanges:
            self._ensure_attached(exchange)


class SuiteEvaluator:
    """多场景评估器：在不同回测场景上评估个体，聚合指标后计算适应度。"""
    def __init__(
        self,
        base_evaluator: Evaluator,
        scenario_contexts: List[ScenarioEvalContext],
        aggregate_cfg: Dict[str, Any],
    ) -> None:
        self.base = base_evaluator
        self.contexts = scenario_contexts
        self.aggregate_cfg = aggregate_cfg
        self.base.build_limit_checks(self.aggregate_cfg)
        # 主数据集附件缓存（场景间共享）
        self._master_attachments: Dict[str, Dict[str, Any]] = {"hlcvs": {}, "btc": {}}
        self._master_arrays: Dict[str, Dict[str, np.ndarray]] = {"hlcvs": {}, "btc": {}}

    def _ensure_master_attachment(self, spec, cache_key: str, array_type: str) -> np.ndarray:
        """如果尚未附加，则附加到主 SharedMemory。"""
        if cache_key not in self._master_arrays[array_type]:
            attachment = attach_shared_array(spec)
            self._master_attachments[array_type][cache_key] = attachment
            self._master_arrays[array_type][cache_key] = attachment.array
        return self._master_arrays[array_type][cache_key]

    def _get_lazy_slice_data(
        self, ctx: ScenarioEvalContext, exchange: str
    ) -> tuple[np.ndarray, np.ndarray | None, list[int] | None]:
        """
        获取惰性切片模式的数据。
        返回 (hlcvs_view, btc_view, coin_indices)。

        此处仅应用时间切片（创建视图，O(1) 内存）。
        币种子集化延迟到 build_backtest_payload 中高效处理。
        """
        master_spec = ctx.master_hlcvs_specs[exchange]
        master_array = self._ensure_master_attachment(master_spec, master_spec.name, "hlcvs")

        time_slice = ctx.time_slice.get(exchange) if ctx.time_slice else None
        coin_indices = ctx.coin_slice_indices.get(exchange) if ctx.coin_slice_indices else None

        # 时间切片创建视图（无拷贝，O(1) 内存）
        if time_slice is not None:
            start_idx, end_idx = time_slice
            hlcvs_view = master_array[start_idx:end_idx]
        else:
            hlcvs_view = master_array

        # BTC 切片（仅时间切片创建视图）
        btc_view = None
        master_btc_spec = ctx.master_btc_specs.get(exchange) if ctx.master_btc_specs else None
        if master_btc_spec is not None:
            master_btc = self._ensure_master_attachment(master_btc_spec, master_btc_spec.name, "btc")
            if time_slice is not None:
                start_idx, end_idx = time_slice
                btc_view = master_btc[start_idx:end_idx]
            else:
                btc_view = master_btc

        # 返回 coin_indices 以便 build_backtest_payload 一步完成子集化
        return hlcvs_view, btc_view, coin_indices

    def _uses_lazy_slicing(self, ctx: ScenarioEvalContext, exchange: str) -> bool:
        """检查上下文是否对给定交易所使用惰性切片。"""
        return (
            ctx.master_hlcvs_specs is not None
            and exchange in ctx.master_hlcvs_specs
            and ctx.master_hlcvs_specs[exchange] is not None
        )

    def _ensure_context_attachment(self, ctx: ScenarioEvalContext, exchange: str) -> None:
        """仅为非惰性切片上下文附加到 SharedMemory。"""
        # 如果使用惰性切片则跳过 - 切片在 evaluate() 中按需计算
        if self._uses_lazy_slicing(ctx, exchange):
            return

        # 原始流程：按场景的 SharedMemory
        if exchange not in ctx.shared_hlcvs_np:
            if exchange in ctx.hlcvs_specs and ctx.hlcvs_specs[exchange] is not None:
                attachment = attach_shared_array(ctx.hlcvs_specs[exchange])
                ctx.attachments["hlcvs"][exchange] = attachment
                ctx.shared_hlcvs_np[exchange] = attachment.array
        if exchange not in ctx.shared_btc_np and exchange in ctx.btc_usd_specs:
            if ctx.btc_usd_specs[exchange] is not None:
                attachment = attach_shared_array(ctx.btc_usd_specs[exchange])
                ctx.attachments["btc"][exchange] = attachment
                ctx.shared_btc_np[exchange] = attachment.array

    def evaluate(self, individual, overrides_list):
        """在所有场景上评估个体，聚合多场景指标后计算适应度。"""
        individual[:] = enforce_bounds(individual, self.base.bounds, self.base.sig_digits)
        config = individual_to_config(
            individual,
            optimizer_overrides,
            overrides_list,
            self.base.config,
            key_paths=self.base.key_paths,
        )
        individual_hash = calc_hash(individual)
        seen_hashes = self.base.seen_hashes
        duplicate_counter = self.base.duplicate_counter

        if self.base.use_duplicate_guard:
            if individual_hash in seen_hashes:
                existing_entry = seen_hashes[individual_hash]
                existing_score = None
                existing_penalty = 0.0
                if existing_entry is not None:
                    existing_score, existing_penalty = existing_entry
                duplicate_counter["total"] += 1
                perturbation_funcs = [
                    self.base.perturb_x_pct,
                    self.base.perturb_step_digits,
                    self.base.perturb_gaussian,
                    self.base.perturb_random_subset,
                    self.base.perturb_sample_some,
                    self.base.perturb_large_uniform,
                ]
                for perturb_fn in perturbation_funcs:
                    perturbed = perturb_fn(individual)
                    perturbed = enforce_bounds(perturbed, self.base.bounds, self.base.sig_digits)
                    new_hash = calc_hash(perturbed)
                    if new_hash not in seen_hashes:
                        individual[:] = perturbed
                        seen_hashes[new_hash] = None
                        config = individual_to_config(
                            perturbed,
                            optimizer_overrides,
                            overrides_list,
                            self.base.config,
                            key_paths=self.base.key_paths,
                        )
                        duplicate_counter["resolved"] += 1
                        break
                else:
                    if existing_score is not None:
                        duplicate_counter["reused"] += 1
                        return tuple(existing_score), existing_penalty, None
            else:
                seen_hashes[individual_hash] = None

        scenario_results: List[ScenarioResult] = []
        liquidated = False

        from tools.iterative_backtester import combine_analyses as combine

        # 遍历所有场景，执行回测并收集指标
        for ctx in self.contexts:
            # 从主配置克隆并应用场景特定参数
            scenario_config = deepcopy(config)
            scenario_config["backtest"]["start_date"] = ctx.config["backtest"]["start_date"]
            scenario_config["backtest"]["end_date"] = ctx.config["backtest"]["end_date"]
            scenario_config["backtest"]["coins"] = deepcopy(ctx.config["backtest"]["coins"])
            scenario_config["backtest"]["cache_dir"] = deepcopy(
                ctx.config["backtest"].get("cache_dir", {})
            )
            scenario_config.setdefault("live", {})
            scenario_config["live"]["approved_coins"] = deepcopy(
                ctx.config["live"].get("approved_coins", {})
            )
            scenario_config["live"]["ignored_coins"] = deepcopy(
                ctx.config["live"].get("ignored_coins", {})
            )
            logging.debug(
                "优化器场景 %s | 开始=%s 结束=%s 币种=%s",
                ctx.label,
                scenario_config["backtest"].get("start_date"),
                scenario_config["backtest"].get("end_date"),
                list(scenario_config["backtest"]["coins"].keys()),
            )
            if ctx.overrides:
                _apply_config_overrides(scenario_config, ctx.overrides)
            scenario_config["disable_plotting"] = True

            analyses = {}
            # 对每个交易所执行回测
            for exchange in ctx.exchanges:
                # 获取数据数组 - 来自惰性切片或缓存的 SharedMemory
                if self._uses_lazy_slicing(ctx, exchange):
                    # 获取时间切片视图（O(1) 内存）+ 币种索引
                    # 币种子集化在 build_backtest_payload 内部完成（单次拷贝）
                    hlcvs_data, btc_data, coin_indices = self._get_lazy_slice_data(ctx, exchange)
                else:
                    self._ensure_context_attachment(ctx, exchange)
                    hlcvs_data = ctx.shared_hlcvs_np[exchange]
                    btc_data = ctx.shared_btc_np.get(exchange)
                    coin_indices = ctx.coin_indices.get(exchange)

                payload = build_backtest_payload(
                    hlcvs_data,
                    ctx.msss[exchange],
                    scenario_config,
                    exchange,
                    btc_data,
                    ctx.timestamps.get(exchange),
                    coin_indices=coin_indices,
                    metrics_only=True,
                )
                try:
                    fills, equities_array, analysis = execute_backtest(payload, scenario_config)
                except BaseException as exc:
                    if not _is_recoverable_backtest_candidate_error(exc):
                        raise
                    error = f"{exc.__class__.__name__}: {exc}"
                    logging.debug(
                        "优化器 suite 候选因可恢复回测失败而无效 | 标签=%s | 交易所=%s | 错误=%s",
                        ctx.label,
                        exchange,
                        error,
                    )
                    objectives, total_penalty, metrics_payload = _build_invalid_candidate_metrics(
                        self.base.config["optimize"]["scoring"],
                        error,
                        include_stats=False,
                        include_suite_metrics=True,
                    )
                    _set_candidate_metrics(individual, metrics_payload)
                    actual_hash = calc_hash(individual)
                    self.base.seen_hashes[actual_hash] = (tuple(objectives), total_penalty)
                    return tuple(objectives), total_penalty, metrics_payload
                analyses[exchange] = analysis
                liquidated = liquidated or _analysis_indicates_liquidation(
                    analysis, scenario_config
                )

                # 释放回测结果以允许内存重用
                del fills
                del equities_array
                del payload

            combined_metrics = combine(analyses)
            stats = combined_metrics.get("stats", {})
            logging.debug(
                "场景指标 | 标签=%s adg_pnl=%s peak_recovery_hours_pnl=%s",
                ctx.label,
                (
                    stats.get("adg_pnl", {}).get("mean")
                    if isinstance(stats.get("adg_pnl"), dict)
                    else stats.get("adg_pnl")
                ),
                (
                    stats.get("peak_recovery_hours_pnl", {}).get("mean")
                    if isinstance(stats.get("peak_recovery_hours_pnl"), dict)
                    else stats.get("peak_recovery_hours_pnl")
                ),
            )
            scenario_results.append(
                ScenarioResult(
                    scenario=SuiteScenario(
                        label=ctx.label,
                        start_date=None,
                        end_date=None,
                        coins=None,
                        ignored_coins=None,
                    ),
                    per_exchange={},
                    metrics={"stats": combined_metrics.get("stats", {})},
                    elapsed_seconds=0.0,
                    output_path=None,
                )
            )

        aggregate_summary = aggregate_metrics(scenario_results, self.aggregate_cfg)
        suite_payload = build_suite_metrics_payload(scenario_results, aggregate_summary)
        aggregate_stats = aggregate_summary.get("stats", {})

        flat_stats = flatten_metric_stats(aggregate_stats)
        # 用正确聚合的值覆盖 _mean，使 calc_fitness
        # 遵循聚合配置（例如使用 "max" 而非 "mean"）。
        aggregated_values = aggregate_summary.get("aggregated", {})
        for metric, agg_value in aggregated_values.items():
            flat_stats[f"{metric}_mean"] = agg_value
        objectives, total_penalty = self.base.calc_fitness(flat_stats)
        objectives_map = {f"w_{i}": val for i, val in enumerate(objectives)}

        metrics_payload = {
            "objectives": objectives_map,
            "suite_metrics": suite_payload,
            "constraint_violation": total_penalty,
            "liquidated": liquidated,
        }

        _set_candidate_metrics(individual, metrics_payload)
        actual_hash = calc_hash(individual)
        if self.base.use_duplicate_guard:
            self.base.seen_hashes[actual_hash] = (tuple(objectives), total_penalty)
        return tuple(objectives), total_penalty, metrics_payload

    def __del__(self):
        for ctx in self.contexts:
            for attachment in ctx.attachments.get("hlcvs", {}).values():
                try:
                    attachment.close()
                except Exception:
                    pass
            for attachment in ctx.attachments.get("btc", {}).values():
                try:
                    attachment.close()
                except Exception:
                    pass


def add_extra_options(parser, *, help_all: bool):
    """向参数解析器注册优化器额外选项（起始配置、微调参数等）。"""
    parser.add_argument(
        "-t",
        "--start",
        type=str,
        required=False,
        dest="starting_configs",
        default=None,
        help=(
            "Start with given live configs. Single json file or dir with multiple json files"
            if help_all
            else argparse.SUPPRESS
        ),
    )
    parser.add_argument(
        "-ft",
        "--fine_tune_params",
        "--fine-tune-params",
        type=str,
        default="",
        dest="fine_tune_params",
        help=(
            "Comma-separated optimize bounds selectors to tune; other parameters are fixed to their current config values"
            if help_all
            else argparse.SUPPRESS
        ),
    )


def _resolve_cli_limits_override(args, existing_limits=None) -> list[dict] | None:
    """从命令行参数解析优化限制覆盖，合并或替换现有限制列表。"""
    raw_limits_payload = getattr(args, "optimize.limits", None)
    raw_limit_entries = list(getattr(args, "limit_entries", []) or [])
    clear_limits = bool(getattr(args, "clear_limits", False))

    if raw_limits_payload is None and not raw_limit_entries and not clear_limits:
        return None

    replacement: list[dict] = []
    if not clear_limits and raw_limits_payload is None and existing_limits is not None:
        replacement.extend(normalize_limit_entries(existing_limits))
    if raw_limits_payload is not None:
        replacement.extend(normalize_limit_entries(raw_limits_payload))
    if raw_limit_entries:
        replacement.extend(parse_limit_cli_entries(raw_limit_entries))
    return normalize_limit_entries(replacement)


def apply_fine_tune_bounds(
    config: dict,
    fine_tune_params: list[str],
    cli_overridden_bounds: set[str],
) -> None:
    """根据微调参数选择器，将非微调参数的边界固定为其当前配置值。"""
    bounds = config.get("optimize", {}).get("bounds", {})

    def _resolve_bound_selectors(selectors, label: str) -> set[str]:
        """将模糊选择器解析为匹配的优化边界键集合。"""
        resolved: set[str] = set()
        selectors_sorted = sorted(
            {str(selector).strip() for selector in selectors if str(selector).strip()}
        )
        if not selectors_sorted:
            return resolved
        logging.info("%s 选择器:", label)
        for selector in selectors_sorted:
            matches = sorted(key for key in bounds if selector in key)
            if not matches:
                logging.warning("%s 选择器未匹配到优化边界: %s", label, selector)
                continue
            logging.info("  %s ->", selector)
            for match in matches:
                logging.info("    %s", match)
            resolved.update(matches)
        return resolved

    def _log_bound_set(header: str, keys: set[str]) -> None:
        if not keys:
            logging.info("%s: 无", header)
            return
        logging.info("%s:", header)
        for key in sorted(keys):
            logging.info("  %s", key)

    def _resolve_bound_key_path(bound_key: str):
        if bound_key in OPTIMIZABLE_BOT_KEY_PATHS:
            return OPTIMIZABLE_BOT_KEY_PATHS[bound_key]
        try:
            pside, param = bound_key.split("_", 1)
        except ValueError:
            return None
        if pside not in ("long", "short"):
            return None
        return ("bot", pside, param)

    def _fix_bound_to_current_value(bound_key: str) -> bool:
        """将指定边界固定为配置中的当前值（low=high），使其不被优化。"""
        path = _resolve_bound_key_path(bound_key)
        if path is None:
            logging.warning("微调边界: 无法解析键 '%s'，跳过", bound_key)
            return False
        target = config
        try:
            for part in path:
                target = target[part]
        except (KeyError, TypeError):
            logging.warning(
                "微调边界: 缺少 '%s' 的当前配置值，保持边界不变",
                bound_key,
            )
            return False
        try:
            value_float = float(target)
            bounds[bound_key] = [value_float, value_float]
        except (TypeError, ValueError):
            bounds[bound_key] = [target, target]
        return True

    # 首先，规范化所有 CLI 覆盖，使单值表示固定边界
    # (将 [val] 或 val 统一为 [val, val])
    for key in cli_overridden_bounds:
        if key not in bounds:
            continue
        raw_val = bounds[key]
        if isinstance(raw_val, (list, tuple)):
            if len(raw_val) == 1:
                bounds[key] = [float(raw_val[0]), float(raw_val[0])]
        else:
            try:
                val = float(raw_val)
            except (TypeError, ValueError):
                continue
            bounds[key] = [val, val]

    fine_tune_set = _resolve_bound_selectors(fine_tune_params, "fine-tune")
    config_fixed_params = _resolve_bound_selectors(
        config.get("optimize", {}).get("fixed_params", []) or [],
        "optimize.fixed_params",
    )

    # 合并配置中指定的固定参数和非微调参数
    effective_fixed_params = set(config_fixed_params)
    if fine_tune_params:
        # 如果指定了微调参数，则不在微调集合中的参数全部固定
        effective_fixed_params.update(key for key in bounds if key not in fine_tune_set)

    if not effective_fixed_params:
        return

    if fine_tune_set:
        _log_bound_set("fine-tune tunable bounds", fine_tune_set)
    _log_bound_set("fixed optimize bounds", effective_fixed_params)

    for key in sorted(effective_fixed_params):
        if key not in bounds:
            continue
        _fix_bound_to_current_value(key)


def extract_configs(path):
    return list(iter_extract_configs(path))


def iter_extract_configs(path):
    """从文件路径迭代提取起始配置，支持 JSON、Pareto 文本和目录递归。"""
    if not os.path.exists(path):
        return
    if path.endswith("_all_results.bin"):
        logging.info(f"跳过 {path}")
        return
    if path.endswith(".json"):
        try:
            raw = load_hjson_config(path, log_errors=False)
            yield _extract_starting_config(raw, source=path)
        except Exception as e:
            logging.warning(f"从起始配置 {path} 提取机器人配置失败: {e}")
        return
    if path.endswith("_pareto.txt"):
        with open(path) as f:
            for line in f:
                try:
                    cfg = json.loads(line)
                    yield _extract_starting_config(cfg, source=path)
                except Exception as e:
                    logging.warning(f"从起始配置 {path} 提取机器人配置失败: {e}")


def _extract_starting_config(raw_config, *, source: str = "<memory>"):
    """从原始配置中提取机器人配置、live 策略和优化边界，供优化器种子使用。"""
    if not isinstance(raw_config, dict):
        raise TypeError(f"expected dict, got {type(raw_config).__name__}")
    current = raw_config
    if isinstance(current.get("config"), dict):
        current = current["config"]
    bot_cfg = current.get("bot")
    if not isinstance(bot_cfg, dict):
        raise KeyError("missing bot config")
    extracted = {
        "bot": format_bot_config(
            bot_cfg,
            live_cfg=current.get("live"),
            verbose=False,
        )
    }
    live_cfg = current.get("live")
    if isinstance(live_cfg, dict) and live_cfg.get("strategy_kind"):
        extracted["live"] = {"strategy_kind": live_cfg["strategy_kind"]}
    optimize_cfg = current.get("optimize")
    if isinstance(optimize_cfg, dict) and isinstance(optimize_cfg.get("bounds"), dict):
        extracted["optimize"] = {"bounds": deepcopy(optimize_cfg["bounds"])}
    record_source = source or "<memory>"
    extracted["_starting_config_source"] = record_source
    return extracted


def _build_starting_seed_config(cfg):
    """从起始配置构建完整的种子配置，合并到模板配置中。"""
    if not isinstance(cfg, dict):
        raise TypeError(f"expected dict, got {type(cfg).__name__}")
    if all(pside in cfg and isinstance(cfg.get(pside), dict) for pside in ("long", "short")):
        extracted = {"bot": format_bot_config(cfg, verbose=False)}
    elif "bot" in cfg and isinstance(cfg.get("bot"), dict):
        extracted = cfg
    else:
        extracted = _extract_starting_config(cfg)
    seed = get_template_config()
    seed["bot"] = deep_updated(seed["bot"], deepcopy(extracted["bot"]))
    if isinstance(extracted.get("live"), dict):
        seed["live"] = deep_updated(seed["live"], deepcopy(extracted["live"]))
    optimize_cfg = extracted.get("optimize")
    if isinstance(optimize_cfg, dict) and isinstance(optimize_cfg.get("bounds"), dict):
        seed["optimize"]["bounds"] = deep_updated(
            seed["optimize"]["bounds"], deepcopy(optimize_cfg["bounds"])
        )
    return seed


def get_starting_configs(starting_configs: str):
    return list(iter_starting_configs(starting_configs))


def iter_starting_configs(starting_configs: str):
    if starting_configs is None:
        return
    if os.path.isdir(starting_configs):
        with os.scandir(starting_configs) as entries:
            for entry in entries:
                yield from iter_starting_configs(entry.path)
        return
    yield from iter_extract_configs(starting_configs)


def configs_to_individuals(
    cfgs,
    bounds,
    sig_digits=0,
    optimization_shape: OptimizationShape | None = None,
):
    inds, _ = configs_to_individuals_streaming(
        cfgs,
        bounds,
        sig_digits=sig_digits,
        optimization_shape=optimization_shape,
    )
    return inds


def configs_to_individuals_streaming(
    cfgs,
    bounds,
    sig_digits=0,
    optimization_shape: OptimizationShape | None = None,
):
    """流式将配置列表转换为去重的优化个体集合，返回个体列表和原始配置数。"""
    inds = set()
    raw_count = 0
    for cfg in cfgs:
        raw_count += 1
        try:
            fcfg = _build_starting_seed_config(cfg)
            individual = config_to_individual(
                fcfg,
                bounds,
                sig_digits,
                optimization_shape=optimization_shape,
            )
            inds.add(tuple(individual))
        except Exception as e:
            logging.warning(f"将起始配置用作优化器种子失败: {e}")
    return list(inds), raw_count


async def main():
    """优化器主入口：解析配置、初始化数据、构建评估器并启动优化后端。"""
    raw_argv = sys.argv[1:]
    help_all = help_all_requested(raw_argv)
    parser = build_command_parser(
        prog=get_cli_prog("optimize"),
        description="run optimizer",
        usage="%(prog)s [config_path] [options]",
        epilog=(
            "Examples:\n"
            "  passivbot optimize configs/examples/default_trailing_grid_long_npos7.json -s XMR -sd 2025 -c 4 --suite n\n"
            "  passivbot optimize -e bybit -s BTC,ETH -i 10000 -ps 200\n"
            "\n"
            "Use --help-all to show every config override flag, including optimize bounds."
        ),
    )
    parser.add_argument(
        "config_path",
        type=str,
        default=None,
        nargs="?",
        help="path to json/hjson passivbot config (defaults to in-code schema defaults if omitted)",
    )
    add_help_all_argument(
        parser,
        help_all=help_all,
        help_text="Show all optimizer override flags, including advanced bounds and backend options.",
    )

    logging_group = parser.add_argument_group("Logging")
    logging_group.add_argument(
        "--log-level",
        dest="log_level",
        default=None,
        help="Logging verbosity (warning, info, debug, trace or 0-3).",
    )
    suite_group = parser.add_argument_group("Suite")
    suite_group.add_argument(
        "--suite",
        nargs="?",
        const="true",
        default=None,
        type=str2bool,
        metavar="y/n",
        help="Enable or disable suite mode for optimizer run (omit to use config's suite_enabled setting).",
    )
    suite_group.add_argument(
        "--scenarios",
        "-sc",
        type=str,
        default=None,
        metavar="LABELS",
        help="Comma-separated list of scenario labels to run (implies --suite y). "
        "Example: --scenarios base,binance_only",
    )
    suite_group.add_argument(
        "--suite-config",
        type=str,
        default=None,
        help="Optional config file providing backtest.scenarios overrides.",
    )

    group_map = {
        "Coin Selection": parser.add_argument_group("Coin Selection"),
        "Date Range": parser.add_argument_group("Date Range"),
        "Optimizer": parser.add_argument_group("Optimizer"),
        "Suite": suite_group,
        "Logging": logging_group,
        "Backtest Runtime": parser.add_argument_group("Backtest Runtime"),
        "Optimize Common": parser.add_argument_group("Optimize Common"),
        "Optimize Bounds": parser.add_argument_group("Optimize Bounds"),
        "Optimize DEAP": parser.add_argument_group("Optimize DEAP"),
        "Optimize Pymoo": parser.add_argument_group("Optimize Pymoo"),
        "Advanced Overrides": parser.add_argument_group("Advanced Overrides"),
    }

    template_config = project_template_config_for_cli(get_template_config(), "optimize")
    allowed_config_keys = add_config_arguments(
        parser,
        template_config,
        command="optimize",
        help_all=help_all,
        group_map=group_map,
    )
    optimize_common_group = group_map["Optimize Common"]
    optimize_common_group.add_argument(
        "-l",
        "--limit",
        action="append",
        dest="limit_entries",
        default=None,
        metavar="SPEC",
        help=(
            "Repeatable optimize limit override. Example: "
            "\"drawdown_worst > 0.35\" or "
            "\"loss_profit_ratio outside_range [0.05,0.7]\""
        ),
    )
    optimize_common_group.add_argument(
        "--clear-limits",
        action="store_true",
        dest="clear_limits",
        help="Replace optimize.limits with an empty list before applying any --limits/--limit entries.",
    )
    add_extra_options(group_map["Advanced Overrides"], help_all=help_all)
    raw_args = merge_negative_cli_values(expand_help_all_argv(raw_argv))
    raw_args = _normalize_optional_bool_flag(raw_args, "--suite")
    args = parser.parse_args(raw_args)
    initial_log_level = resolve_log_level(args.log_level, None, fallback=1)
    configure_logging(debug=initial_log_level)
    source_config, base_config_path, raw_snapshot = load_input_config(args.config_path)
    existing_limits = deepcopy(source_config.get("optimize", {}).get("limits"))
    update_config_with_args(source_config, args, verbose=True, allowed_keys=allowed_config_keys)
    cli_limits_override = _resolve_cli_limits_override(args, existing_limits=existing_limits)
    if cli_limits_override is not None:
        recursive_config_update(
            source_config,
            "optimize.limits",
            cli_limits_override,
            verbose=True,
        )
    config = prepare_config(
        source_config,
        base_config_path=base_config_path,
        verbose=False,
        raw_snapshot=raw_snapshot,
    )
    config_logging_value = get_optional_config_value(config, "logging.level", None)
    effective_log_level = resolve_log_level(args.log_level, config_logging_value, fallback=1)
    if effective_log_level != initial_log_level:
        configure_logging(debug=effective_log_level)
    logging.info(
        "优化配置已规范化 | 模板=%s | 评分=%s",
        TEMPLATE_CONFIG_MODE,
        ",".join(objective_metric_names(config)),
    )
    fine_tune_params = (
        [p.strip() for p in (args.fine_tune_params or "").split(",") if p.strip()]
        if getattr(args, "fine_tune_params", "")
        else []
    )
    cli_bounds_overrides = {
        key.split("optimize.bounds.", 1)[1]
        for key, value in vars(args).items()
        if key.startswith("optimize.bounds.") and value is not None
    }
    apply_fine_tune_bounds(config, fine_tune_params, cli_bounds_overrides)
    suite_override = None
    if args.suite_config:
        logging.info("正在加载 suite 配置 %s", args.suite_config)
        override_cfg = load_prepared_config(args.suite_config, verbose=False)
        override_backtest = override_cfg.get("backtest", {})
        # 支持新格式（场景在顶层）和旧格式（suite 包装器）
        if "scenarios" in override_backtest:
            suite_override = {
                "scenarios": override_backtest.get("scenarios", []),
                "aggregate": override_backtest.get("aggregate", {"default": "mean"}),
            }
        elif "suite" in override_backtest:
            # 旧格式 - 从 suite 包装器中提取
            suite_override = override_backtest["suite"]
        else:
            raise ValueError(f"Suite config {args.suite_config} must define backtest.scenarios.")
    suite_cfg = extract_suite_config(config, suite_override)

    # 处理 --scenarios 过滤器（隐含 --suite y）
    scenario_filter = getattr(args, "scenarios", None)
    if scenario_filter:
        labels = [label.strip() for label in scenario_filter.split(",") if label.strip()]
        suite_cfg["scenarios"] = filter_scenarios_by_label(suite_cfg.get("scenarios", []), labels)
        suite_cfg["enabled"] = True  # --scenarios 隐含 suite 模式
        logging.info("已过滤到 %d 个场景: %s", len(labels), ", ".join(labels))

    # --suite CLI 参数覆盖配置（在 --scenarios 之后应用，因此显式 --suite n 优先）
    if args.suite is not None:
        recursive_config_update(config, "backtest.suite_enabled", bool(args.suite), verbose=True)
        suite_cfg["enabled"] = bool(args.suite)
    backtest_exchanges = require_config_value(config, "backtest.exchanges")
    await format_approved_ignored_coins(config, backtest_exchanges)
    interrupted = False
    pool = None
    manager = None
    pool_terminated = False
    try:
        array_manager = SharedArrayManager()
        hlcvs_specs = {}
        btc_usd_specs = {}
        msss = {}
        timestamps_dict = {}
        config["backtest"]["coins"] = {}
        aggregate_cfg: Dict[str, Any] = {"default": "mean"}
        scenario_contexts: List[ScenarioEvalContext] = []
        suite_enabled = bool(suite_cfg.get("enabled"))

        if suite_enabled:
            scenario_contexts, aggregate_cfg = await prepare_suite_contexts(
                config,
                suite_cfg,
                shared_array_manager=array_manager,
            )
            if not scenario_contexts:
                raise ValueError("Suite configuration produced no scenarios.")
            logging.info("优化器 suite 已启用，共 %d 个场景", len(scenario_contexts))
            first_ctx = scenario_contexts[0]
            hlcvs_specs = first_ctx.hlcvs_specs
            btc_usd_specs = first_ctx.btc_usd_specs
            msss = first_ctx.msss
            timestamps_dict = first_ctx.timestamps
            config["backtest"]["coins"] = deepcopy(first_ctx.config["backtest"]["coins"])
            backtest_exchanges = sorted({ex for ctx in scenario_contexts for ex in ctx.exchanges})

            # 估算内存使用量（按场景 SharedMemory，所有工作进程共享）
            total_shm_bytes = 0
            seen_specs = set()
            for ctx in scenario_contexts:
                for spec_map in (
                    ctx.hlcvs_specs,
                    ctx.btc_usd_specs,
                    ctx.master_hlcvs_specs or {},
                    ctx.master_btc_specs or {},
                ):
                    for spec in spec_map.values():
                        if spec is None:
                            continue
                        if spec.name in seen_specs:
                            continue
                        seen_specs.add(spec.name)
                        total_shm_bytes += np.prod(spec.shape) * np.dtype(spec.dtype).itemsize
            if total_shm_bytes > 0:
                total_shm_gb = total_shm_bytes / (1024**3)
                try:
                    import shutil

                    if hasattr(os, "sysconf"):
                        pages = os.sysconf("SC_PHYS_PAGES")
                        page_size = os.sysconf("SC_PAGE_SIZE")
                        available_gb = (pages * page_size) / (1024**3)
                    else:
                        available_gb = None
                    shm_gb = None
                    if os.path.exists("/dev/shm"):
                        usage = shutil.disk_usage("/dev/shm")
                        shm_gb = usage.total / (1024**3)
                except Exception:
                    available_gb = None
                    shm_gb = None
                logging.info(
                    "内存估算 | 场景=%d | 共享内存=%.1fGB%s",
                    len(scenario_contexts),
                    total_shm_gb,
                    f" | 系统={available_gb:.1f}GB" if available_gb else "",
                )
                if shm_gb is not None:
                    logging.info("共享内存文件系统大小 | /dev/shm=%.1fGB", shm_gb)
                if available_gb and total_shm_gb > available_gb * 0.7:
                    logging.warning(
                        "场景共享内存 (%.1fGB) 相对于 RAM (%.1fGB) 较高。"
                        "建议使用更少/更小的场景。",
                        total_shm_gb,
                        available_gb,
                    )
        else:
            # 新行为：从交易所数量推导数据策略
            # - 单交易所 = 仅使用该交易所数据
            # - 多交易所 = 按币种最佳组合（combined）
            use_combined = len(backtest_exchanges) > 1

            if use_combined:
                exchange = "combined"
                coins, mss = _register_exchange_data(
                    exchange,
                    await prepare_hlcvs_mss(config, exchange),
                    config,
                    msss=msss,
                    hlcvs_specs=hlcvs_specs,
                    btc_usd_specs=btc_usd_specs,
                    timestamps_dict=timestamps_dict,
                    array_manager=array_manager,
                )
                exchange_preference = defaultdict(list)
                for coin in coins:
                    exchange_preference[mss[coin]["exchange"]].append(coin)
                for ex in exchange_preference:
                    logging.info(f"为 {','.join(exchange_preference[ex])} 选择了 {ex}")
            else:
                tasks = {
                    exchange: asyncio.create_task(prepare_hlcvs_mss(config, exchange))
                    for exchange in backtest_exchanges
                }
                for exchange, task in tasks.items():
                    _register_exchange_data(
                        exchange,
                        await task,
                        config,
                        msss=msss,
                        hlcvs_specs=hlcvs_specs,
                        btc_usd_specs=btc_usd_specs,
                        timestamps_dict=timestamps_dict,
                        array_manager=array_manager,
                    )
        exchanges = backtest_exchanges
        # 构建结果目录名称
        exchanges_fname = "combined" if len(backtest_exchanges) > 1 else "_".join(exchanges)
        date_fname = ts_to_date(utc_ms())[:19].replace(":", "_")
        coins = sorted(set([x for y in config["backtest"]["coins"].values() for x in y]))
        suite_flag = suite_enabled or bool(args.suite)
        if suite_flag:
            coins_fname = f"suite_{len(coins)}_coins"
        else:
            coins_fname = "_".join(coins) if len(coins) <= 6 else f"{len(coins)}_coins"
        hash_snippet = uuid4().hex[:8]
        n_days = int(
            round(
                (
                    date_to_ts(require_config_value(config, "backtest.end_date"))
                    - date_to_ts(require_config_value(config, "backtest.start_date"))
                )
                / (1000 * 60 * 60 * 24)
            )
        )
        results_dir = make_get_filepath(
            f"optimize_results/{date_fname}_{exchanges_fname}_{n_days}days_{coins_fname}_{hash_snippet}/"
        )
        os.makedirs(results_dir, exist_ok=True)
        config["results_dir"] = results_dir
        results_filename = os.path.join(results_dir, "all_results.bin")
        config["results_filename"] = results_filename
        overrides_list = config.get("optimize", {}).get("enable_overrides", [])

        # 工作进程用于重复检测的共享状态
        manager = multiprocessing.Manager()
        seen_hashes = manager.dict()
        duplicate_counter = manager.dict()
        duplicate_counter["total"] = 0
        duplicate_counter["resolved"] = 0
        duplicate_counter["reused"] = 0

        # 使用共享内存引用初始化评估器
        evaluator = Evaluator(
            hlcvs_specs=hlcvs_specs,
            btc_usd_specs=btc_usd_specs,
            msss=msss,
            config=config,
            seen_hashes=seen_hashes,
            duplicate_counter=duplicate_counter,
            timestamps=timestamps_dict,
            shared_array_manager=array_manager,
        )

        if suite_enabled:
            # Suite 模式：包装为多场景评估器
            evaluator_for_pool = SuiteEvaluator(evaluator, scenario_contexts, aggregate_cfg)
        else:
            evaluator_for_pool = evaluator

        logging.info(f"评估器初始化完成...")
        # 创建结果记录器
        flush_interval = 60  # 或从配置中读取
        sig_digits = config["optimize"]["round_to_n_significant_digits"]
        pareto_max = config["optimize"].get("pareto_max_size", DEFAULT_PARETO_MAX_SIZE)
        recorder = ResultRecorder(
            results_dir=results_dir,
            sig_digits=sig_digits,
            flush_interval=flush_interval,
            scoring_keys=config["optimize"]["scoring"],
            compress=config["optimize"]["compress_results_file"],
            write_all_results=config["optimize"].get("write_all_results", True),
            pareto_max_size=pareto_max,
            bounds=evaluator.bounds,
        )
        # 选择并运行优化后端
        backend_name = config["optimize"]["backend"]
        logging.info("已选择优化器后端: %s", backend_name)
        backend_runner = get_backend_runner(backend_name)
        backend_result = backend_runner(
            config=config,
            evaluator=evaluator,
            evaluator_for_pool=evaluator_for_pool,
            recorder=recorder,
            overrides_list=overrides_list,
            duplicate_counter=duplicate_counter,
            starting_configs_path=args.starting_configs,
            constraint_fitness_cls=ConstraintAwareFitness,
            ignore_sigint_in_worker=ignore_sigint_in_worker,
            get_starting_configs=get_starting_configs,
            configs_to_individuals=configs_to_individuals,
            iter_starting_configs=iter_starting_configs,
            configs_to_individuals_streaming=configs_to_individuals_streaming,
            optimization_shape=evaluator.optimization_shape,
            record_individual_result=_record_individual_result,
            run_evolution=ea_mu_plus_lambda_stream,
            build_config_fn=individual_to_config,
            overrides_fn=optimizer_overrides,
        )
        pool = backend_result.get("pool")
        pool_terminated = backend_result.get("pool_terminated", False)

    except KeyboardInterrupt:
        interrupted = True
        logging.info("收到 SIGINT；开始优雅关闭")
        if "pool" in locals():
            already = pool_state["terminated"] if "pool_state" in locals() else pool_terminated
            if not already:
                logging.info("正在终止工作进程池...")
                pool.terminate()
                pool_terminated = True
                if "pool_state" in locals():
                    pool_state["terminated"] = True
    except Exception as e:
        logging.error(f"发生错误: {e}")
        traceback.print_exc()
    finally:
        if "recorder" in locals():
            logging.info("正在刷新 Pareto/结果记录器...")
            try:
                recorder.flush()
            except Exception:
                logging.exception("刷新记录器失败")
            logging.info("正在关闭结果记录器...")
            recorder.close()
        if "pool" in locals() and pool is not None:
            if interrupted and not pool_terminated:
                logging.info("正在终止工作进程池...")
                pool.terminate()
                pool_terminated = True
            if pool_terminated or interrupted:
                logging.info("正在等待已终止的工作进程池...")
            else:
                logging.info("正在关闭工作进程池...")
                pool.close()
            try:
                pool.join()
            except KeyboardInterrupt:
                logging.info("在进程池等待期间收到额外 SIGINT；继续关闭")
        if manager is not None:
            logging.info("正在关闭多进程管理器...")
            try:
                manager.shutdown()
            except Exception:
                logging.exception("关闭多进程管理器失败")
        if "array_manager" in locals():
            logging.info("正在释放共享内存...")
            try:
                array_manager.cleanup()
            except Exception:
                logging.exception("释放共享内存失败")

        logging.info("关闭完成。")
        sys.exit(130 if interrupted else 0)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
