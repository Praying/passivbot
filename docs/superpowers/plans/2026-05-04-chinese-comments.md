# 中文注释翻译实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Passivbot 项目 `src/` 目录所有 Python 文件的注释和 docstring 翻译为中文，并翻译 `docs/` 用户文档。

**Architecture:** 按 8 个模块组逐文件顺序翻译，每完成一个模块组做一次 git commit。翻译时保留所有技术术语、交易所名称、配置键名和代码标识符为英文，仅翻译描述性文字。

**Tech Stack:** Python、Markdown、Git

---

## 翻译规则速查

翻译每个文件时遵循以下规则：

1. **技术术语保留英文**：EMA、OHLCV、PnL、ROI、API、JSON、Rust、Python、ccxt、HSL、TWEL 等
2. **交易所名称保留英文**：Binance、Bybit、Hyperliquid、OKX、Bitget、GateIO、KuCoin、Paradex、Defx 等
3. **配置键名保留英文**：如 `entry_qty_pct`、`close_qty_pct`、`ema_span_0` 等
4. **代码标识符不翻译**：函数名、变量名、类名全部保留英文
5. **保持格式**：缩进、空行、注释位置不变
6. **指令性注释保留**：`# type: ignore`、`# noqa`、`# pragma: no cover` 等不翻译
7. **翻译风格**：简洁直译，参考 `src/passivbot.py` 和 `src/candlestick_manager.py` 的已有翻译

## 翻译流程（每个文件）

```
1. 读取文件全文
2. 识别所有英文注释（# 开头的行）和 docstring（""" 包围的块）
3. 按翻译规则逐条翻译为中文
4. 写回文件
5. 用 grep 验证无遗漏的英文注释
```

## 验证命令

翻译完一个文件后，用以下命令检查是否还有英文注释遗漏：

```bash
grep -n '# [A-Za-z]' <file_path> | grep -v 'type: ignore' | grep -v 'noqa' | grep -v 'pragma'
grep -n '"""' <file_path>
```

---

## Task 1: 核心引擎（第 1 组）

**目标：** 翻译核心引擎模块中尚未翻译的 7 个文件。

**已翻译（跳过）：** `src/passivbot.py`、`src/main.py`

**文件列表：**
- `src/passivbot_hsl.py`
- `src/passivbot_exceptions.py`
- `src/passivbot_version.py`
- `src/passivbot_monitor.py`
- `src/procedures.py`
- `src/pure_funcs.py`
- `src/utils.py`

- [ ] **Step 1: 翻译 `src/passivbot_hsl.py`**

读取文件，翻译所有英文注释和 docstring 为中文。用验证命令检查无遗漏。

- [ ] **Step 2: 翻译 `src/passivbot_exceptions.py`**

同上。

- [ ] **Step 3: 翻译 `src/passivbot_version.py`**

同上。

- [ ] **Step 4: 翻译 `src/passivbot_monitor.py`**

同上。

- [ ] **Step 5: 翻译 `src/procedures.py`**

同上。

- [ ] **Step 6: 翻译 `src/pure_funcs.py`**

同上。

- [ ] **Step 7: 翻译 `src/utils.py`**

同上。

- [ ] **Step 8: 提交第 1 组**

```bash
git add src/passivbot_hsl.py src/passivbot_exceptions.py src/passivbot_version.py src/passivbot_monitor.py src/procedures.py src/pure_funcs.py src/utils.py
git commit -m "docs: 添加中文注释 — 核心引擎"
```

---

## Task 2: 配置系统（第 2 组）

**目标：** 翻译 `src/config/` 目录下所有 25 个 Python 文件。

**文件列表：**
- `src/config/__init__.py`
- `src/config/access.py`
- `src/config/bot.py`
- `src/config/coerce.py`
- `src/config/hydrate.py`
- `src/config/limits.py`
- `src/config/load.py`
- `src/config/log_output.py`
- `src/config/logging_summary.py`
- `src/config/metrics.py`
- `src/config/normalize.py`
- `src/config/overrides.py`
- `src/config/parse.py`
- `src/config/pnl_lookback.py`
- `src/config/project.py`
- `src/config/runtime_compile.py`
- `src/config/schema.py`
- `src/config/scoring.py`
- `src/config/transform_log.py`
- `src/config/tree_ops.py`
- `src/config/validate.py`
- `src/config/migrations/__init__.py`
- `src/config/migrations/detect.py`
- `src/config/migrations/legacy_v7.py`
- `src/config/migrations/renames.py`

- [ ] **Step 1: 翻译 `src/config/__init__.py`**

读取文件，翻译所有英文注释和 docstring 为中文。

- [ ] **Step 2: 翻译 `src/config/access.py`**

- [ ] **Step 3: 翻译 `src/config/bot.py`**

- [ ] **Step 4: 翻译 `src/config/coerce.py`**

- [ ] **Step 5: 翻译 `src/config/hydrate.py`**

- [ ] **Step 6: 翻译 `src/config/limits.py`**

- [ ] **Step 7: 翻译 `src/config/load.py`**

- [ ] **Step 8: 翻译 `src/config/log_output.py`**

- [ ] **Step 9: 翻译 `src/config/logging_summary.py`**

- [ ] **Step 10: 翻译 `src/config/metrics.py`**

- [ ] **Step 11: 翻译 `src/config/normalize.py`**

- [ ] **Step 12: 翻译 `src/config/overrides.py`**

- [ ] **Step 13: 翻译 `src/config/parse.py`**

- [ ] **Step 14: 翻译 `src/config/pnl_lookback.py`**

- [ ] **Step 15: 翻译 `src/config/project.py`**

- [ ] **Step 16: 翻译 `src/config/runtime_compile.py`**

- [ ] **Step 17: 翻译 `src/config/schema.py`**

- [ ] **Step 18: 翻译 `src/config/scoring.py`**

- [ ] **Step 19: 翻译 `src/config/transform_log.py`**

- [ ] **Step 20: 翻译 `src/config/tree_ops.py`**

- [ ] **Step 21: 翻译 `src/config/validate.py`**

- [ ] **Step 22: 翻译 `src/config/migrations/__init__.py`**

- [ ] **Step 23: 翻译 `src/config/migrations/detect.py`**

- [ ] **Step 24: 翻译 `src/config/migrations/legacy_v7.py`**

- [ ] **Step 25: 翻译 `src/config/migrations/renames.py`**

- [ ] **Step 26: 提交第 2 组**

```bash
git add src/config/
git commit -m "docs: 添加中文注释 — 配置系统"
```

---

## Task 3: 交易所适配器（第 3 组）

**目标：** 翻译 `src/exchanges/` 目录下所有 12 个 Python 文件。

**文件列表：**
- `src/exchanges/__init__.py`
- `src/exchanges/ccxt_bot.py`
- `src/exchanges/binance.py`
- `src/exchanges/bitget.py`
- `src/exchanges/bybit.py`
- `src/exchanges/defx.py`
- `src/exchanges/fake.py`
- `src/exchanges/gateio.py`
- `src/exchanges/hyperliquid.py`
- `src/exchanges/kucoin.py`
- `src/exchanges/okx.py`
- `src/exchanges/paradex.py`

- [ ] **Step 1: 翻译 `src/exchanges/__init__.py`**

- [ ] **Step 2: 翻译 `src/exchanges/ccxt_bot.py`**

- [ ] **Step 3: 翻译 `src/exchanges/binance.py`**

- [ ] **Step 4: 翻译 `src/exchanges/bitget.py`**

- [ ] **Step 5: 翻译 `src/exchanges/bybit.py`**

- [ ] **Step 6: 翻译 `src/exchanges/defx.py`**

- [ ] **Step 7: 翻译 `src/exchanges/fake.py`**

- [ ] **Step 8: 翻译 `src/exchanges/gateio.py`**

- [ ] **Step 9: 翻译 `src/exchanges/hyperliquid.py`**

- [ ] **Step 10: 翻译 `src/exchanges/kucoin.py`**

- [ ] **Step 11: 翻译 `src/exchanges/okx.py`**

- [ ] **Step 12: 翻译 `src/exchanges/paradex.py`**

- [ ] **Step 13: 提交第 3 组**

```bash
git add src/exchanges/
git commit -m "docs: 添加中文注释 — 交易所适配器"
```

---

## Task 4: 数据管理（第 4 组）

**目标：** 翻译数据管理模块中尚未翻译的 11 个文件。

**已翻译（跳过）：** `src/candlestick_manager.py`

**文件列表：**
- `src/ohlcv_catalog.py`
- `src/ohlcv_download.py`
- `src/ohlcv_legacy_import.py`
- `src/ohlcv_planner.py`
- `src/ohlcv_store.py`
- `src/ohlcv_utils.py`
- `src/hlcv_preparation.py`
- `src/fill_events_manager.py`
- `src/warmup_utils.py`
- `src/legacy_data_migrator.py`
- `src/tradfi_data.py`

- [ ] **Step 1: 翻译 `src/ohlcv_catalog.py`**

- [ ] **Step 2: 翻译 `src/ohlcv_download.py`**

- [ ] **Step 3: 翻译 `src/ohlcv_legacy_import.py`**

- [ ] **Step 4: 翻译 `src/ohlcv_planner.py`**

- [ ] **Step 5: 翻译 `src/ohlcv_store.py`**

- [ ] **Step 6: 翻译 `src/ohlcv_utils.py`**

- [ ] **Step 7: 翻译 `src/hlcv_preparation.py`**

- [ ] **Step 8: 翻译 `src/fill_events_manager.py`**

- [ ] **Step 9: 翻译 `src/warmup_utils.py`**

- [ ] **Step 10: 翻译 `src/legacy_data_migrator.py`**

- [ ] **Step 11: 翻译 `src/tradfi_data.py`**

- [ ] **Step 12: 提交第 4 组**

```bash
git add src/ohlcv_catalog.py src/ohlcv_download.py src/ohlcv_legacy_import.py src/ohlcv_planner.py src/ohlcv_store.py src/ohlcv_utils.py src/hlcv_preparation.py src/fill_events_manager.py src/warmup_utils.py src/legacy_data_migrator.py src/tradfi_data.py
git commit -m "docs: 添加中文注释 — 数据管理"
```

---

## Task 5: 优化引擎（第 5 组）

**目标：** 翻译优化引擎模块所有 20 个文件。

**文件列表：**
- `src/optimize.py`
- `src/opt_utils.py`
- `src/optimize_suite.py`
- `src/pareto_core.py`
- `src/pareto_explorer.py`
- `src/pareto_store.py`
- `src/optimization/__init__.py`
- `src/optimization/backend_shared.py`
- `src/optimization/backends/__init__.py`
- `src/optimization/backends/deap_backend.py`
- `src/optimization/backends/pymoo_backend.py`
- `src/optimization/bounds.py`
- `src/optimization/callback.py`
- `src/optimization/config_adapter.py`
- `src/optimization/deap_adapters.py`
- `src/optimization/output.py`
- `src/optimization/problem.py`
- `src/optimization/repair.py`
- `src/optimization/shape.py`
- `src/optimization/warmup.py`

- [ ] **Step 1: 翻译 `src/optimize.py`**

- [ ] **Step 2: 翻译 `src/opt_utils.py`**

- [ ] **Step 3: 翻译 `src/optimize_suite.py`**

- [ ] **Step 4: 翻译 `src/pareto_core.py`**

- [ ] **Step 5: 翻译 `src/pareto_explorer.py`**

- [ ] **Step 6: 翻译 `src/pareto_store.py`**

- [ ] **Step 7: 翻译 `src/optimization/__init__.py`**

- [ ] **Step 8: 翻译 `src/optimization/backend_shared.py`**

- [ ] **Step 9: 翻译 `src/optimization/backends/__init__.py`**

- [ ] **Step 10: 翻译 `src/optimization/backends/deap_backend.py`**

- [ ] **Step 11: 翻译 `src/optimization/backends/pymoo_backend.py`**

- [ ] **Step 12: 翻译 `src/optimization/bounds.py`**

- [ ] **Step 13: 翻译 `src/optimization/callback.py`**

- [ ] **Step 14: 翻译 `src/optimization/config_adapter.py`**

- [ ] **Step 15: 翻译 `src/optimization/deap_adapters.py`**

- [ ] **Step 16: 翻译 `src/optimization/output.py`**

- [ ] **Step 17: 翻译 `src/optimization/problem.py`**

- [ ] **Step 18: 翻译 `src/optimization/repair.py`**

- [ ] **Step 19: 翻译 `src/optimization/shape.py`**

- [ ] **Step 20: 翻译 `src/optimization/warmup.py`**

- [ ] **Step 21: 提交第 5 组**

```bash
git add src/optimize.py src/opt_utils.py src/optimize_suite.py src/pareto_core.py src/pareto_explorer.py src/pareto_store.py src/optimization/
git commit -m "docs: 添加中文注释 — 优化引擎"
```

---

## Task 6: 可视化与监控（第 6 组）

**目标：** 翻译可视化与监控模块所有 11 个文件。

**文件列表：**
- `src/plotting.py`
- `src/interactive_plot.py`
- `src/monitor_dev.py`
- `src/monitor_publisher.py`
- `src/monitor_relay.py`
- `src/monitor_tui.py`
- `src/monitor_web.py`
- `src/analysis_visibility.py`
- `src/metrics_schema.py`
- `src/trailing_diagnostics.py`
- `src/trailing_diagnostics_tool.py`

- [ ] **Step 1: 翻译 `src/plotting.py`**

- [ ] **Step 2: 翻译 `src/interactive_plot.py`**

- [ ] **Step 3: 翻译 `src/monitor_dev.py`**

- [ ] **Step 4: 翻译 `src/monitor_publisher.py`**

- [ ] **Step 5: 翻译 `src/monitor_relay.py`**

- [ ] **Step 6: 翻译 `src/monitor_tui.py`**

- [ ] **Step 7: 翻译 `src/monitor_web.py`**

- [ ] **Step 8: 翻译 `src/analysis_visibility.py`**

- [ ] **Step 9: 翻译 `src/metrics_schema.py`**

- [ ] **Step 10: 翻译 `src/trailing_diagnostics.py`**

- [ ] **Step 11: 翻译 `src/trailing_diagnostics_tool.py`**

- [ ] **Step 12: 提交第 6 组**

```bash
git add src/plotting.py src/interactive_plot.py src/monitor_dev.py src/monitor_publisher.py src/monitor_relay.py src/monitor_tui.py src/monitor_web.py src/analysis_visibility.py src/metrics_schema.py src/trailing_diagnostics.py src/trailing_diagnostics_tool.py
git commit -m "docs: 添加中文注释 — 可视化与监控"
```

---

## Task 7: 工具与辅助（第 7 组）

**目标：** 翻译工具与辅助模块所有文件（约 50 个）。

**文件列表（顶层）：**
- `src/__init__.py`
- `src/cli_utils.py`
- `src/logging_setup.py`
- `src/multiprocessing_utils.py`
- `src/rust_utils.py`
- `src/shared_arrays.py`
- `src/limit_utils.py`
- `src/config_transform.py`
- `src/config_utils.py`
- `src/ccxt_contracts.py`
- `src/custom_endpoint_overrides.py`
- `src/optimizer_overrides.py`
- `src/repro_harness.py`
- `src/suite_runner.py`
- `src/backtest.py`
- `src/backtest_artifacts.py`
- `src/backtest_dataset.py`
- `src/backtest_dataset_materializer.py`
- `src/backtest_suite.py`
- `src/passivbot_cli/__init__.py`
- `src/passivbot_cli/main.py`

**文件列表（tools/ 子目录）：**
- `src/tools/__init__.py`
- `src/tools/candle_doctor.py`
- `src/tools/capture_ccxt_contracts.py`
- `src/tools/capture_optimize_memory.py`
- `src/tools/diff_ccxt_contracts.py`
- `src/tools/event_loop_policy.py`
- `src/tools/fetch_balance.py`
- `src/tools/fill_events_dash.py`
- `src/tools/fill_events_doctor.py`
- `src/tools/generate_mcap_list.py`
- `src/tools/hyperliquid_probe_common.py`
- `src/tools/inspect_ohlcvs.py`
- `src/tools/iterative_backtester.py`
- `src/tools/iterative_history_plot.py`
- `src/tools/merge_paretos.py`
- `src/tools/migrate_historical_data.py`
- `src/tools/monitor_dev.py`
- `src/tools/monitor_relay.py`
- `src/tools/monitor_tui.py`
- `src/tools/monitor_web.py`
- `src/tools/pad_historical_daily.py`
- `src/tools/pareto_dash.py`
- `src/tools/pareto_explorer.py`
- `src/tools/pareto_transform.py`
- `src/tools/probe_hyperliquid_balance.py`
- `src/tools/probe_hyperliquid_order_margin.py`
- `src/tools/probe_hyperliquid_position_balance.py`
- `src/tools/run_fake_live.py`
- `src/tools/streamline_json.py`
- `src/tools/trailing_diagnostics.py`
- `src/tools/verify_hlcvs_data.py`

- [ ] **Step 1: 翻译 `src/__init__.py`**

- [ ] **Step 2: 翻译 `src/cli_utils.py`**

- [ ] **Step 3: 翻译 `src/logging_setup.py`**

- [ ] **Step 4: 翻译 `src/multiprocessing_utils.py`**

- [ ] **Step 5: 翻译 `src/rust_utils.py`**

- [ ] **Step 6: 翻译 `src/shared_arrays.py`**

- [ ] **Step 7: 翻译 `src/limit_utils.py`**

- [ ] **Step 8: 翻译 `src/config_transform.py`**

- [ ] **Step 9: 翻译 `src/config_utils.py`**

- [ ] **Step 10: 翻译 `src/ccxt_contracts.py`**

- [ ] **Step 11: 翻译 `src/custom_endpoint_overrides.py`**

- [ ] **Step 12: 翻译 `src/optimizer_overrides.py`**

- [ ] **Step 13: 翻译 `src/repro_harness.py`**

- [ ] **Step 14: 翻译 `src/suite_runner.py`**

- [ ] **Step 15: 翻译 `src/backtest.py`**

- [ ] **Step 16: 翻译 `src/backtest_artifacts.py`**

- [ ] **Step 17: 翻译 `src/backtest_dataset.py`**

- [ ] **Step 18: 翻译 `src/backtest_dataset_materializer.py`**

- [ ] **Step 19: 翻译 `src/backtest_suite.py`**

- [ ] **Step 20: 翻译 `src/passivbot_cli/__init__.py`**

- [ ] **Step 21: 翻译 `src/passivbot_cli/main.py`**

- [ ] **Step 22: 翻译 `src/tools/__init__.py`**

- [ ] **Step 23: 翻译 `src/tools/candle_doctor.py`**

- [ ] **Step 24: 翻译 `src/tools/capture_ccxt_contracts.py`**

- [ ] **Step 25: 翻译 `src/tools/capture_optimize_memory.py`**

- [ ] **Step 26: 翻译 `src/tools/diff_ccxt_contracts.py`**

- [ ] **Step 27: 翻译 `src/tools/event_loop_policy.py`**

- [ ] **Step 28: 翻译 `src/tools/fetch_balance.py`**

- [ ] **Step 29: 翻译 `src/tools/fill_events_dash.py`**

- [ ] **Step 30: 翻译 `src/tools/fill_events_doctor.py`**

- [ ] **Step 31: 翻译 `src/tools/generate_mcap_list.py`**

- [ ] **Step 32: 翻译 `src/tools/hyperliquid_probe_common.py`**

- [ ] **Step 33: 翻译 `src/tools/inspect_ohlcvs.py`**

- [ ] **Step 34: 翻译 `src/tools/iterative_backtester.py`**

- [ ] **Step 35: 翻译 `src/tools/iterative_history_plot.py`**

- [ ] **Step 36: 翻译 `src/tools/merge_paretos.py`**

- [ ] **Step 37: 翻译 `src/tools/migrate_historical_data.py`**

- [ ] **Step 38: 翻译 `src/tools/monitor_dev.py`**

- [ ] **Step 39: 翻译 `src/tools/monitor_relay.py`**

- [ ] **Step 40: 翻译 `src/tools/monitor_tui.py`**

- [ ] **Step 41: 翻译 `src/tools/monitor_web.py`**

- [ ] **Step 42: 翻译 `src/tools/pad_historical_daily.py`**

- [ ] **Step 43: 翻译 `src/tools/pareto_dash.py`**

- [ ] **Step 44: 翻译 `src/tools/pareto_explorer.py`**

- [ ] **Step 45: 翻译 `src/tools/pareto_transform.py`**

- [ ] **Step 46: 翻译 `src/tools/probe_hyperliquid_balance.py`**

- [ ] **Step 47: 翻译 `src/tools/probe_hyperliquid_order_margin.py`**

- [ ] **Step 48: 翻译 `src/tools/probe_hyperliquid_position_balance.py`**

- [ ] **Step 49: 翻译 `src/tools/run_fake_live.py`**

- [ ] **Step 50: 翻译 `src/tools/streamline_json.py`**

- [ ] **Step 51: 翻译 `src/tools/trailing_diagnostics.py`**

- [ ] **Step 52: 翻译 `src/tools/verify_hlcvs_data.py`**

- [ ] **Step 53: 提交第 7 组**

```bash
git add src/__init__.py src/cli_utils.py src/logging_setup.py src/multiprocessing_utils.py src/rust_utils.py src/shared_arrays.py src/limit_utils.py src/config_transform.py src/config_utils.py src/ccxt_contracts.py src/custom_endpoint_overrides.py src/optimizer_overrides.py src/repro_harness.py src/suite_runner.py src/backtest.py src/backtest_artifacts.py src/backtest_dataset.py src/backtest_dataset_materializer.py src/backtest_suite.py src/passivbot_cli/ src/tools/
git commit -m "docs: 添加中文注释 — 工具与辅助"
```

---

## Task 8: 用户文档（第 8 组）

**目标：** 翻译 `docs/` 下的用户文档（约 27 个 Markdown 文件）。

**文件列表：**
- `docs/backtesting.md`
- `docs/coin_overrides.md`
- `docs/config_workflow.md`
- `docs/config.bot.md`
- `docs/configuration.md`
- `docs/container_deployment.md`
- `docs/CONTRIBUTING.md`
- `docs/equity_hard_stop_loss.md`
- `docs/equity_hard_stop_loss_cooldown_contracts.md`
- `docs/equity_hard_stop_loss_reference.md`
- `docs/fake_live.md`
- `docs/fill_events_manager_spec.md`
- `docs/fill_events_session_summary.md`
- `docs/forager.md`
- `docs/hyperliquid_guide.md`
- `docs/installation.md`
- `docs/live.md`
- `docs/metrics.md`
- `docs/min_effective_cost.md`
- `docs/monitor.md`
- `docs/optimizing.md`
- `docs/risk_management.md`
- `docs/stock_perps.md`
- `docs/suite_examples.md`
- `docs/tools.md`
- `docs/trailing_grid_ratio.md`
- `docs/troubleshooting.md`

**文档翻译规则（补充）：**
- Markdown 格式保持不变（标题层级、链接、代码块、列表等）
- 代码块内的代码不翻译
- 配置示例中的键名不翻译
- 命令行示例不翻译
- 链接路径不翻译

- [ ] **Step 1: 翻译 `docs/backtesting.md`**

- [ ] **Step 2: 翻译 `docs/coin_overrides.md`**

- [ ] **Step 3: 翻译 `docs/config_workflow.md`**

- [ ] **Step 4: 翻译 `docs/config.bot.md`**

- [ ] **Step 5: 翻译 `docs/configuration.md`**

- [ ] **Step 6: 翻译 `docs/container_deployment.md`**

- [ ] **Step 7: 翻译 `docs/CONTRIBUTING.md`**

- [ ] **Step 8: 翻译 `docs/equity_hard_stop_loss.md`**

- [ ] **Step 9: 翻译 `docs/equity_hard_stop_loss_cooldown_contracts.md`**

- [ ] **Step 10: 翻译 `docs/equity_hard_stop_loss_reference.md`**

- [ ] **Step 11: 翻译 `docs/fake_live.md`**

- [ ] **Step 12: 翻译 `docs/fill_events_manager_spec.md`**

- [ ] **Step 13: 翻译 `docs/fill_events_session_summary.md`**

- [ ] **Step 14: 翻译 `docs/forager.md`**

- [ ] **Step 15: 翻译 `docs/hyperliquid_guide.md`**

- [ ] **Step 16: 翻译 `docs/installation.md`**

- [ ] **Step 17: 翻译 `docs/live.md`**

- [ ] **Step 18: 翻译 `docs/metrics.md`**

- [ ] **Step 19: 翻译 `docs/min_effective_cost.md`**

- [ ] **Step 20: 翻译 `docs/monitor.md`**

- [ ] **Step 21: 翻译 `docs/optimizing.md`**

- [ ] **Step 22: 翻译 `docs/risk_management.md`**

- [ ] **Step 23: 翻译 `docs/stock_perps.md`**

- [ ] **Step 24: 翻译 `docs/suite_examples.md`**

- [ ] **Step 25: 翻译 `docs/tools.md`**

- [ ] **Step 26: 翻译 `docs/trailing_grid_ratio.md`**

- [ ] **Step 27: 翻译 `docs/troubleshooting.md`**

- [ ] **Step 28: 提交第 8 组**

```bash
git add docs/backtesting.md docs/coin_overrides.md docs/config_workflow.md docs/config.bot.md docs/configuration.md docs/container_deployment.md docs/CONTRIBUTING.md docs/equity_hard_stop_loss.md docs/equity_hard_stop_loss_cooldown_contracts.md docs/equity_hard_stop_loss_reference.md docs/fake_live.md docs/fill_events_manager_spec.md docs/fill_events_session_summary.md docs/forager.md docs/hyperliquid_guide.md docs/installation.md docs/live.md docs/metrics.md docs/min_effective_cost.md docs/monitor.md docs/optimizing.md docs/risk_management.md docs/stock_perps.md docs/suite_examples.md docs/tools.md docs/trailing_grid_ratio.md docs/troubleshooting.md
git commit -m "docs: 添加中文注释 — 用户文档"
```

---

## 自检清单

全部翻译完成后，运行以下自检：

```bash
# 检查 src/ 中是否还有英文注释遗漏（排除已翻译文件和指令性注释）
grep -rn '# [A-Za-z]' src/ --include="*.py" | grep -v 'type: ignore' | grep -v 'noqa' | grep -v 'pragma' | grep -v '__pycache__'

# 检查 docs/ 用户文档中是否还有英文段落遗漏
grep -rn '[A-Za-z]\{20,\}' docs/*.md | grep -v '```' | grep -v 'http' | grep -v '|'
```
