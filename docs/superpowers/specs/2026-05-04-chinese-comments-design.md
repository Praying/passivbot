# 中文注释翻译设计规格

## 概述

为 Passivbot 项目添加完整的中文注释，将现有英文注释和 docstring 按语义严格翻译为简体中文。翻译范围覆盖 `src/` 目录和 `docs/` 用户文档。

## 约束与原则

### 翻译范围

- **包含**：`src/` 下所有 Python 文件的注释和 docstring；`docs/` 下用户文档（不含 `ai/`、`plans/` 子目录和发布说明）
- **不包含**：代码标识符（函数名、变量名、类名）；`tests/` 目录；`docs/ai/`、`docs/plans/`；发布说明（`release_notes_*.md`）

### 翻译规则

1. **技术术语保留英文**：EMA、OHLCV、PnL、ROI、API、JSON、Rust、Python、ccxt 等
2. **交易所名称保留英文**：Binance、Bybit、Hyperliquid、OKX、Bitget 等
3. **配置键名保留英文**：如 `entry_qty_pct`、`close_qty_pct` 等
4. **代码标识符不翻译**：函数名、变量名、类名全部保留英文
5. **翻译风格**：简洁直译，符合已翻译文件的风格
6. **保持格式**：缩进、空行、注释位置不变
7. **指令性注释保留**：`# type: ignore`、`# noqa` 等不翻译

### 已翻译文件（基准参考）

- `src/passivbot.py`
- `src/candlestick_manager.py`
- `src/main.py`

## 模块分组与优先级

### 第 1 组 — 核心引擎（最高优先级）

| 文件 | 状态 |
|------|------|
| `src/passivbot.py` | 已翻译 |
| `src/main.py` | 已翻译 |
| `src/passivbot_hsl.py` | 待翻译 |
| `src/passivbot_exceptions.py` | 待翻译 |
| `src/passivbot_version.py` | 待翻译 |
| `src/passivbot_monitor.py` | 待翻译 |
| `src/procedures.py` | 待翻译 |
| `src/pure_funcs.py` | 待翻译 |
| `src/utils.py` | 待翻译 |

### 第 2 组 — 配置系统

`src/config/` 目录下所有文件（约 22 个）：

- `__init__.py`、`access.py`、`bot.py`、`coerce.py`、`hydrate.py`
- `limits.py`、`load.py`、`log_output.py`、`logging_summary.py`
- `metrics.py`、`normalize.py`、`overrides.py`、`parse.py`
- `pnl_lookback.py`、`project.py`、`runtime_compile.py`
- `schema.py`、`scoring.py`、`transform_log.py`、`tree_ops.py`、`validate.py`
- `migrations/` 子目录（`__init__.py`、`detect.py`、`legacy_v7.py`、`renames.py`）

### 第 3 组 — 交易所适配器

`src/exchanges/` 目录下所有文件（约 12 个）：

- `__init__.py`、`ccxt_bot.py`、`binance.py`、`bitget.py`、`bybit.py`
- `defx.py`、`fake.py`、`gateio.py`、`hyperliquid.py`
- `kucoin.py`、`okx.py`、`paradex.py`

### 第 4 组 — 数据管理

| 文件 | 状态 |
|------|------|
| `src/candlestick_manager.py` | 已翻译 |
| `src/ohlcv_catalog.py` | 待翻译 |
| `src/ohlcv_download.py` | 待翻译 |
| `src/ohlcv_legacy_import.py` | 待翻译 |
| `src/ohlcv_planner.py` | 待翻译 |
| `src/ohlcv_store.py` | 待翻译 |
| `src/ohlcv_utils.py` | 待翻译 |
| `src/hlcv_preparation.py` | 待翻译 |
| `src/fill_events_manager.py` | 待翻译 |
| `src/warmup_utils.py` | 待翻译 |
| `src/legacy_data_migrator.py` | 待翻译 |
| `src/tradfi_data.py` | 待翻译 |

### 第 5 组 — 优化引擎

| 文件 | 状态 |
|------|------|
| `src/optimize.py` | 待翻译 |
| `src/opt_utils.py` | 待翻译 |
| `src/optimize_suite.py` | 待翻译 |
| `src/pareto_core.py` | 待翻译 |
| `src/pareto_explorer.py` | 待翻译 |
| `src/pareto_store.py` | 待翻译 |
| `src/optimization/__init__.py` | 待翻译 |
| `src/optimization/backend_shared.py` | 待翻译 |
| `src/optimization/backends/__init__.py` | 待翻译 |
| `src/optimization/backends/deap_backend.py` | 待翻译 |
| `src/optimization/backends/pymoo_backend.py` | 待翻译 |
| `src/optimization/bounds.py` | 待翻译 |
| `src/optimization/callback.py` | 待翻译 |
| `src/optimization/config_adapter.py` | 待翻译 |
| `src/optimization/deap_adapters.py` | 待翻译 |
| `src/optimization/output.py` | 待翻译 |
| `src/optimization/problem.py` | 待翻译 |
| `src/optimization/repair.py` | 待翻译 |
| `src/optimization/shape.py` | 待翻译 |
| `src/optimization/warmup.py` | 待翻译 |

### 第 6 组 — 可视化与监控

| 文件 | 状态 |
|------|------|
| `src/plotting.py` | 待翻译 |
| `src/interactive_plot.py` | 待翻译 |
| `src/monitor_dev.py` | 待翻译 |
| `src/monitor_publisher.py` | 待翻译 |
| `src/monitor_relay.py` | 待翻译 |
| `src/monitor_tui.py` | 待翻译 |
| `src/monitor_web.py` | 待翻译 |
| `src/analysis_visibility.py` | 待翻译 |
| `src/metrics_schema.py` | 待翻译 |
| `src/trailing_diagnostics.py` | 待翻译 |
| `src/trailing_diagnostics_tool.py` | 待翻译 |

### 第 7 组 — 工具与辅助

| 文件 | 状态 |
|------|------|
| `src/cli_utils.py` | 待翻译 |
| `src/logging_setup.py` | 待翻译 |
| `src/multiprocessing_utils.py` | 待翻译 |
| `src/rust_utils.py` | 待翻译 |
| `src/shared_arrays.py` | 待翻译 |
| `src/limit_utils.py` | 待翻译 |
| `src/config_transform.py` | 待翻译 |
| `src/ccxt_contracts.py` | 待翻译 |
| `src/custom_endpoint_overrides.py` | 待翻译 |
| `src/optimizer_overrides.py` | 待翻译 |
| `src/repro_harness.py` | 待翻译 |
| `src/suite_runner.py` | 待翻译 |
| `src/backtest.py` | 待翻译 |
| `src/backtest_artifacts.py` | 待翻译 |
| `src/backtest_dataset.py` | 待翻译 |
| `src/backtest_dataset_materializer.py` | 待翻译 |
| `src/backtest_suite.py` | 待翻译 |
| `src/__init__.py` | 待翻译 |
| `src/passivbot_cli/__init__.py` | 待翻译 |
| `src/passivbot_cli/main.py` | 待翻译 |
| `src/tools/` 目录下约 25 个文件 | 待翻译 |

### 第 8 组 — 用户文档

`docs/` 下的用户文档（约 27 个）：

- `backtesting.md`、`coin_overrides.md`、`config_workflow.md`
- `config.bot.md`、`configuration.md`、`container_deployment.md`
- `CONTRIBUTING.md`、`equity_hard_stop_loss.md`
- `equity_hard_stop_loss_cooldown_contracts.md`
- `equity_hard_stop_loss_reference.md`、`fake_live.md`
- `fill_events_manager_spec.md`、`fill_events_session_summary.md`
- `forager.md`、`hyperliquid_guide.md`、`installation.md`
- `live.md`、`metrics.md`、`min_effective_cost.md`
- `monitor.md`、`optimizing.md`、`risk_management.md`
- `stock_perps.md`、`suite_examples.md`、`tools.md`
- `trailing_grid_ratio.md`、`troubleshooting.md`

## 提交策略

- 每完成一个模块组做一次 git commit
- commit message 格式：`docs: 添加中文注释 — <模块组名称>`
- 共 8 次提交

## 翻译示例

```python
# 英文原文：
# hedge_mode controls whether simultaneous long/short on same coin is allowed.
# This is the config-level setting; exchange-specific bots may override
# self.hedge_mode to False if the exchange doesn't support two-way mode.

# 翻译后：
# hedge_mode 控制是否允许同一币种同时持有多头/空头仓位。
# 这是配置级别的设置；交易所特定的 bot 可能会将
# self.hedge_mode 覆盖为 False（如果交易所不支持双向模式）。
```

```python
# 英文原文：
def calc_pnl(side, entry_price, close_price, qty, fee_rate):
    """Calculate trade PnL by delegating to the appropriate Rust helper."""

# 翻译后：
def calc_pnl(side, entry_price, close_price, qty, fee_rate):
    """通过调用相应的 Rust 辅助函数计算交易 PnL。"`
```

## 成功标准

1. 所有 `src/` 下 Python 文件的英文注释和 docstring 已翻译为中文
2. 所有 `docs/` 用户文档已翻译为中文
3. 技术术语保留英文原文
4. 代码标识符未被修改
5. 翻译风格与已翻译的 3 个文件保持一致
6. 每个模块组有一次独立的 git commit
