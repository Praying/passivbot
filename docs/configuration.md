# Passivbot 参数说明

本文档解释 Passivbot 使用的规范配置 schema。

- 默认值的权威来源是 `src/config/schema.py`。
- 示例配置 `configs/examples/default_trailing_grid_long_npos7.json` 精确镜像了这些硬编码默认值。
- 如果省略 `config_path`，Passivbot 会加载这些代码内默认值。

推荐的用户工作流程、示例和最佳实践请参见[配置工作流程](config_workflow.md)。

## 回测设置

- **base_dir**: 保存回测结果的位置。
- **compress_cache**: 设为 `true` 可节省磁盘空间。设为 `false` 可加快加载速度。
- **end_date**: 回测结束日期，例如 `2024-06-23`。设为 `’now’` 可使用今天的日期作为结束日期。
- **exchanges**: 用于回测和优化的 1m OHLCV 数据来源交易所。支持的交易所包括 `binance`、`bybit`、`gateio` 和 `bitget`。当前默认配置使用 `[‘binance’, ‘bybit’]`。
  **GateIO 注意：** 如果磁盘上已有 `caches/ohlcv/gateio` 数据，请在全新运行前删除它，以便 Passivbot 使用基础量标准化数据重建缓存。
  GateIO 的公共 1m OHLCV 端点仅提供最近约 10,000 根 K 线的窗口；对于更早的 GateIO 回测，请使用 `backtest.ohlcv_source_dir` 或其他 K 线来源。
- **coin_sources**: 可选的 `coin -> exchange` 映射，用于在配置了多个交易所时覆盖自动交易所选择。场景可能会添加更多覆盖；冲突的分配会引发错误。
- **market_settings_sources**: 可选的 `coin -> exchange` 映射，专门用于交易所元数据，如 `price_step`、`qty_step`、手续费和最小规模规则。这与 `coin_sources` 是分开的：你可以从一个交易所获取 K 线，同时从另一个交易所借用市场设置。
- **ohlcv_source_dir**: 可选路径，指向预填充的 OHLCV 目录，在访问交易所归档之前使用。预期结构：`<dir>/<exchange>/1m/<coin_or_symbol>/YYYY-MM-DD.npz` 或 `.npy`。币种键会标准化为基础币种，但也接受 CCXT 风格的符号文件夹名称（如 `ETH_USDC:USDC`）。
- **volume_normalization**: 当为 `true`（默认）时，跨交易所标准化量数据以使组合数据集可比较。
- **start_date**: 回测开始日期。
- **starting_balance**: 回测开始时的 USD 起始余额。
- **filter_by_min_effective_cost**: 当为 `true` 时，跳过预计初始入场
  （余额 × wallet_exposure_limit × entry_initial_qty_pct，包括 WE 超额津贴）
  低于交易所有效最小成本的币种。
- **dynamic_wel_by_tradability**: 仅回测的 WEL 分母模式。
  - `true`（默认）：`wallet_exposure_limit = total_wallet_exposure_limit / min(n_positions, n_tradable_max)`，其中 `n_tradable_max` 是到目前为止任何时间步长上有过真实 K 线的最高币种数量（不缩减）。
  - `false`：固定分母，与实盘相同：`wallet_exposure_limit = total_wallet_exposure_limit / n_positions`。
- **candle_interval_minutes**: 在回测循环运行前将原始 1m OHLCV 聚合为更粗的 K 线。`1` 保持原生 1m 行为；高于 `1` 的值会加速回测和优化器运行，但会损失区间内的成交排序。
- **gap_tolerance_ohlcvs_minutes**: 准备好的 OHLCV 数据中最大可容忍的空洞大小，超过此值则认为该币种/交易所的数据集损坏。较大的值接受更稀疏的历史数据；较小的值在归档空洞上更快失败。
- **liquidation_threshold**: 提前停止回测的权益底线保护。一旦总权益降至或低于 `starting_balance * liquidation_threshold`，运行终止，`backtest_completion_ratio` 将低于 `1.0`。示例：`starting_balance = 1000` 且 `liquidation_threshold = 0.05` 时，回测在权益 `<= 50` 时停止。这不是”5% 回撤”阈值；如果运行从未超过起始值，它大约对应 `0.95` 的最差回撤。必须满足 `0.0 <= liquidation_threshold < 1.0`。
- **maker_fee_override**: 可选的 maker 手续费覆盖（每一份；使用 `0.0002` 表示 0.02%）。留空 `null` 使用交易所派生的 maker 手续费。
- **taker_fee_override**: 可选的 taker 手续费覆盖（每一份；使用 `0.00055` 表示 0.055%）。留空 `null` 使用交易所派生的 taker 手续费。
- **market_order_slippage_pct**: 仅回测的滑点，当回测器模拟市价单执行时应用。这适用于 `bot.{long,short}.hsl_panic_close_order_type` 为 `”market”` 时的 HSL 恐慌平仓，以及被 `live.market_orders_allowed` 提升为市价执行的普通 orchestrator 订单。卖单以 `close * (1 - slippage_pct)` 向下取整到 `price_step` 成交；买单以 `close * (1 + slippage_pct)` 向上取整成交。一旦选择市价执行路径，成交即被保证，且 resulting 成交也使用 taker 手续费。默认 `0.0005`（5 bps）。
- **visible_metrics**: 控制独立回测后在终端打印哪些指标。`null` 显示 `optimize.scoring` 和 `optimize.limits` 隐含的指标，`[]` 显示所有指标，显式列表会向默认视图添加额外的命名指标。这仅影响 CLI 可见性；完整的指标集仍会被计算和持久化。
- **config_version**: 配置文件的顶层 schema 版本字符串。规范的 `v7.10` 配置使用 `v7.10.0`。没有此字段的旧配置被视为遗留配置，并在加载时迁移。
- **balance_sample_divider**: 为 `balance_and_equity.csv.gz` 和相关图表采样余额/权益时每个桶的分钟数。`1` 保持完整的每分钟分辨率；较高的值会稀疏化序列（例如 `15` 每 15 分钟存储一个点）以减小文件大小。CSV 包含 USD 和 BTC 的账户余额/权益，以及与抵押品无关的 `strategy_equity`。
- **btc_collateral_cap**: 持有 BTC 抵押品占账户权益的目标（和上限）份额。`0` 保持账户完全以 USD 计价；`1.0` 目标为完全 BTC 抵押品；值 `>1` 允许杠杆化 BTC 抵押品，接受负 USD 余额。回测在第一个活跃交易步骤初始化 BTC 抵押品仓位，而不是在 EMA 预热期间。
- **btc_collateral_ltv_cap**: 可选的贷款价值上限（`USD debt ÷ equity`），在补充 BTC 时强制执行。留空 `null`（默认）允许无限债务，或设为浮点数（如 `0.6`）以在杠杆超过该阈值时停止购买 BTC。
### Suite 场景

Suite 配置使用 `backtest` 下的扁平化结构：

- **backtest.suite_enabled**: Suite 运行的主开关（`--suite [y/n]` 在运行时覆盖）。规范 schema 和示例配置中默认为 `false`。
- **backtest.scenarios**: 场景字典列表。支持的每场景键：
  - `label`: `backtests/suite_runs/<timestamp>/` 下的目录名。
  - `start_date`、`end_date`: 覆盖全局日期窗口。
  - `coins`、`ignored_coins`: 限制或跳过符号。
  - `exchanges`: 限制哪些交易所可以为此场景提供数据。
  - `coin_sources`: 场景特定的 `coin_sources` 覆盖。
  - `overrides`: 任意配置路径覆盖（如 `{"bot.long.total_wallet_exposure_limit": 2}`）。
- **backtest.aggregate**: 指标特定聚合模式的字典（默认 `mean`）。未指定的键回退到 `default` 条目。

实际示例和建议用法请参见 [Suite 示例](suite_examples.md)。

每指标聚合示例：

```json
"backtest": {
  "aggregate": {
    "default": "mean",
    "mdg_usd": "median",
    "sharpe_ratio": "std",
    "drawdown_worst_usd": "max"
  }
}
```

## 日志

- **level**: 控制 Passivbot 和工具的全局详细程度。
  - 接受的值：`0`（警告）、`1`（信息）、`2`（调试）、`3`（跟踪）。
  - `passivbot live` 和 `passivbot backtest` 上的 CLI 标志 `--debug-level`/`--log-level` 可为单次运行覆盖配置值。
  - CandlestickManager 等组件继承此级别，因此 EMA 预热和 K 线维护日志遵循相同的详细程度。
- **persist_to_file**: 当为 `true` 时，`passivbot live` 还会将控制台日志流写入磁盘上的带时间戳文件，并刷新 `logs/{user}.log` 作为当前运行的稳定别名。规范默认值为 `true`，因此实盘运行会写入 `logs/`，除非你显式禁用。在这个第一个集成版本中，回测/优化仍使用控制台日志，除非你在外部包装它们。
- **dir**: 当 `persist_to_file` 启用时，用于持久化实盘日志文件和稳定当前运行别名的目录。默认 `logs`。
- **rotation**: 启用轮转实盘日志文件，而不是每个进程追加到一个文件。默认 `false`。
- **max_bytes_mb**: 轮转前每个实盘日志文件的最大大小（MB）。仅在 `rotation = true` 时使用。默认 `10`。
- **backup_count**: 启用轮转时保留的轮转备份数量。默认 `5`。
- **memory_snapshot_interval_minutes**: `_log_memory_snapshot` 遥测条目（RSS、缓存占用、asyncio 任务计数）之间的间隔。默认 `30`；较低的值更早暴露泄漏，较高的值减少噪音。
- **volume_refresh_info_threshold_seconds**: 批量 volume-EMA 刷新在提升为 INFO 日志之前必须花费的最短持续时间。更快完成的运行仅发出 DEBUG 输出（启用调试日志时）。设为 `0` 可在 INFO 级别记录每次刷新。

## 监控

监控发布器启用时会将只读仪表板数据根写入磁盘。

- **enabled**: 监控发布的主开关。默认 `true`。
- **root_dir**: 监控输出的基础目录。每个 bot 的数据写入 `root_dir/{exchange}/{user}` 下。
- **snapshot_interval_seconds**: `state.latest.json` 写入之间的尽力最小间隔。
- **checkpoint_interval_minutes**: 压缩检查点快照之间的间隔。设为 `0` 可禁用检查点。
- **event_rotation_mb**: `events/current.ndjson` 超过此大小后轮转。
- **event_rotation_minutes**: 即使未达到大小阈值，也在经过此时间后轮转 `events/current.ndjson`。
- **retain_days**: 轮转的事件/历史/检查点文件的基于时间的保留策略。
- **max_total_bytes**: 监控根的全局字节上限。首先修剪旧的轮转事件/历史/检查点文件。
- **retain_price_ticks**、**retain_candles**、**retain_fills**: 启用或禁用价格 tick、已完成 K 线和标准化成交的当前历史流。
- **compress_rotated_segments**: 如果为 `true`，对轮转的事件段和检查点进行 gzip 压缩。
- **price_tick_min_interval_ms**: 每个符号发出 `history/price_ticks.current.ndjson` 条目的最小间隔。
- **emit_completed_candles**: 启用或禁用已完成的 1m/1h K 线历史发布。
- **include_raw_fill_payloads**: 如果为 `true`，在标准化成交历史载荷旁边包含交易所/原始成交载荷。

当前输出文件和事件类型请参见 [monitor.md](monitor.md)。

## Bot 设置

### 按方向的 HSL 参数

HSL 现在直接位于每个 `pside` 下：

1. `bot.long.hsl_*`
2. `bot.short.hsl_*`
3. `live.hsl_signal_mode`

另见：

1. [权益硬止损](equity_hard_stop_loss.md)
2. [风险管理](risk_management.md)

### 权益硬止损 (`bot.{long,short}.hsl_*`)

按方向的回撤断路器。

每个 `pside` 有相同的参数集：

- **hsl_enabled**:
  - 启用或禁用该 `pside` 的 HSL。
- **hsl_red_threshold**:
  - HSL 回撤分数的 RED 触发阈值。
- **hsl_ema_span_minutes**:
  - 用于平滑回撤的 EMA 跨度。
  - 在回测中，如果此值小于 `backtest.candle_interval_minutes`，平滑实际上被禁用，HSL 对 EMA 部分使用原始回撤。
- **hsl_cooldown_minutes_after_red**:
  - 该 `pside` RED 停止后自动重启前等待的分钟数。
  - `0.0` 表示停止且不自动重启。
  - 重启时 HSL 重放将完全平仓该 `pside` 所有仓位的历史 RED 恐慌平仓视为已完成的 RED 止损，并在评估后续冷却/重启行为之前从该恐慌之后重置跟踪。
- **hsl_no_restart_drawdown_threshold**:
  - 该 `pside` 的终端不重启阈值。
  - 从持久的跨重启 HSL 回撤评估。
  - 低于 `hsl_red_threshold` 的值会被钳位到 `hsl_red_threshold`。
  - 必须满足：`hsl_red_threshold <= hsl_no_restart_drawdown_threshold <= 1.0`。
- **hsl_tier_ratios.yellow / hsl_tier_ratios.orange**:
  - 用于从 `hsl_red_threshold` 推导 YELLOW 和 ORANGE 阈值的乘数。
  - 必须满足：`0 < yellow < orange < 1`。
- **hsl_orange_tier_mode**:
  - 允许的值：
    - `graceful_stop`
    - `tp_only_with_active_entry_cancellation`
  - 确定 bot 在该 `pside` 的 ORANGE 状态下的行为。
- **hsl_panic_close_order_type**:
  - 允许的值：
    - `market`
    - `limit`
  - 确定该 `pside` 的 RED 恐慌退出如何执行或模拟。

行为摘要：

1. YELLOW：该 `pside` 的警告级别
2. ORANGE：该 `pside` 的降低风险模式
3. RED：恐慌平仓，等待该 `pside` 上所有仓位完全关闭，停止，可选的冷却重启

信号模式：

1. `live.hsl_signal_mode = "unified"`（默认）
   - 多头和空头保持独立的 HSL 控制器
   - 两者都从相同的组合账户级策略信号馈送
2. `live.hsl_signal_mode = "pside"`
   - 每个 `pside` 控制器使用自己的已实现/未实现策略 PnL

回测特定说明：

1. 如果 `hsl_panic_close_order_type = "market"`，回测器使用 `backtest.market_order_slippage_pct` 进行模拟 taker 执行，并收取 taker 手续费（默认为交易所派生，或设置时使用 `backtest.taker_fee_override`）。

关键 HSL 分析指标：

1. 全局账户指标：
   - `drawdown_worst_strategy_eq`
   - `drawdown_worst_mean_1pct_strategy_eq`
   - `peak_recovery_days_strategy_eq`
   - `hard_stop_triggers`
   - `hard_stop_restarts`
2. 按方向指标：
   - `drawdown_worst_strategy_eq_long`
   - `drawdown_worst_strategy_eq_short`
   - `drawdown_worst_mean_1pct_strategy_eq_long`
   - `drawdown_worst_mean_1pct_strategy_eq_short`
   - `peak_recovery_days_strategy_eq_long`
   - `peak_recovery_days_strategy_eq_short`
   - `hard_stop_triggers_long`
   - `hard_stop_triggers_short`
   - `hard_stop_restarts_long`
   - `hard_stop_restarts_short`

### 多头和空头的通用参数

- **ema_span_0**、**ema_span_1**:
  - 跨度以分钟为单位。
  - 公式：`next_EMA = prev_EMA * (1 - alpha) + new_val * alpha`，其中 `alpha = 2 / (span + 1)`。
  - 额外的 EMA 跨度计算为 `(ema_span_0 * ema_span_1)**0.5`。
  - 三个 EMA 形成上下 EMA 带：
    - `ema_band_lower = min(emas)`
    - `ema_band_upper = max(emas)`
  - 这些带用于初始入场和自动解套平仓。
- **n_positions**: 最大开仓数量。设为 `0` 可禁用多头/空头。
- **total_wallet_exposure_limit**: 允许的最大敞口。
  - 示例：`total_wallet_exposure_limit = 0.75` 表示使用（未杠杆化）钱包余额的 75%。
  - 示例：`total_wallet_exposure_limit = 1.6` 表示使用（未杠杆化）钱包余额的 160%。
  - 实盘分母是固定的：`wallet_exposure_limit = total_wallet_exposure_limit / n_positions`。
  - 回测分母由 `backtest.dynamic_wel_by_tradability` 控制。
  - 更多信息：`docs/risk_management.md`。

### 网格入场参数

Passivbot 可配置为创建入场订单网格，价格和数量由以下参数确定：

- **entry_grid_double_down_factor**:
  - 下一个网格入场的数量是仓位大小乘以加倍因子。
  - 示例：如果仓位大小为 `1.4` 且 `double_down_factor` 为 `0.9`，则下一个入场数量为 `1.4 * 0.9 = 1.26`。
  - 也适用于 trailing 入场。
- **entry_grid_spacing_pct**、**entry_grid_spacing_we_weight**:
  - 网格重新入场价格确定如下：
    - `next_reentry_price_long = pos_price * (1 - entry_grid_spacing_pct * multiplier)`
    - `next_reentry_price_short = pos_price * (1 + entry_grid_spacing_pct * multiplier)`
  - `multiplier = 1 + (wallet_exposure / wallet_exposure_limit) * entry_grid_spacing_we_weight + log_component`
  - 设置 `entry_grid_spacing_we_weight` > 0 会在仓位接近钱包敞口限制时扩大间距；负值会在敞口较小时收紧间距。
- **entry_grid_spacing_volatility_weight**、**entry_volatility_ema_span_hours**:
  - 上面乘数中的 `log_component` 来自每根 K 线对数范围 `ln(high/low)` 的 EMA。
  - `entry_grid_spacing_volatility_weight` 控制最近对数范围扩大或缩小间距的强度。值为 `0` 可禁用基于对数的调整。
  - `entry_volatility_ema_span_hours` 设置在应用权重之前平滑波动率（对数范围）信号时使用的 EMA 跨度（以小时为单位）。相同的波动率 EMA 也为 `entry_trailing_threshold_volatility_weight` 和 `entry_trailing_retracement_volatility_weight` 的乘数提供动力。
- **entry_initial_ema_dist**:
  - 距离下/上 EMA 带的偏移量。
  - 多头初始入场/空头解套平仓价格为下 EMA 带减去偏移量。
  - 空头初始入场/多头解套平仓价格为上 EMA 带加上偏移量。
  - 参见 `ema_span_0`/`ema_span_1`。
- **entry_initial_qty_pct**:
  - `initial_entry_cost = balance * wallet_exposure_limit * entry_initial_qty_pct`
- **entry_trailing_double_down_factor**:
  - 控制 trailing 重新入场升级激进程度的乘数。与网格等效参数一样，任何正值都会增加连续成交的大小（较大的值增长更快）。
- **entry_trailing_threshold_pct**、**entry_trailing_retracement_pct**:
  - 与下面的 trailing 平仓参数语义相同，但应用于 trailing 入场。bot 等待有利移动（`threshold_pct`）和随后的回撤（`retracement_pct`）后才触发 trailing 重新入场。
- **entry_trailing_threshold_we_weight**、**entry_trailing_retracement_we_weight**:
  - 基于钱包敞口的额外缩放。随着敞口接近每符号限制，正值权重会扩大 trailing 带以减缓额外入场。设为 `0.0` 可禁用调整。
- **entry_trailing_threshold_volatility_weight**、**entry_trailing_retracement_volatility_weight**:
  - 使用共享的 `entry_volatility_ema_span_hours` EMA 增加对最近波动率的敏感性。正值权重在波动市场中增加阈值；`0.0` 移除波动率调制。

### Trailing 参数

相同的逻辑适用于 trailing 入场和 trailing 平仓。

- **trailing_grid_ratio**:
  - 设置 trailing 和 grid 分配。
  - 如果 `trailing_grid_ratio = 0.0`，仅 grid 订单。
  - 如果 `trailing_grid_ratio = 1.0` 或 `trailing_grid_ratio = -1.0`，仅 trailing 订单。
  - 如果 `trailing_grid_ratio > 0.0`，先 trailing 订单，然后 grid 订单。
  - 如果 `trailing_grid_ratio < 0.0`，先 grid 订单，然后 trailing 订单。
    - 示例：`trailing_grid_ratio = 0.3`：Trailing 订单直到仓位 30% 满，然后 grid 订单完成剩余部分。
    - 示例：`trailing_grid_ratio = -0.9`：Grid 订单直到仓位 `(1 - 0.9) = 10%` 满，然后 trailing 订单完成剩余部分。
    - 示例：`trailing_grid_ratio = -0.12`：Grid 订单直到仓位 `(1 - 0.12) = 88%` 满，然后 trailing 订单完成剩余部分。
- **trailing_retracement_pct**、**trailing_threshold_pct**:
  - 两个条件触发 trailing 订单：1) 阈值和 2) 回撤。
  - 如果 `trailing_threshold_pct <= 0.0`，阈值条件始终触发。
  - 对于多头仓位：
    - `if 最高价格自仓位变动以来 > 仓位价格 * (1 + trailing_threshold_pct)`，第一个条件满足。
    - `if 最低价格自最高价格以来 < 最高价格自仓位变动以来 * (1 - trailing_retracement_pct)`，第二个条件满足。下单。
  - Passivbot 跟踪自己的 trailing 价格，不使用交易所的特殊 trailing 订单类型。
  - Trailing 价格跟踪器在仓位变动（加仓或部分平仓）时重置。
  - Trailing 价格跟踪基于 1m OHLCV，每新的整分钟更新。

### 网格平仓参数

- **close_grid_markup_start**、**close_grid_markup_end**、**close_grid_qty_pct**:
  - 止盈（TP）价格在以下范围内线性分布：
    - **多头**：`pos_price * (1 + markup_start)` 到 `pos_price * (1 + markup_end)`。
    - **空头**：`pos_price * (1 - markup_start)` 到 `pos_price * (1 - markup_end)`。
  - TP 方向取决于 `markup_start` 和 `markup_end` 的相对值：
    - 如果 `markup_start > markup_end`：TP 网格**反向**构建（从较高价格开始，多头递减/空头递增）。
    - 如果 `markup_start < markup_end`：TP 网格**正向**构建（从较低价格开始，多头递增/空头递减）。
  - 示例（**多头**，反向 TP）：如果 `pos_price = 100`、`markup_start = 0.01`、`markup_end = 0.005`、`close_grid_qty_pct = 0.2`，TP 价格为：`[101.0, 100.9, 100.8, 100.7, 100.6]`。
  - 示例（**多头**，正向 TP）：如果 `markup_start = 0.005`、`markup_end = 0.01`，TP 价格为：`[100.5, 100.6, 100.7, 100.8, 100.9]`。
  - 示例（**空头**，正向 TP）：如果 `pos_price = 100`、`markup_start = 0.005`、`markup_end = 0.01`，TP 价格为：`[99.5, 99.4, 99.3, 99.2, 99.1]`。
  - 示例（**空头**，反向 TP）：如果 `markup_start = 0.01`、`markup_end = 0.005`，TP 价格为：`[99.0, 99.1, 99.2, 99.3, 99.4]`。
  - 每个订单的数量为 `full pos size * close_grid_qty_pct`。
  - 注意：满仓位大小指的是最大化后的大小。如果实际仓位较小，可能创建少于 `1 / close_grid_qty_pct` 个订单。
  - TP 网格按从 `markup_start` 到 `markup_end` 的顺序填充，每个切片分配到相应数量：
    - 第一个 TP 最多 `close_grid_qty_pct * full_pos_size`。
    - 第二个 TP 从 `close_grid_qty_pct` 到 `2 * close_grid_qty_pct`，依此类推。
  - 示例：如果 `full_pos_size = 100` 且 `long_pos_size = 55`，价格反向构建，则 TP 订单可能为 `[15@100.8, 20@100.9, 20@101.0]`。
  - 如果仓位超过满仓位大小，多余大小会添加到最接近 `markup_start` 的 TP 订单。
    - 示例：如果 `long_pos_size = 130` 且网格正向，TP 订单为 `[50@100.5, 20@100.6, 20@100.7, 20@100.8, 20@100.9]`。

### Trailing 平仓参数

- **close_trailing_grid_ratio**: 参见上面的 Trailing 参数。
- **close_trailing_qty_pct**: 平仓数量为 `full pos size * close_trailing_qty_pct`。
- **close_trailing_retracement_pct**: 参见上面的 Trailing 参数。
- **close_trailing_threshold_pct**: 参见上面的 Trailing 参数。

### 解套参数

如果仓位被套牢，bot 使用其他仓位的利润来实现被套仓位的亏损。如果多个仓位被套牢，选择价格行为距离最小的仓位进行解套。

- **unstuck_close_pct**:
  - 每个解套订单平仓 `full pos size * wallet_exposure_limit` 的百分比。
- **unstuck_ema_dist**:
  - 距离 EMA 带放置解套订单的距离：
    - `long_unstuck_close_price = upper_EMA_band * (1 + unstuck_ema_dist)`
    - `short_unstuck_close_price = lower_EMA_band * (1 - unstuck_ema_dist)`
- **unstuck_loss_allowance_pct**:
  - 低于过去峰值余额的加权百分比，用于允许亏损。
  - `loss_allowance = past_peak_balance * (1 - unstuck_loss_allowance_pct * total_wallet_exposure_limit)`
  - 示例：如果过去峰值余额为 `$10,000`、`unstuck_loss_allowance_pct = 0.02`、`total_wallet_exposure_limit = 1.5`，当余额达到 `$10,000 * (1 - 0.02 * 1.5) = $9,700` 时 bot 停止承受亏损。
- **unstuck_threshold**:
  - 如果仓位大于阈值，认为它被套牢并激活解套。
  - `if wallet_exposure / wallet_exposure_limit > unstuck_threshold: 解套启用`
  - 示例：如果仓位大小为 `$500`，最大允许仓位大小为 `$1000`，仓位 50% 满。如果 `unstuck_threshold = 0.45`，解套仓位直到其大小为 `$450`。

### 过滤参数

Forager 币种选择现在使用两阶段模型：粗略的量修剪，然后跨量、EMA 就绪性和波动率的加权排名。

- **forager_volume_drop_pct**: 粗略低量修剪。在最终排名前丢弃最低的相对量分数，同时保留足够的候选以填充配置的槽位。
  - 示例：`forager_volume_drop_pct = 0.1` 丢弃底部 10% 的相对量。设为 `0` 可跳过修剪阶段。
- **forager_volatility_ema_span / forager_volume_ema_span**: 回溯计算 1m 波动率（对数范围）和报价量 EMA 的分钟数，供 forager 模式使用。
  - 对数范围从 1m OHLCV 计算为 `mean(ln(high / low))`。
  - 这些跨度控制 forager 排名的原始输入；它们与用于入场逻辑的 `entry_volatility_ema_span_hours` 是分开的。
- **forager_score_weights**: 最终加权 forager 排名权重。
  - 必需键：`volume`、`ema_readiness`、`volatility`。
  - 默认：`{"volume": 0.0, "ema_readiness": 0.0, "volatility": 1.0}`。
  - 正值权重是相对的，使用前标准化为单位和。
  - 如果三个都是 `0.0`，Passivbot 将它们标准化为仅 EMA 就绪性排名。
  - `ema_readiness` 按距离实际偏移初始入场阈值的距离排名，而不是原始 EMA 带。

完整的动机、排名规则、注意事项和使用示例请参见 [docs/forager.md](forager.md)。

## 币种覆盖
- **coin_overrides**:
  - 为单个币种指定完整或部分配置，覆盖主配置的值。
  - 格式：`{"COIN1": overrides1, "COIN2": overrides2}`
  - 可使用参数 "override_config_path" 加载完整配置。可以是配置的完整路径，也可以是与主配置文件同目录的备用配置文件名。
  - 特定覆盖参数优先于从外部配置加载的覆盖参数。
  - 只有配置参数的子集有资格覆盖主配置：
    - config.bot.long/short：
      ```
      [
        close_grid_markup_end, close_grid_markup_start, close_grid_qty_pct, close_trailing_grid_ratio, close_trailing_qty_pct,
    close_trailing_retracement_pct, close_trailing_threshold_pct, ema_span_0, ema_span_1,
        entry_grid_double_down_factor, entry_grid_spacing_pct, entry_grid_spacing_we_weight,
        entry_grid_spacing_volatility_weight, entry_volatility_ema_span_hours, entry_initial_ema_dist,
        entry_initial_qty_pct, entry_trailing_double_down_factor, entry_trailing_grid_ratio, entry_trailing_retracement_pct,
        entry_trailing_threshold_pct, unstuck_close_pct, unstuck_ema_dist, unstuck_threshold, wallet_exposure_limit
      ]
      ```
    - config.live：
    ```
    [forced_mode_long, forced_mode_short, leverage]
    ```
  - 示例：
    - `{"COIN1": {"override_config_path": "path/to/override_config.json"}}` -- 尝试加载 "path/to/override_config.json" 并为 COIN1 应用所有符合条件的参数
    - `{"COIN2": {"override_config_path": "path/to/other_override_config.json", {"bot": {"long": {"close_grid_markup_start": 0.005}}}}}` -- 先尝试加载 `"path/to/other_override_config.json"`，然后应用 `{"bot": {"long": {"close_grid_markup_start": 0.005}}}`。
    - `{"COIN3": {"bot": {"short": {"entry_initial_qty_pct": 0.01}}, "live": {"forced_mode_long": "panic"}}}` -- 为 COIN3 应用给定的覆盖。
- **forced_modes**:
  - 选项：`[n (normal), m (manual), gs (graceful_stop), t (tp_only), p (panic)]`。
    - **Normal 模式**：Passivbot 正常管理仓位。
    - **Manual 模式**：Passivbot 忽略仓位。
    - **Graceful stop**：如果有仓位，Passivbot 管理它；否则不开新仓位。
    - **Take Profit Only 模式**：Passivbot 仅管理平仓订单。
    - **Panic 模式**：Passivbot 立即平仓。

## 实盘交易设置

- **approved_coins**:
  - 批准交易的币种列表。
    - 回测器和优化器使用 `live.approved_coins` 减去 `live.ignored_coins`。
  - 可以作为外部文件的路径给出，由 Passivbot 持续读取。
  - 可以分为多头和空头：
    - 示例：`{“long”: [“COIN1”, “COIN2”], “short”: [“COIN2”, “COIN3”]}`
    - 示例：`{“long”: [“COIN1”, “COIN2”], “short”: “all”}`
  - 显式空值禁用受影响方向的交易：
    - `approved_coins = []`、`{}`、`””` 或 `null` 禁用两个方向
    - `approved_coins = {“long”: [“BTC”], “short”: []}` 保持多头精选并禁用空头
  - 显式值 `”all”` 表示受影响方向的所有合格币种：
    - `approved_coins = “all”` 启用两个方向的所有合格币种
    - `approved_coins = {“long”: “all”, “short”: [“BTC”, “ETH”]}` 启用所有合格多头和精选空头
  - 使用 `live.empty_means_all_approved=true` 的旧配置目前仍会迁移：
    - 全局空的 `approved_coins` 输入被转换为 `approved_coins = “all”`
    - 解析器记录 `live.empty_means_all_approved` 已弃用
- **auto_gs**: 自动为不批准币种的仓位启用 graceful stop。
  - Graceful stop：Bot 继续正常交易，但在当前仓位完全平仓后不开新仓位。
  - 如果 `auto_gs=false`，不批准币种的仓位被置于 manual 模式。
- **enable_archive_candle_fetch**: 在实盘模式下启用归档 K 线回退路径。除非你特别希望实盘 bot 从交易所归档端点补充其本地 K 线状态，否则保持 `false`。
- **execution_delay_seconds**: 执行到交易所后等待 `x` 秒。
- **hedge_mode**: 当交易所支持时，请求在同一币种上同时持有多头和空头仓位。有效行为是 `config.live.hedge_mode AND exchange_capability`；在仅单向的场所，即使此值为 `true`，实盘 bot 仍会以单向模式运行。
- **hsl_position_during_cooldown_policy**: 仅实盘的策略，用于在 HSL RED 冷却期间出现在已停止 `pside` 上的仓位。
  - `panic`：再次恐慌平仓，并在该 `pside` 上所有仓位完全平仓后重启冷却。
  - `normal`：一旦在冷却期间出现真实开仓仓位，将其视为显式操作员覆盖；当没有开仓仓位时，bot 仍会阻止该 `pside` 的新初始入场，只有在仓位出现后才清除停止并从当前状态重启 HSL 回撤跟踪。
  - `manual`：将该仓位保持在 `manual` 模式，同时保持原始冷却运行并阻止新初始入场。
  - `tp_only`：保持原始冷却运行，阻止新入场，仅允许该 `pside` 的平仓管理。
  - `graceful_stop`：保持原始冷却运行，并使用 `graceful_stop` 语义管理任何现有仓位，同时仍阻止新初始入场。
- **hsl_signal_mode**: 选择 HSL 回撤是从一个组合账户级策略信号（`”unified”`，默认）跟踪还是按方向独立跟踪（`”pside”`）。参见[权益硬止损](equity_hard_stop_loss.md)。
- **max_memory_candles_per_symbol**: 每个符号在 RAM 中保留的最大 1m K 线数。超过此上限后会修剪旧条目。默认为 `200_000`。
- **max_disk_candles_per_symbol_per_tf**: 每个符号和时间框架在磁盘上持久化的最大 K 线数。达到限制后会修剪最旧的分片（默认 `2_000_000`）。
- **candle_lock_timeout_seconds**: 当另一个进程持有 CandlestickManager 每符号 K 线获取锁时等待的秒数（默认 `10`）。在运行多个 bot 共享同一缓存目录时增加此值，以避免在慢速 API 调用期间出现虚假超时。
- **inactive_coin_candle_ttl_minutes**: 非活跃符号的 1m K 线在实盘 bot 刷新之前可在 RAM 中停留的时间。较低的值保持非活跃符号更新鲜，但会增加网络/磁盘抖动。
- **filter_by_min_effective_cost**: 如果为 `true`，禁止 `balance * WE_limit * entry_initial_qty_pct < min_effective_cost` 的币种。
  - 示例：如果交易所对某个币种的有最小成本为 `$5`，但 bot 想要下 `$2` 的订单，则禁止该币种。
- **forced_mode_long**、**forced_mode_short**: 强制所有币种的多头/空头为给定模式。
  - 选项：`[m (manual), gs (graceful_stop), p (panic), t (take_profit_only)]`。
- **ignored_coins**:
  - Bot 不会开仓的币种列表。如果该币种有仓位，启用 graceful stop 或 manual 模式。
  - 可以作为外部文件的路径给出，由 Passivbot 持续读取。
  - 可以分为多头和空头：
    - 示例：`{“long”: [“COIN1”, “COIN2”], “short”: [“COIN2”, “COIN3”]}`
- **leverage**: 在交易所设置的杠杆。默认为 `10`。
- **margin_mode_preference**: 当符号支持全仓和逐仓时的首选实盘保证金模式。
  - `auto` / `auto_cross`：两种模式都可用时优先全仓。
  - `auto_isolated`：两种模式都可用时优先逐仓。
  - `cross`：新入场要求全仓；仅逐仓的符号会被跳过，但现有仓位/订单仍可管理。
  - `isolated`：新入场要求逐仓；仅全仓的符号会被跳过，但现有仓位/订单仍可管理。
  - 如果交易所报告某个符号已有开仓仓位或未结订单，实盘 bot 会保留该符号的实际实盘保证金模式进行状态管理，而不是在仓位中途强制配置的偏好。
  - Hyperliquid HIP-3 例外：逐仓 HIP-3 实盘交易目前不支持。支持全仓的 HIP-3 市场被强制为全仓入场，仅逐仓的 HIP-3 市场会被跳过，现有的逐仓 HIP-3 实盘状态会导致启动时大声失败。
- **market_orders_allowed**: 如果为 `true`，允许 Passivbot 在订单价格非常接近当前市场价格时下市价单。如果为 `false`，仅下限价单。当前默认配置使用 `false`。
- **market_order_near_touch_threshold**: 当 `market_orders_allowed` 启用时，Rust 订单编排使用的统一阈值。如果订单价格在当前市场价格的此分数距离内，Rust 将其作为市价单发出。穿越订单也会成为市价单（买单 `bid >= market`，卖单 `ask <= market`）。此执行意图现在由实盘和回测共享。默认为 `0.001`。
  - 决策规则：
    - 非恐慌买单 `price >= market_price` => `market`
    - 非恐慌卖单 `price <= market_price` => `market`
    - 否则，如果 `abs(order_price_diff) <= market_order_near_touch_threshold` => `market`
    - 否则 => `limit`
    - 恐慌平仓仍由 `bot.{long,short}.hsl_panic_close_order_type` 单独控制
  - 所有权为 `config.live`。回测始终继承 `live.market_orders_allowed` 和 `live.market_order_near_touch_threshold`；`config.backtest` 不接受对这两个字段的覆盖。
- **order_match_tolerance_pct**: 用于匹配近乎相同的取消/创建对以避免订单流失的百分比容差（%）。当新提出的订单在现有未结订单的此容差范围内时，Passivbot 可能会保留现有订单而不是取消/替换它。
- **max_n_cancellations_per_batch**: 每次执行取消 `n` 个未结订单。
- **max_n_creations_per_batch**: 每次执行创建 `n` 个新订单。
- **max_n_restarts_per_day**: 如果 bot 崩溃，每天最多重启 `n` 次后完全停止。
- **max_ohlcv_fetches_per_minute**: 实盘 OHLCV/网络预算，用于 K 线支持的指标，如 forager 排名和预热维护。设低可减少 REST 压力；设为 `0` 可禁止新获取，仅依赖已缓存的内容。
- **minimum_coin_age_days**: 禁止早于给定天数的币种。
- **balance_override**: 可选的实盘 bot 使用的钱包余额数值覆盖（适用于干运行和调试）。设置后，bot 不会从交易所获取余额。在使用 BTC 抵押品且你希望保持有效的”固定 USD 余额”进行头寸计算时也很有用，而不是让 USD 计价的余额随 BTC/USD 价格波动。
- **balance_hysteresis_snap_pct**: 应用于余额更新的滞后快照百分比，以减少噪音。设 `0.0` 可禁用滞后。
- **recv_window_ms**: 经过身份验证的 REST 调用的毫秒容差（默认 `5000`）。如果你的交易所因时钟漂移间歇性拒绝请求并显示 `invalid request ... recv_window` 错误，请增加此值。
- K 线管理由 CandlestickManager 处理，具有磁盘缓存和基于 TTL 的刷新。旧版设置 `ohlcvs_1m_rolling_window_days` 和 `ohlcvs_1m_update_after_minutes` 不再使用。
- **pnls_max_lookback_days**: 获取 PnL 历史的回溯深度。这也为实盘风险逻辑和回测使用的滚动已实现 PnL 窗口提供数据。所有权为 `config.live`；`config.backtest` 不接受覆盖。
  - `0`：消费者原生采样分辨率的最小回溯窗口（重置频率与该路径有意义观察的频率一致）。
  - `> 0`：那么多天的滚动窗口。
  - `”all”`：完整的可用历史。
  - 实盘和回测使用相同的已实现 PnL 风险窗口合约：将已实现成交事件过滤到活跃回溯窗口，然后仅从该过滤序列重新计算累计 PnL、当前值和峰值。
- **price_distance_threshold**: EMA 限价单所需的距当前价格行为的最小距离。
- **risk_wel_enforcer_threshold**: 触发 WEL 执行器的每符号乘数。当仓位的敞口超过 `wallet_exposure_limit * (1 + risk_we_excess_allowance_pct) * risk_wel_enforcer_threshold` 时，bot 发出 reduce-only 订单将其控制回来。设 <1.0 进行持续修剪，`1.0` 为硬上限，≤0 禁用。
- **risk_twel_enforcer_threshold**: 触发 TWEL 执行器的已配置 `total_wallet_exposure_limit` 的分数。当总敞口超过此阈值时，bot 排队减少订单而不是新入场。设 >1.0 允许宽限期，`1.0` 严格执行，≤0 禁用。
- **risk_we_excess_allowance_pct**: 执行器在修剪前容忍的每符号超出配置钱包敞口限制的津贴。有助于平滑减少；保持 `0.0` 作为硬上限。
- **max_realized_loss_pct**: 平仓订单的全局已实现亏损门控，锚定于成交历史的峰值已实现余额。对于每个平仓订单，如果预计已实现 PnL 会将余额推至 `peak_balance * (1 - max_realized_loss_pct)` 以下，则订单被阻止。适用于所有平仓订单类型（包括 WEL/TWEL 自动减少和解套），恐慌平仓除外。
  - 默认：`1.0`（禁用）。
  - `<= 0.0`：阻止所有亏损平仓。
  - `>= 1.0`：禁用门控。
  - 示例：峰值余额 `$10,000` 且 `max_realized_loss_pct = 0.05` 时，一旦预计余额降至 `$9,500` 以下，亏损平仓被阻止。
- **max_warmup_minutes**: 应用于回测和实盘预热的历史预热窗口的硬上限。使用 `0` 可禁用上限；否则高于 `0` 的值会钳位从 EMA 跨度计算的每符号预热。
- **warmup_ratio**: 应用于多头/空头设置中最长 EMA 或对数范围跨度（以分钟为单位）的乘数，用于决定在交易前预取多少 1m 历史。例如，值 `0.2` 预热最深回溯的约 20%，受 `max_warmup_minutes` 限制。
- **warmup_jitter_seconds**: 在预热工作开始前应用的随机启动延迟分布。这有助于多个 bot 共享一台机器或缓存时避免在同一秒涌入相同的文件和 API。
- **warmup_concurrency**: 实盘预热任务的并发上限。`0` 让 Passivbot 自动选择；正值限制并行预热的符号数量。
- **max_concurrent_api_requests**: 可选的全局实盘 REST 并发上限。留空 `null` 使用交易所/默认行为；设置整数可更积极地限制经过身份验证和公共请求的扇出。
- **warmup_minutes**: 不是配置键。这是从 `warmup_ratio`、指标跨度和 `max_warmup_minutes` 内部计算的每币种派生预热窗口。
- **time_in_force**: 默认为 Good-Till-Cancelled。
- **user**: 从 `api-keys.json` 获取 API key/secret。

## 优化设置

### 边界

优化时，参数值被约束在上下界之间。边界支持可选的第三个元素，指定基于网格优化的离散步长。

**边界格式：**

- `[low, high]` - `low` 和 `high` 之间的连续优化（当前行为，不变）
- `[low, high, step]` - 离散优化，值约束在网格上：`low`、`low + step`、`low + 2*step`、...、`high`
- `[low, high, 0]` 或 `[low, high, null]` - 视为连续（等同于 `[low, high]`）
- 单个值（如 `0.5`）- 固定参数（不优化）

**步长行为：**

当定义了步长时，优化器仅探索离散网格上的值。遗传算法在*索引空间*（即有效网格值的索引）中执行交叉和变异，以确保后代值始终落在网格上。

例如，边界 `[0.01, 0.10, 0.02]`：
- 有效值为：0.01、0.03、0.05、0.07、0.09
- 优化器永远不会产生 0.02 或 0.04 这样的值

**何时使用步进边界：**

- **整数参数**：对应该为整数的参数使用步长 `1`（如 `n_positions`）
- **粗略搜索**：使用较大的步长减少搜索空间并加速优化
- **已知粒度**：当你知道参数仅在特定间隔有意义时

**示例配置：**

```json
"optimize": {
    "bounds": {
        "long_n_positions": [1, 20, 1],
        "long_total_wallet_exposure_limit": [0.1, 2.0, 0.1],
        "long_entry_grid_spacing_pct": [0.005, 0.05, 0.005],
        "long_ema_span_0": [100, 10000],
        "long_ema_span_1": [200, 20000]
    }
}
```

在此示例中：
- `n_positions`：1 到 20 的整数
- `total_wallet_exposure_limit`：值 0.1、0.2、0.3、...、2.0
- `entry_grid_spacing_pct`：值 0.005、0.01、0.015、...、0.05
- `ema_span_0` 和 `ema_span_1`：连续优化（未定义步长）

HSL 边界现在使用按方向的前缀：

1. `long_hsl_red_threshold`
2. `long_hsl_ema_span_minutes`
3. `long_hsl_cooldown_minutes_after_red`
4. `short_hsl_red_threshold`
5. `short_hsl_ema_span_minutes`
6. `short_hsl_cooldown_minutes_after_red`

`long_hsl_no_restart_drawdown_threshold` 和 `short_hsl_no_restart_drawdown_threshold` 故意不是默认优化边界的一部分。运行时参数仍位于 `bot.{long,short}.hsl_*` 下，但优化器运行默认通过以下方式禁用终端不重启：

1. `optimize.fixed_runtime_overrides["bot.long.hsl_no_restart_drawdown_threshold"] = 1.0`
2. `optimize.fixed_runtime_overrides["bot.short.hsl_no_restart_drawdown_threshold"] = 1.0`

风险应通过规范的 `*_strategy_eq` 指标来约束。已弃用的 `*_hsl` 指标名称仍作为旧配置/结果的别名被接受。

**验证：**

- 步长必须为正；负或零步长视为连续
- 步长不得超过范围（`high - low`）；如果超过，会记录警告并将参数视为连续

### 其他优化参数

- **compress_results_file**: 如果为 `true`，压缩优化输出结果文件以节省空间。
- **enable_overrides**: 优化期间应用的约束覆盖列表，用于强制执行特定参数关系。优化器评估器检查这些条件并在运行每次回测前应用覆盖（默认无）：
  - **"lossless_close_trailing"**: 通过强制 `close_trailing_threshold_pct` > `close_trailing_retracement_pct` 确保 trailing 止盈有利可图。这防止回撤在达到最小利润阈值之前触发。
  - **"forward_tp_grid"**: 创建递增的止盈网格，其中 `close_grid_markup_start` < `close_grid_markup_end`
  - **"backward_tp_grid"**: 创建递减的止盈网格，其中 `close_grid_markup_start` > `close_grid_markup_end`。
- **crossover_probability**: 遗传算法中两个个体之间执行交叉的概率。确定父母交换遗传信息以创建后代的频率。
- **crossover_eta**: 模拟二进制交叉的拥挤因子（η）。较低的值（<20）允许后代离父母更远；较高的值保持它们更接近。默认为 `20.0`。
- **fixed_params**: 在整个运行期间冻结在当前配置值的 `optimize.bounds` 选择器列表。选择器是对边界键的字面子串匹配，因此 `close_grid` 会固定多头和空头的 close-grid 边界。像 `close` 这样的广泛选择器可能匹配超过预期，优化器会在运行前记录排序后的展开。
- **fixed_runtime_overrides**: 在优化评估期间应用的仅运行时覆盖，不改变存储的配置。用于优化器特定的安全旋钮，如禁用终端 HSL 不重启，同时保持实盘/回测配置在磁盘上不变。
- **iters**: 每次优化会话的回测次数。
- **mutation_probability**: 遗传算法中变异个体的概率。确定引入随机变化以维持多样性的频率。
- **mutation_eta**: 多项式变异的拥挤因子（η）。较小的值（<20）产生更重尾的步长，更积极地探索；较大的值将变异限制在当前值附近。默认为 `20.0`。
- **mutation_indpb**: 触发变异时每个属性变异的概率。设为 `0`（默认）自动缩放为 `1 / 参数数量`，或提供 `0` 到 `1` 之间的显式概率。
- **n_cpus**: 并行使用的 CPU 核心数。
- **offspring_multiplier**: 应用于 `population_size` 的乘数，用于确定 μ+λ 进化策略中每代产生多少后代（`λ`）。值 >1.0 通过每代采样更多子代来增加探索。默认为 `1.0`。
- **pareto_max_size**: 在 `optimize_results/.../pareto/` 下保留的 Pareto 最优配置的最大数量。成员按拥挤度修剪（多样性最低的先移除，同时保留每个目标的极值），而不是按时间。默认为 `1000`。
- **population_size**: 遗传优化算法的种群大小。
- **backend**: 优化器后端。默认为 `pymoo`。使用默认的 `optimize.pymoo.algorithm: "auto"` 时，Passivbot 对 `3` 个或更少目标使用 `nsga2`，对 `4+` 个目标使用 `nsga3`。
- **round_to_n_significant_digits**: 用于哈希配置、去重候选和写入优化器工件的量化精度。较低的值更积极地合并近乎相同的候选；较高的值保留更多不同的变体。
- **scoring**:
  - 优化器最小化配置的目标列表并保留 Pareto 前沿。
  - 当前默认配置使用：
    - `adg_strategy_eq`
    - `adg_strategy_eq_w`
    - `mdg_strategy_eq`
    - `mdg_strategy_eq_w`
    - `peak_recovery_days_strategy_eq`
    - `position_held_days_max`
    - `drawdown_worst_strategy_eq`
    - `drawdown_worst_mean_1pct_strategy_eq`
  - 使用默认 `pymoo` 后端时，Passivbot 对 `3` 个或更少目标使用 `nsga2`，对 `4+` 个目标使用 `nsga3`，除非显式覆盖。
  - 完整选项列表：`[adg, adg_w, calmar_ratio, calmar_ratio_w, drawdown_worst, drawdown_worst_mean_1pct, equity_balance_diff_neg_max, equity_balance_diff_neg_mean, equity_balance_diff_pos_max, equity_balance_diff_pos_mean, expected_shortfall_1pct, gain, hard_stop_duration_minutes_max, hard_stop_duration_minutes_mean, hard_stop_flatten_time_minutes_mean, hard_stop_halt_to_restart_equity_loss_pct, hard_stop_panic_close_loss_max, hard_stop_panic_close_loss_sum, hard_stop_post_restart_retrigger_pct, hard_stop_time_in_orange_pct, hard_stop_time_in_red_pct, hard_stop_time_in_yellow_pct, hard_stop_trigger_drawdown_mean, high_exposure_days_max_long, high_exposure_days_max_short, high_exposure_hours_max_long, high_exposure_hours_max_short, loss_profit_ratio, loss_profit_ratio_w, mdg, mdg_w, omega_ratio, omega_ratio_w, peak_recovery_days_equity, peak_recovery_days_pnl, peak_recovery_days_strategy_eq, peak_recovery_hours_equity, peak_recovery_hours_pnl, peak_recovery_hours_strategy_eq, position_held_days_max, position_held_days_mean, position_held_days_median, position_held_hours_max, position_held_hours_mean, position_held_hours_median, position_unchanged_days_max, position_unchanged_hours_max, positions_held_per_day, sharpe_ratio, sharpe_ratio_w, sortino_ratio, sortino_ratio_w, sterling_ratio, sterling_ratio_w]`
  - 后缀 `_w` 表示跨 10 个时间子集（全部、后半、后三分之一、...、后十分之一）的均值，以更重地加权最近数据。
  - 示例：`["mdg", "sharpe_ratio", "loss_profit_ratio"]`、`["adg", "sortino_ratio", "drawdown_worst"]`、`["sortino_ratio", "omega_ratio", "adg_w", "position_unchanged_hours_max"]`、`["adg_pnl_w", "hard_stop_time_in_red_pct", "hard_stop_panic_close_loss_sum"]`
    - 注意：指标可以后缀 `_usd` 或 `_btc` 来选择计价单位。如果 `config.backtest.btc_collateral_cap` 为 `0`，BTC 值仍代表转换为 BTC 术语的 USD 权益。
- **write_all_results**: 控制是否将每个评估的候选追加到 `all_results.bin`。保持 `true` 以获得完整的重放/分析历史；设为 `false` 可减少磁盘写入，仅存储维护的 Pareto/状态工件。

### 优化器 Suite

当启用 `--suite [y/n]` 时，优化器复用回测 suite 配置。

- **backtest.suite_enabled**: 可通过 `passivbot optimize` 上的 `--suite [y/n]` 为优化器运行切换。
- **backtest.aggregate**: 在馈入 `optimize.scoring` 和 `optimize.limits` 之前应用于场景结果的每指标聚合规则。
- **backtest.scenarios**: 场景字典。每个可以覆盖 `coins`、`ignored_coins`、`start_date`、`end_date`、`exchanges`、`coin_sources` 和 `overrides`（任意配置路径覆盖）。

使用 `--suite-config path/to/file.json` 在运行时分层额外的场景定义。

### 优化限制

优化器会惩罚指标值超过或未达到指定阈值的回测。惩罚被添加到适应度分数中以阻止不理想的配置，但不会取消配置资格。

上面列出的任何指标都可以在定义限制时使用。货币特定指标在两种计价单位都可用时使用 `_usd` 和 `_btc` 后缀；即使 `backtest.btc_collateral_cap = 0`，BTC 计价指标也可用。这包括共享的 HSL 指标，如 `hard_stop_time_in_red_pct`、`hard_stop_post_restart_retrigger_pct` 和 `hard_stop_halt_to_restart_equity_loss_pct`，加上用于拒绝截断运行的 `backtest_completion_ratio`。HSL 指标是账户级共享指标，因此保持单值而不是分为 `_usd` 和 `_btc`。每个限制条目是一个字典，包含：

- `metric`：规范指标名称（`drawdown_worst_btc`、`loss_profit_ratio`、`peak_recovery_hours_pnl` 等）。
- `penalize_if`：`<`、`<=`、`>`、`>=`、`==`、`outside_range` 或 `inside_range` 之一（也接受 `less_than`、`greater_than`、`auto` 等别名）。使用 `outside_range` 将指标保持在 `[low, high]` 内，使用 `inside_range` 禁止特定范围。
- `value`：`<`/`>` 模式的数值阈值。
- `range`：范围模式的双值列表 `[low, high]`。
- 可选 `enabled`：设为 `false` 可禁用默认限制而不删除它。这防止配置标准化稍后重新添加该指标的默认限制。
- 可选 `stat`：当你想与特定统计量（`min`、`max`、`mean`、`std`）比较时。`>` 检查默认为 `_max`，`<` 检查默认为 `_min`，范围检查默认为 `_mean`。

#### 格式

在 `optimize.limits` 中定义限制为列表：

```json
"limits": [
  {"metric": "drawdown_worst_btc", "penalize_if": ">", "value": 0.3},
  {"metric": "loss_profit_ratio", "penalize_if": "outside_range", "range": [0.05, 0.7]},
  {"metric": "adg_btc", "penalize_if": "<", "value": 0.0005, "stat": "mean"},
  {"metric": "hard_stop_time_in_red_pct", "penalize_if": ">", "value": 0.02},
  {"metric": "backtest_completion_ratio", "penalize_if": "<", "value": 1.0}
]
```

要故意退出默认限制，保留指标名称但禁用它：

```json
{"metric": "backtest_completion_ratio", "enabled": false}
```

对于 CLI 覆盖，你可以用 JSON/HJSON 载荷替换完整列表：

```
passivbot optimize --limits '[{"metric":"drawdown_worst","penalize_if":">","value":0.35}]'
```

对于可重复的一次性条目，使用 `--limit`。`--limit` 中的符号标量运算符写为保持条件，匹配 `pareto_store.py` 过滤：

```bash
passivbot optimize \
  --clear-limits \
  --limit 'drawdown_worst <= 0.35' \
  --limit 'backtest_completion_ratio>=1.0' \
  --limit 'loss_profit_ratio outside_range [0.05,0.7]' \
  --limit 'adg > 0.0008 stat=mean'
```

CLI 替换规则：

- `--limits` 替换该运行的 `config.optimize.limits`。
- `--limit` 追加一个解析的限制条目，可重复。
- `--limit` 字符串表达式对标量运算符（`>`、`>=`、`<`、`<=`、`==`）使用保持条件语义。显式 JSON/HJSON 限制对象仍使用直接的 `penalize_if` 语义。
- `--clear-limits` 在应用任何 `--limits` 或 `--limit` 条目之前从空限制列表开始。

## 配置内部

Passivbot 在标准化配置旁边存储一些元数据键：

- `_raw` 保留格式化/标准化之前磁盘上的确切用户输入。它用于检查和差异比较——调用者应将其视为只读。
- `_coins_sources` 记录批准/忽略的币种列表来源（内联字符串、外部文件、CLI 覆盖）。未来的覆盖会更新标准化列表及其 `_coins_sources` 条目，以便实盘重新加载尊重最新意图。
- `_transform_log` 捕获高级配置变更（加载、格式化、CLI 覆盖等）的时间顺序记录。每个条目存储 `step`、可选的 `details` 和时间戳，使审计运行时配置如何偏离 `_raw` 更容易。

未来版本中可能会出现额外的保留键；所有以下划线开头的键都被持久化辅助工具忽略，以保持用户配置整洁。
