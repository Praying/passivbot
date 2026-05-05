# 股票永续合约交易指南

本指南介绍如何使用 Passivbot 在 Hyperliquid 上交易股票永续合约（equity perps）。

## 当前支持状态

Passivbot 目前**仅支持** Hyperliquid HIP-3 市场的全仓实盘交易。

- 支持全仓的 HIP-3 市场仍可以全仓模式交易。
- 交易所元数据标记为仅逐仓的 HIP-3 市场目前会被跳过，不进行新入场。
- 如果 bot 在启动时检测到现有的逐仓 HIP-3 仓位或未结订单，会直接失败而不是尝试管理该状态。

逐仓保证金代码路径仍保留在仓库中以备未来可能的工作，但逐仓 HIP-3 实盘交易目前不支持。

## 概述

股票永续合约是跟踪传统股票价格（TSLA、NVDA、AAPL 等）的永续期货合约。在 Hyperliquid 上，这些通过 **HIP-3**（Hyperliquid Improvement Proposal 3）部署，它支持无许可的永续合约市场创建。

Hyperliquid 上的主要股票永续合约提供商是 **TradeXYZ**（trade.xyz），它部署带有 `xyz:` 前缀的市场（如 `xyz:TSLA`、`xyz:NVDA`）。

### 关键特性

| 方面 | 股票永续合约 | 加密永续合约 |
|------|-------------|--------------|
| 符号格式 | `xyz:TSLA/USDC:USDC` | `BTC/USDC:USDC` |
| 保证金模式 | 由交易所元数据决定；Passivbot 目前仅支持全仓实盘交易 | 全仓或逐仓 |
| 最大杠杆 | 10x | 最高 50x |
| 交易时间 | 24/7 | 24/7 |
| 手续费 | 标准 Hyperliquid 手续费的 2 倍 | 标准手续费 |
| 抵押品 | USDC | USDC |

### 可用的股票永续合约

截至 2026 年初，TradeXYZ 提供以下永续合约：
- **科技巨头**：TSLA、NVDA、AAPL、MSFT、META、AMZN、GOOGL、NFLX、AMD
- **金融科技/加密相关**：COIN、HOOD、PLTR、MSTR
- **大宗商品**：GOLD、SILVER、COPPER、NATGAS、URANIUM
- **货币**：EUR、JPY
- **指数**：XYZ100（类似纳斯达克的指数）

其他 HIP-3 构建者（FLX、KM、CASH、VNTL、HYNA、ABCD）也提供各种永续合约。支持全仓的构建者市场可能仍然有效，但仅逐仓的 HIP-3 交易目前被禁用。

## 理解余额和保证金

### 逐仓保证金如何工作

HIP-3 股票永续合约可能是**仅逐仓**或**支持全仓**的，取决于交易所元数据。许多仍使用逐仓保证金，这与大多数加密永续合约使用的全仓保证金有根本不同。Passivbot 目前避免逐仓实盘路径，在可能时将 HIP-3 视为仅全仓。

| 全仓保证金（HL 加密永续合约） | 逐仓保证金（HIP-3/XYZ 永续合约） |
|-------------------------------|-----------------------------------|
| 整个余额为所有仓位担保 | 每个仓位有专用保证金 |
| 一次清算可能级联到其他仓位 | 清算限制在每个仓位内 |
| 资本效率更高 | 更安全但需要更多前期保证金 |
| BTC、ETH 等的默认模式 | Passivbot 实盘交易暂不支持 |

### 余额显示差异

根据使用的界面，你会看到不同的余额分解：

**Hyperliquid Web UI（app.hyperliquid.xyz）**：
- 显示你的**总账户价值**（例如 105 USDC）
- 这包括所有可用余额 + 仓位中锁定的保证金

**TradeXYZ Web UI（trade.xyz）**：
- 显示**按构建者分解**：
  - `USDC (HL perps)`：可用于 HL 原生永续合约或提取的可用余额
  - `USDC (XYZ perps)`：当前锁定在 XYZ 股票永续仓位中的保证金

**示例**：
```
Hyperliquid UI:     105 USDC 总计
TradeXYZ UI:        11.91 USDC (HL perps) + 93.27 USDC (XYZ perps) = 105.18 USDC
```

锁定在"XYZ perps"中的约 $93 是你开仓 TSLA 仓位的保证金。如果这些仓位被清算，只有那 $93 有风险——你剩余的 $12 是安全的。

### 保证金计算

当你在逐仓保证金市场开仓时：

```
margin_required = position_notional / leverage
                = (quantity × price) / leverage
```

例如，以 2x 杠杆在 $423 开 0.24 TSLA：
```
margin = (0.24 × $423) / 2 = $50.76
```

此保证金在你平仓之前**锁定**给该仓位。你的可用余额相应减少。

### Passivbot 余额显示

Passivbot 显示来自 Hyperliquid API 的**总账户价值**，包括：
- 可用余额
- 所有锁定的保证金
- 未实现 PnL

你在日志中看到的余额变化反映仓位开/平时保证金的分配/释放：
```
[balance] 105.21 -> 84.27   # 新仓位锁定的保证金
[balance] 84.27 -> 67.35    # 第二次入场锁定更多保证金
```

## 要求

### 1. Hyperliquid 账户

你需要一个存入 USDC 的 Hyperliquid 账户。无需 KYC——你的钱包就是你的身份。

### 2. 一次性 TradeXYZ 注册

**重要**：在通过 API 交易 XYZ 股票永续合约之前，你必须在 TradeXYZ 平台上完成一次性钱包注册：

1. 前往 [trade.xyz](https://trade.xyz)
2. 点击"Connect Wallet"并连接你的 Hyperliquid 钱包
3. 签署验证交易以证明钱包所有权
4. 接受服务条款和隐私政策
5. 提示时点击 **"Enable Trading"** 并签署确认

此注册将你的钱包链接到 TradeXYZ 构建者，启用对其 HIP-3 市场的 API 访问。没有此步骤，即使你有足够余额，订单也会因"Insufficient margin"错误而失败。

### 3. 地区限制

TradeXYZ 禁止以下地区访问：
- 美国
- OFAC 制裁国家

确保你不是从受限地区连接。

## 配置

### 符号选择

你可以使用以下任何格式在 `approved_coins` 中指定股票永续合约：

```json
{
  "live": {
    "approved_coins": ["TSLA"]
  }
}
```

或显式带前缀：

```json
{
  "live": {
    "approved_coins": ["xyz:TSLA", "xyz:NVDA"]
  }
}
```

Passivbot 自动将 `TSLA` 映射到 Hyperliquid 上的 `XYZ-TSLA/USDC:USDC`。

### 混合加密和股票永续合约

你可以在同一个 bot 中运行加密永续合约和受支持的股票永续合约：

```json
{
  "live": {
    "approved_coins": ["BTC", "ETH", "TSLA", "NVDA"]
  }
}
```

Passivbot 自动为每个符号设置正确的保证金模式：
- **加密永续合约**（BTC、ETH、SOL 等）→ 全仓保证金
- **股票永续合约**（TSLA、NVDA、AAPL 等）→ 仅当市场支持全仓时使用全仓；仅逐仓的市场会被跳过

```
BTC/USDC:USDC: margin=ok (cross)
XYZ-XYZ100/USDC:USDC: margin=ok (cross)
```

**混合模式下余额如何工作：**

| 组件 | 担保什么 |
|------|----------|
| 可用余额 | 由所有全仓保证金仓位共享（BTC、ETH 等）|
| 锁定保证金（每股票）| 仅该特定股票永续仓位 |

**风险隔离**：这是交易所层面逐仓保证金的工作方式。Passivbot 目前不支持实盘 HIP-3 逐仓交易，但此区分对于理解交易所有用。

**实际考虑**：逐仓保证金仓位锁定资本，减少可用于全仓保证金仓位的资金。相应地规划你的 `n_positions` 和 `total_wallet_exposure_limit`。

### 自动检测

Passivbot 通过以下方式自动检测股票永续合约：
1. `xyz:` 符号前缀（或 CCXT 格式中的 `XYZ-`）
2. `onlyIsolated: true` 市场标志
3. 属于已知股票代码列表（TSLA、NVDA、AAPL 等）

### 杠杆和保证金

股票永续合约并非都具有相同的保证金能力。Passivbot 目前：

1. 从交易所元数据检测 HIP-3 市场是仅逐仓还是支持全仓
2. 在支持全仓的 HIP-3 市场上使用全仓模式
3. 忽略仅逐仓的 HIP-3 市场进行新入场
4. 如果在启动时检测到现有的逐仓 HIP-3 实盘状态，直接失败
5. 保留逐仓保证金代码以备未来可能的支持工作

**逐仓保证金的杠杆计算：**

以下剩余的逐仓保证金说明作为交易所/背景参考保留。它们并不意味着 Passivbot 实盘模式目前支持逐仓 HIP-3 交易。

对于逐仓保证金，你的保证金要求是：`margin = exposure / leverage`

为确保你永远不会超过余额，Passivbot 使用：
```
min_leverage = ceil(max(long_TWEL, short_TWEL))
```

例如，TWEL = 1.25：
- 最小杠杆 = ceil(1.25) = 2x
- $100k 余额的最大敞口 = $125k
- 所需保证金 = $125k / 2 = $62.5k（在余额范围内）

### 最小订单大小

Hyperliquid 上的股票永续合约有 **$10 最小订单价值**。对于小额余额，这限制了你可以放置的网格入场数量。

对于 $100 余额在 $400 交易 TSLA：
- 最小数量 = $10 / $400 = 0.025 TSLA
- TWEL 1.0 且 2x 杠杆：最大敞口 = $200，最大数量 = 0.5 TSLA
- 实际网格深度：约 5-8 次入场后达到最小值

如果你想用较小余额交易并接受某些网格级别可能被跳过，考虑设置 `filter_by_min_effective_cost: false`。

### 示例配置

股票永续合约的最小测试配置：

```json
{
  "live": {
    "user": "hyperliquid_01",
    "approved_coins": ["TSLA"],
    "leverage": 2,
    "filter_by_min_effective_cost": false,
    "hedge_mode": false,
    "minimum_coin_age_days": 0
  },
  "bot": {
    "long": {
      "n_positions": 2,
      "total_wallet_exposure_limit": 1.0,
      "entry_initial_qty_pct": 0.4
    },
    "short": {
      "n_positions": 0,
      "total_wallet_exposure_limit": 0
    }
  },
  "logging": {
    "level": 2
  }
}
```

关键设置说明：
- `leverage: 2` - 安全的起点，满足逐仓保证金要求
- `filter_by_min_effective_cost: false` - 即使余额低也允许交易
- `minimum_coin_age_days: 0` - 股票永续合约是新的，不按年龄过滤
- `entry_initial_qty_pct: 0.4` - 较大的初始入场（小余额时网格级别较少）

## Oracle 定价行为

股票永续合约使用来自 RedStone 的 HyperStone oracle 的 oracle 定价。在市场开盘期间，价格跟踪实时股票价格。在市场收盘期间（周末、节假日）：

- Oracle 对收盘价保持"粘性"
- 价格边界基于最终开盘价设定
- 大额交易可以在这些边界内移动价格

**风险警告**：周末交易具有额外风险。2025 年 12 月的一起事件中，一条鲸鱼在周日引发了 3.5% 的抛售，导致清算。考虑在市场收盘期间减小仓位大小或暂停 bot。

## 回测和实盘交易的数据源

Passivbot 对股票永续合约使用多个数据源，当主要来源不可用时自动回退到替代方案：

### 1. Hyperliquid API（实盘交易的主要来源）
- **覆盖范围**：最近约 3.5 天（5000 根 1m K 线）
- **格式**：带 oracle 定价的原生永续合约数据
- **用途**：实盘交易、近期回测
- **无需设置** - 自动工作

### 2. Yahoo Finance（免费，历史数据默认）
- **覆盖范围**：最近 7 天的 1m 数据
- **成本**：免费，无需 API key
- **设置**：随 yfinance 包自动安装
- **限制**：仅市场开盘时间数据，无周末数据

### 3. Finnhub / Alpha Vantage（可选，用于扩展历史）
- **覆盖范围**：因提供商而异
- **成本**：需要 API key（有免费层级）
- **设置**：添加到 api-keys.json

```json
{
  "tradfi": {
    "provider": "finnhub",
    "api_key": "your_api_key"
  }
}
```

### 数据源优先级

CandlestickManager 自动选择最佳来源：
1. 本地缓存（如果可用）
2. Hyperliquid API（最近 3.5 天）
3. Yahoo Finance（最近 7 天，免费）
4. 配置的 TradFi 提供商（更早的数据）

### TradFi 数据的重要说明

TradFi 数据代表实际股票价格，**不包括**：
- 永续合约资金费率
- 市场收盘期间的 oracle 驱动定价
- 周末/盘后价格变动

此数据适用于：
- 策略开发和初始回测
- 理解一般价格行为
- EMA 的预热期

要准确回测实际永续合约行为，请在可用时使用原生 Hyperliquid 数据。

## 限制

### 当前限制

1. **仅逐仓保证金** - 全仓保证金支持计划在未来的 HIP-3 升级中
2. **10x 最大杠杆** - 低于加密永续合约
3. **更高的手续费** - 标准 Hyperliquid 手续费的 2 倍
4. **需要构建者注册** - 每个构建者（TradeXYZ、FLX 等）一次性设置
5. **无对冲模式** - 与常规 Hyperliquid 相同
6. **$10 最小订单** - 限制小账户的网格深度

## 故障排除

### "Insufficient margin to place order"

**原因**：钱包未在 TradeXYZ 注册

**解决方案**：在 [trade.xyz](https://trade.xyz) 完成一次性注册（参见要求部分）

### 加载市场时 "Too many DEXes found"

**原因**：CCXT 需要 HIP-3 DEX 规范

**解决方案**：Passivbot 自动处理此问题。如果直接使用 CCXT：
```python
exchange.options["fetchMarkets"] = {
    "types": ["swap", "hip3"],
    "hip3": {"dex": ["xyz"]}
}
```

### "No long symbols are approved due to min effective cost too high"

**原因**：余额太低，不满足 $10 最小订单大小

**解决方案**：二选一：
1. 增加账户余额
2. 在配置中设置 `filter_by_min_effective_cost: false`
3. 减少 `n_positions` 以集中资本

### 市场收盘期间订单被拒绝

**原因**：Oracle 价格边界或流动性问题

**解决方案**：考虑在延长的市场收盘期间（周末、节假日）暂停交易

### 符号未找到 / "Skipping unsupported markets"

**原因**：符号映射问题或市场未加载

**解决方案**：确保你使用的是有效的代码（TSLA、NVDA 等），并且 Hyperliquid 市场缓存是最新的。删除 `caches/hyperliquid/markets.json` 以强制刷新。

## 实盘测试结果（2026 年 1 月）

成功测试：
- **账户**：约 $105 USDC
- **符号**：TSLA（xyz:TSLA）
- **杠杆**：2x
- **结果**：
  - Bot 正确检测到逐仓保证金要求
  - 保证金模式自动设为逐仓
  - 在约 $423 处执行了多笔成交
  - 仓位跟踪和订单管理正常工作

## 资源

- [TradeXYZ 文档](https://docs.trade.xyz)
- [Hyperliquid HIP-3 文档](https://hyperliquid.gitbook.io/hyperliquid-docs/hyperliquid-improvement-proposals-hips/hip-3-builder-deployed-perpetuals)
- [HyperStone Oracle（RedStone）](https://blog.redstone.finance/2025/11/13/felix-launches-its-first-hyperliquid-hip-3-market-with-tsla-powered-by-hyperstone/)

---

*最后更新：2026 年 1 月*
*状态：已实盘测试并正常工作*
