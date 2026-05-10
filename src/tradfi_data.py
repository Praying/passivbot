"""
TradFi 数据提供商

从外部 API（Finnhub、Alpha Vantage）获取传统金融 OHLCV 数据。
当原生永续合约数据不可用时，用于股票永续合约的历史回测。

符号映射：
- xyz:TSLA (Hyperliquid HIP-3) -> TSLA (TradFi)
- xyz:NVDA (Hyperliquid HIP-3) -> NVDA (TradFi)

注意：TradFi 数据代表实际股票价格，不包含：
- 永续合约资金费率
- 市场关闭期间的预言机驱动定价
- 周末/盘后交易

仅将此数据用于 HIP-3 之前的历史回测。
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import numpy as np

# 与 CandlestickManager 匹配的 OHLCV dtype
CANDLE_DTYPE = np.dtype(
    [
        ("ts", "int64"),  # UTC 毫秒
        ("o", "float32"),  # 开盘价
        ("h", "float32"),  # 最高价
        ("l", "float32"),  # 最低价
        ("c", "float32"),  # 收盘价
        ("bv", "float32"),  # 基础成交量
    ]
)

ONE_MIN_MS = 60_000
ONE_HOUR_MS = 3_600_000
ONE_DAY_MS = 86_400_000

logger = logging.getLogger(__name__)


def hip3_to_tradfi_symbol(hip3_symbol: str) -> str:
    """将 HIP-3 符号转换为 TradFi 代号。

    Args:
        hip3_symbol: HIP-3 符号，如 "xyz:TSLA"、"XYZ-TSLA/USDC:USDC" 等。

    Returns:
        TradFi 代号，如 "TSLA"
    """
    # 从 CCXT 格式符号中提取基础部分
    if "/" in hip3_symbol:
        base = hip3_symbol.split("/")[0]
    else:
        base = hip3_symbol

    # 处理各种 HIP-3 前缀：
    # - xyz:TSLA（小写前缀加冒号）
    # - XYZ-TSLA（CCXT 格式加连字符）
    # - XYZ:TSLA（大写加冒号）
    if base.startswith("xyz:"):
        return base[4:]
    if base.startswith("XYZ-"):
        return base[4:]
    if base.startswith("XYZ:"):
        return base[4:]

    return base


def tradfi_to_hip3_symbol(tradfi_symbol: str, quote: str = "USDC") -> str:
    """将 TradFi 代号转换为 HIP-3 符号。

    Args:
        tradfi_symbol: TradFi 代号，如 "TSLA"
        quote: 报价货币（默认：USDC）

    Returns:
        HIP-3 符号，如 "xyz:TSLA/USDC:USDC"
    """
    return f"xyz:{tradfi_symbol}/{quote}:{quote}"


@dataclass
class TradFiCandle:
    """来自 TradFi 来源的单根 OHLCV K 线。"""

    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class TradFiProvider(ABC):
    """TradFi 数据提供商的抽象基类。"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=60, connect=15)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session is not None:
            await self._session.close()
            self._session = None

    @abstractmethod
    async def fetch_1m_candles(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[TradFiCandle]:
        """获取指定符号的 1 分钟 K 线。

        Args:
            symbol: TradFi 代号（如 "TSLA"）
            start_ts: 开始时间戳（毫秒）
            end_ts: 结束时间戳（毫秒）

        Returns:
            TradFiCandle 对象列表
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """用于日志记录的提供商名称。"""
        pass

    @property
    @abstractmethod
    def rate_limit_delay(self) -> float:
        """API 调用之间的最小延迟（秒）。"""
        pass


class FinnhubProvider(TradFiProvider):
    """Finnhub API TradFi 数据提供商。

    免费层：60 次 API 调用/分钟
    文档：https://finnhub.io/docs/api/stock-candles
    """

    BASE_URL = "https://finnhub.io/api/v1"

    @property
    def name(self) -> str:
        return "finnhub"

    @property
    def rate_limit_delay(self) -> float:
        return 1.1  # 约 54 次调用/分钟，留有安全余量

    async def fetch_1m_candles(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[TradFiCandle]:
        if not self.api_key:
            raise ValueError("Finnhub API key required")
        if self._session is None:
            raise RuntimeError("Session not initialized. Use 'async with' context.")

        candles = []
        # Finnhub 使用秒而非毫秒
        from_ts = start_ts // 1000
        to_ts = end_ts // 1000

        url = f"{self.BASE_URL}/stock/candle"
        params = {
            "symbol": symbol,
            "resolution": "1",  # 1 分钟
            "from": from_ts,
            "to": to_ts,
            "token": self.api_key,
        }

        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 429:
                    logger.warning("Finnhub rate limit hit, backing off")
                    await asyncio.sleep(60)
                    return []
                resp.raise_for_status()
                data = await resp.json()

            if data.get("s") != "ok":
                logger.debug("Finnhub no data for %s: %s", symbol, data.get("s"))
                return []

            timestamps = data.get("t", [])
            opens = data.get("o", [])
            highs = data.get("h", [])
            lows = data.get("l", [])
            closes = data.get("c", [])
            volumes = data.get("v", [])

            for i in range(len(timestamps)):
                candles.append(
                    TradFiCandle(
                        timestamp_ms=timestamps[i] * 1000,
                        open=opens[i],
                        high=highs[i],
                        low=lows[i],
                        close=closes[i],
                        volume=volumes[i],
                    )
                )

            logger.debug(
                "Finnhub fetched %d candles for %s (%s - %s)",
                len(candles),
                symbol,
                datetime.fromtimestamp(from_ts, tz=UTC).isoformat(),
                datetime.fromtimestamp(to_ts, tz=UTC).isoformat(),
            )

        except aiohttp.ClientError as e:
            logger.warning("Finnhub API error for %s: %s", symbol, e)

        return candles


class AlphaVantageProvider(TradFiProvider):
    """Alpha Vantage API TradFi 数据提供商。

    免费层：25 次 API 调用/天（非常有限）
    文档：https://www.alphavantage.co/documentation/
    """

    BASE_URL = "https://www.alphavantage.co/query"

    @property
    def name(self) -> str:
        return "alphavantage"

    @property
    def rate_limit_delay(self) -> float:
        return 12.0  # 对免费层非常保守

    async def fetch_1m_candles(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[TradFiCandle]:
        if not self.api_key:
            raise ValueError("Alpha Vantage API key required")
        if self._session is None:
            raise RuntimeError("Session not initialized. Use 'async with' context.")

        candles = []
        # Alpha Vantage 按月返回数据，先请求当前月份
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "1min",
            "outputsize": "full",
            "apikey": self.api_key,
        }

        try:
            async with self._session.get(self.BASE_URL, params=params) as resp:
                if resp.status == 429:
                    logger.warning("Alpha Vantage rate limit hit")
                    return []
                resp.raise_for_status()
                data = await resp.json()

            # 检查速率限制消息
            if "Note" in data or "Information" in data:
                logger.warning(
                    "Alpha Vantage rate limit: %s",
                    data.get("Note", data.get("Information")),
                )
                return []

            time_series = data.get("Time Series (1min)", {})
            if not time_series:
                logger.debug("Alpha Vantage no data for %s", symbol)
                return []

            for timestamp_str, values in time_series.items():
                # 解析时间戳（格式："2025-01-15 16:00:00"）
                dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                # Alpha Vantage 返回美国东部时间，转换为 UTC
                # 这是简化处理 - 正确的时区处理需要 pytz
                ts_ms = int(dt.timestamp() * 1000)

                if start_ts <= ts_ms <= end_ts:
                    candles.append(
                        TradFiCandle(
                            timestamp_ms=ts_ms,
                            open=float(values["1. open"]),
                            high=float(values["2. high"]),
                            low=float(values["3. low"]),
                            close=float(values["4. close"]),
                            volume=float(values["5. volume"]),
                        )
                    )

            logger.debug("Alpha Vantage fetched %d candles for %s", len(candles), symbol)

        except aiohttp.ClientError as e:
            logger.warning("Alpha Vantage API error for %s: %s", symbol, e)

        return sorted(candles, key=lambda c: c.timestamp_ms)


class PolygonProvider(TradFiProvider):
    """Polygon.io (Massive) API TradFi 数据提供商。

    免费层：2 年 1 分钟历史数据
    速率限制：5 次 API 调用/分钟
    每次查询最多 50,000 条结果（约 35 天的 1 分钟 K 线）

    文档：https://polygon.readthedocs.io/en/latest/Stocks.html
    """

    BASE_URL = "https://api.polygon.io/v2/aggs/ticker"

    @property
    def name(self) -> str:
        return "polygon"

    @property
    def rate_limit_delay(self) -> float:
        return 12.5  # 5 次调用/分钟 = 每 12 秒 1 次，加缓冲

    async def fetch_1m_candles(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[TradFiCandle]:
        if not self.api_key:
            raise ValueError("Polygon API key required")
        if self._session is None:
            raise RuntimeError("Session not initialized. Use 'async with' context.")

        candles = []

        # Polygon API 使用毫秒时间戳
        # 构建聚合端点的 URL
        url = f"{self.BASE_URL}/{symbol}/range/1/minute/{start_ts}/{end_ts}"
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,  # 最大允许值
            "apiKey": self.api_key,
        }

        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 429:
                    logger.warning("Polygon rate limit hit, backing off")
                    await asyncio.sleep(60)
                    return []
                if resp.status == 403:
                    logger.warning("Polygon API key invalid or unauthorized")
                    return []
                resp.raise_for_status()
                data = await resp.json()

            if data.get("status") != "OK":
                logger.debug("Polygon no data for %s: status=%s", symbol, data.get("status"))
                return []

            results = data.get("results", [])
            if not results:
                logger.debug("Polygon no results for %s in range", symbol)
                return []

            for bar in results:
                # Polygon 返回：t（时间戳毫秒）、o、h、l、c、v
                candles.append(
                    TradFiCandle(
                        timestamp_ms=bar["t"],
                        open=bar["o"],
                        high=bar["h"],
                        low=bar["l"],
                        close=bar["c"],
                        volume=bar.get("v", 0),
                    )
                )

            logger.debug(
                "Polygon fetched %d candles for %s (%s - %s)",
                len(candles),
                symbol,
                datetime.fromtimestamp(start_ts / 1000, tz=UTC).isoformat(),
                datetime.fromtimestamp(end_ts / 1000, tz=UTC).isoformat(),
            )

        except aiohttp.ClientError as e:
            logger.warning("Polygon API error for %s: %s", symbol, e)

        return candles


class AlpacaProvider(TradFiProvider):
    """Alpaca Markets API TradFi 数据提供商。

    免费 - 无需付费，只需免费 API 密钥！
    - 5 年以上 1 分钟历史数据
    - 免费层使用 IEX 数据源
    - 15 分钟延迟（对回测无影响）
    - 速率限制：200 次请求/分钟

    文档：https://docs.alpaca.markets/docs/about-market-data-api
    注册：https://alpaca.markets/
    """

    BASE_URL = "https://data.alpaca.markets/v2/stocks"

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        super().__init__(api_key)
        self.api_secret = api_secret

    @property
    def name(self) -> str:
        return "alpaca"

    @property
    def rate_limit_delay(self) -> float:
        return 0.5  # 200 次请求/分钟 = 3.3 次请求/秒，保守处理

    async def fetch_1m_candles(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[TradFiCandle]:
        if not self.api_key or not self.api_secret:
            raise ValueError("Alpaca API key and secret required")
        if self._session is None:
            raise RuntimeError("Session not initialized. Use 'async with' context.")

        candles = []

        # Alpaca 使用 RFC3339 时间戳
        start_dt = datetime.fromtimestamp(start_ts / 1000, tz=UTC)
        end_dt = datetime.fromtimestamp(end_ts / 1000, tz=UTC)

        url = f"{self.BASE_URL}/{symbol}/bars"
        params = {
            "timeframe": "1Min",
            "start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "limit": 10000,  # 每次请求最大数量
            "adjustment": "split",
            "feed": "iex",  # 免费层使用 IEX
        }
        headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        }

        try:
            next_page_token = None
            while True:
                if next_page_token:
                    params["page_token"] = next_page_token

                async with self._session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 429:
                        logger.warning("Alpaca rate limit hit, backing off")
                        await asyncio.sleep(60)
                        return candles
                    if resp.status == 403:
                        logger.warning("Alpaca API key invalid or unauthorized")
                        return []
                    if resp.status == 422:
                        # 不可处理的实体 - 通常意味着该范围没有数据
                        logger.debug("Alpaca no data for %s in range", symbol)
                        return []
                    resp.raise_for_status()
                    data = await resp.json()

                bars = data.get("bars", [])
                if not bars:
                    break

                for bar in bars:
                    # 解析 ISO 时间戳为毫秒
                    ts_str = bar["t"]
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    ts_ms = int(dt.timestamp() * 1000)

                    candles.append(
                        TradFiCandle(
                            timestamp_ms=ts_ms,
                            open=bar["o"],
                            high=bar["h"],
                            low=bar["l"],
                            close=bar["c"],
                            volume=bar.get("v", 0),
                        )
                    )

                # 检查分页
                next_page_token = data.get("next_page_token")
                if not next_page_token:
                    break

            logger.debug(
                "Alpaca fetched %d candles for %s (%s - %s)",
                len(candles),
                symbol,
                start_dt.isoformat(),
                end_dt.isoformat(),
            )

        except aiohttp.ClientError as e:
            logger.warning("Alpaca API error for %s: %s", symbol, e)

        return candles


class YFinanceProvider(TradFiProvider):
    """Yahoo Finance TradFi 数据提供商。

    免费 - 无需 API 密钥！
    限制：
    - 1 分钟数据：仅最近 7 天
    - 5 分钟数据：最近 60 天
    - 1 小时数据：最近 730 天（2 年）
    - 1 天数据：完整历史

    文档：https://github.com/ranaroussi/yfinance
    """

    @property
    def name(self) -> str:
        return "yfinance"

    @property
    def rate_limit_delay(self) -> float:
        return 0.5  # 对 Yahoo 友好

    async def fetch_1m_candles(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int,
    ) -> List[TradFiCandle]:
        """从 Yahoo Finance 获取 1 分钟 K 线。

        注意：yfinance 仅提供最近 7 天的 1 分钟数据。
        """
        try:
            import yfinance as yf
        except ImportError:
            logger.warning("yfinance not installed. Install with: pip install yfinance")
            return []

        candles = []

        try:
            # yfinance 使用同步 API，在执行器中运行
            import asyncio

            loop = asyncio.get_event_loop()

            def fetch_sync():
                ticker = yf.Ticker(symbol)
                # 将时间戳转换为日期时间
                start_dt = datetime.fromtimestamp(start_ts / 1000, tz=UTC)
                end_dt = datetime.fromtimestamp(end_ts / 1000, tz=UTC)

                # yfinance 1 分钟数据限制为最近 7 天
                seven_days_ago = datetime.now(UTC) - timedelta(days=7)
                if start_dt < seven_days_ago:
                    start_dt = seven_days_ago
                    logger.debug(
                        "yfinance 1m data limited to 7 days, adjusted start to %s",
                        start_dt,
                    )

                # 获取数据
                df = ticker.history(
                    interval="1m",
                    start=start_dt,
                    end=end_dt,
                )
                return df

            df = await loop.run_in_executor(None, fetch_sync)

            if df is None or df.empty:
                logger.debug("yfinance no data for %s", symbol)
                return []

            # 将 DataFrame 转换为 TradFiCandle 列表
            for idx, row in df.iterrows():
                # idx 是带时区信息的 datetime
                ts_ms = int(idx.timestamp() * 1000)

                if start_ts <= ts_ms <= end_ts:
                    candles.append(
                        TradFiCandle(
                            timestamp_ms=ts_ms,
                            open=float(row["Open"]),
                            high=float(row["High"]),
                            low=float(row["Low"]),
                            close=float(row["Close"]),
                            volume=float(row["Volume"]),
                        )
                    )

            logger.debug("yfinance fetched %d candles for %s", len(candles), symbol)

        except Exception as e:
            logger.warning("yfinance API error for %s: %s", symbol, e)

        return sorted(candles, key=lambda c: c.timestamp_ms)


def get_provider(
    name: str, api_key: Optional[str] = None, api_secret: Optional[str] = None
) -> TradFiProvider:
    """工厂函数：获取 TradFi 数据提供商。

    Args:
        name: 提供商名称（"alpaca"、"polygon"、"yfinance"、"finnhub"、"alphavantage"）
        api_key: 提供商的 API 密钥（yfinance 不需要）
        api_secret: Alpaca 的 API 密钥（仅 alpaca 提供商需要）

    Returns:
        TradFiProvider 实例
    """
    if name == "alpaca":
        return AlpacaProvider(api_key=api_key, api_secret=api_secret)

    providers = {
        "polygon": PolygonProvider,
        "yfinance": YFinanceProvider,
        "finnhub": FinnhubProvider,
        "alphavantage": AlphaVantageProvider,
    }

    if name not in providers:
        raise ValueError(f"Unknown provider: {name}. Available: {list(providers.keys())}")

    return providers[name](api_key=api_key)


def candles_to_array(candles: List[TradFiCandle]) -> np.ndarray:
    """将 TradFiCandle 列表转换为 numpy 结构化数组。

    Args:
        candles: TradFiCandle 对象列表

    Returns:
        使用 CANDLE_DTYPE 的结构化数组
    """
    if not candles:
        return np.empty((0,), dtype=CANDLE_DTYPE)

    arr = np.empty((len(candles),), dtype=CANDLE_DTYPE)
    for i, c in enumerate(candles):
        arr[i] = (c.timestamp_ms, c.open, c.high, c.low, c.close, c.volume)

    return arr


class TradFiDataFetcher:
    """带缓存和速率限制的高级 TradFi 数据获取器。"""

    def __init__(
        self,
        provider: TradFiProvider,
        cache_dir: Optional[str] = None,
    ):
        self.provider = provider
        self.cache_dir = cache_dir
        self._last_request_time = 0.0

    async def __aenter__(self):
        await self.provider.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.provider.__aexit__(exc_type, exc_val, exc_tb)

    async def _rate_limit_wait(self):
        """等待以遵守速率限制。"""
        elapsed = time.monotonic() - self._last_request_time
        delay = self.provider.rate_limit_delay
        if elapsed < delay:
            await asyncio.sleep(delay - elapsed)
        self._last_request_time = time.monotonic()

    async def fetch_day(
        self,
        hip3_symbol: str,
        day_key: str,
    ) -> np.ndarray:
        """获取 HIP-3 符号的完整一天 1 分钟 K 线。

        Args:
            hip3_symbol: HIP-3 符号（如 "xyz:TSLA/USDC:USDC"）
            day_key: 日期字符串（YYYY-MM-DD）

        Returns:
            使用 CANDLE_DTYPE 的结构化数组（可能仅在交易时段有数据）
        """
        tradfi_symbol = hip3_to_tradfi_symbol(hip3_symbol)

        # 计算日期边界（UTC）
        day_start = datetime.strptime(day_key, "%Y-%m-%d").replace(tzinfo=UTC)
        start_ts = int(day_start.timestamp() * 1000)
        end_ts = start_ts + ONE_DAY_MS - ONE_MIN_MS

        await self._rate_limit_wait()

        candles = await self.provider.fetch_1m_candles(tradfi_symbol, start_ts, end_ts)
        if not candles:
            logger.info(
                "No TradFi data for %s on %s (possibly non-trading day)",
                tradfi_symbol,
                day_key,
            )
            return np.empty((0,), dtype=CANDLE_DTYPE)

        arr = candles_to_array(candles)
        logger.info(
            "Fetched %d TradFi candles for %s on %s",
            len(arr),
            tradfi_symbol,
            day_key,
        )
        return arr

    async def fetch_range(
        self,
        hip3_symbol: str,
        start_date: str,
        end_date: str,
    ) -> Dict[str, np.ndarray]:
        """获取日期范围内的 K 线。

        Args:
            hip3_symbol: HIP-3 符号
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）

        Returns:
            字典，日期键映射到 K 线数组
        """
        results = {}
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        current = start
        while current <= end:
            day_key = current.strftime("%Y-%m-%d")
            arr = await self.fetch_day(hip3_symbol, day_key)
            if arr.size > 0:
                results[day_key] = arr
            current += timedelta(days=1)

        return results


# 已知在 Hyperliquid/TradeXYZ 上可作为 HIP-3 永续合约的股票代号
# 可使用 xyz: 前缀或不带前缀
KNOWN_STOCK_TICKERS = {
    "TSLA",
    "NVDA",
    "AAPL",
    "MSFT",
    "META",
    "AMZN",
    "GOOGL",
    "PLTR",
    "COIN",
    "AMD",
    "NFLX",
    "HOOD",
    "CRCL",
    "SBET",
    "XYZ100",  # 类纳斯达克指数
}

# 可用的带 xyz: 前缀格式的股票永续合约
AVAILABLE_STOCK_PERPS = [f"xyz:{ticker}" for ticker in KNOWN_STOCK_TICKERS]


def is_stock_ticker(coin: str) -> bool:
    """检查币种名称是否为已知股票代号。

    允许用户直接将 "TSLA" 添加到 approved_coins，
    无需了解 xyz: 前缀。

    Args:
        coin: 币种名称（如 "TSLA"、"xyz:TSLA"、"XYZ-TSLA"、"BTC"）

    Returns:
        如果是已知股票代号则返回 True
    """
    # 移除 HIP-3 前缀
    if coin.startswith("xyz:"):
        coin = coin[4:]
    elif coin.startswith("XYZ-"):
        coin = coin[4:]
    elif coin.startswith("XYZ:"):
        coin = coin[4:]

    # 移除报价后缀（例如来自 CCXT 符号）
    if "/" in coin:
        coin = coin.split("/")[0]

    return coin.upper() in KNOWN_STOCK_TICKERS


def is_stock_perp_symbol(symbol: str) -> bool:
    """检查符号是否为股票永续合约。

    通过以下方式检测：
    1. xyz: 或 XYZ- 前缀（HIP-3 格式）
    2. 已知股票代号名称（TSLA、NVDA 等）

    Args:
        symbol: CCXT 风格符号或币种名称

    Returns:
        如果是股票永续合约符号则返回 True
    """
    # 检查 HIP-3 前缀
    if symbol.startswith("xyz:") or symbol.startswith("XYZ-") or symbol.startswith("XYZ:"):
        return True
    base = symbol.split("/")[0] if "/" in symbol else symbol
    if base.startswith("xyz:") or base.startswith("XYZ-") or base.startswith("XYZ:"):
        return True

    # 检查是否为已知股票代号
    return is_stock_ticker(symbol)
