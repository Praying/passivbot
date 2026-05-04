"""
加载和应用自定义 REST 端点覆盖的工具函数。

这些辅助函数目前有意与代码库的其余部分隔离。
后续集成步骤可以导入此模块，在任何网络调用之前修改 ccxt 交易所
实例或其他 HTTP 客户端。

配置概述（在 ``custom_endpoints.json.example`` 中正式定义）：

{
    "defaults": {
        "disable_ws": false,
        "rest": {
            "rewrite_domains": {
                "fapi.binance.com": "proxy.example.exchange"
            },
            "url_overrides": {
                "fapiPrivate": "https://proxy.example.exchange/fapi/v1"
            },
            "extra_headers": {
                "X-Demo-Header": "example"
            }
        }
    },
    "exchanges": {
        "binanceusdm": {
            "disable_ws": true,
            "rest": {
                "rewrite_domains": {
                    "fapi.binance.com": "proxy.example.exchange"
                },
                "url_overrides": {
                    "fapiPrivate": "https://proxy.example.exchange/fapi/v1",
                    "fapiPrivateV2": "https://proxy.example.exchange/fapi/v2",
                    "fapiPrivateV3": "https://proxy.example.exchange/fapi/v3"
                }
            }
        }
    }
}

此阶段仅处理 REST 覆盖。如果 ``disable_ws`` 为 ``True``，
websocket 辅助函数应决定是否完全跳过初始化。
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# 可选部分的标准后备值
_BASE_EXCHANGE_TEMPLATE = {
    "disable_ws": False,
    "rest": {
        "rewrite_domains": {},
        "url_overrides": {},
        "extra_headers": {},
    },
}

DEFAULT_CONFIG_SEARCH_PATHS: Tuple[str, ...] = (os.path.join("configs", "custom_endpoints.json"),)


class CustomEndpointConfigError(RuntimeError):
    """当自定义端点配置无法解析时抛出。"""


@dataclass(frozen=True)
class ResolvedEndpointOverride:
    """
    表示单个交易所的完全合并覆盖。

    ``rest_domain_rewrites`` 将主机名（或完整基础 URL）映射到替换主机名。
    ``rest_url_overrides`` 将具体的 ccxt URL 键（如 ``fapiPrivate``）
    替换为显式 URL。``rest_extra_headers`` 列出下游 HTTP 客户端
    在通过覆盖路由的所有 REST 请求中应发送的头部。
    """

    exchange_id: str
    rest_domain_rewrites: Dict[str, str] = field(default_factory=dict)
    rest_url_overrides: Dict[str, str] = field(default_factory=dict)
    rest_extra_headers: Dict[str, str] = field(default_factory=dict)
    disable_ws: bool = False

    def is_noop(self) -> bool:
        return (
            not self.disable_ws
            and not self.rest_domain_rewrites
            and not self.rest_url_overrides
            and not self.rest_extra_headers
        )

    def rewrite_url(self, url: str, *, hostname: Optional[str] = None) -> str:
        """
        返回应用了域名级重写的 ``url``。

        任何匹配 ``url`` 开头的已配置替换都会被应用。
        匹配可以是裸主机名（``fapi.binance.com``）或完整基础
        URL（``https://fapi.binance.com``）。
        """
        if not url:
            return url
        resolved_url = url
        if hostname and "{hostname}" in url:
            resolved_url = url.replace("{hostname}", hostname)

        for old, new in self.rest_domain_rewrites.items():
            if not old:
                continue

            candidates = {old}
            if hostname and "{hostname}" in old:
                candidates.add(old.replace("{hostname}", hostname))

            for candidate in candidates:
                if not candidate:
                    continue
                if resolved_url.startswith(candidate):
                    suffix = resolved_url[len(candidate) :]
                    return new.rstrip("/") + suffix
                if "://" not in candidate:
                    needle = "://" + candidate
                    idx = resolved_url.find(needle)
                    if idx != -1:
                        prefix = resolved_url[: idx + 3]
                        suffix = resolved_url[idx + len(needle) :]
                        return prefix + new + suffix
        return resolved_url

    def apply_to_api_urls(
        self, urls: Mapping[str, str], *, hostname: Optional[str] = None
    ) -> Dict[str, str]:
        """
        返回一个新的 ``dict``，其中 REST URL 覆盖已应用到提供的
        ccxt ``urls['api']`` 映射上。
        """
        updated = dict(urls)
        for key, value in self.rest_url_overrides.items():
            updated[key] = value
        for key, value in list(updated.items()):
            updated[key] = self.rewrite_url(value, hostname=hostname)
        return updated


class CustomEndpointConfig:
    """
    自定义端点配置的高级辅助类。

    此类加载原始 JSON 结构并提供可合并的 API，
    以便后续集成步骤可以按交易所解析覆盖。
    """

    def __init__(
        self,
        *,
        source_path: Optional[Path],
        defaults: Mapping[str, object],
        exchanges: Mapping[str, Mapping[str, object]],
    ) -> None:
        self._source_path = source_path
        self._defaults = _ensure_exchange_shape(defaults)
        self._exchanges = {
            key.lower(): _ensure_exchange_shape(value) for key, value in exchanges.items()
        }

    @property
    def source_path(self) -> Optional[Path]:
        return self._source_path

    def available_exchanges(self) -> Iterable[str]:
        return self._exchanges.keys()

    def get_override(self, exchange_id: str) -> Optional[ResolvedEndpointOverride]:
        """
        解析 ``exchange_id`` 的覆盖（不区分大小写）。如果不存在自定义则返回 ``None``。
        """
        if not exchange_id:
            return None
        key = exchange_id.lower()
        merged = _deep_merge_dicts(self._defaults, self._exchanges.get(key))
        resolved = _build_resolved(exchange_id=key, payload=merged)
        return None if resolved.is_noop() else resolved

    def is_empty(self) -> bool:
        return (
            not self.available_exchanges()
            and _build_resolved(exchange_id="defaults", payload=self._defaults).is_noop()
        )


def load_custom_endpoint_config(
    path: Optional[str] = None,
    *,
    search_paths: Iterable[str] = DEFAULT_CONFIG_SEARCH_PATHS,
) -> CustomEndpointConfig:
    """
    从 JSON 加载自定义端点配置。

    如果提供了 ``path``，则优先使用。否则加载器按顺序搜索
    ``search_paths`` 并返回找到的第一个文件。缺失的文件
    会导致空配置而非错误。
    """
    candidate_path: Optional[Path] = None
    if path:
        candidate_path = Path(path).expanduser().resolve()
        if not candidate_path.is_file():
            raise CustomEndpointConfigError(f"custom endpoint config not found: {candidate_path}")
    else:
        for entry in search_paths:
            resolved = Path(entry).expanduser().resolve()
            if resolved.is_file():
                candidate_path = resolved
                break

    if not candidate_path:
        return CustomEndpointConfig(source_path=None, defaults=_BASE_EXCHANGE_TEMPLATE, exchanges={})

    try:
        with candidate_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise CustomEndpointConfigError(
            f"failed to parse custom endpoint config ({candidate_path}): {exc}"
        ) from exc
    except Exception as exc:
        raise CustomEndpointConfigError(
            f"failed to read custom endpoint config ({candidate_path}): {exc}"
        ) from exc

    defaults = data.get("defaults", {})
    exchanges = data.get("exchanges", {})
    if not isinstance(exchanges, Mapping):
        raise CustomEndpointConfigError("'exchanges' section must be an object mapping exchange ids")

    config = CustomEndpointConfig(
        source_path=candidate_path,
        defaults=defaults,
        exchanges=exchanges,
    )

    logger.debug(
        "Loaded custom endpoint config from %s (exchanges: %s)",
        candidate_path,
        ", ".join(sorted(config.available_exchanges())) or "none",
    )
    return config


# ---------------------------------------------------------------------------
# 内部辅助函数
# ---------------------------------------------------------------------------


def _ensure_exchange_shape(data: Optional[Mapping[str, object]]) -> Dict[str, object]:
    if data is None:
        data = {}
    if not isinstance(data, Mapping):
        raise CustomEndpointConfigError("exchange override must be an object")
    payload = _deep_merge_dicts(_BASE_EXCHANGE_TEMPLATE, data)
    rest = payload.get("rest", {})
    if not isinstance(rest, Mapping):
        raise CustomEndpointConfigError("'rest' override must be an object when provided")
    for key in ("rewrite_domains", "url_overrides", "extra_headers"):
        value = rest.get(key, {})
        if value is None:
            value = {}
        if not isinstance(value, Mapping):
            raise CustomEndpointConfigError(f"'rest.{key}' must be an object mapping strings")
        rest[key] = {str(k): str(v) for k, v in value.items()}
    payload["disable_ws"] = bool(payload.get("disable_ws", False))
    payload["rest"] = dict(rest)
    return dict(payload)


def _deep_merge_dicts(
    base: Mapping[str, object],
    override: Optional[Mapping[str, object]],
) -> Dict[str, object]:
    result: Dict[str, object] = dict(base)
    if not override:
        return result
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), Mapping):
            result[key] = _deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def _build_resolved(exchange_id: str, payload: Mapping[str, object]) -> ResolvedEndpointOverride:
    rest = payload.get("rest", {})
    return ResolvedEndpointOverride(
        exchange_id=exchange_id,
        rest_domain_rewrites=dict(rest.get("rewrite_domains", {})),
        rest_url_overrides=dict(rest.get("url_overrides", {})),
        rest_extra_headers=dict(rest.get("extra_headers", {})),
        disable_ws=bool(payload.get("disable_ws", False)),
    )


_CONFIG_CACHE: Optional[CustomEndpointConfig] = None
_CONFIG_LOAD_PARAMS: Tuple[Optional[str], bool] = (None, True)
_CONFIG_SOURCE_PATH: Optional[Path] = None


def configure_custom_endpoint_loader(
    path: Optional[str],
    *,
    autodiscover: bool = True,
    preloaded: Optional[CustomEndpointConfig] = None,
) -> None:
    """
    配置加载器使用特定路径或禁用自动发现。

    Args:
        path: 要加载的显式 JSON 文件路径；提供时加载器忽略自动发现。
              将 ``None`` 与 ``autodiscover=False`` 一起使用可完全禁用自定义端点。
        autodiscover: 当 ``path`` 为 None 时是否搜索默认位置。
        preloaded: 可选的已解析配置，用于重用以避免下次访问时额外的文件读取。
    """
    global _CONFIG_CACHE, _CONFIG_LOAD_PARAMS, _CONFIG_SOURCE_PATH
    _CONFIG_LOAD_PARAMS = (path, bool(autodiscover))
    if preloaded is not None:
        _CONFIG_SOURCE_PATH = preloaded.source_path
    else:
        _CONFIG_SOURCE_PATH = Path(path).expanduser().resolve() if path else None
    _CONFIG_CACHE = preloaded


def get_cached_custom_endpoint_config() -> CustomEndpointConfig:
    """
    返回缓存的自定义端点配置，首次使用时加载。

    如果加载因解析错误而失败，函数会记录问题并返回空配置以保持应用程序运行。
    """
    global _CONFIG_CACHE
    if _CONFIG_CACHE is None:
        path_override, autodiscover = _CONFIG_LOAD_PARAMS
        try:
            if path_override is not None:
                _CONFIG_CACHE = load_custom_endpoint_config(path_override)
            elif autodiscover:
                _CONFIG_CACHE = load_custom_endpoint_config()
            else:
                _CONFIG_CACHE = CustomEndpointConfig(
                    source_path=None,
                    defaults={},
                    exchanges={},
                )
            if _CONFIG_CACHE is not None:
                global _CONFIG_SOURCE_PATH
                _CONFIG_SOURCE_PATH = _CONFIG_CACHE.source_path
        except CustomEndpointConfigError as exc:
            logger.error("Failed to load custom endpoint config: %s", exc)
            _CONFIG_CACHE = CustomEndpointConfig(
                source_path=None,
                defaults={},
                exchanges={},
            )
    return _CONFIG_CACHE


def resolve_custom_endpoint_override(exchange_id: str) -> Optional[ResolvedEndpointOverride]:
    """
    返回 ``exchange_id`` 的已解析覆盖，未找到时返回 ``None``。

    ``exchange_id`` 应为标准化的 ccxt 交易所标识符
    （如 ``binanceusdm``）。
    """
    config = get_cached_custom_endpoint_config()
    return config.get_override(exchange_id) if config else None


def get_custom_endpoint_source() -> Optional[Path]:
    """返回当前覆盖加载自的文件系统路径。"""
    return _CONFIG_SOURCE_PATH


def apply_rest_overrides_to_ccxt(
    exchange,
    override: Optional[ResolvedEndpointOverride],
) -> None:
    """
    修改 ccxt 交易所实例，使 REST 请求遵循 ``override``。

    辅助函数更新 ``exchange.urls['api']`` 并合并任何 ``extra_headers``。
    交易所实例被原地修改。
    """
    if not override:
        return
    try:
        urls = getattr(exchange, "urls", {})
        if isinstance(urls, Mapping) and "api" in urls:
            original_api = dict(urls["api"])
            hostname = getattr(exchange, "hostname", None)
            updated = override.apply_to_api_urls(original_api, hostname=hostname)
            exchange.urls["api"] = updated
            for key, original_value in original_api.items():
                new_value = updated.get(key)
                if new_value != original_value:
                    logger.info(
                        "Custom endpoint active for %s.%s: %s -> %s",
                        override.exchange_id,
                        key,
                        original_value,
                        new_value,
                    )
            for key in updated:
                if key not in original_api:
                    logger.info(
                        "Custom endpoint added for %s.%s: %s",
                        override.exchange_id,
                        key,
                        updated[key],
                    )
        headers = getattr(exchange, "headers", {}) or {}
        if override.rest_extra_headers:
            merged = dict(headers)
            merged.update(override.rest_extra_headers)
            exchange.headers = merged
            logger.info(
                "Custom endpoint headers for %s merged: %s",
                override.exchange_id,
                override.rest_extra_headers,
            )
    except Exception as exc:
        logger.warning(
            "Failed to apply custom endpoint override for %s: %s",
            getattr(override, "exchange_id", "unknown"),
            exc,
        )


__all__ = [
    "CustomEndpointConfig",
    "CustomEndpointConfigError",
    "ResolvedEndpointOverride",
    "apply_rest_overrides_to_ccxt",
    "configure_custom_endpoint_loader",
    "get_cached_custom_endpoint_config",
    "get_custom_endpoint_source",
    "load_custom_endpoint_config",
    "resolve_custom_endpoint_override",
]
