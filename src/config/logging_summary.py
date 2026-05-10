import logging
from collections import Counter
from typing import Iterable, List


def _top_prefix(path: str, depth: int = 2) -> str:
    """取点分路径的前 depth 层前缀。"""
    parts = [part for part in str(path).split(".") if part]
    if not parts:
        return ""
    return ".".join(parts[:depth])


def _sort_by_count_then_name(counter: Counter) -> list[tuple[str, int]]:
    """按计数降序、名称升序排列计数器项。"""
    return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))


def summarize_transform_events(events: Iterable[dict]) -> List[str]:
    """将变换事件列表汇总为人类可读的消息（最多 8 条）。"""
    adds = Counter()
    renames = Counter()
    removes = Counter()
    updates: list[str] = []

    for event in events:
        action = event.get("action")
        if action == "add":
            path = str(event.get("path", ""))
            if path.count(".") == 0 and path:
                adds[path] += 1
        elif action == "rename":
            src = _top_prefix(str(event.get("from", "")))
            if src:
                renames[src] += 1
        elif action == "remove":
            path = str(event.get("path", ""))
            prefix = _top_prefix(path)
            if prefix:
                removes[prefix] += 1
        elif action == "update":
            path = str(event.get("path", ""))
            if path in {
                "optimize.scoring",
                "optimize.limits",
                "optimize.backend",
                "optimize.population_size",
                "optimize.pymoo.algorithm",
                "live.approved_coins",
                "live.ignored_coins",
                "backtest.btc_collateral_cap",
                "bot.long.forager_score_weights",
                "bot.short.forager_score_weights",
            }:
                updates.append(path)

    messages: list[str] = []
    for section, _ in _sort_by_count_then_name(adds):
        messages.append(f"已从默认值补充缺失的 {section} 段")
    for prefix, count in _sort_by_count_then_name(renames):
        messages.append(f"已重命名 {prefix} 下的 {count} 个遗留配置键")
    for prefix, count in _sort_by_count_then_name(removes):
        messages.append(f"已移除 {prefix} 下的 {count} 个废弃或未使用的键")
    for path in sorted(dict.fromkeys(updates)):
        messages.append(f"已归一化 {path}")
    return messages[:8]


def emit_transform_summary(config: dict, *, step: str, verbose: bool) -> None:
    """在 verbose 模式下输出指定步骤的变换摘要日志。"""
    if not verbose or not isinstance(config, dict):
        return
    transform_log = config.get("_transform_log", [])
    if not isinstance(transform_log, list):
        return
    target = None
    for entry in reversed(transform_log):
        if entry.get("step") == step:
            target = entry
            break
    if not isinstance(target, dict):
        return
    events = target.get("details", {}).get("changes", [])
    if not isinstance(events, list) or not events:
        return
    for message in summarize_transform_events(events):
        logging.info("[config] %s", message)
