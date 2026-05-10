import logging
from typing import Iterable, Optional

from .log_output import log_config_message


def add_missing_keys_recursively(
    src,
    dst,
    parent=None,
    verbose=True,
    tracker=None,
    preserve: Optional[Iterable[Iterable[str]]] = None,
    _preserve_set: Optional[set[tuple[str, ...]]] = None,
):
    """递归地将 src 中存在但 dst 中缺失的键添加到 dst。"""
    if parent is None:
        parent = []
    if _preserve_set is None:
        _preserve_set = set() if preserve is None else {tuple(p) for p in preserve}

    def _path_is_preserved(path: Iterable[str]) -> bool:
        if not _preserve_set:
            return False
        path_tuple = tuple(path)
        for preserved in _preserve_set:
            if path_tuple[: len(preserved)] == preserved:
                return True
        return False

    if _path_is_preserved(parent):
        return
    for key in src:
        if key not in dst:
            # dst 缺失的键直接从 src 复制
            log_config_message(verbose, logging.INFO, "Added missing %s to config.", ".".join(parent + [key]))
            dst[key] = src[key]
            if tracker is not None:
                tracker.add(parent + [key], src[key])
        elif isinstance(src[key], dict) and isinstance(dst.get(key), dict):
            # 双方都是字典时递归处理
            add_missing_keys_recursively(
                src[key],
                dst[key],
                parent + [key],
                verbose,
                tracker=tracker,
                _preserve_set=_preserve_set,
            )
        elif isinstance(src[key], dict):
            # src 是字典但 dst 不是，跳过此子树
            log_config_message(
                verbose,
                logging.INFO,
                "Skipping template subtree %s (template is dict, config is %s)",
                ".".join(parent + [key]),
                type(dst.get(key)).__name__,
            )
            continue
        else:
            if key not in dst:
                log_config_message(
                    verbose,
                    logging.INFO,
                    "Adding missing key -> val %s -> %s to config",
                    ".".join(parent + [key]),
                    src[key],
                )
                dst[key] = src[key]
                if tracker is not None:
                    tracker.add(parent + [key], src[key])


def remove_unused_keys_recursively(
    src,
    dst,
    parent=None,
    verbose=True,
    preserve: Optional[Iterable[Iterable[str]]] = None,
    tracker=None,
):
    """递归移除 dst 中存在但 src 中不存在的键。"""
    if parent is None:
        parent = []
        if preserve is None:
            preserve_set = set()
        else:
            preserve_set = {tuple(p) for p in preserve}
    else:
        preserve_set = getattr(remove_unused_keys_recursively, "_preserve_set", set())

    def _path_is_preserved(path: Iterable[str]) -> bool:
        if not preserve_set:
            return False
        path_tuple = tuple(path)
        for preserved in preserve_set:
            if path_tuple[: len(preserved)] == preserved:
                return True
        return False

    if parent == []:
        remove_unused_keys_recursively._preserve_set = preserve_set

    if _path_is_preserved(parent):
        return
    if not isinstance(dst, dict) or not isinstance(src, dict):
        return  # 双方之一不是字典时无法比较

    # 先移除非字符串键（非标准键）
    for key in list(dst.keys()):
        if isinstance(key, str):
            continue  # 跳过字符串键，后面处理
        removed = dst.pop(key)
        current_path = parent + [str(key)]
        log_config_message(
            verbose, logging.INFO, "Removed unused key from config: %s", ".".join(current_path)
        )
        if tracker is not None:
            tracker.remove(current_path, removed)

    def _sort_key(value) -> tuple[str, str]:
        return (type(value).__name__, str(value))

    for key in sorted(list(dst.keys()), key=_sort_key):
        current_path = parent + [key]
        if _path_is_preserved(current_path):
            continue
        if isinstance(key, str) and key.startswith("_"):
            continue  # 跳过内部元数据键
        if key not in src:
            removed = dst.pop(key)
            log_config_message(
                verbose,
                logging.INFO,
                "Removed unused key from config: %s",
                ".".join(map(str, current_path)),
            )
            if tracker is not None:
                tracker.remove(current_path, removed)
            continue
        src_val = src[key]
        dst_val = dst[key]
        if isinstance(dst_val, dict) and isinstance(src_val, dict):
            remove_unused_keys_recursively(
                src_val, dst_val, current_path, verbose=verbose, tracker=tracker
            )

    if parent == [] and hasattr(remove_unused_keys_recursively, "_preserve_set"):
        delattr(remove_unused_keys_recursively, "_preserve_set")
