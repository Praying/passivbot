import logging

import hjson

from pure_funcs import remove_OD


def load_raw_config(config_path: str, *, log_errors: bool = True) -> dict:
    """从 HJSON 文件加载原始配置字典。"""
    try:
        with open(config_path, encoding="utf-8") as f:
            return remove_OD(hjson.load(f))
    except Exception:
        if log_errors:
            logging.exception("加载配置文件失败 %s", config_path)
        raise
