import logging


def log_config_message(verbose: bool, level: int, message: str, *args) -> None:
    """输出带 [config] 前缀的日志消息，对高频 INFO 消息降级为 DEBUG。"""
    prefixed_message = "[config] " + message
    noisy_info_prefixes = (
        "Added missing ",
        "Removed unused key",
        "adding missing ",
        "renaming parameter ",
        "dropping obsolete parameter ",
        "Skipping template subtree ",
    )
    if level == logging.INFO and any(message.startswith(prefix) for prefix in noisy_info_prefixes):
        logging.debug(prefixed_message, *args)
    elif verbose or level >= logging.WARNING:
        logging.log(level, prefixed_message, *args)
    else:
        logging.debug(prefixed_message, *args)
