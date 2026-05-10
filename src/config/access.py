def require_config_value(config: dict, dotted_path: str):
    """按点分路径获取配置值，缺失则抛出 KeyError。"""
    parts = dotted_path.split(".")
    if not parts:
        raise KeyError("empty dotted_path")
    current = config
    traversed = []
    for part in parts:
        traversed.append(part)
        if not isinstance(current, dict):
            raise KeyError(
                f"config path {'/'.join(traversed[:-1])} is not a dict (required for '{dotted_path}')"
            )
        if part not in current:
            raise KeyError(f"config missing required key '{'.'.join(traversed)}'")
        current = current[part]
    return current


def require_config_dict(config: dict, dotted_path: str) -> dict:
    """获取配置中的字典值，非字典则抛出 TypeError。"""
    value = require_config_value(config, dotted_path)
    if not isinstance(value, dict):
        raise TypeError(f"config.{dotted_path} must be a dict; got {type(value).__name__}")
    return value


def get_optional_config_value(config: dict, dotted_path: str, default=None):
    """按点分路径获取可选配置值，缺失则返回默认值。"""
    parts = dotted_path.split(".")
    if not parts:
        return default
    current = config
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return default
        current = current[part]
    return current


def require_live_value(config: dict, key: str):
    """获取 live 段下必需的配置值。"""
    return require_config_value(config, f"live.{key}")


def get_optional_live_value(config: dict, key: str, default=None):
    """获取 live 段下可选的配置值。"""
    return get_optional_config_value(config, f"live.{key}", default)
