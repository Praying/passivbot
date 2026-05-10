from optimization.backends.deap_backend import run_backend as run_deap_backend
from optimization.backends.pymoo_backend import run_backend as run_pymoo_backend

# 优化后端注册表
BACKEND_RUNNERS = {
    "deap": run_deap_backend,
    "pymoo": run_pymoo_backend,
}


def get_backend_runner(name: str):
    """按名称获取优化后端运行函数，默认使用 deap。"""
    backend = str(name or "deap").strip().lower()
    if backend not in BACKEND_RUNNERS:
        raise ValueError(f"unsupported optimizer backend {name!r}")
    return BACKEND_RUNNERS[backend]
