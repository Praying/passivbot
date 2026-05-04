import signal


def ignore_sigint_in_worker() -> None:
    """确保工作进程忽略 SIGINT，由父进程控制关闭。"""
    try:
        signal.signal(signal.SIGINT, signal.SIG_IGN)
    except (AttributeError, ValueError):
        pass
