class RestartBotException(Exception):
    """触发机器人干净重启，不增加错误计数。"""

    pass


class FatalBotException(Exception):
    """干净地停止机器人，不进入自动重启循环。"""

    pass
