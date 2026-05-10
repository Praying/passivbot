import sys
import asyncio


def set_windows_event_loop_policy():
    """在 Windows 平台上设置 SelectorEventLoop 策略以兼容 asyncio。"""
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
