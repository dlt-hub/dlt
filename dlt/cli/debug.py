"""Provides a global debug setting for the CLI"""

_DEBUG_FLAG = False


def enable_debug() -> None:
    global _DEBUG_FLAG
    _DEBUG_FLAG = True


def disable_debug() -> None:
    global _DEBUG_FLAG
    _DEBUG_FLAG = False


def is_debug_enabled() -> bool:
    global _DEBUG_FLAG
    return _DEBUG_FLAG
