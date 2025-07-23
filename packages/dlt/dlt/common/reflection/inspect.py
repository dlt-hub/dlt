import functools
from typing import Any
from inspect import _has_code_flag  # type: ignore[attr-defined]

try:
    from inspect import CO_GENERATOR as _CO_GENERATOR
    from inspect import CO_COROUTINE as _CO_COROUTINE
    from inspect import CO_ASYNC_GENERATOR as _CO_ASYNC_GENERATOR
except ImportError:  # pre-3.11
    _CO_GENERATOR = 0x20  # type: ignore[misc]
    _CO_COROUTINE = 0x0080  # type: ignore[misc]
    _CO_ASYNC_GENERATOR = 0x0200  # type: ignore[misc]


def _find_code_flag(obj: Any, flag: int, *, follow_wrapped: bool = True) -> bool:
    """Return True if obj (or an inner layer) has given code flag.

    The test mimics inspect.isgeneratorfunction but is more permissive:

    * It unwraps functools.partial and functools.partialmethod wrappers.
    * When follow_wrapped is True (default) it also follows the
      __wrapped__ breadcrumb that decorators created with
      functools.wraps leave behind.

    Walking stops as soon as a layer whose definition contains a yield
    statement is encountered - even if outer wrappers later hide that fact.

    Args:
        obj: The callable to inspect.
        flag: The code flag to check for.
        follow_wrapped: Follow __wrapped__ links left by decorators that
            respect the functools.wraps convention. Set to False to
            inspect only the outermost object (after stripping partial
            wrappers).

    Returns:
        True if a function definition with the specified flag was found,
        otherwise False.

    Notes:
        The check is syntactic: it looks at the code-flag on each layer.
        A regular function that merely returns a generator object will not
        satisfy the test.
    """
    while True:
        if _has_code_flag(obj, flag):
            return True

        nxt: Any = None

        # functools.partial / partialmethod wrappers
        if isinstance(obj, functools.partial):
            nxt = obj.func
        elif isinstance(obj, getattr(functools, "partialmethod", tuple())):
            nxt = obj.func
        # decorator layers exposing __wrapped__
        elif follow_wrapped and hasattr(obj, "__wrapped__"):
            nxt = obj.__wrapped__

        if nxt is None:
            # reached a layer we must trust; nothing generator-like so far.
            return False

        obj = nxt


def isgeneratorfunction(obj: Any, *, follow_wrapped: bool = True) -> bool:
    """Return True if obj (or chain of wrappers) is a generator function.

    It will follow both partial and wrapped chains and will stop on the first
    encountered generator function - contrary to `inspect` module implementation
    it checks flags on all wrappers.

    NOTE: intended to be used on user-supplied function where we do not control the wrapping

    Args:
        obj: The callable to inspect.
        follow_wrapped: Follow __wrapped__ links left by decorators that
            respect the functools.wraps convention. Set to False to
            inspect only the outermost object.

    Returns:
        True if obj is a generator function, otherwise False.
    """
    return _find_code_flag(obj, _CO_GENERATOR, follow_wrapped=follow_wrapped)


def iscoroutinefunction(obj: Any, *, follow_wrapped: bool = True) -> bool:
    """Return True if obj (or chain of wrappers) is a generator function.

    It will follow both partial and wrapped chains and will stop on the first
    encountered coroutine function - contrary to `inspect` module implementation
    it checks flags on all wrappers.

    NOTE: intended to be used on user-supplied function where we do not control the wrapping

    Args:
        obj: The callable to inspect.
        follow_wrapped: Follow __wrapped__ links left by decorators that
            respect the functools.wraps convention. Set to False to
            inspect only the outermost object.

    Returns:
        True if obj is a coroutine function, otherwise False.
    """
    return _find_code_flag(obj, _CO_COROUTINE, follow_wrapped=follow_wrapped)


def isasyncgenfunction(obj: Any, *, follow_wrapped: bool = True) -> bool:
    """Return True if obj (or chain of wrappers) is a generator function.

    It will follow both partial and wrapped chains and will stop on the first
    encountered async generator function - contrary to `inspect` module implementation
    it checks flags on all wrappers.

    NOTE: intended to be used on user-supplied function where we do not control the wrapping

    Args:
        obj: The callable to inspect.
        follow_wrapped: Follow __wrapped__ links left by decorators that
            respect the functools.wraps convention. Set to False to
            inspect only the outermost object.

    Returns:
        True if obj is an async generator function, otherwise False.
    """
    return _find_code_flag(obj, _CO_ASYNC_GENERATOR, follow_wrapped=follow_wrapped)
