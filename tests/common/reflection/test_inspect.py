import functools
import inspect
from typing import Any
import pytest
import sys

from dlt.common.reflection.inspect import (
    _find_code_flag,
    isgeneratorfunction,
    isasyncgenfunction,
    iscoroutinefunction,
)


def decorator(use_wraps: bool):
    """Return a decorator.

    If *use_wraps* is True, sets ``__wrapped__`` via ``functools.wraps``.
    Otherwise the wrapper hides the wrapped function completely.
    """

    def _deco(fn):
        if use_wraps:

            @functools.wraps(fn)
            def wrapper(*a: Any, **kw: Any):
                return fn(*a, **kw)

        else:

            def wrapper(*a: Any, **kw: Any):  # type: ignore[misc]
                return fn(*a, **kw)

        return wrapper

    return _deco


def test_simple_generator():
    def gen():
        yield 1

    assert isgeneratorfunction(gen) is True
    assert inspect.isgeneratorfunction(gen) is True


def test_simple_regular():
    def reg():
        return 42

    assert isgeneratorfunction(reg) is False
    assert inspect.isgeneratorfunction(reg) is False


def test_chain_bottom_regular_middle_generator_top_regular() -> None:
    """bottom → regular | middle → generator wrapper | top → regular wrapper"""

    # bottom – regular function
    def bottom(val: str = "leaf") -> str:  # noqa: D401 – returns a string
        return val

    # middle – generator **wrapper** around bottom
    @functools.wraps(bottom)
    def middle(*a: Any, **kw: Any):  # noqa: D401
        # note: yield ensures generator definition
        yield bottom(*a, **kw)

    # top – regular wrapper around middle
    @functools.wraps(middle)
    def top(*a: Any, **kw: Any):  # noqa: D401
        return middle(*a, **kw)

    assert list(top(val="test")) == ["test"]

    assert isgeneratorfunction(top) is True
    assert inspect.isgeneratorfunction(top) is False


def test_chain_bottom_generator_middle_regular_top_regular():
    """bottom → generator | middle → regular wrapper | top → regular wrapper"""

    def bottom_gen(x: int = 0):
        yield x

    @functools.wraps(bottom_gen)
    def middle(*a: Any, **kw: Any):  # noqa: D401 – no yield in body
        return bottom_gen(*a, **kw)

    @functools.wraps(middle)
    def top(*a: Any, **kw: Any):  # noqa: D401
        return middle(*a, **kw)

    assert list(top(x=10)) == [10]

    assert isgeneratorfunction(top) is True
    assert inspect.isgeneratorfunction(top) is False


def test_chain_bottom_generator_middle_class_top_regular():
    """bottom → generator | middle → regular wrapper | top → regular wrapper"""

    def bottom_gen(x: int = 0):
        yield x

    class CallableWrapper:
        def __call__(self, *a: Any, **kw: Any):
            return bottom_gen(*a, **kw)

    middle = functools.wraps(bottom_gen)(CallableWrapper())

    @functools.wraps(middle)
    def top(*a: Any, **kw: Any):  # noqa: D401
        return middle(*a, **kw)

    assert list(top(x=10)) == [10]

    assert isgeneratorfunction(top) is True
    assert inspect.isgeneratorfunction(top) is False


def test_chain_hidden_generator_without_wraps():
    """Generator buried under a wrapper that *does not* expose __wrapped__."""

    def real_gen():
        yield 1

    exposed = decorator(use_wraps=True)(real_gen)
    hidden = decorator(use_wraps=False)(exposed)

    assert isgeneratorfunction(hidden) is False
    assert inspect.isgeneratorfunction(hidden) is False


def test_partial_over_mixed_chain():
    """Apply functools.partial to the chain from the first complex case."""

    def bottom() -> str:
        return "leaf"

    @functools.wraps(bottom)
    def middle():
        yield bottom()

    @functools.wraps(middle)
    def top(extra: int):  # noqa: D401 – dummy positional arg
        return middle()

    part = functools.partial(top, 5)
    assert isgeneratorfunction(part) is True
    assert inspect.isgeneratorfunction(part) is False


def test_partial_of_regular():
    def reg(x):  # noqa: D401 – simple regular
        return x

    part = functools.partial(reg, 1)
    assert isgeneratorfunction(part) is False
    assert inspect.isgeneratorfunction(part) is False


def test_partialmethod_keeps_generator_flag():
    class Greeter:
        def _impl(self, greeting):
            yield greeting

        greet = functools.partialmethod(_impl, "hello")

    # not following method wraps (seems to have changed in 3.10)
    assert inspect.isgeneratorfunction(Greeter().greet) is (sys.version_info[:2] > (3, 9))
    # following method wraps
    assert isgeneratorfunction(Greeter().greet) is True


def test_simple_coroutine():
    async def coro():
        return 1

    assert iscoroutinefunction(coro) is True
    assert inspect.iscoroutinefunction(coro) is True


def test_simple_not_coroutine():
    def reg():
        return 42

    assert iscoroutinefunction(reg) is False
    assert inspect.iscoroutinefunction(reg) is False


def test_simple_asyncgen():
    async def asyncgen():
        yield 1

    assert isasyncgenfunction(asyncgen) is True
    assert inspect.isasyncgenfunction(asyncgen) is True
