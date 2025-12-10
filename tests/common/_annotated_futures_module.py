"""Defines a few objects to test forward annotations"""

from __future__ import annotations
from typing import TypedDict
from typing_extensions import ForwardRef

from dlt.common.configuration import configspec
from dlt.common.configuration.specs import BaseConfiguration


class _Sentinel(str):
    pass


class AnnTypedDict(TypedDict):
    word: str
    sentinel: _Sentinel  # automatically converted to ForwardRef


@configspec
class AnnConfigSpec(BaseConfiguration):
    word: str = None
    sentinel: _Sentinel = None
    sentinel_f: ForwardRef("_Sentinel") = None  # type: ignore


def ann_func(
    sentinel: _Sentinel, sentinel_f: ForwardRef("_Sentinel"), word: str = "word"  # type: ignore
) -> _Sentinel:
    pass
