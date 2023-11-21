"""Module with built in sources and source building blocks"""
from dlt.common.typing import TDataItem, TDataItems
from dlt.extract import DltSource, DltResource, Incremental as incremental
from . import credentials
from . import config
from . import filesystem

__all__ = [
    "DltSource",
    "DltResource",
    "TDataItem",
    "TDataItems",
    "incremental",
    "credentials",
    "config",
    "filesystem",
]
