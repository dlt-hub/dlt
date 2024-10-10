"""Module with built in sources and source building blocks"""
from dlt.common.typing import TDataItem, TDataItems
from dlt.extract import DltSource, DltResource, Incremental as incremental
from dlt.extract.source import SourceReference
from . import credentials, config


__all__ = [
    "DltSource",
    "DltResource",
    "SourceReference",
    "TDataItem",
    "TDataItems",
    "incremental",
    "credentials",
    "config",
]
