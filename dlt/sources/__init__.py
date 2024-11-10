"""Module with built in sources and source building blocks"""
from dlt.common.typing import TDataItem, TDataItems
from dlt.extract import DltSource, DltResource, Incremental as incremental
from dlt.extract.source import SourceReference, UnknownSourceReference
from . import credentials, config


__all__ = [
    "DltSource",
    "DltResource",
    "SourceReference",
    "UnknownSourceReference",
    "TDataItem",
    "TDataItems",
    "incremental",
    "credentials",
    "config",
]
