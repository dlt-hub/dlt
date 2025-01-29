"""Module with built in sources and source building blocks"""
from dlt.common.typing import TDataItem, TDataItems
from dlt.extract import DltSource, DltResource, Incremental as incremental
from dlt.extract.reference import AnySourceFactory, SourceReference, UnknownSourceReference
from . import credentials, config


__all__ = [
    "DltSource",
    "DltResource",
    "SourceReference",
    "UnknownSourceReference",
    "AnySourceFactory",
    "TDataItem",
    "TDataItems",
    "incremental",
    "credentials",
    "config",
]
