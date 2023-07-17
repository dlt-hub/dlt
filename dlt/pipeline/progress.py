"""Measure the extract, normalize and load progress"""
from typing import Literal, Union

from dlt.common.runtime.collector import NULL_COLLECTOR as _NULL_COLLECTOR
from dlt.common.runtime.collector import AliveCollector as alive_progress
from dlt.common.runtime.collector import Collector as _Collector
from dlt.common.runtime.collector import EnlightenCollector as enlighten
from dlt.common.runtime.collector import LogCollector as log
from dlt.common.runtime.collector import TqdmCollector as tqdm

TSupportedCollectors = Literal["tqdm", "enlighten", "log", "alive_progress"]
TCollectorArg = Union[_Collector, TSupportedCollectors]


def _from_name(collector: TCollectorArg) -> _Collector:
    """Create default collector by name"""
    if collector is None:
        return _NULL_COLLECTOR

    if isinstance(collector, str):
        if collector == "tqdm":
            return tqdm()
        if collector == "enlighten":
            return enlighten()
        if collector == "log":
            return log()
        if collector == "alive_progress":
            return alive_progress()
        raise ValueError(collector)
    return collector
