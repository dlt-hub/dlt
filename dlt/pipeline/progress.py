"""Measure the extract, normalize and load progress"""
from typing import Union, Literal

from dlt.common.runtime.collector import TqdmCollector as tqdm, LogCollector as log, EnlightenCollector as enlighten, AliveCollector as alive_progress
from dlt.common.runtime.collector import Collector as _Collector, NULL_COLLECTOR as _NULL_COLLECTOR

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
