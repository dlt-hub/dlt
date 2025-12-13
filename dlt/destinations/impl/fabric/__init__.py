"""Fabric Warehouse destination for dlt"""

from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.destinations.impl.fabric.configuration import FabricCredentials, FabricClientConfiguration
from dlt.destinations.impl.fabric.factory import fabric
from dlt.destinations.impl.fabric.fabric_adapter import fabric_adapter

__all__ = [
    "fabric",
    "fabric_adapter",
    "FabricCredentials",
    "FabricClientConfiguration",
    "CsvFormatConfiguration",
]
