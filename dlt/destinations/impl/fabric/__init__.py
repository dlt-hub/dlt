"""Fabric Warehouse destination for dlt"""

from dlt.common.destination.configuration import CsvFormatConfiguration
from dlt.destinations.impl.fabric.configuration import FabricCredentials, FabricClientConfiguration
from dlt.destinations.impl.fabric.factory import fabric

__all__ = [
    "fabric",
    "FabricCredentials",
    "FabricClientConfiguration",
    "CsvFormatConfiguration",
]
