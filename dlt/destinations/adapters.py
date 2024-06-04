"""This module collects all destination adapters present in `impl` namespace"""

from dlt.destinations.impl.weaviate import weaviate_adapter
from dlt.destinations.impl.qdrant import qdrant_adapter
from dlt.destinations.impl.bigquery import bigquery_adapter
from dlt.destinations.impl.synapse import synapse_adapter
from dlt.destinations.impl.clickhouse import clickhouse_adapter
from dlt.destinations.impl.athena import athena_adapter

__all__ = [
    "weaviate_adapter",
    "qdrant_adapter",
    "bigquery_adapter",
    "synapse_adapter",
    "clickhouse_adapter",
    "athena_adapter",
]
