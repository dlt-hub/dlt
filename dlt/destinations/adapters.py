"""This module collects all destination adapters present in `impl` namespace"""

from dlt.destinations.impl.weaviate.weaviate_adapter import weaviate_adapter
from dlt.destinations.impl.qdrant.qdrant_adapter import qdrant_adapter
from dlt.destinations.impl.lancedb import lancedb_adapter
from dlt.destinations.impl.bigquery.bigquery_adapter import bigquery_adapter, bigquery_partition
from dlt.destinations.impl.synapse.synapse_adapter import synapse_adapter
from dlt.destinations.impl.clickhouse.clickhouse_adapter import clickhouse_adapter
from dlt.destinations.impl.athena.athena_adapter import athena_adapter, athena_partition
from dlt.destinations.impl.postgres.postgres_adapter import postgres_adapter
from dlt.destinations.impl.databricks.databricks_adapter import databricks_adapter

__all__ = [
    "weaviate_adapter",
    "qdrant_adapter",
    "lancedb_adapter",
    "bigquery_adapter",
    "bigquery_partition",
    "synapse_adapter",
    "clickhouse_adapter",
    "athena_adapter",
    "athena_partition",
    "postgres_adapter",
    "databricks_adapter",
]
