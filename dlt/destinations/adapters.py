"""This module collects all destination adapters present in `impl` namespace"""

from dlt.destinations.impl.weaviate import weaviate_adapter
from dlt.destinations.impl.qdrant import qdrant_adapter

__all__ = ["weaviate_adapter", "qdrant_adapter"]
