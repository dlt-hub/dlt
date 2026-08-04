import time
from typing import TYPE_CHECKING, Callable, Optional, Union, List, Any, Dict, cast

import numpy as np
import pyarrow as pa
from lancedb.embeddings import TextEmbeddingFunction
from lancedb.table import Table as LanceTable

import dlt
from dlt.common.typing import TColumnNames
from dlt.extract.resource import DltResource

from tests.load.utils import destinations_configs, DestinationTestConfiguration


if TYPE_CHECKING:
    from dlt.destinations.impl.lance.lance_client import LanceClient
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

    TLanceDestinationClient = Union[LanceDBClient, LanceClient]
else:
    TLanceDestinationClient = Any


LANCE_DEST_CONFS = destinations_configs(default_vector_configs=True, subset=("lance", "lancedb"))
LANCE_ONLY_CONFS = destinations_configs(default_vector_configs=True, subset=("lance",))
"""Configs of the open format destination, for features that need local dataset access."""
LANCEDB_ONLY_CONFS = destinations_configs(default_vector_configs=True, subset=("lancedb",))
"""Configs of the managed destination, for features of a cluster."""


def supports_sql_reads(destination_config: DestinationTestConfiguration) -> bool:
    """Tells if the destination can serve SQL reads. `lancedb` needs a configured cluster
    endpoint, `lance` always reads through duckdb."""
    if destination_config.destination_type != "lancedb":
        return True

    from dlt.common.configuration import resolve_configuration
    from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration

    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="dataset"),
        sections=("destination", "lancedb"),
        accept_partial=True,
    )
    return bool(config.credentials and config.credentials.has_flightsql)


def is_lancedb_client(client: TLanceDestinationClient) -> bool:
    """Checks if client is instance of LanceDBClient without requiring that class to be imported."""
    return client.__class__.__name__ == "LanceDBClient"


def is_lance_client(client: TLanceDestinationClient) -> bool:
    """Checks if client is instance of LanceClient without requiring that class to be imported."""
    return client.__class__.__name__ == "LanceClient"


def vector_column_name(client: TLanceDestinationClient) -> str:
    """Returns the column holding embeddings, `vector` when embeddings are not configured."""
    embeddings = client.config.embeddings
    return embeddings.vector_column if embeddings else "vector"


def open_lance_table(client: TLanceDestinationClient, table_name: str) -> LanceTable:
    # NOTE: we cannot use `isinstance` because classes are only imported for type checking
    if is_lancedb_client(client):
        from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

        assert isinstance(client, LanceDBClient)
        return client.open_table(table_name)
    elif is_lance_client(client):
        from dlt.destinations.impl.lance.lance_client import LanceClient

        assert isinstance(client, LanceClient)
        return client.open_lancedb_table(table_name)


def read_arrow_table(tbl: LanceTable) -> pa.Table:
    """Reads all rows of an open lance table. The managed client does not implement `to_arrow`."""
    return tbl.search().to_arrow()


def read_over_sql(
    pipeline: dlt.Pipeline, table_name: str, expected_rows: Optional[int] = None
) -> pa.Table:
    """Reads a table through the dataset, which for `lancedb` goes over Arrow Flight SQL.

    The endpoint takes no consistency setting, so it may lag by the cluster's
    `weak_read_consistency_interval_seconds`. When `expected_rows` is given the read is retried
    until the rows appear or that bound elapses, which keeps loading tests deterministic on a
    cluster configured for weak reads.

    Args:
        pipeline (dlt.Pipeline): Pipeline whose dataset is queried.
        table_name (str): Table to read, which must live in the root namespace: Flight SQL does not
            resolve named namespaces.
        expected_rows (Optional[int]): Row count to wait for. Reads once when not given.

    Returns:
        pa.Table: Rows of the table.
    """
    deadline = time.monotonic() + sql_staleness_bound() + 1.0
    while True:
        table = pipeline.dataset().table(table_name).arrow()
        if expected_rows is None or table.num_rows == expected_rows:
            return table
        if time.monotonic() > deadline:
            return table
        time.sleep(0.5)


def sql_staleness_bound() -> float:
    """Returns how long a Flight SQL read may lag behind a write, in seconds."""
    from dlt.common.configuration import resolve_configuration
    from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration

    # a dataset is required to resolve, but only the credentials are read here
    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="staleness_probe"),
        sections=("destination", "lancedb"),
        accept_partial=True,
    )
    if not config.credentials:
        return 0.0
    return float(config.credentials.weak_read_consistency_interval_seconds or 0)


def get_table_location(client: TLanceDestinationClient, table_name: str) -> str:
    """Returns the storage location of a table, which only the open format destination exposes."""
    assert is_lance_client(client), "table location is available for the `lance` destination only"
    return open_lance_table(client, table_name)._location


def get_adapter(destination_config: DestinationTestConfiguration) -> Callable[..., DltResource]:
    """Returns appropriate adapter function for given destination configuration.

    For `lance` destination, wraps the adapter to accept `no_remove_orphans` (the `lancedb`
    destination convention) and translates it to `remove_orphans` so tests can use a
    uniform interface.
    """
    if destination_config.destination_type == "lance":
        from dlt.destinations.impl.lance.lance_adapter import lance_adapter

        def _lance_adapter(
            data: Any,
            embed: TColumnNames = None,
            merge_key: TColumnNames = None,
            no_remove_orphans: bool = False,
        ) -> DltResource:
            return lance_adapter(
                data, embed=embed, merge_key=merge_key, remove_orphans=not no_remove_orphans
            )

        return _lance_adapter
    elif destination_config.destination_type == "lancedb":
        from dlt.destinations.impl.lancedb.lancedb_adapter import lancedb_adapter

        return lancedb_adapter
    else:
        raise ValueError(f"Unexpected destination type: {destination_config.destination_type}")


def get_vectorize_hint(destination_config: DestinationTestConfiguration) -> str:
    """Returns appropriate vectorize hint key for destination configuration."""
    if destination_config.destination_type == "lance":
        from dlt.destinations.impl.lance.lance_adapter import VECTORIZE_HINT
    elif destination_config.destination_type == "lancedb":
        from dlt.destinations.impl.lancedb.lancedb_adapter import VECTORIZE_HINT
    else:
        raise ValueError(f"Unexpected destination type: {destination_config.destination_type}")
    return VECTORIZE_HINT


def assert_unordered_dicts_equal(
    dict_list1: List[Dict[str, Any]], dict_list2: List[Dict[str, Any]]
) -> None:
    """
    Assert that two lists of dictionaries contain the same dictionaries, ignoring None values.

    Args:
        dict_list1 (List[Dict[str, Any]]): The first list of dictionaries to compare.
        dict_list2 (List[Dict[str, Any]]): The second list of dictionaries to compare.

    Raises:
        AssertionError: If the lists have different lengths or contain different dictionaries.
    """
    assert len(dict_list1) == len(dict_list2), "Lists have different length"

    dict_set1 = {tuple(sorted((k, v) for k, v in d.items() if v is not None)) for d in dict_list1}
    dict_set2 = {tuple(sorted((k, v) for k, v in d.items() if v is not None)) for d in dict_list2}

    assert dict_set1 == dict_set2, "Lists contain different dictionaries"


# TODO: merge with assert_table in main pipeline utils...
def assert_table(
    pipeline: dlt.Pipeline,
    table_name: str,
    expected_items_count: int = None,
    items: List[Any] = None,
) -> None:
    client = pipeline.destination_client()
    client = cast(TLanceDestinationClient, client)
    records = read_arrow_table(open_lance_table(client, table_name)).to_pylist()

    if expected_items_count is not None:
        assert expected_items_count == len(records)

    if items is None:
        return

    drop_keys = ["_dlt_id", "_dlt_load_id", vector_column_name(client)]
    objects_without_dlt_or_special_keys = [
        {k: v for k, v in record.items() if k not in drop_keys} for record in records
    ]

    assert_unordered_dicts_equal(objects_without_dlt_or_special_keys, items)


class MockEmbeddingFunc(TextEmbeddingFunction):
    def generate_embeddings(
        self,
        texts: Union[List[str], np.ndarray],  # type: ignore[type-arg,unused-ignore]
        *args,
        **kwargs,
    ) -> List[np.ndarray]:  # type: ignore[type-arg,unused-ignore]
        return [np.array(None)]

    def ndims(self) -> int:
        return 2


def mock_embed(
    dim: int = 10,
) -> str:
    return str(np.random.random_sample(dim))


def chunk_document(doc: str, chunk_size: int = 10) -> List[str]:
    return [doc[i : i + chunk_size] for i in range(0, len(doc), chunk_size)]
