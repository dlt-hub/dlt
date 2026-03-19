from typing import TYPE_CHECKING, Union, List, Any, Dict, cast

import numpy as np
import pytest
from lancedb.embeddings import TextEmbeddingFunction
from lancedb.table import Table as LanceTable

import dlt

from tests.load.utils import DestinationTestConfiguration, destinations_configs

if TYPE_CHECKING:
    from dlt.destinations.impl.lance.lance_client import LanceClient
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

    TLanceDestinationClient = Union[LanceDBClient, LanceClient]
else:
    TLanceDestinationClient = Any


@pytest.fixture(
    params=destinations_configs(default_vector_configs=True, subset=("lance", "lancedb")),
    ids=lambda c: c.name,
)
def destination_config(request: pytest.FixtureRequest) -> DestinationTestConfiguration:
    return request.param


def open_lance_table(client: TLanceDestinationClient, table_name: str) -> LanceTable:
    # NOTE: we cannot use `isinstance` because classes are only imported for type checking; we resort to duck typing
    # if isinstance(client, LanceDBClient):
    if hasattr(client, "db_client"):  # LanceDBClient
        qualified_table_name = client.make_qualified_table_name(table_name)
        return client.db_client.open_table(qualified_table_name)
    # elif isinstance(client, LanceClient):
    elif hasattr(client, "open_lance_table"):  # LanceClient
        return client.open_lance_table(table_name)


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
    records = open_lance_table(client, table_name).to_arrow().to_pylist()

    if expected_items_count is not None:
        assert expected_items_count == len(records)

    if items is None:
        return

    drop_keys = [
        "_dlt_id",
        "_dlt_load_id",
        dlt.config.get("destination.lancedb.credentials.vector_field_name", str) or "vector",
    ]
    objects_without_dlt_or_special_keys = [
        {k: v for k, v in record.items() if k not in drop_keys} for record in records
    ]

    assert_unordered_dicts_equal(objects_without_dlt_or_special_keys, items)


class MockEmbeddingFunc(TextEmbeddingFunction):
    def generate_embeddings(
        self,
        texts: Union[List[str], np.ndarray],  # type: ignore[type-arg]
        *args,
        **kwargs,
    ) -> List[np.ndarray]:  # type: ignore[type-arg]
        return [np.array(None)]

    def ndims(self) -> int:
        return 2


def mock_embed(
    dim: int = 10,
) -> str:
    return str(np.random.random_sample(dim))


def chunk_document(doc: str, chunk_size: int = 10) -> List[str]:
    return [doc[i : i + chunk_size] for i in range(0, len(doc), chunk_size)]
