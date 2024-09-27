from typing import Union, List, Any, Dict

import numpy as np
from lancedb.embeddings import TextEmbeddingFunction  # type: ignore

import dlt
from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


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


def assert_table(
    pipeline: dlt.Pipeline,
    table_name: str,
    expected_items_count: int = None,
    items: List[Any] = None,
) -> None:
    client: LanceDBClient = pipeline.destination_client()  # type: ignore[assignment]
    qualified_table_name = client.make_qualified_table_name(table_name)

    exists = client.table_exists(qualified_table_name)
    assert exists

    records = client.db_client.open_table(qualified_table_name).search().limit(50).to_list()

    if expected_items_count is not None:
        assert expected_items_count == len(records)

    if items is None:
        return

    drop_keys = [
        "_dlt_id",
        "_dlt_load_id",
        dlt.config.get("destination.lancedb.credentials.id_field_name", str) or "id__",
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
