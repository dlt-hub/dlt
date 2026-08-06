from typing import Any, Dict, Iterator, List, Tuple

import pyarrow as pa
import pytest

import dlt
from dlt.common.configuration import resolve_configuration
from dlt.common.utils import uniq_id
from dlt.destinations.impl.lancedb.configuration import LanceDBClientConfiguration

from tests.load.lancedb.utils import read_over_sql
from tests.utils import skip_if_not_active

# every test here loads into a cluster, so it must not run when only `lance` is active
skip_if_not_active("lancedb")

pytestmark = pytest.mark.essential

VECTOR_DIMENSIONS = 4
CHARACTERS: List[Dict[str, Any]] = [
    {"id": "1", "name": "King Arthur", "vector": [0.9, 0.1, 0.5, 0.4]},
    {"id": "2", "name": "Merlin", "vector": [0.2, 0.9, 0.4, 0.9]},
    {"id": "3", "name": "Sir Lancelot", "vector": [0.7, 0.1, 0.9, 0.7]},
]


def characters_table() -> pa.Table:
    """Vectors must be a fixed size list, a python list of floats normalizes into a nested table."""
    return pa.table(
        {
            "id": pa.array([character["id"] for character in CHARACTERS]),
            "name": pa.array([character["name"] for character in CHARACTERS]),
            "vector": pa.array(
                [character["vector"] for character in CHARACTERS],
                pa.list_(pa.float32(), VECTOR_DIMENSIONS),
            ),
        }
    )


@pytest.fixture(scope="module")
def flightsql_enabled() -> None:
    """Skips the module when the cluster has no Arrow Flight SQL endpoint configured."""
    # a dataset is required to resolve, but this only asks whether the endpoint is configured
    config = resolve_configuration(
        LanceDBClientConfiguration()._bind_dataset_name(dataset_name="flightsql_probe"),
        sections=("destination", "lancedb"),
        accept_partial=True,
    )
    if not (config.credentials and config.credentials.has_flightsql):
        pytest.skip("`destination.lancedb.credentials.flightsql_host` is not configured")


@pytest.fixture(scope="module")
def flightsql_pipeline(flightsql_enabled: None) -> Iterator[Tuple[dlt.Pipeline, str]]:
    """Loads vectors into a dataset of its own, which is a database the SQL endpoint can read."""
    table_name = "characters"
    pipeline = dlt.pipeline(
        pipeline_name="lancedb_flightsql_" + uniq_id(),
        destination="lancedb",
        dataset_name="flightsql_" + uniq_id(),
    )
    pipeline.run(characters_table(), table_name=table_name)
    yield pipeline, table_name
    with pipeline.destination_client() as client:
        client.drop_storage()


def test_load_lands_in_sql(flightsql_enabled: None) -> None:
    """Writes go through the managed client, so SQL is what proves the rows are queryable."""
    table_name = "characters"
    pipeline = dlt.pipeline(
        pipeline_name="lancedb_landed_" + uniq_id(),
        destination="lancedb",
        dataset_name="landed_" + uniq_id(),
    )
    pipeline.run(characters_table(), table_name=table_name)
    try:
        landed = read_over_sql(pipeline, table_name, expected_rows=len(CHARACTERS))
        assert sorted(landed.to_pydict()["name"]) == sorted(c["name"] for c in CHARACTERS)

        # a second load must be visible over SQL too, within the cluster's staleness bound
        pipeline.run(
            pa.table(
                {
                    "id": pa.array(["4"]),
                    "name": pa.array(["Galahad"]),
                    "vector": pa.array(
                        [[0.1, 0.2, 0.3, 0.4]], pa.list_(pa.float32(), VECTOR_DIMENSIONS)
                    ),
                }
            ),
            table_name=table_name,
        )
        appended = read_over_sql(pipeline, table_name, expected_rows=len(CHARACTERS) + 1)
        assert "Galahad" in appended.to_pydict()["name"]
    finally:
        with pipeline.destination_client() as client:
            client.drop_storage()


def test_dataset_table_reads_over_flightsql(
    flightsql_pipeline: Tuple[dlt.Pipeline, str],
) -> None:
    pipeline, table_name = flightsql_pipeline
    characters = pipeline.dataset().table(table_name)

    assert sorted(characters.df()["id"].tolist()) == ["1", "2", "3"]
    assert characters.arrow().num_rows == len(CHARACTERS)
    assert set(characters.columns) >= {"id", "name", "vector"}
    # the arrow path is native, so chunking is served by the Flight stream
    assert [table.num_rows for table in characters.iter_arrow(chunk_size=2)] == [2, 1]


def test_dataset_vector_search_over_flightsql(
    flightsql_pipeline: Tuple[dlt.Pipeline, str],
) -> None:
    """Nearest neighbour search runs server side as plain SQL, without an object store round trip."""
    pipeline, table_name = flightsql_pipeline
    dataset = pipeline.dataset()
    query_vector = CHARACTERS[1]["vector"]
    # the literal must be cast to the column type, otherwise it is compared as a float64 list
    vector_literal = f"arrow_cast({query_vector}, 'FixedSizeList({VECTOR_DIMENSIONS}, Float32)')"
    qualified_name = dataset.sql_client.make_qualified_table_name(table_name)

    relation = dataset(
        f"select name, array_distance(vector, {vector_literal}) as _distance"
        f" from {qualified_name} order by _distance limit 2",
        _execute_raw_query=True,
    )
    result = relation.arrow().to_pydict()

    # the query vector belongs to Merlin, so he is his own nearest neighbour at distance 0
    assert result["name"][0] == "Merlin"
    assert result["_distance"][0] == pytest.approx(0.0)
    assert len(result["name"]) == 2
    assert result["_distance"][0] < result["_distance"][1]


def test_cross_dataset_join_over_flightsql(flightsql_enabled: None) -> None:
    """Each dataset is a database, so a join across two of them is plain SQL over two catalogs."""
    characters = dlt.pipeline(
        pipeline_name="lancedb_join_left_" + uniq_id(),
        destination="lancedb",
        dataset_name="join_left_" + uniq_id(),
    )
    quests = dlt.pipeline(
        pipeline_name="lancedb_join_right_" + uniq_id(),
        destination="lancedb",
        dataset_name="join_right_" + uniq_id(),
    )
    try:
        characters.run(
            [{"id": "1", "name": "King Arthur"}, {"id": "2", "name": "Merlin"}],
            table_name="characters",
        )
        quests.run(
            [
                {"character_id": "1", "quest": "grail"},
                {"character_id": "1", "quest": "excalibur"},
                {"character_id": "9", "quest": "orphan"},
            ],
            table_name="quests",
        )

        joined = (
            characters.dataset()
            .table("characters")
            .join(quests.dataset().table("quests"), on="characters.id = quests.character_id")
        )

        sql = joined.to_sql()
        assert characters.dataset_name in sql, sql
        assert quests.dataset_name in sql, sql

        result = joined.df()
        # the orphan quest is dropped by the inner join
        assert sorted(result["quests__quest"].tolist()) == ["excalibur", "grail"]
        assert set(result["name"].tolist()) == {"King Arthur"}
    finally:
        for pipeline in (characters, quests):
            with pipeline.destination_client() as client:
                client.drop_storage()
