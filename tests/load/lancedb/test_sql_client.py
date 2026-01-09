import pathlib

import pytest

import dlt
from dlt.destinations.impl.lancedb.sql_client import LanceDBSQLClient


@pytest.fixture
def tmp_lance_uri(tmp_path: pathlib.Path) -> str:
    return str(tmp_path/"data.lancedb")

@pytest.fixture
def tmp_lance_destination(tmp_lance_uri: str):
    return dlt.destinations.lancedb(lance_uri=tmp_lance_uri)

@pytest.fixture(scope="function")
def tmp_pipelines_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    return tmp_path / "pipelines_dir"

@pytest.fixture(scope="function")
def tmp_pipeline(
    request,
    tmp_lance_destination: dlt.destinations.lancedb,
    tmp_pipelines_dir: pathlib.Path,
) -> dlt.Pipeline:
    """Temporary pipeline with a name that matches the test name. It has its
    own `pipelines_dir` and duckdb destination instance.
    """
    return dlt.pipeline(
        pipeline_name=request.node.name,
        pipelines_dir=str(tmp_pipelines_dir),
        destination=tmp_lance_destination,
    )

def test_sql_client_access(tmp_pipeline: dlt.Pipeline, tmp_lance_uri: str):
    table_name = "foo"
    data = [{"id": 1, "value": "foo"}, {"id": 2, "value": "bar"}]
    tmp_pipeline.run(data=data, table_name=table_name)

    sql_client = tmp_pipeline.sql_client()

    dataset = tmp_pipeline.dataset()
    table = dataset.table(table_name)
    
    retrieved_data = table.select("id", "value").fetchall()
    assert retrieved_data == [(r["id"], r["value"]) for r in data]
