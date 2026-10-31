import os

import pytest

import dlt
from dlt.common.utils import uniq_id

from tests.pipeline.utils import assert_load_info

sa = pytest.importorskip("sqlalchemy")
pytest.importorskip("duckdb_engine")


@pytest.mark.parametrize(
    "db_file",
    ["mydata.db", "my.data.duckdb", "weird name.db", "123abc.db", ":memory:"],
    ids=["simple", "dotted", "space", "leading-digit", "memory"],
)
def test_duckdb_engine_detects_existing_dataset(tmp_path, db_file: str) -> None:
    """Second run against the same duckdb database must detect the existing dataset:
    duckdb_engine returns catalog-qualified schema names so a bare name lookup fails
    and dlt attempts CREATE SCHEMA again.
    """
    if db_file == ":memory:":
        # in-memory duckdb is private per connection: share a single one and load serially
        engine = sa.create_engine("duckdb:///:memory:", poolclass=sa.pool.StaticPool)
        os.environ["LOAD__WORKERS"] = "1"
    else:
        engine = sa.create_engine(f"duckdb:///{tmp_path / db_file}")
    pipeline_name = "duckdb_engine_dataset_" + uniq_id()

    try:
        for item_id in (1, 2):
            pipeline = dlt.pipeline(
                destination=dlt.destinations.sqlalchemy(engine),
                pipeline_name=pipeline_name,
                dataset_name="myschema",
                pipelines_dir=str(tmp_path),
            )

            info = pipeline.run([{"id": item_id}], table_name="numbers")

            assert_load_info(info)
    finally:
        engine.dispose()
