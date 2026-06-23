import dlt
import pytest

from tests.pipeline.utils import assert_load_info

sa = pytest.importorskip("sqlalchemy")
pytest.importorskip("duckdb_engine")


def test_duckdb_engine_detects_existing_dataset(tmp_path) -> None:
    db_path = tmp_path / "mydata.db"
    engine = sa.create_engine(f"duckdb:///{db_path}")

    try:
        for item_id in (1, 2):
            pipeline = dlt.pipeline(
                destination=dlt.destinations.sqlalchemy(engine),
                pipeline_name="mydata",
                dataset_name="myschema",
                pipelines_dir=str(tmp_path),
            )

            info = pipeline.run([{"id": item_id}], table_name="numbers")

            assert_load_info(info)
    finally:
        engine.dispose()
