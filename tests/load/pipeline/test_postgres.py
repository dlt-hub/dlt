import os
import hashlib
import random
from string import ascii_lowercase
import pytest

from dlt.common.utils import uniq_id

from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration
from tests.cases import arrow_table_all_data_types, prepare_shuffled_tables
from tests.pipeline.utils import assert_data_table_counts, assert_load_info, load_tables_to_dicts


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_postgres_load_csv_from_arrow(destination_config: DestinationTestConfiguration) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), full_refresh=True)
    table, shuffled_table, shuffled_removed_column = prepare_shuffled_tables()

    load_info = pipeline.run(
        [shuffled_removed_column, shuffled_table, table],
        table_name="table",
        loader_file_format="csv",
    )
    assert_load_info(load_info)
    job = load_info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    assert_data_table_counts(pipeline, {"table": 5432 * 3})


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_postgres_encoded_binary(destination_config: DestinationTestConfiguration) -> None:
    import pyarrow

    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    blob = hashlib.sha3_256(random.choice(ascii_lowercase).encode()).digest()
    # encode as \x... which postgres understands
    blob_table = pyarrow.Table.from_pylist([{"hash": b"\\x" + blob.hex().encode("ascii")}])
    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), full_refresh=True)
    load_info = pipeline.run(blob_table, table_name="table", loader_file_format="csv")
    assert_load_info(load_info)
    job = load_info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")

    data = load_tables_to_dicts(pipeline, "table")
    # print(bytes(data["table"][0]["hash"]))
    # data in postgres equals unencoded blob
    assert bytes(data["table"][0]["hash"]) == blob


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_postgres_empty_csv_from_arrow(destination_config: DestinationTestConfiguration) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), full_refresh=True)
    table, _ = arrow_table_all_data_types("table", include_json=False)

    load_info = pipeline.run(
        table.schema.empty_table(), table_name="table", loader_file_format="csv"
    )
    assert_load_info(load_info)
    job = load_info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    assert_data_table_counts(pipeline, {"table": 0})
    with pipeline.sql_client() as client:
        with client.execute_query('SELECT * FROM "table"') as cur:
            columns = [col.name for col in cur.description]
            assert len(cur.fetchall()) == 0

    # all columns in order
    assert columns == list(pipeline.default_schema.get_table_columns("table").keys())
