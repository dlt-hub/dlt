import os
from typing import List
import pytest

import dlt
from dlt.common.data_writers.configuration import CsvFormatConfiguration
from dlt.common.schema.typing import TColumnSchema
from dlt.common.typing import TLoaderFileFormat
from dlt.common.utils import uniq_id

from tests.cases import arrow_table_all_data_types, prepare_shuffled_tables
from tests.pipeline.utils import (
    assert_data_table_counts,
    assert_load_info,
    assert_only_table_columns,
    load_tables_to_dicts,
)
from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.utils import TestDataItemFormat


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres", "snowflake"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["object", "table"])
def test_load_csv(
    destination_config: DestinationTestConfiguration, item_type: TestDataItemFormat
) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), dev_mode=True)
    # do not save state so the state job is not created
    pipeline.config.restore_from_destination = False

    table, shuffled_table, shuffled_removed_column = prepare_shuffled_tables()
    # convert to pylist when loading from objects, this will kick the csv-reader in
    if item_type == "object":
        table, shuffled_table, shuffled_removed_column = (
            table.to_pylist(),
            shuffled_table.to_pylist(),
            shuffled_removed_column.to_pylist(),
        )

    load_info = pipeline.run(
        [shuffled_removed_column, shuffled_table, table],
        table_name="table",
        loader_file_format="csv",
    )
    assert_load_info(load_info)
    job = load_info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    assert_data_table_counts(pipeline, {"table": 5432 * 3})
    load_tables_to_dicts(pipeline, "table")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres", "snowflake"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("file_format", (None, "csv"))
@pytest.mark.parametrize("compression", (True, False))
def test_custom_csv_no_header(
    destination_config: DestinationTestConfiguration,
    file_format: TLoaderFileFormat,
    compression: bool,
) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = str(not compression)
    csv_format = CsvFormatConfiguration(delimiter="|", include_header=False)
    # apply to collected config
    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), dev_mode=True)
    # this will apply this to config when client instance is created
    pipeline.destination.config_params["csv_format"] = csv_format
    # verify
    assert pipeline.destination_client().config.csv_format == csv_format  # type: ignore[attr-defined]
    # create a resource that imports file

    columns: List[TColumnSchema] = [
        {"name": "id", "data_type": "bigint"},
        {"name": "name", "data_type": "text"},
        {"name": "description", "data_type": "text"},
        {"name": "ordered_at", "data_type": "date"},
        {"name": "price", "data_type": "decimal"},
    ]
    hints = dlt.mark.make_hints(columns=columns)
    import_file = "tests/load/cases/loading/csv_no_header.csv"
    if compression:
        import_file += ".gz"
    info = pipeline.run(
        [dlt.mark.with_file_import(import_file, "csv", 2, hints=hints)],
        table_name="no_header",
        loader_file_format=file_format,
    )
    print(info)
    assert_only_table_columns(pipeline, "no_header", [col["name"] for col in columns])
    rows = load_tables_to_dicts(pipeline, "no_header")
    assert len(rows["no_header"]) == 2
    # we should have twp files loaded
    jobs = info.load_packages[0].jobs["completed_jobs"]
    assert len(jobs) == 2
    job_extensions = [os.path.splitext(job.job_file_info.file_name())[1] for job in jobs]
    assert ".csv" in job_extensions
    # we allow state to be saved to make sure it is not in csv format (which would broke)
    # the loading. state is always saved in destination preferred format
    preferred_ext = "." + pipeline.destination.capabilities().preferred_loader_file_format
    assert preferred_ext in job_extensions


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres", "snowflake"]),
    ids=lambda x: x.name,
)
def test_custom_wrong_header(destination_config: DestinationTestConfiguration) -> None:
    # do not raise on failed jobs
    os.environ["RAISE_ON_FAILED_JOBS"] = "false"
    csv_format = CsvFormatConfiguration(delimiter="|", include_header=True)
    # apply to collected config
    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), dev_mode=True)
    # this will apply this to config when client instance is created
    pipeline.destination.config_params["csv_format"] = csv_format
    # verify
    assert pipeline.destination_client().config.csv_format == csv_format  # type: ignore[attr-defined]
    # create a resource that imports file

    columns: List[TColumnSchema] = [
        {"name": "object_id", "data_type": "bigint", "nullable": False},
        {"name": "name", "data_type": "text"},
        {"name": "description", "data_type": "text"},
        {"name": "ordered_at", "data_type": "date"},
        {"name": "price", "data_type": "decimal"},
    ]
    hints = dlt.mark.make_hints(columns=columns)
    import_file = "tests/load/cases/loading/csv_header.csv"
    # snowflake will pass here because we do not match
    info = pipeline.run(
        [dlt.mark.with_file_import(import_file, "csv", 2, hints=hints)],
        table_name="no_header",
    )
    assert info.has_failed_jobs
    assert len(info.load_packages[0].jobs["failed_jobs"]) == 1


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres", "snowflake"]),
    ids=lambda x: x.name,
)
def test_empty_csv_from_arrow(destination_config: DestinationTestConfiguration) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "True"
    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), dev_mode=True)
    table, _, _ = arrow_table_all_data_types("arrow-table", include_json=False)

    load_info = pipeline.run(
        table.schema.empty_table(), table_name="arrow_table", loader_file_format="csv"
    )
    assert_load_info(load_info)
    assert len(load_info.load_packages[0].jobs["completed_jobs"]) == 1
    job = load_info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    assert_data_table_counts(pipeline, {"arrow_table": 0})
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT * FROM arrow_table") as cur:
            columns = [col.name for col in cur.description]
            assert len(cur.fetchall()) == 0

    # all columns in order, also casefold to the destination casing (we use cursor.description)
    casefold = pipeline.destination.capabilities().casefold_identifier
    assert columns == list(
        map(casefold, pipeline.default_schema.get_table_columns("arrow_table").keys())
    )
