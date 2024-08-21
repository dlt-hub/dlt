import os
import hashlib
import random
from string import ascii_lowercase
import pytest

import dlt
from dlt.common.utils import uniq_id

from tests.load.utils import destinations_configs, DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info, load_tables_to_dicts
from tests.utils import TestDataItemFormat


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("item_type", ["object", "table"])
def test_postgres_encoded_binary(
    destination_config: DestinationTestConfiguration, item_type: TestDataItemFormat
) -> None:
    import pyarrow

    os.environ["RESTORE_FROM_DESTINATION"] = "False"
    blob = hashlib.sha3_256(random.choice(ascii_lowercase).encode()).digest()
    # encode as \x... which postgres understands
    blob_table = pyarrow.Table.from_pylist([{"hash": b"\\x" + blob.hex().encode("ascii")}])
    if item_type == "object":
        blob_table = blob_table.to_pylist()
        print(blob_table)

    pipeline = destination_config.setup_pipeline("postgres_" + uniq_id(), dev_mode=True)
    load_info = pipeline.run(blob_table, table_name="table", loader_file_format="csv")
    assert_load_info(load_info)
    job = load_info.load_packages[0].jobs["completed_jobs"][0].file_path
    assert job.endswith("csv")
    # assert if column inferred correctly
    assert pipeline.default_schema.get_table_columns("table")["hash"]["data_type"] == "binary"

    data = load_tables_to_dicts(pipeline, "table")
    # print(bytes(data["table"][0]["hash"]))
    # data in postgres equals unencoded blob
    assert data["table"][0]["hash"].tobytes() == blob


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True, subset=["postgres"]),
    ids=lambda x: x.name,
)
def test_postgres_column_hint_timezone(destination_config: DestinationTestConfiguration) -> None:
    # table: events_timezone_off
    @dlt.resource(
        columns={"event_tstamp": {"data_type": "timestamp", "timezone": False}},
        primary_key="event_id",
    )
    def events_timezone_off():
        yield [
            {"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"},
            {"event_id": 2, "event_tstamp": "2024-07-30T10:00:00.123456+02:00"},
            {"event_id": 3, "event_tstamp": "2024-07-30T10:00:00.123456"},
        ]

    # table: events_timezone_on
    @dlt.resource(
        columns={"event_tstamp": {"data_type": "timestamp", "timezone": True}},
        primary_key="event_id",
    )
    def events_timezone_on():
        yield [
            {"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"},
            {"event_id": 2, "event_tstamp": "2024-07-30T10:00:00.123456+02:00"},
            {"event_id": 3, "event_tstamp": "2024-07-30T10:00:00.123456"},
        ]

    # table: events_timezone_unset
    @dlt.resource(
        primary_key="event_id",
    )
    def events_timezone_unset():
        yield [
            {"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"},
            {"event_id": 2, "event_tstamp": "2024-07-30T10:00:00.123456+02:00"},
            {"event_id": 3, "event_tstamp": "2024-07-30T10:00:00.123456"},
        ]

    pipeline = destination_config.setup_pipeline(
        "postgres_" + uniq_id(), dataset_name="experiments"
    )

    pipeline.run([events_timezone_off(), events_timezone_on(), events_timezone_unset()])

    with pipeline.sql_client() as client:
        expected_results = {
            "events_timezone_off": "timestamp without time zone",
            "events_timezone_on": "timestamp with time zone",
            "events_timezone_unset": "timestamp with time zone",
        }
        for table in expected_results.keys():
            # check data type
            column_info = client.execute_sql(
                "SELECT data_type FROM information_schema.columns WHERE table_schema ="
                f" 'experiments' AND table_name = '{table}' AND column_name = 'event_tstamp';"
            )
            assert column_info[0][0] == expected_results[table]

            # check timestamp data
            rows = client.execute_sql(f"SELECT event_tstamp FROM {table} ORDER BY event_id")

            values = [r[0].strftime("%Y-%m-%dT%H:%M:%S.%f") for r in rows]
            assert values == [
                "2024-07-30T10:00:00.123000",
                "2024-07-30T08:00:00.123456",
                "2024-07-30T10:00:00.123456",
            ]
