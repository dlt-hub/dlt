import os
import importlib.util
from typing import Any, ClassVar, Dict, Iterator, List, Optional
import pytest

from dlt.pipeline.exceptions import PipelineStepFailed

try:
    from pydantic import BaseModel
    from dlt.common.libs.pydantic import DltConfig
except ImportError:
    # mock pydantic with dataclasses. allow to run tests
    # not requiring pydantic
    from dataclasses import dataclass

    @dataclass
    class BaseModel:  # type: ignore[no-redef]
        pass


import dlt
from dlt.common import json, pendulum
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.common.runtime.collector import (
    AliveCollector,
    EnlightenCollector,
    LogCollector,
    TqdmCollector,
)
from dlt.common.storages import FileStorage

from dlt.extract.storage import ExtractStorage
from dlt.extract.validation import PydanticValidator

from dlt.destinations import dummy

from dlt.pipeline import TCollectorArg

from tests.utils import TEST_STORAGE_ROOT
from tests.extract.utils import expect_extracted_file
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import (
    assert_load_info,
    load_data_table_counts,
    load_json_case,
    many_delayed,
)

DUMMY_COMPLETE = dummy(completed_prob=1)  # factory set up to complete jobs


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        default_sql_configs=True, default_vector_configs=True, local_filesystem_configs=True
    ),
    ids=lambda x: x.name,
)
def test_create_pipeline_all_destinations(destination_config: DestinationTestConfiguration) -> None:
    # create pipelines, extract and normalize. that should be possible without installing any dependencies
    p = dlt.pipeline(
        pipeline_name=destination_config.destination_type + "_pipeline",
        destination=destination_config.destination_type,
        staging=destination_config.staging,
    )
    # are capabilities injected
    caps = p._container[DestinationCapabilitiesContext]
    if caps.naming_convention:
        assert p.naming.name() == caps.naming_convention
    else:
        assert p.naming.name() == "snake_case"

    p.extract([1, "2", 3], table_name="data")
    # is default schema with right naming convention
    assert p.default_schema.naming.max_length == min(
        caps.max_column_identifier_length, caps.max_identifier_length
    )
    p.normalize()
    assert p.default_schema.naming.max_length == min(
        caps.max_column_identifier_length, caps.max_identifier_length
    )


@pytest.mark.parametrize("progress", ["tqdm", "enlighten", "log", "alive_progress"])
def test_pipeline_progress(progress: TCollectorArg) -> None:
    # do not raise on failed jobs
    os.environ["RAISE_ON_FAILED_JOBS"] = "false"
    os.environ["TIMEOUT"] = "3.0"

    p = dlt.pipeline(destination="dummy", progress=progress)
    p.extract(many_delayed(5, 10))
    p.normalize()

    collector = p.collector

    # attach pipeline
    p = dlt.attach(progress=collector)
    p.extract(many_delayed(5, 10))
    p.run(dataset_name="dummy")

    assert collector == p.drop().collector

    # make sure a valid logger was used
    if progress == "tqdm":
        assert isinstance(collector, TqdmCollector)
    if progress == "enlighten":
        assert isinstance(collector, EnlightenCollector)
    if progress == "alive_progress":
        assert isinstance(collector, AliveCollector)
    if progress == "log":
        assert isinstance(collector, LogCollector)


@pytest.mark.parametrize("method", ("extract", "run"))
def test_column_argument_pydantic(method: str) -> None:
    """Test columns schema is created from pydantic model"""
    p = dlt.pipeline(destination="duckdb")

    @dlt.resource
    def some_data() -> Iterator[Dict[str, Any]]:
        yield {}

    class Columns(BaseModel):
        a: Optional[int] = None
        b: Optional[str] = None

    if method == "run":
        p.run(some_data(), columns=Columns)
    else:
        p.extract(some_data(), columns=Columns)

    assert p.default_schema.tables["some_data"]["columns"]["a"]["data_type"] == "bigint"
    assert p.default_schema.tables["some_data"]["columns"]["a"]["nullable"] is True
    assert p.default_schema.tables["some_data"]["columns"]["b"]["data_type"] == "text"
    assert p.default_schema.tables["some_data"]["columns"]["b"]["nullable"] is True


@pytest.mark.parametrize("yield_list", [True, False])
def test_pydantic_columns_with_contracts(yield_list: bool) -> None:
    from datetime import datetime  # noqa

    class UserLabel(BaseModel):
        label: str

    class User(BaseModel):
        user_id: int
        name: str
        created_at: datetime
        labels: List[str]
        user_label: UserLabel
        user_labels: List[UserLabel]

        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

    user = User(
        user_id=1,
        name="u1",
        created_at=pendulum.now(),
        labels=["l1", "l2"],
        user_label=UserLabel(label="in_l1"),
        user_labels=[UserLabel(label="l_l1"), UserLabel(label="l_l1")],
    )

    @dlt.resource(columns=User)
    def users(users_list: List[Any]) -> Iterator[Any]:
        if yield_list:
            yield users_list
        else:
            yield from users_list

    pipeline = dlt.pipeline(destination="duckdb")
    info = pipeline.run(users([user.dict(), user.dict()]))
    assert_load_info(info)
    print(pipeline.last_trace.last_normalize_info)
    # data is passing validation, all filled in
    assert load_data_table_counts(pipeline) == {
        "users": 2,
        "users__labels": 4,
        "users__user_labels": 4,
    }

    # produce two users with extra attrs in the child model but set the rows to discard so nothing is loaded
    u1 = user.dict()
    u1["user_labels"][0]["extra_1"] = "extra"
    u1["user_labels"][1]["extra_1"] = "extra"
    u2 = user.dict()
    u2["user_labels"][0]["is_extra"] = True

    r = users([u1, u2])
    r.apply_hints(schema_contract="discard_row")
    validator: PydanticValidator[User] = r.validator  # type: ignore[assignment]
    assert validator.data_mode == "discard_row"
    assert validator.column_mode == "discard_row"
    pipeline.run(r)
    assert load_data_table_counts(pipeline) == {
        "users": 2,
        "users__labels": 4,
        "users__user_labels": 4,
    }
    print(pipeline.last_trace.last_normalize_info)


def test_extract_pydantic_models() -> None:
    pipeline = dlt.pipeline(destination="duckdb")

    class User(BaseModel):
        user_id: int
        name: str

    @dlt.resource
    def users() -> Iterator[User]:
        yield User(user_id=1, name="a")
        yield User(user_id=2, name="b")

    pipeline.extract(users())

    storage = ExtractStorage(pipeline._normalize_storage_config())
    expect_extracted_file(
        storage,
        pipeline.default_schema_name,
        "users",
        json.dumps([{"user_id": 1, "name": "a"}, {"user_id": 2, "name": "b"}]),
    )


def test_mark_hints_pydantic_columns() -> None:
    pipeline = dlt.pipeline(destination="duckdb")

    class User(BaseModel):
        user_id: int
        name: str

    # this resource emits table schema with first item
    @dlt.resource
    def with_mark():
        yield dlt.mark.with_hints(
            {"user_id": 1, "name": "zenek"},
            dlt.mark.make_hints(columns=User, primary_key="user_id"),
        )

    pipeline.run(with_mark)
    # pydantic schema used to create columns
    assert "with_mark" in pipeline.default_schema.tables
    # resource name is kept
    table = pipeline.default_schema.tables["with_mark"]
    assert table["resource"] == "with_mark"
    assert table["columns"]["user_id"]["data_type"] == "bigint"
    assert table["columns"]["user_id"]["primary_key"] is True
    assert table["columns"]["name"]["data_type"] == "text"


def test_dump_trace_freeze_exception() -> None:
    class TestRow(BaseModel):
        id_: int
        example_string: str

    # yield model in resource so incremental fails when looking for "id"

    @dlt.resource(name="table_name", primary_key="id", write_disposition="replace")
    def generate_rows_incremental(
        ts: dlt.sources.incremental[int] = dlt.sources.incremental(cursor_path="id"),
    ):
        for i in range(10):
            yield TestRow(id_=i, example_string="abc")
            if ts.end_out_of_range:
                return

    pipeline = dlt.pipeline(pipeline_name="test_dump_trace_freeze_exception", destination="duckdb")

    with pytest.raises(PipelineStepFailed):
        # must raise because incremental failed
        pipeline.run(generate_rows_incremental())

    # force to reload trace from storage
    pipeline._last_trace = None
    # trace file not present because we tried to pickle TestRow which is a local object
    assert pipeline.last_trace is None


@pytest.mark.parametrize("file_format", ("parquet", "insert_values", "jsonl"))
def test_columns_hint_with_file_formats(file_format: TLoaderFileFormat) -> None:
    @dlt.resource(write_disposition="replace", columns=[{"name": "text", "data_type": "text"}])
    def generic(start=8):
        yield [{"id": idx, "text": "A" * idx} for idx in range(start, start + 10)]

    pipeline = dlt.pipeline(destination="duckdb")
    pipeline.run(generic(), loader_file_format=file_format)


class Child(BaseModel):
    child_attribute: str
    optional_child_attribute: Optional[str] = None


def test_flattens_model_when_skip_nested_types_is_set() -> None:
    class Parent(BaseModel):
        child: Child
        optional_parent_attribute: Optional[str] = None
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

    example_data = {
        "optional_parent_attribute": None,
        "child": {
            "child_attribute": "any string",
            "optional_child_attribute": None,
        },
    }

    p = dlt.pipeline("example", destination="duckdb")
    p.run([example_data], table_name="items", columns=Parent)

    with p.sql_client() as client:
        with client.execute_query("SELECT * FROM items") as cursor:
            loaded_values = {
                col[0]: val
                for val, col in zip(cursor.fetchall()[0], cursor.description)
                if col[0] not in ("_dlt_id", "_dlt_load_id")
            }

            # Check if child dictionary is flattened and added to schema
            assert loaded_values == {
                "child__child_attribute": "any string",
                "child__optional_child_attribute": None,
                "optional_parent_attribute": None,
            }

    keys = p.default_schema.tables["items"]["columns"].keys()
    columns = p.default_schema.tables["items"]["columns"]

    assert keys == {
        "child__child_attribute",
        "child__optional_child_attribute",
        "optional_parent_attribute",
        "_dlt_load_id",
        "_dlt_id",
    }

    assert columns["child__child_attribute"] == {
        "name": "child__child_attribute",
        "data_type": "text",
        "nullable": False,
    }

    assert columns["child__optional_child_attribute"] == {
        "name": "child__optional_child_attribute",
        "data_type": "text",
        "nullable": True,
    }

    assert columns["optional_parent_attribute"] == {
        "name": "optional_parent_attribute",
        "data_type": "text",
        "nullable": True,
    }


def test_considers_model_as_complex_when_skip_nested_types_is_not_set():
    class Parent(BaseModel):
        child: Child
        optional_parent_attribute: Optional[str] = None
        data_dictionary: Dict[str, Any] = None
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": False}

    example_data = {
        "optional_parent_attribute": None,
        "data_dictionary": {
            "child_attribute": "any string",
        },
        "child": {
            "child_attribute": "any string",
            "optional_child_attribute": None,
        },
    }

    p = dlt.pipeline("example", destination="duckdb")
    p.run([example_data], table_name="items", columns=Parent)

    with p.sql_client() as client:
        with client.execute_query("SELECT * FROM items") as cursor:
            loaded_values = {
                col[0]: val
                for val, col in zip(cursor.fetchall()[0], cursor.description)
                if col[0] not in ("_dlt_id", "_dlt_load_id")
            }

            # Check if nested fields preserved
            # their contents and were not flattened
            assert loaded_values == {
                "child": '{"child_attribute":"any string","optional_child_attribute":null}',
                "optional_parent_attribute": None,
                "data_dictionary": '{"child_attribute":"any string"}',
            }

    keys = p.default_schema.tables["items"]["columns"].keys()
    assert keys == {
        "child",
        "optional_parent_attribute",
        "data_dictionary",
        "_dlt_load_id",
        "_dlt_id",
    }

    columns = p.default_schema.tables["items"]["columns"]

    assert columns["optional_parent_attribute"] == {
        "name": "optional_parent_attribute",
        "data_type": "text",
        "nullable": True,
    }

    assert columns["data_dictionary"] == {
        "name": "data_dictionary",
        "data_type": "json",
        "nullable": False,
    }


def test_skips_complex_fields_when_skip_nested_types_is_true_and_field_is_not_a_pydantic_model():
    class Parent(BaseModel):
        data_list: List[int] = []
        data_dictionary: Dict[str, Any] = None
        dlt_config: ClassVar[DltConfig] = {"skip_nested_types": True}

    example_data = {
        "optional_parent_attribute": None,
        "data_list": [12, 12, 23, 23, 45],
        "data_dictionary": {
            "child_attribute": "any string",
        },
    }

    p = dlt.pipeline("example", destination="duckdb")
    p.run([example_data], table_name="items", columns=Parent)

    table_names = [item["name"] for item in p.default_schema.data_tables()]
    assert "items__data_list" in table_names

    # But `data_list` and `data_dictionary` will be loaded
    with p.sql_client() as client:
        with client.execute_query("SELECT * FROM items") as cursor:
            loaded_values = {
                col[0]: val
                for val, col in zip(cursor.fetchall()[0], cursor.description)
                if col[0] not in ("_dlt_id", "_dlt_load_id")
            }

            assert loaded_values == {"data_dictionary__child_attribute": "any string"}


@pytest.mark.skipif(
    importlib.util.find_spec("pandas") is not None,
    reason="Test skipped because pandas IS installed",
)
def test_arrow_no_pandas() -> None:
    import pyarrow as pa

    data = {
        "Numbers": [1, 2, 3, 4, 5],
        "Strings": ["apple", "banana", "cherry", "date", "elderberry"],
    }

    df = pa.table(data)

    @dlt.resource
    def pandas_incremental(numbers=dlt.sources.incremental("Numbers")):
        yield df

    info = dlt.run(
        pandas_incremental(), write_disposition="append", table_name="data", destination="duckdb"
    )

    with info.pipeline.sql_client() as client:  # type: ignore
        with client.execute_query("SELECT * FROM data") as c:
            with pytest.raises(ImportError):
                df = c.df()


def test_empty_parquet(test_storage: FileStorage) -> None:
    from dlt.destinations import filesystem
    from tests.pipeline.utils import users_materialize_table_schema

    local = filesystem(os.path.abspath(TEST_STORAGE_ROOT))

    # we have two options to materialize columns: add columns hint or use dlt.mark to emit schema
    # at runtime. below we use the second option

    # write parquet file to storage
    info = dlt.run(
        users_materialize_table_schema,
        destination=local,
        loader_file_format="parquet",
        dataset_name="user_data",
    )
    assert_load_info(info)
    assert set(info.pipeline.default_schema.tables["users"]["columns"].keys()) == {"id", "name", "_dlt_load_id", "_dlt_id"}  # type: ignore
    # find parquet file
    files = test_storage.list_folder_files("user_data/users")
    assert len(files) == 1

    # check rows and schema
    import pyarrow.parquet as pq

    table = pq.read_table(os.path.abspath(test_storage.make_full_path(files[0])))
    assert table.num_rows == 0
    assert set(table.schema.names) == {"id", "name", "_dlt_load_id", "_dlt_id"}


def test_parquet_with_flattened_columns() -> None:
    # normalize json, write parquet file to filesystem
    pipeline = dlt.pipeline(
        "test_parquet_with_flattened_columns", destination=dlt.destinations.filesystem("_storage")
    )
    info = pipeline.run(
        [load_json_case("github_events")], table_name="events", loader_file_format="parquet"
    )
    assert_load_info(info)

    # make sure flattened columns exist
    assert "issue__reactions__url" in pipeline.default_schema.tables["events"]["columns"]
    assert "issue_reactions_url" not in pipeline.default_schema.tables["events"]["columns"]

    events_table = pipeline._dataset().events.arrow()
    assert "issue__reactions__url" in events_table.schema.names
    assert "issue_reactions_url" not in events_table.schema.names

    # load table back into filesystem
    info = pipeline.run(events_table, table_name="events2", loader_file_format="parquet")
    assert_load_info(info)

    assert "issue__reactions__url" in pipeline.default_schema.tables["events2"]["columns"]
    assert "issue_reactions_url" not in pipeline.default_schema.tables["events2"]["columns"]

    # load back into original table
    info = pipeline.run(events_table, table_name="events", loader_file_format="parquet")
    assert_load_info(info)

    events_table_new = pipeline._dataset().events.arrow()
    assert events_table.schema == events_table_new.schema
    # double row count
    assert events_table.num_rows * 2 == events_table_new.num_rows

    # now add a column that clearly needs normalization
    updated_events_table = events_table_new.append_column(
        "Clearly!Normalize", events_table_new["issue__reactions__url"]
    )
    info = pipeline.run(updated_events_table, table_name="events", loader_file_format="parquet")
    assert_load_info(info)

    assert "clearly_normalize" in pipeline.default_schema.tables["events"]["columns"]
    assert "Clearly!Normalize" not in pipeline.default_schema.tables["events"]["columns"]


def test_resource_file_format() -> None:
    os.environ["RESTORE_FROM_DESTINATION"] = "False"

    def jsonl_data():
        yield [
            {
                "id": 1,
                "name": "item",
                "description": "value",
                "ordered_at": "2024-04-12",
                "price": 128.4,
            },
            {
                "id": 1,
                "name": "item",
                "description": "value with space",
                "ordered_at": "2024-04-12",
                "price": 128.4,
            },
        ]

    # preferred file format will use destination preferred format
    jsonl_preferred = dlt.resource(jsonl_data, file_format="preferred", name="jsonl_preferred")
    assert jsonl_preferred.compute_table_schema()["file_format"] == "preferred"

    jsonl_r = dlt.resource(jsonl_data, file_format="jsonl", name="jsonl_r")
    assert jsonl_r.compute_table_schema()["file_format"] == "jsonl"

    jsonl_pq = dlt.resource(jsonl_data, file_format="parquet", name="jsonl_pq")
    assert jsonl_pq.compute_table_schema()["file_format"] == "parquet"

    info = dlt.pipeline("example", destination="duckdb").run([jsonl_preferred, jsonl_r, jsonl_pq])
    # check file types on load jobs
    load_jobs = {
        job.job_file_info.table_name: job.job_file_info
        for job in info.load_packages[0].jobs["completed_jobs"]
    }
    assert load_jobs["jsonl_r"].file_format == "jsonl"
    assert load_jobs["jsonl_pq"].file_format == "parquet"
    assert load_jobs["jsonl_preferred"].file_format == "insert_values"

    # test not supported format
    csv_r = dlt.resource(jsonl_data, file_format="csv", name="csv_r")
    assert csv_r.compute_table_schema()["file_format"] == "csv"
    info = dlt.pipeline("example", destination="duckdb").run(csv_r)
    # fallback to preferred
    load_jobs = {
        job.job_file_info.table_name: job.job_file_info
        for job in info.load_packages[0].jobs["completed_jobs"]
    }
    assert load_jobs["csv_r"].file_format == "insert_values"


def test_pick_matching_file_format(test_storage: FileStorage) -> None:
    from dlt.destinations import filesystem

    local = filesystem(os.path.abspath(TEST_STORAGE_ROOT))

    import pyarrow as pa

    data = {
        "Numbers": [1, 2, 3, 4, 5],
        "Strings": ["apple", "banana", "cherry", "date", "elderberry"],
    }

    df = pa.table(data)

    # load arrow and object to filesystem. we should get a parquet and a jsonl file
    info = dlt.run(
        [
            dlt.resource([data], name="object"),
            dlt.resource(df, name="arrow"),
        ],
        destination=local,
        dataset_name="user_data",
    )
    assert_load_info(info)
    files = test_storage.list_folder_files("user_data/arrow")
    assert len(files) == 1
    assert files[0].endswith("parquet")
    files = test_storage.list_folder_files("user_data/object")
    assert len(files) == 1
    assert files[0].endswith("jsonl")

    # load as csv
    info = dlt.run(
        [
            dlt.resource([data], name="object"),
            dlt.resource(df, name="arrow"),
        ],
        destination=local,
        dataset_name="user_data_csv",
        loader_file_format="csv",
    )
    assert_load_info(info)
    files = test_storage.list_folder_files("user_data_csv/arrow")
    assert len(files) == 1
    assert files[0].endswith("csv")
    files = test_storage.list_folder_files("user_data_csv/object")
    assert len(files) == 1
    assert files[0].endswith("csv")


def test_filesystem_column_hint_timezone() -> None:
    import pyarrow.parquet as pq
    import posixpath

    os.environ["DESTINATION__FILESYSTEM__BUCKET_URL"] = "_storage"

    # talbe: events_timezone_off
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

    # talbe: events_timezone_on
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

    # talbe: events_timezone_unset
    @dlt.resource(
        primary_key="event_id",
    )
    def events_timezone_unset():
        yield [
            {"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"},
            {"event_id": 2, "event_tstamp": "2024-07-30T10:00:00.123456+02:00"},
            {"event_id": 3, "event_tstamp": "2024-07-30T10:00:00.123456"},
        ]

    pipeline = dlt.pipeline(destination="filesystem")

    pipeline.run(
        [events_timezone_off(), events_timezone_on(), events_timezone_unset()],
        loader_file_format="parquet",
    )

    client: FilesystemClient = pipeline.destination_client()  # type: ignore[assignment]

    expected_results = {
        "events_timezone_off": None,
        "events_timezone_on": "UTC",
        "events_timezone_unset": "UTC",
    }

    for t in expected_results.keys():
        events_glob = posixpath.join(client.dataset_path, f"{t}/*")
        events_files = client.fs_client.glob(events_glob)

        with open(events_files[0], "rb") as f:
            table = pq.read_table(f)

            # convert the timestamps to strings
            timestamps = [
                ts.as_py().strftime("%Y-%m-%dT%H:%M:%S.%f") for ts in table.column("event_tstamp")
            ]
            assert timestamps == [
                "2024-07-30T10:00:00.123000",
                "2024-07-30T08:00:00.123456",
                "2024-07-30T10:00:00.123456",
            ]

            # check if the Parquet file contains timezone information
            schema = table.schema
            field = schema.field("event_tstamp")
            assert field.type.tz == expected_results[t]
