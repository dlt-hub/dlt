import os
from typing import Any, ClassVar, Dict, Iterator, List, Optional
import pytest
from pydantic import BaseModel

import dlt
from dlt.common import json, pendulum
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.libs.pydantic import DltConfig
from dlt.common.runtime.collector import (
    AliveCollector,
    EnlightenCollector,
    LogCollector,
    TqdmCollector,
)
from dlt.extract.storage import ExtractStorage
from dlt.extract.validation import PydanticValidator

from dlt.pipeline import TCollectorArg

from tests.extract.utils import expect_extracted_file
from tests.load.utils import DestinationTestConfiguration, destinations_configs
from tests.pipeline.utils import assert_load_info, load_data_table_counts, many_delayed


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
def test_create_pipeline_all_destinations(destination_config: DestinationTestConfiguration) -> None:
    # create pipelines, extract and normalize. that should be possible without installing any dependencies
    p = dlt.pipeline(
        pipeline_name=destination_config.destination + "_pipeline",
        destination=destination_config.destination,
        staging=destination_config.staging,
    )
    # are capabilities injected
    caps = p._container[DestinationCapabilitiesContext]
    print(caps.naming_convention)
    # are right naming conventions created
    assert p._default_naming.max_length == min(
        caps.max_column_identifier_length, caps.max_identifier_length
    )
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

        dlt_config: ClassVar[DltConfig] = {"skip_complex_types": True}

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


@pytest.mark.parametrize("file_format", ("parquet", "insert_values", "jsonl"))
def test_columns_hint_with_file_formats(file_format: TLoaderFileFormat) -> None:
    @dlt.resource(write_disposition="replace", columns=[{"name": "text", "data_type": "text"}])
    def generic(start=8):
        yield [{"id": idx, "text": "A" * idx} for idx in range(start, start + 10)]

    pipeline = dlt.pipeline(destination="duckdb")
    pipeline.run(generic(), loader_file_format=file_format)
