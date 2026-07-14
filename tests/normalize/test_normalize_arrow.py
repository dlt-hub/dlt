from typing import Any, Iterator

import pytest

from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.schema.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.storages import NormalizeStorage, ParsedLoadJobFileName

from dlt.extract.extract import ExtractStorage
from dlt.normalize import Normalize

from dlt.destinations import duckdb

from tests.utils import MockPipeline
from tests.normalize.utils import INSERT_CAPS, JSONL_CAPS

# reuse fixtures and helpers from the main normalize test module
from tests.normalize.test_normalize import (  # noqa: F401
    raw_normalize,
    caps,
    default_caps,
    logger_autouse,
    normalize_pending,
)

pyarrow = pytest.importorskip("pyarrow")

pytestmark = pytest.mark.serial


def extract_arrow_items(
    normalize_storage: NormalizeStorage, arrow_table: Any, schema: Schema, table_name: str
) -> str:
    extractor = ExtractStorage(normalize_storage.config)
    load_id = extractor.create_load_package(schema)
    extractor.item_storages["arrow"].write_data_item(
        load_id,
        schema.name,
        table_name,
        arrow_table,
        schema.get_table_columns(table_name, include_incomplete=True),
    )
    extractor.close_writers(load_id)
    extractor.commit_new_load_package(load_id, schema)
    return load_id


def _items_schema(write_disposition: str = "replace") -> Schema:
    schema = Schema("arrow_empties")
    schema.update_table(
        new_table(
            "items",
            write_disposition=write_disposition,  # type: ignore[arg-type]
            columns=[{"name": "id", "data_type": "bigint", "nullable": True}],
        )
    )
    return schema


@pytest.fixture
def parquet_caps() -> Iterator[DestinationCapabilitiesContext]:
    # parquet output: bypass the `caps` fixture override that rewrites parquet->jsonl
    _caps = duckdb().capabilities()
    _caps.preferred_loader_file_format = "parquet"
    with Container().injectable_context(_caps):
        yield _caps


@pytest.mark.parametrize("caps", INSERT_CAPS + JSONL_CAPS, indirect=True)
def test_normalize_empty_arrow_input_writes_empty_job(
    caps: DestinationCapabilitiesContext, raw_normalize: Normalize
) -> None:
    """An empty arrow input file yields an empty root-table job (row count 0) for jsonl/insert
    output writers."""
    schema = _items_schema()
    empty = pyarrow.table({"id": pyarrow.array([], type=pyarrow.int64())})
    load_id = extract_arrow_items(raw_normalize.normalize_storage, empty, schema, "items")
    normalize_pending(raw_normalize)

    # one or more empty `items` jobs (some writers emit both a streamed-empty and an explicit
    # empty file); all are physically present and the aggregate row count is 0
    files = raw_normalize.load_storage.list_new_jobs(load_id)
    assert {ParsedLoadJobFileName.parse(f).table_name for f in files} == {"items"}
    storage = raw_normalize.load_storage.normalized_packages.storage
    assert all(storage.has_file(f) for f in files)
    step_info = raw_normalize.get_step_info(MockPipeline("arrow_empty_pipeline", True))  # type: ignore[abstract]
    assert step_info.metrics[load_id][0]["table_metrics"]["items"].items_count == 0


@pytest.mark.parametrize("add_dlt_id", [True, False])
def test_normalize_empty_arrow_input_parquet_output(
    parquet_caps: DestinationCapabilitiesContext, raw_normalize: Normalize, add_dlt_id: bool
) -> None:
    """An empty arrow input file yields an empty root-table job with a physically present empty
    parquet file (num_rows == 0)."""
    raw_normalize.config.parquet_normalizer.add_dlt_id = add_dlt_id
    schema = _items_schema()
    empty = pyarrow.table({"id": pyarrow.array([], type=pyarrow.int64())})
    load_id = extract_arrow_items(raw_normalize.normalize_storage, empty, schema, "items")
    normalize_pending(raw_normalize)

    files = raw_normalize.load_storage.list_new_jobs(load_id)
    assert {ParsedLoadJobFileName.parse(f).table_name for f in files} == {"items"}
    storage = raw_normalize.load_storage.normalized_packages.storage
    # every emitted parquet job is physically present with a header and zero rows
    assert all(storage.has_file(f) for f in files)
    assert all(pyarrow.parquet.read_table(storage.make_full_path(f)).num_rows == 0 for f in files)
    step_info = raw_normalize.get_step_info(MockPipeline("arrow_parquet_pipeline", True))  # type: ignore[abstract]
    assert step_info.metrics[load_id][0]["table_metrics"]["items"].items_count == 0
