from __future__ import annotations

from typing import Any, Iterator, TYPE_CHECKING

import dlt
import pytest

from tests.dataset.utils import (
    LOAD_0_STATS,
    LOAD_1_STATS,
    TLoadsFixture,
    annotated_references,
    crm,
)
from tests.utils import (
    auto_test_run_context,
    autouse_test_storage,
    deactivate_pipeline,
    preserve_environ,
)

if TYPE_CHECKING:
    import pathlib


@pytest.fixture(scope="module")
def module_tmp_path(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """Temporary directory that persist for the lifetime of test `.py` file."""
    return tmp_path_factory.mktemp("pytest-test_relation")


@pytest.fixture(scope="module")
def loads_with_root_key(module_tmp_path: pathlib.Path) -> TLoadsFixture:
    pipeline = dlt.pipeline(
        pipeline_name="with_root_key",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "duckdb.db")),
        dev_mode=True,
    )

    source = crm(0)
    source.root_key = True
    pipeline.run(source)
    load_id_1 = pipeline.last_trace.last_normalize_info.loads_ids[0]

    source = crm(1)
    source.root_key = True
    pipeline.run(source)
    load_id_2 = pipeline.last_trace.last_normalize_info.loads_ids[0]

    return (pipeline.dataset(), (load_id_1, load_id_2), (LOAD_0_STATS, LOAD_1_STATS))


@pytest.fixture(scope="module")
def loads_without_root_key(module_tmp_path: pathlib.Path) -> TLoadsFixture:
    pipeline = dlt.pipeline(
        pipeline_name="without_root_key",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "duckdb.db")),
        dev_mode=True,
    )

    source = crm(0)
    source.root_key = False
    pipeline.run(source)
    load_id_1 = pipeline.last_trace.last_normalize_info.loads_ids[0]

    source = crm(1)
    source.root_key = False
    pipeline.run(source)
    load_id_2 = pipeline.last_trace.last_normalize_info.loads_ids[0]

    return (pipeline.dataset(), (load_id_1, load_id_2), (LOAD_0_STATS, LOAD_1_STATS))


@pytest.fixture(params=["with_root_key"])
def dataset_with_loads(
    request: pytest.FixtureRequest,
    loads_with_root_key: TLoadsFixture,
    loads_without_root_key: TLoadsFixture,
) -> TLoadsFixture:
    if request.param == "with_root_key":
        return loads_with_root_key
    if request.param == "without_root_key":
        return loads_without_root_key
    raise ValueError(f"Unknown dataset fixture: {request.param}")


@pytest.fixture(scope="module")
def dataset_with_annotated_references(module_tmp_path: pathlib.Path) -> dlt.Dataset:
    pipeline = dlt.pipeline(
        pipeline_name="annotated_references",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "annotated_references.db")),
        dev_mode=True,
    )

    pipeline.run(annotated_references())

    return pipeline.dataset()


@pytest.fixture(scope="module")
def dataset_with_incomplete_join_target(module_tmp_path: pathlib.Path) -> dlt.Dataset:
    """Two sibling tables joined by an explicit reference, where the join target
    declares an incomplete column hint via `columns=`.

    `phantom_field` is declared on `categories` with no `data_type`, so it never
    materializes at the destination. `Schema.get_table_columns()` filters it out
    via `is_complete_column`; raw `schema.tables[...]["columns"]` does not.
    """
    pipeline = dlt.pipeline(
        pipeline_name="relation_incomplete_join_target",
        pipelines_dir=str(module_tmp_path / "pipelines_dir_incomplete"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "incomplete.db")),
        dev_mode=True,
    )

    @dlt.resource(
        name="categories",
        primary_key="id",
        columns=[{"name": "phantom_field", "nullable": True}],
    )
    def categories() -> Iterator[Any]:
        yield [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]

    @dlt.resource(
        name="products",
        primary_key="id",
        columns=[{"name": "category_id", "data_type": "bigint"}],
        references=[
            {
                "referenced_table": "categories",
                "columns": ["category_id"],
                "referenced_columns": ["id"],
            }
        ],
    )
    def products() -> Iterator[Any]:
        yield [
            {"id": 10, "category_id": 1},
            {"id": 11, "category_id": 2},
            {"id": 12, "category_id": 1},
        ]

    pipeline.run([categories(), products()])
    return pipeline.dataset()
