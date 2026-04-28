from __future__ import annotations

from typing import TYPE_CHECKING

import dlt
import pytest

from tests.dataset.utils import (
    LOAD_0_STATS,
    LOAD_1_STATS,
    TCrossDsFixture,
    TLoadsFixture,
    annotated_references,
    crm,
    inventory,
    relational_tables,
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
def dataset_with_relational_tables(module_tmp_path: pathlib.Path) -> dlt.Dataset:
    pipeline = dlt.pipeline(
        pipeline_name="relational_tables",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(str(module_tmp_path / "relational.db")),
        dev_mode=True,
    )
    pipeline.run(relational_tables())
    return pipeline.dataset()


@pytest.fixture(scope="module")
def cross_dataset_duckdb(module_tmp_path: pathlib.Path) -> TCrossDsFixture:
    db_path = str(module_tmp_path / "cross_dataset.db")

    # dataset A: CRM data (users + orders)
    pipeline_a = dlt.pipeline(
        pipeline_name="cross_ds_a",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="crm_data",
        dev_mode=True,
    )
    source_a = crm(0)
    source_a.root_key = True
    pipeline_a.run(source_a)

    # dataset B: inventory data (products + warehouses)
    pipeline_b = dlt.pipeline(
        pipeline_name="cross_ds_b",
        pipelines_dir=str(module_tmp_path / "pipelines_dir"),
        destination=dlt.destinations.duckdb(db_path),
        dataset_name="inv_data",
        dev_mode=True,
    )
    pipeline_b.run(inventory())

    return pipeline_a.dataset(), pipeline_b.dataset()


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
