from typing import Dict, Optional, Any
import pytest
import os

import dlt
from dlt.common import json
from dlt.common.data_types.typing import TDataType
from dlt.common.schema.utils import is_nested_table, may_be_nested
from dlt.common.storages import (
    SchemaStorage,
    SchemaStorageConfiguration,
    NormalizeStorageConfiguration,
)
from dlt.common.storages.schema_storage import SchemaStorage
from dlt.common.utils import uniq_id

from dlt.common.typing import TTableNames, TDataItems
from dlt.extract import DltResource, DltSource
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints, ResourceExtractionError
from dlt.extract.extract import ExtractStorage, Extract
from dlt.extract.hints import TResourceNestedHints, make_hints
from dlt.extract.items_transform import ValidateItem

from dlt.extract.items import TableNameMeta, DataItemWithMeta
from tests.utils import MockPipeline, clean_test_storage, TEST_STORAGE_ROOT
from tests.extract.utils import expect_extracted_file

NESTED_DATA = [
    {
        "id": 1,
        "outer1": [
            {"outer1_id": "2", "innerfoo": [{"innerfoo_id": "3"}]},
        ],
        "outer2": [
            {"outer2_id": "4", "innerbar": [{"innerbar_id": "5"}]},
        ],
    }
]


@pytest.fixture
def extract_step() -> Extract:
    clean_test_storage(init_normalize=True)
    schema_storage = SchemaStorage(
        SchemaStorageConfiguration(schema_volume_path=os.path.join(TEST_STORAGE_ROOT, "schemas")),
        makedirs=True,
    )
    return Extract(schema_storage, NormalizeStorageConfiguration())


def test_storage_reuse_package() -> None:
    storage = ExtractStorage(NormalizeStorageConfiguration())
    load_id = storage.create_load_package(dlt.Schema("first"))
    # assign the same load id if schema "fists" is being extracted
    assert storage.create_load_package(dlt.Schema("first")) == load_id
    load_id_2 = storage.create_load_package(dlt.Schema("second"))
    assert load_id_2 != load_id
    # make sure we have only two packages
    assert set(storage.new_packages.list_packages()) == {load_id, load_id_2}
    # commit
    storage.commit_new_load_package(load_id, dlt.Schema("first"))
    # we have a new load id (the package with schema moved to extracted)
    load_id_3 = storage.create_load_package(dlt.Schema("first"))
    assert load_id != load_id_3
    load_id_4 = storage.create_load_package(dlt.Schema("first"), reuse_exiting_package=False)
    assert load_id_4 != load_id_3

    # this will fail - not all extracts committed
    with pytest.raises(OSError):
        storage.delete_empty_extract_folder()
    # commit the rest
    storage.commit_new_load_package(load_id_2, dlt.Schema("second"))
    storage.commit_new_load_package(load_id_3, dlt.Schema("first"))
    storage.commit_new_load_package(load_id_4, dlt.Schema("first"))
    storage.delete_empty_extract_folder()

    # list extracted packages
    assert set(storage.extracted_packages.list_packages()) == {
        load_id,
        load_id_2,
        load_id_3,
        load_id_4,
    }


def test_extract_select_tables_mark(extract_step: Extract) -> None:
    n_f = lambda i: ("odd" if i % 2 == 1 else "even") + "_table"

    @dlt.resource
    def table_with_name_selectable(_range):
        for i in range(_range):
            yield dlt.mark.with_table_name(i, n_f(i))

    schema = expect_tables(extract_step, table_with_name_selectable)
    assert "table_with_name_selectable" not in schema.tables


def test_extract_select_tables_lambda(extract_step: Extract) -> None:
    n_f = lambda i: ("odd" if i % 2 == 1 else "even") + "_table"

    # try the same with lambda function, this is actually advised: should be faster and resource gets removed from schema

    @dlt.resource(table_name=n_f)
    def table_name_with_lambda(_range):
        yield list(range(_range))

    schema = expect_tables(extract_step, table_name_with_lambda)
    assert "table_name_with_lambda" not in schema.tables


def test_make_hints_default() -> None:
    hints = make_hints()
    assert hints == {"columns": {}}

    hints = make_hints(write_disposition=None)
    assert hints == {"columns": {}}


def test_extract_hints_mark(extract_step: Extract) -> None:
    @dlt.resource
    def with_table_hints():
        yield dlt.mark.with_hints(
            {"id": 1, "pk": "A"},
            make_hints(columns=[{"name": "id", "data_type": "bigint"}], primary_key="pk"),
        )
        schema = dlt.current.source_schema()
        # table and columns got updated in the schema
        assert "with_table_hints" in schema.tables
        table = schema.tables["with_table_hints"]
        assert "pk" in table["columns"]
        assert "id" in table["columns"]
        assert table["columns"]["pk"]["primary_key"] is True
        assert table["columns"]["id"]["data_type"] == "bigint"
        # get the resource
        resource = dlt.current.resource()
        table = resource.compute_table_schema()
        # also there we see the hints
        assert table["columns"]["pk"]["primary_key"] is True
        assert table["columns"]["id"]["data_type"] == "bigint"

        # add more columns and primary key
        yield dlt.mark.with_hints(
            {"id": 1, "pk2": "B"},
            make_hints(
                write_disposition="merge",
                file_format="preferred",
                columns=[{"name": "id", "precision": 16}, {"name": "text", "data_type": "decimal"}],
                primary_key="pk2",
            ),
        )
        # previous columns kept
        table = resource.compute_table_schema()
        assert schema is dlt.current.source().schema
        # previous primary key is gone from the resource
        assert "pk" not in table["columns"]
        assert table["columns"]["id"]["data_type"] == "bigint"
        assert table["columns"]["id"]["precision"] == 16
        assert "text" in table["columns"]
        assert table["write_disposition"] == "merge"
        # still it is kept in the schema that is merged from resource each time it changes
        table = schema.tables["with_table_hints"]
        assert "pk" in table["columns"]
        assert "text" in table["columns"]
        assert table["write_disposition"] == "merge"
        assert table["file_format"] == "preferred"

        # make table name dynamic
        yield dlt.mark.with_hints(
            {"namer": "dynamic"}, make_hints(table_name=lambda item: f"{item['namer']}_table")
        )
        # dynamic table was created in the schema and it contains the newest resource table schema
        table = schema.tables["dynamic_table"]
        # so pk is not available
        assert "pk" not in table["columns"]
        assert "pk2" in table["columns"]
        assert "id" in table["columns"]
        assert "text" in table["columns"]
        # get dynamic schema from resource
        with pytest.raises(DataItemRequiredForDynamicTableHints):
            table = resource.compute_table_schema()

        # add table-level hints
        yield dlt.mark.with_hints(
            {"namer": "dynamic"}, make_hints(additional_table_hints={"x-special-hint": "123-S"})
        )
        table = schema.tables["dynamic_table"]
        # table-level hint applied
        assert table["x-special-hint"] == "123-S"  # type: ignore[typeddict-item]

        # modify table-level hints
        yield dlt.mark.with_hints(
            {"namer": "dynamic"},
            make_hints(additional_table_hints={"x-special-hint": None, "x-ext": 123}),
        )
        table = schema.tables["dynamic_table"]
        assert table["x-ext"] == 123  # type: ignore[typeddict-item]
        assert table["x-special-hint"] is None  # type: ignore[typeddict-item]

    source = DltSource(dlt.Schema("hintable"), "module", [with_table_hints])
    extract_step.extract(source, 20, 1)
    table = source.schema.tables["dynamic_table"]
    assert "pk" not in table["columns"]


def test_extract_hints_table_variant(extract_step: Extract) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    @dlt.resource(primary_key="pk")
    def with_table_hints():
        yield dlt.mark.with_hints(
            {"id": 1, "pk": "A"},
            make_hints(table_name="table_a", columns=[{"name": "id", "data_type": "bigint"}]),
            create_table_variant=True,
        )
        # get the resource
        resource = dlt.current.resource()
        assert "table_a" in resource._hints_variants
        # get table
        table = resource.compute_table_schema(meta=TableNameMeta("table_a"))
        assert "pk" in table["columns"]
        assert "id" in table["columns"]
        assert table["columns"]["pk"]["primary_key"] is True
        assert table["columns"]["id"]["data_type"] == "bigint"

        schema = dlt.current.source_schema()
        # table table_a will be created
        assert "table_a" in schema.tables
        schema_table = schema.tables["table_a"]
        assert table == schema_table

        # dispatch to table b
        yield dlt.mark.with_hints(
            {"id": 2, "pk": "B"},
            make_hints(table_name="table_b", write_disposition="replace"),
            create_table_variant=True,
        )
        assert "table_b" in resource._hints_variants
        # get table
        table = resource.compute_table_schema(meta=TableNameMeta("table_b"))
        assert table["write_disposition"] == "replace"
        schema_table = schema.tables["table_b"]
        assert table == schema_table

        # item to resource
        yield {"id": 3, "pk": "C"}

        # dispatch to table a with table meta
        yield dlt.mark.with_table_name({"id": 4, "pk": "D"}, "table_a")

    source = DltSource(dlt.Schema("hintable"), "module", [with_table_hints])
    extract_step.extract(source, 20, 1)


def test_extract_hints_mark_incremental(extract_step: Extract) -> None:
    os.environ["DATA_WRITER__DISABLE_COMPRESSION"] = "TRUE"

    @dlt.resource(columns=[{"name": "id", "data_type": "bigint"}], primary_key="id")
    def with_table_hints():
        # yield a regular dataset first, simulate backfil
        yield [{"id": id_, "pk": "A"} for id_ in range(1, 10)]

        # get the resource
        resource = dlt.current.resource()
        table = resource.compute_table_schema()
        # also there we see the hints
        assert table["columns"]["id"]["primary_key"] is True
        assert table["columns"]["id"]["data_type"] == "bigint"

        # start emitting incremental
        yield dlt.mark.with_hints(
            [{"id": id_, "pk": "A", "created_at": id_ + 10} for id_ in range(100, 110)],
            make_hints(incremental=dlt.sources.incremental("created_at", initial_value=105)),
        )

        # get the resource
        resource = dlt.current.resource()
        assert resource.incremental.cursor_path == "created_at"  # type: ignore[attr-defined]
        assert resource.incremental.primary_key == "id"
        # we are able to add the incremental to the pipe. but it won't
        # join actually executing pipe which is a clone of a (partial) pipe of the resource
        assert isinstance(resource._pipe._steps[1], dlt.sources.incremental)
        # NOTE: this results in unbounded exception
        # assert resource.incremental.last_value == 299
        table = resource.compute_table_schema()
        assert table["columns"]["created_at"]["incremental"] is not None

        yield [{"id": id_, "pk": "A", "created_at": id_ + 10} for id_ in range(110, 120)]

    source = DltSource(dlt.Schema("hintable"), "module", [with_table_hints])
    extract_step.extract(source, 20, 1)
    # make sure incremental is in the source schema
    table = source.schema.get_table("with_table_hints")
    assert table["columns"]["created_at"]["incremental"] is not None


def test_extract_nested_hints(extract_step: Extract) -> None:
    resource_name = "with_nested_hints"
    nested_resource = DltResource.from_data(NESTED_DATA, name=resource_name)

    # Check 1: apply nested hints
    outer1_id_new_type: TDataType = "double"
    outer2_innerbar_id_new_type: TDataType = "bigint"
    nested_hints: Dict[TTableNames, TResourceNestedHints] = {
        "outer1": dict(
            columns={"outer1_id": {"name": "outer1_id", "data_type": outer1_id_new_type}}
        ),
        ("outer2", "innerbar"): dict(
            columns={
                "innerbar_id": {"name": "innerbar_id", "data_type": outer2_innerbar_id_new_type}
            }
        ),
        "outer2": {},  # should be sorted so comes before its child
    }
    nested_resource.apply_hints(nested_hints=nested_hints)
    assert nested_resource.nested_hints == nested_hints

    # check 2: discover the full schema on the source; includes root and nested tables
    implicit_parent = "with_nested_hints__outer2"

    source = DltSource(dlt.Schema("hintable"), "module", [nested_resource])
    pre_extract_schema = source.discover_schema()

    # root table exists even though there are no explicit hints
    assert pre_extract_schema.get_table(resource_name)
    outer1_tab = pre_extract_schema.get_table("with_nested_hints__outer1")
    assert outer1_tab["parent"] == "with_nested_hints"
    assert outer1_tab["columns"] == nested_hints["outer1"]["columns"]
    # no resource on nested table
    assert "resource" not in outer1_tab
    assert is_nested_table(outer1_tab) is True
    assert may_be_nested(outer1_tab) is True

    outer2_innerbar_tab = pre_extract_schema.get_table("with_nested_hints__outer2__innerbar")
    assert outer2_innerbar_tab["parent"] == "with_nested_hints__outer2"
    assert outer2_innerbar_tab["columns"] == nested_hints[("outer2", "innerbar")]["columns"]
    assert "resource" not in outer2_innerbar_tab
    assert is_nested_table(outer2_innerbar_tab) is True
    assert may_be_nested(outer2_innerbar_tab) is True

    # this table is generated to ensure `innerbar` has a parent that links it to the root table
    # NOTE: nested tables do not have parent set
    assert pre_extract_schema.get_table(implicit_parent) == {
        "name": implicit_parent,
        "parent": "with_nested_hints",
        "columns": {},
    }

    extract_step.extract(source, 20, 1)
    # schema after extractions must be same as discovered schema
    assert source.schema._schema_tables == pre_extract_schema._schema_tables


def test_break_nesting_with_primary_key(extract_step: Extract) -> None:
    resource_name = "with_nested_hints"
    nested_resource = DltResource.from_data(NESTED_DATA, name=resource_name)
    nested_hints: Dict[TTableNames, TResourceNestedHints] = {
        "outer1": {"columns": {"outer1_id": {"name": "outer1_id", "data_type": "bigint"}}},
        ("outer1", "innerbar"): {"primary_key": "innerfoo_id"},
    }
    nested_resource.apply_hints(nested_hints=nested_hints)
    assert nested_resource.nested_hints == nested_hints

    source = DltSource(dlt.Schema("hintable"), "module", [nested_resource])
    pre_extract_schema = source.discover_schema()
    # primary key will break nesting
    # print(pre_extract_schema.to_pretty_yaml())
    innerfoo_tab = pre_extract_schema.tables["with_nested_hints__outer1__innerbar"]
    assert innerfoo_tab["columns"]["innerfoo_id"]["primary_key"] is True
    # resource must be present
    assert innerfoo_tab["resource"] == "with_nested_hints"
    # parent cannot be present
    assert "parent" not in innerfoo_tab["columns"]["innerfoo_id"]
    # is_nested_table must be false
    assert is_nested_table(innerfoo_tab) is False
    assert may_be_nested(innerfoo_tab) is False
    extract_step.extract(source, 20, 1)
    # schema after extractions must be same as discovered schema
    assert source.schema._schema_tables == pre_extract_schema._schema_tables


def test_nested_hints_dynamic_table_names(extract_step: Extract) -> None:
    data = [
        {"Event": "issue", "DataBlob": [{"ID": 1, "Name": "first", "Date": "2024-01-01"}]},
        {"Event": "purchase", "DataBlob": [{"PID": "20-1", "Name": "first", "Date": "2024-01-01"}]},
    ]
    events = DltResource.from_data(
        data,
        name="events",
        hints=dlt.mark.make_hints(
            table_name=lambda e: e["Event"],
            nested_hints={
                "DataBlob": dlt.mark.make_nested_hints(
                    columns=[{"name": "Date", "data_type": "date"}]
                )
            },
        ),
    )

    source = DltSource(dlt.Schema("hintable"), "module", [events])
    extract_step.extract(source, 20, 1)
    # make sure that tables exist and types are applies
    assert "issue" in source.schema.tables
    assert "purchase" in source.schema.tables
    assert source.schema.tables["issue__data_blob"]["columns"]["date"]["data_type"] == "date"
    assert source.schema.tables["purchase__data_blob"]["columns"]["date"]["data_type"] == "date"


def test_nested_hints_table_name(extract_step: Extract) -> None:
    data = [
        {"Event": "issue", "DataBlob": [{"ID": 1, "Name": "first", "Date": "2024-01-01"}]},
        {"Event": "purchase", "DataBlob": [{"PID": "20-1", "Name": "first", "Date": "2024-01-01"}]},
    ]
    events = DltResource.from_data(
        data,
        name="events",
        hints=dlt.mark.make_hints(
            table_name="events_table",
            nested_hints={
                "DataBlob": dlt.mark.make_nested_hints(
                    columns=[{"name": "Date", "data_type": "date"}]
                )
            },
        ),
    )

    source = DltSource(dlt.Schema("hintable"), "module", [events])
    extract_step.extract(source, 20, 1)
    assert "events_table" in source.schema.tables
    assert source.schema.tables["events_table__data_blob"]["columns"]["date"]["data_type"] == "date"


def test_extract_metrics_on_exception_no_flush(extract_step: Extract) -> None:
    @dlt.resource
    def letters():
        # extract 7 items
        yield from "ABCDEFG"
        # then fail
        raise RuntimeError()
        yield from "HI"

    source = DltSource(dlt.Schema("letters"), "module", [letters])
    with pytest.raises(ResourceExtractionError):
        extract_step.extract(source, 20, 1)
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    # no jobs were created
    assert len(step_info.load_packages[0].jobs["new_jobs"]) == 0
    # make sure all writers are closed but not yet removed
    current_load_id = step_info.loads_ids[-1] if len(step_info.loads_ids) > 0 else None
    # get buffered writers
    writers = extract_step.extract_storage.item_storages["object"].buffered_writers
    assert len(writers) == 1
    for name, writer in writers.items():
        assert name.startswith(current_load_id)
        assert writer._file is None


def test_extract_metrics_on_exception_without_flush(extract_step: Extract) -> None:
    @dlt.resource
    def letters():
        # extract 7 items
        yield from "ABCDEFG"
        # then fail
        raise RuntimeError()
        yield from "HI"

    # flush buffer
    os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "4"
    source = DltSource(dlt.Schema("letters"), "module", [letters])
    with pytest.raises(ResourceExtractionError):
        extract_step.extract(source, 20, 1)
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    # one job created because the file was flushed
    jobs = step_info.load_packages[0].jobs["new_jobs"]
    # print(jobs[0].job_file_info.job_id())
    assert len(jobs) == 1
    current_load_id = step_info.loads_ids[-1] if len(step_info.loads_ids) > 0 else None
    # 7 items were extracted
    assert (
        step_info.metrics[current_load_id][0]["job_metrics"][
            jobs[0].job_file_info.job_id()
        ].items_count
        == 4
    )
    # get buffered writers
    writers = extract_step.extract_storage.item_storages["object"].buffered_writers
    assert len(writers) == 1
    for name, writer in writers.items():
        assert name.startswith(current_load_id)
        assert writer._file is None


def test_extract_empty_metrics(extract_step: Extract) -> None:
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    assert step_info.load_packages == step_info.loads_ids == []


# def test_extract_pipe_from_unknown_resource():
#         pass


def test_extract_shared_pipe(extract_step: Extract):
    def input_gen():
        yield from [1, 2, 3]

    input_r = DltResource.from_data(input_gen)
    source = DltSource(
        dlt.Schema("selectables"), "module", [input_r, input_r.with_name("gen_clone")]
    )
    extract_step.extract(source, 20, 1)
    # both tables got generated
    assert "input_gen" in source.schema._schema_tables
    assert "gen_clone" in source.schema._schema_tables


def test_extract_renamed_clone_and_parent(extract_step: Extract):
    def input_gen():
        yield from [1, 2, 3]

    def tx_step(item):
        return item * 2

    input_r = DltResource.from_data(input_gen)
    input_tx = DltResource.from_data(tx_step, data_from=DltResource.Empty)

    source = DltSource(
        dlt.Schema("selectables"), "module", [input_r, (input_r | input_tx).with_name("tx_clone")]
    )
    extract_step.extract(source, 20, 1)
    assert "input_gen" in source.schema._schema_tables
    assert "tx_clone" in source.schema._schema_tables
    # mind that pipe name of the evaluated parent will have different name than the resource
    assert source.tx_clone._pipe.parent.name == "input_gen_tx_clone"


def expect_tables(extract_step: Extract, resource: DltResource) -> dlt.Schema:
    source = DltSource(dlt.Schema("selectables"), "module", [resource(10)])
    load_id = extract_step.extract_storage.create_load_package(source.discover_schema())
    extract_step._extract_single_source(load_id, source, max_parallel_items=5, workers=1)
    # odd and even tables must be in the source schema
    assert len(source.schema.data_tables(include_incomplete=True)) == 2
    assert "odd_table" in source.schema._schema_tables
    assert "even_table" in source.schema._schema_tables
    # you must commit the files
    assert len(extract_step.extract_storage.list_files_to_normalize_sorted()) == 0
    extract_step.extract_storage.commit_new_load_package(load_id, source.schema)
    # check resulting files
    assert len(extract_step.extract_storage.list_files_to_normalize_sorted()) == 2
    expect_extracted_file(
        extract_step.extract_storage, "selectables", "odd_table", json.dumps([1, 3, 5, 7, 9])
    )
    expect_extracted_file(
        extract_step.extract_storage, "selectables", "even_table", json.dumps([0, 2, 4, 6, 8])
    )
    schema = source.schema

    # same thing but select only odd
    source = DltSource(dlt.Schema("selectables"), "module", [resource])
    source = source.with_resources(resource.name)
    source.selected_resources[resource.name].bind(10).select_tables("odd_table")
    load_id = extract_step.extract_storage.create_load_package(source.discover_schema())
    extract_step._extract_single_source(load_id, source, max_parallel_items=5, workers=1)
    assert len(source.schema.data_tables(include_incomplete=True)) == 1
    assert "odd_table" in source.schema._schema_tables
    extract_step.extract_storage.commit_new_load_package(load_id, source.schema)
    assert len(extract_step.extract_storage.list_files_to_normalize_sorted()) == 3
    expect_extracted_file(
        extract_step.extract_storage,
        "selectables",
        "odd_table",
        json.dumps([1, 3, 5, 7, 9]),
        expected_files=2,
    )
    extract_step.extract_storage.delete_empty_extract_folder()

    return schema


def test_materialize_table_schema_with_pipe_items():
    """
    Ensure that yielding a materialized empty list results in a job being created if all
    pipe items that we have are applied: incremental, limit, map, filter
    """

    class LazyValidator(ValidateItem):
        def __init__(self):
            super().__init__(lambda x: x)

        def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
            return item

    @dlt.resource
    def empty_list(
        some_id: dlt.sources.incremental[int] = dlt.sources.incremental(
            cursor_path="some_id", initial_value=0
        )
    ):
        yield dlt.mark.materialize_table_schema()

    empty_list.add_limit(10)
    empty_list.add_filter(lambda x: True)
    empty_list.add_map(lambda x: x)
    empty_list.add_yield_map(lambda x: (yield from x))
    empty_list.validator = LazyValidator()

    p = dlt.pipeline(pipeline_name="materialize", destination="duckdb", dev_mode=True)
    extract_info = p.extract(empty_list())

    found_empty_list = False
    for job in extract_info.load_packages[0].jobs["new_jobs"]:
        if job.job_file_info.table_name == "empty_list":
            found_empty_list = True
    assert found_empty_list


@pytest.mark.parametrize(
    "with_custom_metrics", [True, False], ids=["with_custom_metrics", "without_custom_metrics"]
)
def test_resource_custom_metrics(extract_step: Extract, with_custom_metrics: bool) -> None:
    """Ensure that custom metrics from resources are collected and transform steps are available in extract info"""

    if with_custom_metrics:
        expected_custom_metrics = {
            "resource_with_metrics": {
                "custom_count": 42,
                "random_constant": 1.5,
                "random_nested": {"value": 100, "unit": "items"},
                "items_count": 90,
            },
            "resource_with_other_metrics": {
                "custom_count": 3,
                "random_constant": 251.3,
                "random_nested": {"value": 4, "unit": None},
            },
        }
    else:
        expected_custom_metrics = {"resource_with_metrics": {}, "resource_with_other_metrics": {}}

    @dlt.resource
    def resource_with_metrics():
        custom_metrics = dlt.current.resource_metrics()
        for metric, value in expected_custom_metrics["resource_with_metrics"].items():
            custom_metrics[metric] = value
        yield [{"id": 1}, {"id": 2}]

    resource_with_metrics.add_limit(10)
    resource_with_metrics.add_map(lambda x: x)
    resource_with_metrics.add_yield_map(lambda x: (yield from x))

    @dlt.resource
    def resource_with_other_metrics():
        custom_metrics = dlt.current.resource_metrics()
        for metric, value in expected_custom_metrics["resource_with_other_metrics"].items():
            custom_metrics[metric] = value
        yield [{"id": 1}, {"id": 2}]

    source = DltSource(
        dlt.Schema("metrics"), "module", [resource_with_metrics(), resource_with_other_metrics()]
    )
    load_id = extract_step.extract(source, 20, 1)

    assert (
        expected_custom_metrics["resource_with_metrics"]
        == source.resources["resource_with_metrics"].custom_metrics
    )
    assert (
        expected_custom_metrics["resource_with_other_metrics"]
        == source.resources["resource_with_other_metrics"].custom_metrics
    )

    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]

    all_resource_metrics = step_info.metrics[load_id][0]["resource_metrics"]
    assert "resource_with_metrics" in all_resource_metrics
    assert "resource_with_other_metrics" in all_resource_metrics

    assert (
        expected_custom_metrics["resource_with_metrics"]
        == all_resource_metrics["resource_with_metrics"].custom_metrics
    )
    assert (
        expected_custom_metrics["resource_with_other_metrics"]
        == all_resource_metrics["resource_with_other_metrics"].custom_metrics
    )


@pytest.mark.parametrize(
    "with_custom_metrics", [True, False], ids=["with_custom_metrics", "without_custom_metrics"]
)
def test_resource_step_custom_metrics(extract_step: Extract, with_custom_metrics: bool) -> None:
    """Ensure that custom metrics from both resources and their transform steps are collected and merged"""

    class SimpleStep(ValidateItem):
        def __init__(self):
            super().__init__(lambda x: x)

        def __call__(self, item: TDataItems, meta: Any = None) -> Optional[TDataItems]:
            if with_custom_metrics:
                self.custom_metrics["from_step"] = "hi"
                self.custom_metrics["overrided"] = 2
            return item

    @dlt.resource
    def resource_with_step_metrics():
        if with_custom_metrics:
            custom_metrics = dlt.current.resource_metrics()
            custom_metrics["from_resource"] = "hey"
            # Overlapping metrics will be overrided by those in steps
            custom_metrics["overrided"] = 1
        yield {"id": 1}

    resource = resource_with_step_metrics()
    resource._pipe._steps.append(SimpleStep())

    source = DltSource(dlt.Schema("step_metrics"), "module", [resource])
    load_id = extract_step.extract(source, 20, 1)

    step_info = extract_step.get_step_info(MockPipeline("test", first_run=False))  # type: ignore[abstract]
    resource_metrics = step_info.metrics[load_id][0]["resource_metrics"][
        "resource_with_step_metrics"
    ]

    if with_custom_metrics:
        expected_metrics = {
            "from_resource": "hey",
            "from_step": "hi",
            "overrided": 2,
        }
        assert resource_metrics.custom_metrics == expected_metrics
    else:
        assert resource_metrics.custom_metrics == {}


@pytest.mark.parametrize(
    "as_single_batch",
    [True, False],
    ids=["single_batch", "multiple_batches"],
)
def test_add_metrics(extract_step: Extract, as_single_batch: bool) -> None:
    """Test metrics collection with add_metrics"""

    # 1: Test metrics at different pipeline stages (before/after filter)
    @dlt.resource
    def some_data():
        data = [1, 2, 3, 4, 5, 6]
        if as_single_batch:
            yield data
        else:
            yield from data

    def early_counter(items: TDataItems, meta: Any, metrics: Dict[str, Any]) -> None:
        metrics["early_count"] = metrics.get("early_count", 0) + 1

    def late_counter(items: TDataItems, meta: Any, metrics: Dict[str, Any]) -> None:
        metrics["late_count"] = metrics.get("late_count", 0) + 1

    some_data.add_metrics(early_counter).add_filter(lambda x: x > 3).add_metrics(late_counter)

    # 2. Test metrics with TableNameMeta
    @dlt.resource
    def multi_table_data():
        yield dlt.mark.with_table_name({"id": 1, "name": "Alice"}, "users")
        yield dlt.mark.with_table_name({"id": 2, "name": "Bob"}, "users")
        yield dlt.mark.with_table_name({"product": "A"}, "products")
        yield dlt.mark.with_table_name({"product": "B"}, "products")

    def count_by_table(items: TDataItems, meta: Any, metrics: Dict[str, Any]) -> None:
        if isinstance(meta, TableNameMeta):
            table_key = f"count_{meta.table_name}"
            metrics[table_key] = metrics.get(table_key, 0) + 1

    multi_table_data.add_metrics(count_by_table)

    # 3. Test metrics with custom metadata
    @dlt.resource
    def data_with_priority():
        yield DataItemWithMeta(meta={"priority": "high"}, data={"id": 1})
        yield DataItemWithMeta(meta={"priority": "high"}, data={"id": 2})
        yield DataItemWithMeta(meta={"priority": "low"}, data={"id": 3})
        yield DataItemWithMeta(meta={"priority": "low"}, data={"id": 4})
        yield DataItemWithMeta(meta={"priority": "low"}, data={"id": 5})

    def count_by_priority(items: TDataItems, meta: Any, metrics: Dict[str, Any]) -> None:
        if isinstance(meta, dict) and "priority" in meta:
            priority = meta["priority"]
            key = f"{priority}_priority_count"
            metrics[key] = metrics.get(key, 0) + 1

    data_with_priority.add_metrics(count_by_priority)

    source = DltSource(
        dlt.Schema("metrics"), "module", [some_data, multi_table_data, data_with_priority]
    )
    load_id = extract_step.extract(source, 20, 1)

    assert source.resources["some_data"].custom_metrics == {
        "early_count": 1 if as_single_batch else 6,
        "late_count": 1 if as_single_batch else 3,
    }
    assert source.resources["multi_table_data"].custom_metrics == {
        "count_users": 2,
        "count_products": 2,
    }
    assert source.resources["data_with_priority"].custom_metrics == {
        "high_priority_count": 2,
        "low_priority_count": 3,
    }

    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    all_resource_metrics = step_info.metrics[load_id][0]["resource_metrics"]
    assert "some_data" in all_resource_metrics
    assert "multi_table_data" in all_resource_metrics
    assert "data_with_priority" in all_resource_metrics
    assert all_resource_metrics["some_data"].custom_metrics == {
        "early_count": 1 if as_single_batch else 6,
        "late_count": 1 if as_single_batch else 3,
    }
    assert all_resource_metrics["multi_table_data"].custom_metrics == {
        "count_users": 2,
        "count_products": 2,
    }
    assert all_resource_metrics["data_with_priority"].custom_metrics == {
        "high_priority_count": 2,
        "low_priority_count": 3,
    }
