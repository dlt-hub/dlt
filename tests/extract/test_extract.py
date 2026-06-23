from typing import Dict, List, Optional, Any
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
from dlt.common.schema.typing import TColumnSchema, TWriteDisposition
from dlt.common.typing import TTableNames, TDataItems
from dlt.common.utils import uniq_id

from dlt.extract import DltResource, DltSource
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints, ResourceExtractionError
from dlt.extract.extract import ExtractStorage, Extract
from dlt.extract.hints import TResourceNestedHints, make_hints, make_nested_hints
from dlt.extract.items_transform import ValidateItem, MetricsItem
from dlt.extract.items import TableNameMeta, DataItemWithMeta

from tests.utils import MockPipeline, clean_test_storage, get_test_storage_root
from tests.extract.utils import expect_extracted_file, assert_written_tables_are_computed

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
        SchemaStorageConfiguration(
            schema_volume_path=os.path.join(get_test_storage_root(), "schemas")
        ),
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
    # dynamically dispatched tables are not variants - they carry no variant_name
    assert "variant_name" not in schema.tables["odd_table"]
    assert "variant_name" not in schema.tables["even_table"]


def test_extract_select_tables_lambda(extract_step: Extract) -> None:
    n_f = lambda i: ("odd" if i % 2 == 1 else "even") + "_table"

    # try the same with lambda function, this is actually advised: should be faster and resource gets removed from schema

    @dlt.resource(table_name=n_f)
    def table_name_with_lambda(_range):
        yield list(range(_range))

    schema = expect_tables(extract_step, table_name_with_lambda)
    assert "table_name_with_lambda" not in schema.tables
    # event-dispatch via a table_name function is not a variant - no variant_name is set
    assert "variant_name" not in schema.tables["odd_table"]
    assert "variant_name" not in schema.tables["even_table"]


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
        # a registered table variant carries its variant name
        assert table["variant_name"] == "table_a"

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
        assert table["variant_name"] == "table_b"
        schema_table = schema.tables["table_b"]
        assert table == schema_table

        # item to resource
        yield {"id": 3, "pk": "C"}

        # dispatch to table a with table meta: a known variant keeps its variant name
        table = resource.compute_table_schema(meta=TableNameMeta("table_a"))
        assert table["variant_name"] == "table_a"
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

    # the extractor computes the root and the tables declared via nested hints; nested tables that
    # only exist in the data (ie. `__outer1__innerfoo`) are split later by the normalizer and are
    # NOT computed here. only the root receives items at extract time.
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == {
        resource_name,
        "with_nested_hints__outer1",
        "with_nested_hints__outer2",
        "with_nested_hints__outer2__innerbar",
    }
    assert "with_nested_hints__outer1__innerfoo" not in object_extractor.computed_tables
    assert object_extractor.tables_with_items == {resource_name}
    assert object_extractor.tables_with_empty == set()
    assert_written_tables_are_computed(object_extractor)


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

    # the root, the hinted nested table and the primary-keyed pseudo-root are computed; only the
    # root receives items at extract time
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == {
        resource_name,
        "with_nested_hints__outer1",
        "with_nested_hints__outer1__innerbar",
    }
    assert object_extractor.tables_with_items == {resource_name}
    assert object_extractor.tables_with_empty == set()
    assert_written_tables_are_computed(object_extractor)

    # write disposition on a table broken from nesting is set to defaul (append)
    # on reload
    pseudo_root = "with_nested_hints__outer1__innerbar"
    # in-memory the broken-out table carries no write disposition
    assert "write_disposition" not in source.schema.tables[pseudo_root]
    reloaded_schema = dlt.Schema.from_dict(source.schema.to_dict())  # type: ignore[arg-type]
    assert reloaded_schema.tables[pseudo_root]["write_disposition"] == "append"


def test_nested_hints_variant_lookup_uses_raw_name() -> None:
    schema = dlt.Schema("schema")

    @dlt.resource(
        name="items",
        nested_hints={
            "default_child": make_nested_hints(columns=[{"name": "d", "data_type": "bigint"}])
        },
    )
    def items() -> Any:
        yield {}

    resource = items()
    resource.apply_hints(
        table_name="OtherItems",
        nested_hints={
            "variant_child": make_nested_hints(columns=[{"name": "v", "data_type": "bigint"}])
        },
        create_table_variant=True,
    )

    # root_table_name is already normalized (`other_items`), meta carries the raw variant name
    nested = resource.compute_nested_table_schemas(
        "other_items", schema.naming, meta=TableNameMeta("OtherItems")
    )
    assert {t["name"] for t in nested} == {"other_items__variant_child"}


def test_nested_hints_subpath_with_separator() -> None:
    schema = dlt.Schema("h")

    @dlt.resource(
        name="items",
        nested_hints={
            "sub__MoreItems": make_nested_hints(columns=[{"name": "x", "data_type": "bigint"}]),
            # a deeper nested hint (subchild) declared under the same non-normalized sub_path
            ("sub__MoreItems", "TheChild"): make_nested_hints(
                columns=[{"name": "y", "data_type": "bigint"}]
            ),
        },
    )
    def items() -> Any:
        yield {}

    # each nested-hint path fragment is normalized immediately in the resource, so a sub_path that
    # contains the `__` separator / camel case yields a normalized table name AND a normalized parent
    nested = {t["name"]: t for t in items().compute_nested_table_schemas("items", schema.naming)}

    # the direct child is a single level under the root
    child = nested["items__sub_more_items"]
    assert child["parent"] == "items"
    assert len(schema.naming.break_path(child["name"])) == 2

    # the subchild's parent is the normalized name of the direct child (not the raw
    # `items__sub__MoreItems`), proving the parent path is normalized right away
    subchild = nested["items__sub_more_items__the_child"]
    assert subchild["parent"] == "items__sub_more_items"
    assert subchild["parent"] == child["name"]
    assert len(schema.naming.break_path(subchild["name"])) == 3


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
    "yield_one,yield_two",
    [(True, False), (False, True), (False, False), (True, True)],
    ids=["only_first", "only_second", "neither", "both"],
)
def test_materialize_table_schema_multi_table(yield_one: bool, yield_two: bool) -> None:
    """Empty table materialization works correctly for resources that produce multiple tables."""

    # non-normalized table names so the empty-table handling is exercised with normalized identifiers
    @dlt.resource
    def multi_table():
        yield dlt.mark.with_hints(
            dlt.mark.materialize_table_schema(),
            dlt.mark.make_hints(
                table_name="TableOne",
                write_disposition="replace",
                columns={"col_one": {"data_type": "text"}},
            ),
            create_table_variant=True,
        )
        yield dlt.mark.with_hints(
            dlt.mark.materialize_table_schema(),
            dlt.mark.make_hints(
                table_name="TableTwo",
                write_disposition="replace",
                columns={"col_two": {"data_type": "bigint"}},
            ),
            create_table_variant=True,
        )
        if yield_one:
            yield dlt.mark.with_table_name({"col_one": "val"}, table_name="TableOne")
        if yield_two:
            yield dlt.mark.with_table_name({"col_two": 5}, table_name="TableTwo")

    p = dlt.pipeline(
        pipeline_name="materialize_multi_" + uniq_id(),
        destination="duckdb",
        dev_mode=True,
    )
    extract_info = p.extract(multi_table())

    extracted_tables = {
        job.job_file_info.table_name for job in extract_info.load_packages[0].jobs["new_jobs"]
    }
    # both tables should always have jobs (data or empty files) under their normalized names
    assert "table_one" in extracted_tables
    assert "table_two" in extracted_tables
    assert "TableOne" not in extracted_tables
    assert "TableTwo" not in extracted_tables
    # variant tables keep their RAW (non-normalized) variant name even though the table identifier
    # itself is normalized
    assert p.default_schema.tables["table_one"]["variant_name"] == "TableOne"
    assert p.default_schema.tables["table_two"]["variant_name"] == "TableTwo"


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
                "events": [{"ts": 1}, {"ts": 2}],
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

    # verify _asdict() promotes list-valued metrics to top-level keys
    if with_custom_metrics:
        d = all_resource_metrics["resource_with_metrics"]._asdict()
        assert d["events"] == [{"ts": 1}, {"ts": 2}]
        assert "events" not in d.get("custom_metrics", {})
        assert d["custom_metrics"]["custom_count"] == 42


def test_asdict_all_list_metrics(extract_step: Extract) -> None:
    """When all custom metrics are list-valued, no custom_metrics key appears in _asdict()."""

    @dlt.resource
    def only_lists():
        m = dlt.current.resource_metrics()
        m["rows"] = [{"a": 1}]
        m["errors"] = [{"msg": "oops"}]
        yield [{"id": 1}]

    source = DltSource(dlt.Schema("all_list"), "module", [only_lists()])
    load_id = extract_step.extract(source, 20, 1)
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    d = step_info.metrics[load_id][0]["resource_metrics"]["only_lists"]._asdict()
    assert "custom_metrics" not in d
    assert d["rows"] == [{"a": 1}]
    assert d["errors"] == [{"msg": "oops"}]


def test_asdict_list_metric_collision_with_standard_field(extract_step: Extract) -> None:
    """List-valued custom metric whose key collides with a standard NamedTuple field
    stays nested under custom_metrics so it cannot overwrite standard metrics."""

    @dlt.resource
    def collision():
        m = dlt.current.resource_metrics()
        # items_count is a standard DataWriterMetrics field
        m["items_count"] = [{"v": 99}]
        m["safe_list"] = [{"v": 1}]
        yield [{"id": 1}]

    source = DltSource(dlt.Schema("collision"), "module", [collision()])
    load_id = extract_step.extract(source, 20, 1)
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    metrics = step_info.metrics[load_id][0]["resource_metrics"]["collision"]
    d = metrics._asdict()
    # standard field preserved
    assert isinstance(d["items_count"], int)
    # colliding list metric stays in custom_metrics
    assert d["custom_metrics"]["items_count"] == [{"v": 99}]
    # non-colliding list metric promoted
    assert d["safe_list"] == [{"v": 1}]
    assert "safe_list" not in d.get("custom_metrics", {})


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
            # collect list-valued metric for child table promotion
            metrics.setdefault("seen_priorities", []).append({"priority": priority})

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
        "seen_priorities": [
            {"priority": "high"},
            {"priority": "high"},
            {"priority": "low"},
            {"priority": "low"},
            {"priority": "low"},
        ],
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
        "seen_priorities": [
            {"priority": "high"},
            {"priority": "high"},
            {"priority": "low"},
            {"priority": "low"},
            {"priority": "low"},
        ],
    }

    # verify _asdict() promotes list-valued metrics to top-level keys
    d = all_resource_metrics["data_with_priority"]._asdict()
    assert d["seen_priorities"] == [
        {"priority": "high"},
        {"priority": "high"},
        {"priority": "low"},
        {"priority": "low"},
        {"priority": "low"},
    ]
    assert "seen_priorities" not in d.get("custom_metrics", {})
    assert d["custom_metrics"]["high_priority_count"] == 2


def test_custom_metrics_preserved_when_all_items_filtered(extract_step: Extract) -> None:
    """Zero-yield resources still surface their custom metrics in extract_info."""

    @dlt.resource
    def all_filtered():
        # API 1: `dlt.current.resource_metrics()` mutated from inside the resource
        dlt.current.resource_metrics()["seen_via_current"] = True
        yield [1, 2, 3]

    def early_counter(items: TDataItems, meta: Any, metrics: Dict[str, Any]) -> None:
        # runs BEFORE the filter — fires once on the single input batch
        metrics["early_count"] = metrics.get("early_count", 0) + 1

    # API 2: add_metrics runs before the filter; filter then drops every item so
    # no file is ever written for this resource
    all_filtered.add_metrics(early_counter).add_filter(lambda _: False)

    source = DltSource(dlt.Schema("metrics"), "module", [all_filtered])
    load_id = extract_step.extract(source, 20, 1)

    # the MetricsItem step held its own counter from the pre-filter callback
    resource = source.resources["all_filtered"]
    metrics_steps = [s for s in resource._pipe.steps if isinstance(s, MetricsItem)]
    assert len(metrics_steps) == 1
    assert metrics_steps[0].custom_metrics == {"early_count": 1}

    # persisted metrics: resource is present with zero items_count and merged customs
    step_info = extract_step.get_step_info(MockPipeline("buba", first_run=False))  # type: ignore[abstract]
    all_resource_metrics = step_info.metrics[load_id][0]["resource_metrics"]
    assert "all_filtered" in all_resource_metrics
    rm = all_resource_metrics["all_filtered"]
    assert rm.items_count == 0
    assert rm.custom_metrics == {"early_count": 1, "seen_via_current": True}


def test_object_mixed_case_columns_normalized(extract_step: Extract) -> None:
    """Column hints with PascalCase names are normalized to snake_case in the schema.

    Also verifies that a nullable hint-only column (not present in the data) is persisted
    with its normalized name and properties.
    """

    @dlt.resource(
        name="mixed_case",
        columns={
            "Numbers": {"data_type": "bigint"},
            "Strings": {"data_type": "text"},
            # hint-only column not present in yielded data
            "ExtraCol": {"data_type": "double", "nullable": True},
        },
    )
    def mixed_case_resource():
        yield {"Numbers": 1, "Strings": "a"}

    source = DltSource(dlt.Schema("object_test"), "module", [mixed_case_resource])
    extract_step.extract(source, 20, 1)

    schema_table = source.schema.tables["mixed_case"]
    col_names = list(schema_table["columns"].keys())
    # only normalized (snake_case) names
    assert "numbers" in col_names
    assert "strings" in col_names
    assert "Numbers" not in col_names
    assert "Strings" not in col_names
    assert "ExtraCol" not in col_names
    # hint properties preserved through normalization
    assert schema_table["columns"]["numbers"]["data_type"] == "bigint"
    assert schema_table["columns"]["strings"]["data_type"] == "text"
    # hint-only column persisted with normalized name
    assert "extra_col" in col_names
    assert schema_table["columns"]["extra_col"]["data_type"] == "double"
    assert schema_table["columns"]["extra_col"]["nullable"] is True


def test_object_special_char_columns_normalized(extract_step: Extract) -> None:
    """Column hints with special characters (e.g. ^) are normalized in the schema."""

    @dlt.resource(
        name="special_chars",
        columns={"col^New": {"data_type": "bigint"}, "col2": {"data_type": "bigint"}},
    )
    def special_chars_resource():
        yield {"col^New": 1, "col2": 2}

    source = DltSource(dlt.Schema("object_test"), "module", [special_chars_resource])
    extract_step.extract(source, 20, 1)

    schema_table = source.schema.tables["special_chars"]
    col_names = list(schema_table["columns"].keys())
    # col^New normalized to col_new
    assert "col_new" in col_names
    assert "col2" in col_names
    assert "col^New" not in col_names


def test_object_dynamic_table_mixed_case_normalized(extract_step: Extract) -> None:
    """Dynamic table names with mixed case are normalized in the schema."""

    @dlt.resource(name="dynamic_res")
    def dynamic_resource():
        yield dlt.mark.with_table_name({"id": 1}, "MyTable")
        yield dlt.mark.with_table_name({"id": 2}, "AnotherTable")

    source = DltSource(dlt.Schema("object_test"), "module", [dynamic_resource])
    extract_step.extract(source, 20, 1)

    table_names = list(source.schema.tables.keys())
    assert "my_table" in table_names
    assert "another_table" in table_names
    assert "MyTable" not in table_names
    assert "AnotherTable" not in table_names
    # dynamically dispatched tables are not variants - they carry no variant_name
    assert "variant_name" not in source.schema.tables["my_table"]
    assert "variant_name" not in source.schema.tables["another_table"]


def _mark_seen_data(schema: dlt.Schema, *table_names: str) -> None:
    # simulate a completed prior run so tables are treated as existing tables that have seen data
    for table_name in table_names:
        schema.tables[table_name].setdefault("x-normalizer", {})["seen-data"] = True


def _extract_resource(
    extract_step: Extract, schema: dlt.Schema, resource: DltResource
) -> Dict[str, Any]:
    """Extract a single resource into the shared `schema` (mirrors the per-run extraction) and
    return its per-table writer metrics (table name -> DataWriterMetrics)."""
    source = DltSource(schema, "module", [resource])
    load_id = extract_step.extract_storage.create_load_package(schema)
    extract_step._extract_single_source(load_id, source, max_parallel_items=5, workers=1)
    table_metrics: Dict[str, Any] = extract_step._step_info_metrics(load_id)[0]["table_metrics"]
    extract_step.extract_storage.commit_new_load_package(load_id, schema)
    for extractor in extract_step._last_extractors.values():
        assert_written_tables_are_computed(extractor)
    return table_metrics


@pytest.mark.parametrize(
    "table_name,expected",
    [("items", "items"), ("MyItems", "my_items")],
    ids=["table_is_resource_name", "table_differs_from_resource_name"],
)
def test_handle_empty_tables_refreshes_static_write_disposition(
    extract_step: Extract, table_name: str, expected: str
) -> None:
    schema = dlt.Schema("empty_tables")

    # a column declared via the decorator makes the table complete, so it survives the
    # `seen_data_only` filter once data has been seen
    @dlt.resource(
        name="items",
        table_name=table_name,
        write_disposition="replace",
        primary_key="id",
        columns=[
            {"name": "id", "data_type": "bigint"},
            {"name": "value", "data_type": "text", "cluster": True},
        ],
    )
    def items_replace(data: Any) -> Any:
        yield from data

    # first run with data creates the table
    _extract_resource(extract_step, schema, items_replace([{"Id": 1}]))
    _mark_seen_data(schema, expected)
    items_table = schema.tables[expected]
    items_table["x-custom"] = "keep-me"  # type: ignore[typeddict-unknown-key]
    assert items_table["write_disposition"] == "replace"
    assert items_table["columns"]["id"]["primary_key"] is True
    # the single root table is computed from data and received items
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == {expected}
    assert object_extractor.tables_with_items == {expected}
    assert object_extractor.tables_with_empty == set()

    # a replace table that yields no data still gets an empty file (so it is truncated): it appears
    # in the writer metrics with zero items
    metrics = _extract_resource(extract_step, schema, items_replace([]))
    assert metrics[expected].items_count == 0
    # an empty run computes nothing and writes no items; the empty file came from
    # `_handle_empty_tables`, not the extractor
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == set()
    assert object_extractor.tables_with_items == set()
    assert object_extractor.tables_with_empty == set()

    # switch to scd2 with non-normalized validity columns, no data, primary key not redeclared
    @dlt.resource(
        name="items",
        table_name=table_name,
        write_disposition={
            "disposition": "merge",
            "strategy": "scd2",
            "validity_column_names": ["ValidFrom", "ValidTo"],
        },
    )
    def items_merge() -> Any:
        yield from []

    # a non-replace table that yields no data must NOT get an empty file - it is absent from metrics
    metrics = _extract_resource(extract_step, schema, items_merge())
    assert expected not in metrics

    # write disposition refreshed from the resource, with the full scd2 merge config applied
    assert items_table["write_disposition"] == "merge"
    assert items_table["x-merge-strategy"] == "scd2"
    # scd2 validity columns were added by the merge config, normalized like on the data path
    assert "valid_from" in items_table["columns"]
    assert "valid_to" in items_table["columns"]
    assert "ValidFrom" not in items_table["columns"]
    # existing hints survive the isolated disposition update
    assert items_table["columns"]["id"]["primary_key"] is True
    assert items_table["columns"]["id"]["data_type"] == "bigint"
    assert items_table["columns"]["value"]["cluster"] is True
    assert items_table["x-custom"] == "keep-me"


def test_handle_empty_tables_variant_pseudo_root_no_cascade(extract_step: Extract) -> None:
    """Nested hints that break nesting (a primary key) create a pseudo-root table - both for the
    default table and for a variant. On an empty run every table is refreshed from the current
    hints: the default root from the resource hints, the variant (declared with a non-normalized
    name) from its own variant hints keyed by the raw name, and each pseudo-root by re-deriving its
    write disposition from the parent's nested hints. Re-deriving reads the parent's nested hints
    only (it never recomputes a pseudo-root as a root), so no spurious cascade tables are created.
    """
    schema = dlt.Schema("empty_tables")

    def make_resource(
        wd: TWriteDisposition,
        variant_wd: TWriteDisposition,
        nested_wd: TWriteDisposition,
        data: Any,
    ) -> DltResource:
        # a column makes the root complete (so it survives the seen_data filter); the nested hint's
        # primary key breaks nesting into a pseudo-root that carries its own disposition
        @dlt.resource(
            name="items",
            write_disposition=wd,
            columns=[{"name": "id", "data_type": "bigint"}],
            nested_hints={
                "SubItems": make_nested_hints(
                    primary_key="Id",
                    write_disposition=nested_wd,
                    columns=[{"name": "id", "data_type": "bigint"}],
                )
            },
        )
        def items() -> Any:
            yield from data

        # the variant has a non-normalized name and its own write disposition; it inherits the
        # (nesting-breaking) nested hints
        items.apply_hints(
            table_name="OtherItems", write_disposition=variant_wd, create_table_variant=True
        )
        return items

    # the four tables exercised below: the default root + its pseudo-root, and the variant root +
    # its pseudo-root
    roots = ["items", "other_items"]
    pseudo_roots = ["items__sub_items", "other_items__sub_items"]
    all_tables = roots + pseudo_roots

    # run 1: replace with data for both the default table and the variant creates all four tables
    # (each root plus its broken-out, primary-keyed pseudo-root)
    seed = [
        {"Id": 1, "SubItems": [{"Id": 101}]},
        dlt.mark.with_table_name({"Id": 2, "SubItems": [{"Id": 102}]}, "OtherItems"),
    ]
    _extract_resource(extract_step, schema, make_resource("replace", "replace", "replace", seed))
    _mark_seen_data(schema, *all_tables)
    for pseudo in pseudo_roots:
        assert is_nested_table(schema.tables[pseudo]) is False
    for table in all_tables:
        assert schema.tables[table]["write_disposition"] == "replace"
    # both roots and their broken-out pseudo-roots are computed from data; only the roots receive
    # items at extract time
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == set(all_tables)
    assert object_extractor.tables_with_items == set(roots)
    assert object_extractor.tables_with_empty == set()

    # run 2: replace with no data - every table (roots and pseudo-roots) is replace, so each gets an
    # empty file written (so it is truncated), and no spurious cascade table is created
    metrics = _extract_resource(
        extract_step, schema, make_resource("replace", "replace", "replace", [])
    )
    for table in all_tables:
        assert metrics[table].items_count == 0
    assert "items__sub_items__sub_items" not in schema.tables
    assert "other_items__sub_items__sub_items" not in schema.tables
    # an empty run computes nothing and writes no items - the empty files came from
    # `_handle_empty_tables`
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == set()
    assert object_extractor.tables_with_items == set()
    assert object_extractor.tables_with_empty == set()

    # run 3: default root -> append, variant -> merge, and the nested hint flips replace -> merge,
    # all with no data. nothing is replace, so no table is truncated, but every disposition is
    # refreshed from the current hints - including each pseudo-root, re-derived from the nested hints
    metrics = _extract_resource(extract_step, schema, make_resource("append", "merge", "merge", []))
    for table in all_tables:
        assert table not in metrics
    # the default root from the resource hints, the variant from its own (raw-name-keyed) hints -
    # a normalized lookup would miss the variant and wrongly fall back to the resource root's append
    assert schema.tables["items"]["write_disposition"] == "append"
    assert schema.tables["other_items"]["write_disposition"] == "merge"
    # each pseudo-root is re-derived from the parent's nested hints: replace -> merge
    for pseudo in pseudo_roots:
        assert schema.tables[pseudo]["write_disposition"] == "merge"
    # re-deriving reads the parent's nested hints only, never recomputing a pseudo-root as a root,
    # so no spurious cascade tables are created
    assert "items__sub_items__sub_items" not in schema.tables
    assert "other_items__sub_items__sub_items" not in schema.tables


@pytest.mark.parametrize("dispatch", ["marked", "dynamic"])
def test_handle_empty_tables_updates_dispatched_tables(
    extract_step: Extract, dispatch: str
) -> None:
    """Event-dispatch tables - created via with_table_name marks or a dynamic table_name function -
    have their (static) write disposition refreshed on an empty run, and a replace table that gets
    no data has an empty file written so it is truncated."""
    schema = dlt.Schema("empty_tables")

    def make_resource(wd: TWriteDisposition, data: Any) -> DltResource:
        # a declared column makes the dispatched tables complete so they survive the seen_data filter
        if dispatch == "marked":

            @dlt.resource(
                name="events", write_disposition=wd, columns=[{"name": "id", "data_type": "bigint"}]
            )
            def events() -> Any:
                for d in data:
                    yield dlt.mark.with_table_name(d, d["kind"])

        else:

            @dlt.resource(
                name="events",
                table_name=lambda e: e["kind"],
                write_disposition=wd,
                columns=[{"name": "id", "data_type": "bigint"}],
            )
            def events() -> Any:
                yield from data

        return events()

    tables = ["my_issue", "my_purchase"]

    # run 1: replace with data creates both dispatched tables
    seed = [{"kind": "MyIssue", "id": 1}, {"kind": "MyPurchase", "id": 2}]
    _extract_resource(extract_step, schema, make_resource("replace", seed))
    _mark_seen_data(schema, *tables)
    for table in tables:
        assert schema.tables[table]["write_disposition"] == "replace"
        # dispatched tables are not variants - they carry no variant_name
        assert "variant_name" not in schema.tables[table]
    # both dispatched tables were computed from data and received items
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == set(tables)
    assert object_extractor.tables_with_items == set(tables)
    assert object_extractor.tables_with_empty == set()

    # run 2 (totally empty): every replace table gets an empty file (so it is truncated)
    metrics = _extract_resource(extract_step, schema, make_resource("replace", []))
    for table in tables:
        assert metrics[table].items_count == 0

    # run 3 (only one table gets data): the table with data is loaded normally, the other still gets
    # an empty file
    metrics = _extract_resource(
        extract_step, schema, make_resource("replace", [{"kind": "MyIssue", "id": 3}])
    )
    assert metrics["my_issue"].items_count == 1
    assert metrics["my_purchase"].items_count == 0
    # only the table that received an item is computed and tracked with items; the truncated
    # `my_purchase` got its empty file from `_handle_empty_tables`, not the extractor, so it is in
    # neither set
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == {"my_issue"}
    assert object_extractor.tables_with_items == {"my_issue"}
    assert object_extractor.tables_with_empty == set()

    # run 4: switch to append with no data - tables are refreshed to append and NOT truncated
    metrics = _extract_resource(extract_step, schema, make_resource("append", []))
    for table in tables:
        assert table not in metrics
        assert schema.tables[table]["write_disposition"] == "append"


@pytest.mark.parametrize("with_hints", [False, True], ids=["resource_columns", "import_hints"])
def test_extractor_tables_tracked_file_import(extract_step: Extract, with_hints: bool) -> None:
    """Imported files should have their schemas computed and be marked as containing items"""
    columns: List[TColumnSchema] = [
        {"name": "id", "data_type": "bigint", "nullable": False},
        {"name": "name", "data_type": "text"},
        {"name": "description", "data_type": "text"},
        {"name": "ordered_at", "data_type": "date"},
        {"name": "price", "data_type": "decimal"},
    ]
    import_file = "tests/load/cases/loading/header.jsonl"

    if with_hints:

        @dlt.resource(name="imported")
        def imported() -> Any:
            yield dlt.mark.with_file_import(
                import_file, "jsonl", 2, hints=dlt.mark.make_hints(columns=columns)
            )

    else:

        @dlt.resource(name="imported", columns=columns)
        def imported() -> Any:
            yield dlt.mark.with_file_import(import_file, "jsonl", 2)

    source = DltSource(dlt.Schema("file_import"), "module", [imported()])
    extract_step.extract(source, 20, 1)

    object_extractor = extract_step._last_extractors["object"]
    # the imported table is tracked with items (via `_import_item`) and was computed
    assert object_extractor.computed_tables == {"imported"}
    assert object_extractor.tables_with_items == {"imported"}
    assert object_extractor.tables_with_empty == set()
    assert_written_tables_are_computed(object_extractor)


def test_handle_empty_tables_ignores_dynamic_write_disposition(extract_step: Extract) -> None:
    """Resources whose write disposition is computed from data are ignored: the disposition cannot
    be known without data, so the schema is left unchanged when no data is yielded."""
    schema = dlt.Schema("empty_tables")

    def make_resource(data: Any) -> DltResource:
        @dlt.resource(
            name="events",
            table_name=lambda e: e["kind"],
            write_disposition=lambda e: e["wd"],
        )
        def events() -> Any:
            yield from data

        return events()

    _extract_resource(
        extract_step, schema, make_resource([{"kind": "MyIssue", "wd": "replace", "id": 1}])
    )
    _mark_seen_data(schema, "my_issue")
    assert schema.tables["my_issue"]["write_disposition"] == "replace"

    # no data: dynamic write disposition cannot be resolved, so the table is left untouched and is
    # not truncated (no empty file written)
    metrics = _extract_resource(extract_step, schema, make_resource([]))
    assert "my_issue" not in metrics
    assert schema.tables["my_issue"]["write_disposition"] == "replace"


@pytest.mark.parametrize(
    "resource_wd,member_kind,member_wd,root_truncated,member_truncated",
    [
        # a replace resource truncates the root, and a member only if it also accepts replace
        ("replace", "variant", "replace", True, True),
        ("replace", "variant", "merge", True, False),
        ("replace", "pseudo", "replace", True, True),
        ("replace", "pseudo", "merge", True, False),
        # an append resource never truncates - not even a replace variant or pseudo-root
        ("append", "variant", "replace", False, False),
        ("append", "pseudo", "replace", False, False),
    ],
    ids=[
        "replace_resource-replace_variant",
        "replace_resource-merge_variant",
        "replace_resource-replace_pseudo",
        "replace_resource-merge_pseudo",
        "append_resource-replace_variant",
        "append_resource-replace_pseudo",
    ],
)
def test_handle_empty_tables_skips_tables_not_accepting_replace(
    extract_step: Extract,
    resource_wd: TWriteDisposition,
    member_kind: str,
    member_wd: TWriteDisposition,
    root_truncated: bool,
    member_truncated: bool,
) -> None:
    """An empty file (truncation) is written only when BOTH the resource and the table accept
    replace. A replace resource does not truncate a variant or pseudo-root whose own write
    disposition is not replace; an append resource never truncates, not even a replace variant or
    pseudo-root."""
    schema = dlt.Schema("empty_tables")
    member_table = "other_items" if member_kind == "variant" else "items__sub_items"

    def make_resource(data: Any) -> DltResource:
        nested: Optional[Dict[TTableNames, TResourceNestedHints]] = (
            {
                "SubItems": make_nested_hints(
                    primary_key="id",
                    write_disposition=member_wd,
                    columns=[{"name": "id", "data_type": "bigint"}],
                )
            }
            if member_kind == "pseudo"
            else None
        )

        @dlt.resource(
            name="items",
            write_disposition=resource_wd,
            columns=[{"name": "id", "data_type": "bigint"}],
            nested_hints=nested,
        )
        def items() -> Any:
            yield from data

        if member_kind == "variant":
            # a variant with its own write disposition, declared with a non-normalized name
            items.apply_hints(
                table_name="OtherItems", write_disposition=member_wd, create_table_variant=True
            )
        return items

    if member_kind == "variant":
        seed: Any = [{"id": 1}, dlt.mark.with_table_name({"id": 2}, "OtherItems")]
    else:
        seed = [{"id": 1, "SubItems": [{"id": 11}]}]

    # run 1: create the root and the member table
    _extract_resource(extract_step, schema, make_resource(seed))
    _mark_seen_data(schema, "items", member_table)
    # simulate a schema created before `variant_name`: lookup must not need the hint
    schema.tables[member_table].pop("variant_name", None)

    # run 2: empty - only tables where the resource and the table both accept replace are truncated
    metrics = _extract_resource(extract_step, schema, make_resource([]))
    assert ("items" in metrics) is root_truncated
    assert (member_table in metrics) is member_truncated


def test_handle_empty_tables_variant_not_redeclared_left_untouched(extract_step: Extract) -> None:
    """A variant not re-declared on the empty run can't be re-derived, so truncation falls back to the
    stored disposition - both for the variant and for a pseudo-root broken out under it: a stored-merge
    variant is left untouched, a stored-replace variant (and its replace pseudo-root) are truncated.
    """
    schema = dlt.Schema("empty_tables")

    def make_resource(declare_variants: bool, data: Any) -> DltResource:
        @dlt.resource(
            name="items",
            write_disposition="replace",
            columns=[{"name": "id", "data_type": "bigint"}],
        )
        def items() -> Any:
            if declare_variants:
                # a merge variant declared inside the generator (only when there is data)
                yield dlt.mark.with_hints(
                    {"id": 1},
                    make_hints(table_name="OtherItems", write_disposition="merge"),
                    create_table_variant=True,
                )
                # a replace variant carrying its own primary-keyed nested hint -> a replace pseudo-root
                yield dlt.mark.with_hints(
                    {"id": 2, "SubItems": [{"id": 21}]},
                    make_hints(
                        table_name="ReplaceItems",
                        write_disposition="replace",
                        nested_hints={
                            "SubItems": make_nested_hints(
                                primary_key="id",
                                write_disposition="replace",
                                columns=[{"name": "id", "data_type": "bigint"}],
                            )
                        },
                    ),
                    create_table_variant=True,
                )
            yield from data

        return items

    # run 1: both variants (and the replace pseudo-root under the replace variant) are registered with
    # data; the replace root gets data too
    _extract_resource(extract_step, schema, make_resource(True, [{"id": 3}]))
    _mark_seen_data(schema, "items", "other_items", "replace_items", "replace_items__sub_items")
    assert schema.tables["other_items"]["write_disposition"] == "merge"
    assert schema.tables["replace_items"]["write_disposition"] == "replace"
    assert schema.tables["replace_items__sub_items"]["write_disposition"] == "replace"
    # the broken-out table is a pseudo-root, not a nested table
    assert is_nested_table(schema.tables["replace_items__sub_items"]) is False

    # run 2: empty - no variant is re-declared, so none is in `_hints_variants`; the decision falls
    # back to the stored disposition
    metrics = _extract_resource(extract_step, schema, make_resource(False, []))
    # the stored-merge variant differs from the replace resource -> left untouched, not truncated
    assert "other_items" not in metrics
    assert schema.tables["other_items"]["write_disposition"] == "merge"
    # the stored-replace variant is truncated even though it cannot be re-derived
    assert metrics["replace_items"].items_count == 0
    # the replace pseudo-root under the (now absent) variant cannot be re-derived either, yet is
    # truncated because its stored disposition is replace
    assert metrics["replace_items__sub_items"].items_count == 0
    # the replace root is truncated
    assert metrics["items"].items_count == 0


def test_handle_empty_tables_refresh_changed_to_replace_truncates(extract_step: Extract) -> None:
    schema = dlt.Schema("empty_tables")

    def make_resource(wd: TWriteDisposition, data: Any) -> DltResource:
        @dlt.resource(
            name="items", write_disposition=wd, columns=[{"name": "id", "data_type": "bigint"}]
        )
        def items() -> Any:
            yield from data

        return items()

    # run 1: append with data - the table is stored with the (stale) append disposition
    _extract_resource(extract_step, schema, make_resource("append", [{"id": 1}]))
    _mark_seen_data(schema, "items")
    assert schema.tables["items"]["write_disposition"] == "append"

    # run 2: the resource switches to replace and yields no data. the table is not computed this
    # run, so the refresh runs, flips the stored disposition to replace, and the table is truncated
    metrics = _extract_resource(extract_step, schema, make_resource("replace", []))
    assert metrics["items"].items_count == 0
    assert schema.tables["items"]["write_disposition"] == "replace"


def test_handle_empty_tables_unseen_data_not_truncated(extract_step: Extract) -> None:
    schema = dlt.Schema("empty_tables")

    @dlt.resource(
        name="items", write_disposition="replace", columns=[{"name": "id", "data_type": "bigint"}]
    )
    def items(data: Any) -> Any:
        yield from data

    # run 1 creates the (complete) table but we deliberately do NOT mark it as having seen data
    _extract_resource(extract_step, schema, items([{"id": 1}]))
    assert "items" in schema.tables

    # run 2: empty - the table never saw data, so it is not truncated (no empty file)
    metrics = _extract_resource(extract_step, schema, items([]))
    assert "items" not in metrics

    # once the table has seen data, an empty run truncates it
    _mark_seen_data(schema, "items")
    metrics = _extract_resource(extract_step, schema, items([]))
    assert metrics["items"].items_count == 0


def test_handle_empty_tables_materialized_empty_written_unconditionally(
    extract_step: Extract,
) -> None:
    """`materialize_table_schema()` always writes an empty file - even for an append resource and
    even when never seen data - while a resource that yields nothing at all writes none."""
    schema = dlt.Schema("empty_tables")

    # append (NOT replace) resource that only materializes the table schema, never seen data
    @dlt.resource(
        name="materialized",
        write_disposition="append",
        columns=[{"name": "id", "data_type": "bigint"}],
    )
    def materialized() -> Any:
        yield dlt.mark.materialize_table_schema()

    metrics = _extract_resource(extract_step, schema, materialized())
    # empty file written despite append disposition and no prior seen data
    assert metrics["materialized"].items_count == 0
    object_extractor = extract_step._last_extractors["object"]
    assert object_extractor.computed_tables == {"materialized"}
    assert object_extractor.tables_with_items == set()
    assert object_extractor.tables_with_empty == {"materialized"}

    # contrast: an append resource that yields nothing at all writes no empty file
    @dlt.resource(
        name="nothing", write_disposition="append", columns=[{"name": "id", "data_type": "bigint"}]
    )
    def nothing() -> Any:
        yield from []

    metrics = _extract_resource(extract_step, schema, nothing())
    assert "nothing" not in metrics


def test_handle_empty_tables_nested_child_not_truncated(extract_step: Extract) -> None:
    schema = dlt.Schema("empty_tables")

    def make_resource(data: Any) -> DltResource:
        @dlt.resource(
            name="items",
            write_disposition="replace",
            columns=[{"name": "id", "data_type": "bigint"}],
            # no primary key -> `items__children` stays a real nested table (has a parent)
            nested_hints={
                "children": make_nested_hints(columns=[{"name": "cid", "data_type": "bigint"}])
            },
        )
        def items() -> Any:
            yield from data

        return items()

    # run 1: root + genuinely nested child
    _extract_resource(extract_step, schema, make_resource([{"id": 1, "children": [{"cid": 11}]}]))
    assert is_nested_table(schema.tables["items__children"]) is True
    _mark_seen_data(schema, "items", "items__children")

    # run 2: empty - only the root gets an empty file; the nested child is not truncated directly
    metrics = _extract_resource(extract_step, schema, make_resource([]))
    assert metrics["items"].items_count == 0
    assert "items__children" not in metrics


def test_handle_empty_tables_pseudo_root_refreshed_then_truncated(extract_step: Extract) -> None:
    """A pseudo-root is re-derived from the current nested hints on an empty run: when the nested hint
    flips merge -> replace, the stored pseudo-root is updated to replace and then truncated."""
    schema = dlt.Schema("empty_tables")

    def make_resource(nested_wd: TWriteDisposition, data: Any) -> DltResource:
        @dlt.resource(
            name="items",
            write_disposition="replace",
            columns=[{"name": "id", "data_type": "bigint"}],
            nested_hints={
                "SubItems": make_nested_hints(
                    primary_key="id",
                    write_disposition=nested_wd,
                    columns=[{"name": "id", "data_type": "bigint"}],
                )
            },
        )
        def items() -> Any:
            yield from data

        return items()

    pseudo = "items__sub_items"
    # run 1: the pseudo-root is created with a merge disposition
    _extract_resource(
        extract_step, schema, make_resource("merge", [{"id": 1, "SubItems": [{"id": 11}]}])
    )
    _mark_seen_data(schema, "items", pseudo)
    assert schema.tables[pseudo]["write_disposition"] == "merge"

    # run 2: the nested hint now declares replace; on the empty run the pseudo-root is re-derived,
    # its stored disposition flips merge -> replace, and it is then truncated
    metrics = _extract_resource(extract_step, schema, make_resource("replace", []))
    assert schema.tables[pseudo]["write_disposition"] == "replace"
    assert metrics[pseudo].items_count == 0
