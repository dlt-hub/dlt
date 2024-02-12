import pytest

import dlt

from dlt.common.typing import TDataItems
from dlt.common.schema.typing import TCdcConfig
from dlt.common.schema.exceptions import SchemaException
from dlt.extract import DltResource

from tests.pipeline.utils import assert_load_info
from tests.load.pipeline.utils import load_table_counts, select_data
from tests.load.pipeline.utils import destinations_configs, DestinationTestConfiguration


CDC_CONFIGS = [
    {
        "operation_column": "operation",
        "operation_mapper": {"insert": "I", "update": "U", "delete": "D"},
        "sequence_column": "lsn",
    },
    {
        "operation_column": "op",
        "operation_mapper": {"insert": 1, "update": 2, "delete": 3},
        "sequence_column": "commit_id",
    },
]


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
@pytest.mark.parametrize("cdc_config", CDC_CONFIGS)
def test_replicate_core_functionality(
    destination_config: DestinationTestConfiguration,
    cdc_config: TCdcConfig,
) -> None:
    op_col = cdc_config["operation_column"]
    seq_col = cdc_config["sequence_column"]
    op_map = cdc_config["operation_mapper"]
    i = op_map["insert"]
    u = op_map["update"]
    d = op_map["delete"]

    # define batches of CDC data
    batches_simple = [
        [
            {"id": 1, "val": "foo", op_col: i, seq_col: 1},
            {"id": 2, "val": "bar", op_col: i, seq_col: 2},
        ],
        [
            {"id": 1, "val": "foo_new", op_col: i, seq_col: 3},
            {"id": 3, "val": "baz", op_col: i, seq_col: 4},
        ],
        [
            {"id": 2, "val": "bar_new", op_col: u, seq_col: 5},
        ],
        [
            {"id": 4, "val": "foo", op_col: u, seq_col: 6},
        ],
        [
            {"id": 2, op_col: d, seq_col: 7},
            {"id": 2, "val": "bar_new_new", op_col: i, seq_col: 8},
        ],
        [
            {"id": 5, "val": "foo", op_col: i, seq_col: 9},
            {"id": 5, "val": "foo_new", op_col: u, seq_col: 10},
        ],
        [
            {"id": 6, "val": "foo", op_col: i, seq_col: 11},
            {"id": 6, op_col: d, seq_col: 12},
        ],
        [
            {"id": 1, op_col: d, seq_col: 13},
        ],
    ]

    table_name = "test_replicate_core_functionality"

    @dlt.resource(
        table_name=table_name,
        write_disposition="replicate",
        primary_key="id",
        cdc_config=cdc_config,
    )
    def data_resource(batches: TDataItems, batch: int):
        yield batches[batch]

    p = destination_config.setup_pipeline("pl_test_replicate_core_functionality", full_refresh=True)

    # insert keys in a new empty table
    info = p.run(data_resource(batches_simple, batch=0))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2

    # insert a key that already exists (unexpected scenario)
    info = p.run(data_resource(batches_simple, batch=1))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 3

    # update a key that already exists
    info = p.run(data_resource(batches_simple, batch=2))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 3

    # update a key that doesn't exist yet (unexpected scenario)
    info = p.run(data_resource(batches_simple, batch=3))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 4

    # delete an existing key, then insert it again
    info = p.run(data_resource(batches_simple, batch=4))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 4

    # insert a new key, then update it
    info = p.run(data_resource(batches_simple, batch=5))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 5

    # insert a new key, then delete it
    info = p.run(data_resource(batches_simple, batch=6))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 5

    # delete an existing key
    info = p.run(data_resource(batches_simple, batch=7))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 4

    # compare observed records with expected records
    qual_name = p.sql_client().make_qualified_table_name(table_name)
    observed = [
        {"id": row[0], "val": row[1]} for row in select_data(p, f"SELECT id, val FROM {qual_name}")
    ]
    expected = [
        {"id": 2, "val": "bar_new_new"},
        {"id": 3, "val": "baz"},
        {"id": 4, "val": "foo"},
        {"id": 5, "val": "foo_new"},
    ]
    assert sorted(observed, key=lambda d: d["id"]) == expected


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
@pytest.mark.parametrize("cdc_config", [CDC_CONFIGS[0]])
def test_replicate_complex_data(
    destination_config: DestinationTestConfiguration,
    cdc_config: TCdcConfig,
) -> None:
    op_col = cdc_config["operation_column"]
    seq_col = cdc_config["sequence_column"]
    op_map = cdc_config["operation_mapper"]
    i = op_map["insert"]
    u = op_map["update"]
    d = op_map["delete"]

    # define batches of CDC data
    batches_complex = [
        [
            {"id": 1, "val": ["foo", "bar"], op_col: i, seq_col: 1},
            {"id": 2, "val": ["baz"], op_col: i, seq_col: 2},
        ],
        [
            {"id": 1, "val": ["foo", "bar", "baz"], op_col: u, seq_col: 3},
            {"id": 3, "val": ["foo"], op_col: i, seq_col: 4},
        ],
        [
            {"id": 1, op_col: d, seq_col: 5},
            {"id": 4, "val": ["foo", "bar"], op_col: i, seq_col: 6},
        ],
    ]

    table_name = "test_replicate_complex_data"

    @dlt.resource(
        table_name=table_name,
        write_disposition="replicate",
        primary_key="id",
        cdc_config=cdc_config,
    )
    def data_resource(batches: TDataItems, batch: int):
        yield batches[batch]

    # nesting disabled -> no child table
    @dlt.source(max_table_nesting=0)
    def data_nesting_disabled(batches: TDataItems, batch: int) -> DltResource:
        return data_resource(batches, batch)

    p = destination_config.setup_pipeline("pl_test_replicate_complex_data", full_refresh=True)
    info = p.run(data_nesting_disabled(batches_complex, batch=0))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2

    info = p.run(data_nesting_disabled(batches_complex, batch=1))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 3

    info = p.run(data_nesting_disabled(batches_complex, batch=2))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 3

    # nesting enabled -> child table
    @dlt.source(max_table_nesting=1, root_key=True)
    def data_nesting_enabled(batches: TDataItems, batch: int) -> DltResource:
        return data_resource(batches, batch)

    p = destination_config.setup_pipeline("pl_test_replicate_complex_data", full_refresh=True)

    info = p.run(data_nesting_enabled(batches_complex, batch=0))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 2
    assert load_table_counts(p, table_name + "__val")[table_name + "__val"] == 3

    info = p.run(data_nesting_enabled(batches_complex, batch=1))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 3
    assert load_table_counts(p, table_name + "__val")[table_name + "__val"] == 5

    info = p.run(data_nesting_enabled(batches_complex, batch=2))
    assert_load_info(info)
    assert load_table_counts(p, table_name)[table_name] == 3
    assert load_table_counts(p, table_name + "__val")[table_name + "__val"] == 4

    # compare observed records with expected records
    qual_name = p.sql_client().make_qualified_table_name(table_name)
    observed = [row[0] for row in select_data(p, f"SELECT id FROM {qual_name}")]
    expected = [2, 3, 4]
    assert sorted(observed) == expected
    qual_name = p.sql_client().make_qualified_table_name(table_name + "__val")
    observed = [row[0] for row in select_data(p, f"SELECT value FROM {qual_name}")]
    expected = ["bar", "baz", "foo", "foo"]  # type: ignore[list-item]
    assert sorted(observed) == expected


@pytest.mark.parametrize(
    "destination_config", destinations_configs(default_sql_configs=True), ids=lambda x: x.name
)
@pytest.mark.parametrize("cdc_config", [CDC_CONFIGS[0]])
def test_replicate_missing_config(
    destination_config: DestinationTestConfiguration,
    cdc_config: TCdcConfig,
) -> None:
    op_col = cdc_config["operation_column"]
    seq_col = cdc_config["sequence_column"]
    op_map = cdc_config["operation_mapper"]
    i = op_map["insert"]
    u = op_map["update"]

    # define batches of CDC data
    batches_simple = [
        [
            {"id": 1, "val": "foo", op_col: i, seq_col: 1},
            {"id": 2, "val": "bar", op_col: i, seq_col: 2},
        ],
        [
            {"id": 1, "val": "foo_new", op_col: u, seq_col: 3},
            {"id": 3, "val": "baz", op_col: i, seq_col: 4},
        ],
    ]

    @dlt.resource(
        table_name="test_replicate_no_pk",
        write_disposition="replicate",
        cdc_config=cdc_config,
    )
    def data_resource_no_pk(batches: TDataItems, batch: int):
        yield batches[batch]

    # a SchemaException should be raised when using "replicate" and no primary key is specified
    p = destination_config.setup_pipeline("pl_test_replicate_no_pk", full_refresh=True)
    with pytest.raises(SchemaException):
        p.run(data_resource_no_pk(batches_simple, batch=0))

    @dlt.resource(
        table_name="test_replicate_no_cdc_config",
        write_disposition="replicate",
        primary_key="id",
    )
    def data_resource_no_cdc_config(batches: TDataItems, batch: int):
        yield batches[batch]

    # a SchemaException should be raised when using "replicate" and no "cdc_config" is specified
    p = destination_config.setup_pipeline("pl_test_replicate_no_cdc_config", full_refresh=True)
    with pytest.raises(SchemaException):
        p.run(data_resource_no_pk(batches_simple, batch=0))
