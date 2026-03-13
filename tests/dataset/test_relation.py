import sys
import pathlib
from typing import Any

import pytest
from sqlglot import expressions as sge

import dlt
from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.dataset.dataset import _get_load_ids, _get_latest_load_id

# TODO move destination-independent tests from `test_read_interfaces.py` to this module

USERS_DATA_0 = [
    {
        "id": 1,
        "name": "Alice",
        "orders": [
            {"order_id": 101, "amount": 100.0, "items": [{"item": "A"}, {"item": "B"}]},
            {"order_id": 102, "amount": 200.0, "items": [{"item": "C"}]},
        ],
    },
    {
        "id": 2,
        "name": "Bob",
        "orders": [{"order_id": 103, "amount": 150.0, "items": [{"item": "D"}]}],
    },
]

USERS_DATA_1 = [
    {
        "id": 3,
        "name": "Charlie",
        "orders": [{"order_id": 104, "amount": 300.0, "items": [{"item": "E"}]}],
    }
]

PRODUCTS_DATA_0 = [{"product_id": 1, "name": "Widget"}, {"product_id": 2, "name": "Gadget"}]
PRODUCTS_DATA_1 = [{"product_id": 3, "name": "Doohickey"}]


@dlt.source(root_key=False)
def crm(i: int = 0):
    @dlt.resource
    def users(i: int):
        if i == 0:
            yield USERS_DATA_0
        elif i == 1:
            yield USERS_DATA_1

    @dlt.resource
    def products(i: int):
        if i == 0:
            yield PRODUCTS_DATA_0
        elif i == 1:
            yield PRODUCTS_DATA_1

    return [users(i), products(i)]


LOAD_0_STATS = {
    "users": len(USERS_DATA_0),
    "products": len(PRODUCTS_DATA_0),
    "users__orders": sum(len(user["orders"]) for user in USERS_DATA_0),  # type: ignore[misc,arg-type]
    "users__orders__items": sum(
        len(order["items"]) for user in USERS_DATA_0 for order in user["orders"]  # type: ignore[misc,attr-defined]
    ),
}
LOAD_1_STATS = {
    "users": len(USERS_DATA_1),
    "products": len(PRODUCTS_DATA_1),
    "users__orders": sum(len(user["orders"]) for user in USERS_DATA_1),  # type: ignore[misc,arg-type]
    "users__orders__items": sum(
        len(order["items"]) for user in USERS_DATA_1 for order in user["orders"]  # type: ignore[misc,attr-defined]
    ),
}


TLoadsFixture = tuple[dlt.Dataset, tuple[str, str], tuple[dict[str, Any], dict[str, Any]]]


@pytest.fixture(scope="module")
def loads_with_root_key(module_tmp_path: pathlib.Path) -> TLoadsFixture:
    """Create a pipeline with nested data across multiple loads."""
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
    """Create a pipeline with nested data across multiple loads."""
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


# params= sets the default value for tests not specifying
@pytest.fixture(params=["with_root_key"])
def dataset_with_loads(
    request: pytest.FixtureRequest,
    loads_with_root_key: TLoadsFixture,
    loads_without_root_key: TLoadsFixture,
) -> TLoadsFixture:
    """Router fixture for indirect parametrization of dataset fixtures."""
    if request.param == "with_root_key":
        return loads_with_root_key
    elif request.param == "without_root_key":
        return loads_without_root_key
    else:
        raise ValueError(f"Unknown dataset fixture: {request.param}")


@pytest.fixture(scope="module")
def dataset() -> dlt.Dataset:
    @dlt.resource
    def purchases():
        yield from (
            {"id": 1, "name": "alice", "city": "berlin"},
            {"id": 2, "name": "bob", "city": "paris"},
            {"id": 3, "name": "charlie", "city": "barcelona"},
        )

    pipeline = dlt.pipeline(
        "_relation_to_ibis", destination="duckdb", full_refresh=True, dev_mode=True
    )
    pipeline.run([purchases])
    return pipeline.dataset()


@pytest.fixture
def purchases(dataset: dlt.Dataset) -> dlt.Relation:
    purchases = dataset.table("purchases")
    assert isinstance(purchases, dlt.Relation)
    return purchases


def _set_name_normalizer_on_schema(schema: dlt.Schema, name_normalizer_ref: str) -> None:
    schema._normalizers_config["names"] = name_normalizer_ref
    schema.update_normalizers()


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason=f"Skipping tests for Python `{sys.version_info}`. Ibis only supports Python >= 3.10.",
)
def test_sql_relation_to_ibis(dataset: dlt.Dataset) -> None:
    """Call `.to_ibis()` on a `dlt.Relation` defined by an SQL query"""
    from ibis import ir

    purchases = dataset.query("SELECT * FROM purchases")
    assert isinstance(purchases, dlt.Relation)

    table = purchases.to_ibis()
    assert isinstance(table, ir.Table)
    # executes without error
    table.execute()


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason=f"Skipping tests for Python `{sys.version_info}`. Ibis only supports Python >= 3.10.",
)
def test_base_relation_to_ibis(purchases: dlt.Relation) -> None:
    """Call `.to_ibis()` on a `dlt.Relation` defined by an existing table name"""
    from ibis import ir

    table = purchases.to_ibis()
    assert isinstance(table, ir.Table)
    # executes without error
    table.execute()


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason=f"Skipping tests for Python `{sys.version_info}`. Ibis only supports Python >= 3.10.",
)
def test_transformed_relation_to_ibis_(purchases: dlt.Relation) -> None:
    """Call `.to_ibis()` on a `dlt.Relation` that was transformed by methods"""
    from ibis import ir

    table = purchases.where("id", "gt", 2).select("name").to_ibis()
    assert isinstance(table, ir.Table)
    # executes without error
    table.execute()


def test_dataset_load_ids(dataset_with_loads: TLoadsFixture):
    dataset, load_ids, _ = dataset_with_loads

    retrieved_load_ids = _get_load_ids(dataset)

    assert tuple(retrieved_load_ids) == load_ids
    assert isinstance(retrieved_load_ids, list)
    assert all(isinstance(load_id, str) for load_id in retrieved_load_ids)
    assert len(retrieved_load_ids) == 2

    assert _get_load_ids(dataset) == dataset.load_ids()


def test_dataset_latest_load_id(dataset_with_loads: TLoadsFixture):
    dataset, load_ids, _ = dataset_with_loads

    load_id = _get_latest_load_id(dataset)

    assert isinstance(load_id, str)
    assert load_id == load_ids[-1]
    assert _get_latest_load_id(dataset) == dataset.latest_load_id()


@pytest.mark.parametrize("selected_load_id_idx", [[0], [1], [0, 1]])
def test_dataset_access_equivalent_relation_access(
    dataset_with_loads: TLoadsFixture,
    selected_load_id_idx: list[int],
) -> None:
    dataset, load_ids, _ = dataset_with_loads
    selected_load_ids = [load_ids[idx] for idx in selected_load_id_idx]

    dataset_output = dataset.table("users", load_ids=selected_load_ids)
    relation_output = dataset.table("users").from_loads(selected_load_ids)

    assert dataset_output._sqlglot_expression == relation_output._sqlglot_expression


@pytest.mark.parametrize("table_name", ["products", "users__orders", "users__orders__items"])
@pytest.mark.parametrize(
    "dataset_with_loads",
    [
        pytest.param("with_root_key", id="root_key-True"),
        pytest.param("without_root_key", id="root_key-False"),
    ],
    indirect=True,
)
def test_relation_with_load_id(
    dataset_with_loads: TLoadsFixture,
    table_name: str,
) -> None:
    """Test filtering a root table with a single load_id string."""
    dataset, load_ids, load_stats = dataset_with_loads
    table = dataset.table(table_name)
    expected_columns = (
        table.columns if C_DLT_LOAD_ID in table.columns else table.columns + [C_DLT_LOAD_ID]
    )

    output = dataset.table(table_name).with_load_id_col()

    assert isinstance(output, dlt.Relation)
    assert output.columns == expected_columns

    df = output.df()

    assert len(df) == len(table.df())
    assert list(df.columns) == expected_columns


@pytest.mark.parametrize("selected_load_id_idx", [[0], [1], [0, 1]])
@pytest.mark.parametrize("table_name", ["products", "users__orders", "users__orders__items"])
@pytest.mark.parametrize("add_load_id_column", [True, False])
@pytest.mark.parametrize(
    "dataset_with_loads",
    [
        pytest.param("with_root_key", id="root_key-True"),
        pytest.param("without_root_key", id="root_key-False"),
    ],
    indirect=True,
)
def test_relation_from_loads(
    dataset_with_loads: TLoadsFixture,
    selected_load_id_idx: list[int],
    add_load_id_column: bool,
    table_name: str,
) -> None:
    """Test filtering a root table with a single load_id string."""
    dataset, load_ids, load_stats = dataset_with_loads
    selected_load_ids = [load_ids[idx] for idx in selected_load_id_idx]
    table = dataset.table(table_name)
    original_columns = table.columns
    if C_DLT_LOAD_ID in original_columns:
        expected_columns = original_columns
    else:
        expected_columns = (
            original_columns + [C_DLT_LOAD_ID] if add_load_id_column else original_columns
        )

    output = table.from_loads(selected_load_ids, add_load_id_column=add_load_id_column)

    assert isinstance(output, dlt.Relation)
    assert output.columns == expected_columns

    df = output.df()

    assert len(df) == sum(load_stats[idx][table_name] for idx in selected_load_id_idx)
    assert list(df.columns) == expected_columns
    if C_DLT_LOAD_ID in expected_columns:
        assert set(df[C_DLT_LOAD_ID]) == set(selected_load_ids)


@pytest.mark.parametrize("selected_load_id_idx", [[0], [1], [0, 1]])
@pytest.mark.parametrize("table_name", ["products", "users__orders", "users__orders__items"])
@pytest.mark.parametrize("add_load_id_column", [True, False])
@pytest.mark.parametrize(
    "dataset_with_loads",
    [
        "with_root_key",
        "without_root_key",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "name_normalizer_ref",
    (
        "tests.common.cases.normalizers.title_case",
        "tests.common.cases.normalizers.sql_upper",
        "tests.common.cases.normalizers.snake_no_x",
    ),
)
def test_relation_from_loads_query(
    dataset_with_loads: TLoadsFixture,
    selected_load_id_idx: list[int],
    add_load_id_column: bool,
    table_name: str,
    name_normalizer_ref: str,
) -> None:
    """Use different naming normalization to check if the internal queries
    properly used normalized ids instead of constants.

    The relation / query isn't executable because the stored data won't
    match the name normalization that we force. Checks are conducted
    against the query itself
    """
    original_dataset, load_ids, _ = dataset_with_loads
    selected_load_ids = [load_ids[idx] for idx in selected_load_id_idx]
    # change normalization; this query won't be executable
    schema = original_dataset.schema.clone()  # copy to avoid mutating the fixture
    schema._normalizers_config["allow_identifier_change_on_table_with_data"] = True
    schema._normalizers_config["names"] = name_normalizer_ref
    schema.update_normalizers()
    dataset = dlt.dataset(
        dataset_name=original_dataset.dataset_name,
        destination=original_dataset._destination_reference,
        schema=schema,
    )
    normalized_table_name = schema.naming.normalize_tables_path(table_name)
    normalized_load_id = schema.naming.normalize_identifier(C_DLT_LOAD_ID)

    rel = dataset.table(normalized_table_name).from_loads(
        selected_load_ids, add_load_id_column=add_load_id_column
    )
    expr = rel._sqlglot_expression
    sql_query = expr.sql()

    assert normalized_table_name in sql_query
    assert all(load_id in sql_query for load_id in selected_load_ids)

    # root tables return star select() when not modifying the selection
    if table_name == "products" and add_load_id_column:
        assert expr.expressions[0] == sge.Star()
    elif C_DLT_LOAD_ID in original_dataset.table(table_name).columns:
        assert any(col.name == normalized_load_id for col in expr.expressions)
    elif add_load_id_column:
        assert any(col.name == normalized_load_id for col in expr.expressions)
    else:
        assert not any(col.name == normalized_load_id for col in expr.expressions)
