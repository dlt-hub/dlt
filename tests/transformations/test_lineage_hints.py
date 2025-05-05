import sqlglot.expressions as sge
from sqlglot.schema import ensure_schema

from dlt.common.schema.typing import COLUMN_PROPS
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.destinations.impl.duckdb.factory import DuckDbTypeMapper
from dlt.transformations import lineage
from dlt.common.libs.sqlglot import to_sqlglot_type


def test_to_sqlglot_type_attaches_hints_to_meta() -> None:
    hints = {
        "name": "col_foo",
        # data type
        "data_type": "bigint",
        "precision": 4,
        "scale": 4,
        "timezone": True,
        "nullable": True,
        "variant": True,
        # hints
        "partition": True,
        "cluster": True,
        "primary_key": True,
        "sort": True,
        "unique": True,
        "merge_key": True,
        "row_key": True,
        "parent_key": True,
        "root_key": True,
        "hard_delete": True,
        "dedup_sort": "asc",
    }
    assert all(prop in COLUMN_PROPS for prop in hints.keys())

    sqlglot_type = lineage.to_sqlglot_type(
        column=hints,
        table={"columns": hints},
        type_mapper=DuckDbTypeMapper(DestinationCapabilitiesContext.generic_capabilities()),
        dialect="duckdb",
    )

    # check props in metadata attached via `DataType.meta`
    for prop, value in hints.items():
        # this metadata is not attached to the DataType.meta
        if prop in ["name", "data_type"]:
            continue

        assert sqlglot_type.meta[prop] == value


# def test_to_sqlglot_type_precision() -> None:


#     # data_type can be mapped back
#     assert lineage.SQLGLOT_TO_DLT_TYPE_MAP[sqlglot_type.this] == hints["data_type"]


def test_compute_columns_schema_noop():
    column_props = {
        "name": "col_foo",
        # data type
        "data_type": "bigint",
        "precision": 4,
        "scale": 4,
        "timezone": True,
        "nullable": True,
        "variant": True,
        # hints
        "partition": True,
        "cluster": True,
        "primary_key": True,
        "sort": True,
        "unique": True,
        "merge_key": True,
        "row_key": True,
        "parent_key": True,
        "root_key": True,
        "hard_delete": True,
        "dedup_sort": "asc",
    }
    assert all(prop in COLUMN_PROPS for prop in column_props.keys())
    table_name = "table"
    sqlglot_schema = ensure_schema(
        {
            "db": {
                table_name: {
                    "col_foo": lineage.to_sqlglot_type(
                        column=column_props,
                        table={"columns": column_props},
                        type_mapper=DuckDbTypeMapper(
                            DestinationCapabilitiesContext.generic_capabilities()
                        ),
                        dialect="duckdb",
                    )
                }
            }
        }
    )

    computed_dlt_schema = lineage.compute_columns_schema(
        sql_query=f"SELECT * FROM {table_name}",
        sqlglot_schema=sqlglot_schema,
        dialect="duckdb",
    )

    assert computed_dlt_schema == {column_props["name"]: column_props}


def test_propagate_hints():
    column_props = {
        "name": "col_foo",
        # data type
        "data_type": "bigint",
        "precision": 4,
        "scale": 4,
        "timezone": True,
        "nullable": True,
        "variant": True,
        # hints
        "partition": True,
        "cluster": True,
        "primary_key": True,
        "sort": True,
        "unique": True,
        "merge_key": True,
        "row_key": True,
        "parent_key": True,
        "root_key": True,
        "hard_delete": True,
        "dedup_sort": "asc",
    }
    assert all(prop in COLUMN_PROPS for prop in column_props.keys())
    table_name = "table"
    sqlglot_schema = ensure_schema(
        {
            "db": {
                table_name: {
                    "col_foo": lineage.to_sqlglot_type(
                        column=column_props,
                        table={"columns": column_props},
                        type_mapper=DuckDbTypeMapper(
                            DestinationCapabilitiesContext.generic_capabilities()
                        ),
                        dialect="duckdb",
                    )
                }
            }
        }
    )

    computed_dlt_schema = lineage.compute_columns_schema(
        sql_query=f"SELECT col_foo + 1 as summed FROM {table_name}",
        sqlglot_schema=sqlglot_schema,
        dialect="duckdb",
    )

    assert computed_dlt_schema == {column_props["name"]: column_props}
    assert False


def test_type_annotator():
    dialect = "duckdb"
    table_name = "table_1"
    sqlglot_schema = ensure_schema(
        {
            "database_name": {
                table_name: {
                    "col_varchar": sge.DataType.build("VARCHAR", dialect=dialect),
                    "col_bool": sge.DataType.build("BOOLEAN", dialect=dialect),
                }
            }
        }
    )

    computed_dlt_schema = lineage.compute_columns_schema(
        sql_query=f"SELECT * FROM {table_name}",
        sqlglot_schema=sqlglot_schema,
        dialect="duckdb",
    )

    assert False
