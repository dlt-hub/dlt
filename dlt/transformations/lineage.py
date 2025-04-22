from typing import TYPE_CHECKING, Any, cast

import sqlglot
import sqlglot.expressions as sge
from sqlglot.expressions import DataType, DATA_TYPE
from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

from dlt.common.schema.typing import TTableSchemaColumns, TColumnSchema, TTableSchema
from dlt.common.schema import Schema
from dlt.destinations.type_mapping import DataTypeMapper
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.destination.dataset import (
    TReadableRelation,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.sql_client import SqlClientBase

if TYPE_CHECKING:
    from dlt.destinations.dataset import ReadableDBAPIDataset
else:
    ReadableDBAPIDataset = Any


SQLGLOT_TO_DLT_TYPE_MAP: dict[DataType.Type, str] = {
    # NESTED_TYPES
    DataType.Type.OBJECT: "json",
    DataType.Type.STRUCT: "json",
    DataType.Type.NESTED: "json",
    DataType.Type.UNION: "json",
    DataType.Type.ARRAY: "json",
    DataType.Type.LIST: "json",
    DataType.Type.JSON: "json",
    # TEXT
    DataType.Type.CHAR: "text",
    DataType.Type.NCHAR: "text",
    DataType.Type.NVARCHAR: "text",
    DataType.Type.TEXT: "text",
    DataType.Type.VARCHAR: "text",
    DataType.Type.NAME: "text",
    # SIGNED_INTEGER
    DataType.Type.BIGINT: "bigint",
    DataType.Type.INT: "bigint",
    DataType.Type.INT128: "bigint",
    DataType.Type.INT256: "bigint",
    DataType.Type.MEDIUMINT: "bigint",
    DataType.Type.SMALLINT: "bigint",
    DataType.Type.TINYINT: "bigint",
    # UNSIGNED_INTEGER
    DataType.Type.UBIGINT: "bigint",
    DataType.Type.UINT: "bigint",
    DataType.Type.UINT128: "bigint",
    DataType.Type.UINT256: "bigint",
    DataType.Type.UMEDIUMINT: "bigint",
    DataType.Type.USMALLINT: "bigint",
    DataType.Type.UTINYINT: "bigint",
    # other INTEGER
    DataType.Type.BIT: "bigint",
    # FLOAT
    DataType.Type.DOUBLE: "double",
    DataType.Type.FLOAT: "double",
    # DECIMAL
    DataType.Type.BIGDECIMAL: "decimal",
    DataType.Type.DECIMAL: "decimal",
    DataType.Type.DECIMAL32: "decimal",
    DataType.Type.DECIMAL64: "decimal",
    DataType.Type.DECIMAL128: "decimal",
    DataType.Type.DECIMAL256: "decimal",
    DataType.Type.MONEY: "decimal",
    DataType.Type.SMALLMONEY: "decimal",
    DataType.Type.UDECIMAL: "decimal",
    DataType.Type.UDOUBLE: "decimal",
    # TEMPORAL
    DataType.Type.DATE: "date",
    DataType.Type.DATE32: "date",
    DataType.Type.DATETIME: "date",
    DataType.Type.DATETIME2: "date",
    DataType.Type.DATETIME64: "date",
    DataType.Type.SMALLDATETIME: "date",
    DataType.Type.TIMESTAMP: "timestamp",
    DataType.Type.TIMESTAMPNTZ: "timestamp",
    DataType.Type.TIMESTAMPLTZ: "timestamp",
    DataType.Type.TIMESTAMPTZ: "timestamp",
    DataType.Type.TIMESTAMP_MS: "timestamp",
    DataType.Type.TIMESTAMP_NS: "timestamp",
    DataType.Type.TIMESTAMP_S: "timestamp",
    DataType.Type.TIME: "time",
    DataType.Type.TIMETZ: "time",
    # BOOLEAN
    DataType.Type.BOOLEAN: "bool",
    # UKNOWN
    DataType.Type.UNKNOWN: None,
}


def to_sqlglot_type(
    column: TColumnSchema, table: TTableSchema, type_mapper: DataTypeMapper, dialect: str
) -> DATA_TYPE:
    if not column.get("data_type"):
        return None

    destination_type = type_mapper.to_destination_type(column, cast(PreparedTableSchema, table))

    # TODO modify nullable arg_types on destination_type
    sqlglot_type = DataType.build(destination_type, dialect=dialect)
    # TODO verify how nullable is used exactly in sqlglot
    if column.get("nullable") is not None:
        sqlglot_type.arg_types["nullable"] = column["nullable"]

    # TODO refine what metadata is tied to the DataType vs the Column expression
    sqlglot_type._meta = {k: v for k, v in column.items() if k not in ["data_type", "name"]}
    return sqlglot_type


def from_sqlglot_type(column: sge.Column) -> TColumnSchema:
    col = {
        "name": column.output_name,
        **column.type.meta,
    }
    if data_type := SQLGLOT_TO_DLT_TYPE_MAP[column.type.this]:
        col["data_type"] = data_type
    return cast(TColumnSchema, col)


def create_sqlglot_schema(
    sql_client: SqlClientBase[Any],
    schema: Schema,
    dialect: str,
    caps: DestinationCapabilitiesContext,
) -> SQLGlotSchema:
    # {dataset: {table: {col: type}}}
    type_mapper = caps.get_type_mapper()
    mapping_schema: dict[str, dict[str, DATA_TYPE]] = {}
    for table_name, table in schema.tables.items():
        table_name = sql_client.make_qualified_table_name_path(table_name, escape=False)[-1]
        if mapping_schema.get(table_name) is None:
            mapping_schema[table_name] = {}

        for column_name, column in table["columns"].items():
            if sqlglot_type := to_sqlglot_type(column, table, type_mapper, dialect):
                mapping_schema[table_name][column_name] = sqlglot_type

    dataset_catalog = sql_client.make_qualified_table_name_path(None, escape=False)

    if len(dataset_catalog) == 2:
        catalog, database = dataset_catalog
        nested_schema = {catalog: {database: mapping_schema}}
    else:
        (database,) = dataset_catalog
        nested_schema = {database: mapping_schema}  # type: ignore

    return ensure_schema(nested_schema)


# TODO should we raise an exception for anonymous columns?
def compute_columns_schema(
    sql_query: str,
    sqlglot_schema: SQLGlotSchema,
    dialect: str,
    allow_unknown_columns: bool = True,
    allow_anonymous_columns: bool = True,
    allow_fail: bool = True,
) -> TTableSchemaColumns:
    expression: Any = sqlglot.maybe_parse(sql_query, dialect=dialect)
    if not isinstance(expression, sge.Select):
        if allow_fail:
            return {}
        raise TypeError(
            f"Received SQL expression of type {expression.type}. Only SELECT queries are accepted."
        )

    expression = qualify(expression, schema=sqlglot_schema, dialect=dialect)
    expression = annotate_types(expression, schema=sqlglot_schema, dialect=dialect)
    return {column.output_name: from_sqlglot_type(column) for column in expression.selects}
