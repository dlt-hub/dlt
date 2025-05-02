import logging
from typing import TYPE_CHECKING, Any, cast, Optional, Union

import sqlglot
import sqlglot.expressions as sge
from sqlglot.errors import ParseError, OptimizeError
from sqlglot.expressions import DataType, DATA_TYPE
from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema
from sqlglot.optimizer.scope import build_scope, find_all_in_scope
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.lineage import lineage as get_lineage

from dlt.common.schema.typing import (
    TTableSchemaColumns,
    TColumnSchema,
    TTableSchema,
    TColumnType,
    TDataType,
)
from dlt.common.schema import Schema
from dlt.destinations.type_mapping import DataTypeMapper
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.dataset import (
    TReadableRelation,
)
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.sql_client import SqlClientBase
from dlt.transformations.exceptions import LineageFailedException

if TYPE_CHECKING:
    from dlt.destinations.dataset import ReadableDBAPIDataset


logger = logging.getLogger(__file__)


SQLGLOT_TO_DLT_TYPE_MAP: dict[DataType.Type, Optional[TDataType]] = {
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
    # binary
    DataType.Type.VARBINARY: "binary",
    # BOOLEAN
    DataType.Type.BOOLEAN: "bool",
    # UKNOWN
    DataType.Type.UNKNOWN: None,
}


def to_sqlglot_type(
    column: TColumnSchema, table: TTableSchema, type_mapper: DataTypeMapper, dialect: str
) -> DATA_TYPE:
    """Convert the data_type found in the dlt ColumnSchema to an SQLGlot type. Attach relevant dlt hints as metadata.

    The destination's type_mapper is used to convert the `data_type` into the destination's physical type.
    This allows more precise type inference by SQLGlot
    """
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


def from_sqlglot_type(column: Union[sge.Column, sge.Alias]) -> TColumnSchema:
    """Convert an SQLGlot and the attached metadata into a dlt ColumnSchema."""
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
    """Create an SQLGlot schema using a dlt Schema and the destination capabilities.

    The SQLGlot schema automatically includes the database and catalog names if available.
    This can allow cross-dataset transformations on the same physical location.
    """
    type_mapper = caps.get_type_mapper()
    mapping_schema: dict[str, dict[str, DATA_TYPE]] = {}  # {table: {col: type}}
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
# NOTE even if `infer_sqlglot_schema=True`, some queries haved undetermined final columns
def compute_columns_schema(
    sql_query: str,
    sqlglot_schema: SQLGlotSchema,
    dialect: str,
    infer_sqlglot_schema: bool = True,
    allow_anonymous_columns: bool = True,
    allow_partial: bool = True,
) -> TTableSchemaColumns:
    """Compute the expected dlt columns schema for the output of an SQL SELECT query.

    Args:
        infer_sqlglot_schema (bool): If False, all columns and tables referenced must be derived from the SQLGlot schema.
            If True, allow columns and tables not found in SQLGlot schema
        allow_anonymous_columns (bool): If False, all columns in final selection must have an explicit name or alias.
            If True, the name of columns from the final selection can be generated by the dialect
        allow_partial (bool): If False, raise exceptions if the schema returned is incomplete.
            If True, this function always returns a dictionary, even in cases of
            SQL parsing errors, missing table reference, unresolved `SELECT *`, etc.
    """
    try:
        expression: Any = sqlglot.maybe_parse(sql_query, dialect=dialect)
    except ParseError as e:
        if allow_partial:
            logger.debug(
                "Failed to parse the SQL query. Returning empty table schema because"
                " `allow_fail=True`"
            )
            return {}

        raise LineageFailedException(
            f"Failed to parse the SQL query using dialect `{dialect}`.\nQuery:\n\t{sql_query}"
        ) from e

    if not isinstance(expression, sge.Select):
        if allow_partial:
            logger.debug(
                "Parsed SQL query is not a SELECT statement. Returning empty table schema because"
                " `allow_fail=True`"
            )
            return {}

        raise LineageFailedException(
            "Parsed SQL query is not a SELECT statement. Received SQL expression of type"
            f" {expression.type}."
        )

    if allow_anonymous_columns is False:
        for col in expression.selects:
            if col.output_name == "":
                raise LineageFailedException(
                    "Found anonymous column in SELECT statement. Use"
                    f" `allow_anonymous_columns=True` for permissive handling.\nColumn:\n\t{col}"
                )

    # false-y values `schema={}` or `schema=None` are identical to `infer_schema=True`
    try:
        expression = qualify(
            expression,
            schema=sqlglot_schema,
            dialect=dialect,
            infer_schema=infer_sqlglot_schema,
        )
    except OptimizeError as e:
        raise LineageFailedException(
            "Failed to resolve SQL query against the schema received."
        ) from e

    expression = annotate_types(expression, schema=sqlglot_schema, dialect=dialect)

    dlt_table_schema = {}
    for col in expression.selects:
        if col.output_name == "*":
            if allow_partial is True:
                logger.debug(
                    "SELECT statement includes a `*` selection that can't be resolved. "
                    "Returning empty table schema because `allow_fail=True`"
                )
                continue

            raise LineageFailedException(
                "SELECT statement includes a `*` selection that can't be resolved. Modify the"
                " query to select columns explicitly or limit `*` to known tables (e.g., `SELECT"
                " known_table.*`). Use `allow_fail=True` to return a partial dlt schema with"
                f" resolvable column hints.\nColumn:\n\t{col}"
            )

        assert isinstance(col, (sge.Column, sge.Alias))
        dlt_table_schema[col.output_name] = from_sqlglot_type(col)

    return dlt_table_schema
