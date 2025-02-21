from typing import (
    Optional,
    Any,
    Type,
    TYPE_CHECKING,
    Literal,
    List,
    Callable,
    Union,
    Dict,
)
from typing_extensions import TypeAlias

from sqlalchemy.exc import NoReferencedTableError

from dlt.common.typing import TypedDict
from dlt.common.libs.sql_alchemy import Table, Column, Row, sqltypes, Select, TypeEngine
from dlt.common import logger
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns, TTableReference

ReflectionLevel = Literal["minimal", "full", "full_with_precision"]


# optionally create generics with any so they can be imported by dlt importer
if TYPE_CHECKING:
    SelectAny: TypeAlias = Select[Any]  # type: ignore[type-arg]
    ColumnAny: TypeAlias = Column[Any]  # type: ignore[type-arg]
    RowAny: TypeAlias = Row[Any]  # type: ignore[type-arg]
    TypeEngineAny = TypeEngine[Any]  # type: ignore[type-arg]
else:
    SelectAny: TypeAlias = Type[Any]
    ColumnAny: TypeAlias = Type[Any]
    RowAny: TypeAlias = Type[Any]
    TypeEngineAny = Type[Any]


TTypeAdapter = Callable[[TypeEngineAny], Optional[Union[TypeEngineAny, Type[TypeEngineAny]]]]


class TReflectedHints(TypedDict, total=False):
    columns: TTableSchemaColumns
    references: Optional[List[TTableReference]]
    primary_key: Optional[List[str]]


def default_table_adapter(table: Table, included_columns: Optional[List[str]]) -> None:
    """Default table adapter being always called before custom one"""
    if included_columns is not None:
        # Delete columns not included in the load
        for col in list(table._columns):  # type: ignore[attr-defined]
            if col.name not in included_columns:
                table._columns.remove(col)  # type: ignore[attr-defined]
    for col in table._columns:  # type: ignore[attr-defined]
        sql_t = col.type
        if hasattr(sqltypes, "Uuid") and isinstance(sql_t, sqltypes.Uuid):
            # emit uuids as string by default
            sql_t.as_uuid = False


def sqla_col_to_column_schema(
    sql_col: ColumnAny,
    reflection_level: ReflectionLevel,
    type_adapter_callback: Optional[TTypeAdapter] = None,
    skip_nested_columns_on_minimal: bool = False,
) -> Optional[TColumnSchema]:
    """Infer dlt schema column type from an sqlalchemy type.

    If `add_precision` is set, precision and scale is inferred from that types that support it,
    such as numeric, varchar, int, bigint. Numeric (decimal) types have always precision added.
    """
    col: TColumnSchema = {
        "name": sql_col.name,
    }
    # NOTE: nullability info may not be available for subquery columns
    if hasattr(sql_col, "nullable") and sql_col.nullable is not None:
        col["nullable"] = sql_col.nullable

    if reflection_level == "minimal":
        # normalized into subtables
        if (
            isinstance(sql_col.type, (sqltypes.JSON, sqltypes.ARRAY))
            and skip_nested_columns_on_minimal
        ):
            return None
        return col

    sql_t = sql_col.type

    if type_adapter_callback:
        sql_t = type_adapter_callback(sql_t)
        # Check if sqla type class rather than instance is returned
        if sql_t is not None and isinstance(sql_t, type):
            sql_t = sql_t()

    if sql_t is None:
        # Column ignored by callback
        return col

    add_precision = reflection_level == "full_with_precision"

    if hasattr(sqltypes, "Uuid") and isinstance(sql_t, sqltypes.Uuid):
        # we represent UUID as text by default, see default_table_adapter
        col["data_type"] = "text"
    elif isinstance(sql_t, sqltypes.Numeric):
        # check for Numeric type first and integer later, some numeric types (ie. Oracle)
        # derive from both
        # all Numeric types that are returned as floats will assume "double" type
        # and returned as decimals will assume "decimal" type
        if sql_t.asdecimal is False:
            col["data_type"] = "double"
        else:
            col["data_type"] = "decimal"
            if sql_t.precision is not None:
                col["precision"] = sql_t.precision
                # must have a precision for any meaningful scale
                if sql_t.scale is not None:
                    col["scale"] = sql_t.scale
                elif sql_t.decimal_return_scale is not None:
                    col["scale"] = sql_t.decimal_return_scale
    elif isinstance(sql_t, sqltypes.SmallInteger):
        col["data_type"] = "bigint"
        if add_precision:
            col["precision"] = 32
    elif isinstance(sql_t, sqltypes.Integer):
        col["data_type"] = "bigint"
    elif isinstance(sql_t, sqltypes.String):
        col["data_type"] = "text"
        if add_precision and sql_t.length:
            col["precision"] = sql_t.length
    elif isinstance(sql_t, sqltypes._Binary):
        col["data_type"] = "binary"
        if add_precision and sql_t.length:
            col["precision"] = sql_t.length
    elif isinstance(sql_t, sqltypes.DateTime):
        col["data_type"] = "timestamp"
        if add_precision:
            col["timezone"] = sql_t.timezone
    elif isinstance(sql_t, sqltypes.Date):
        col["data_type"] = "date"
    elif isinstance(sql_t, sqltypes.Time):
        col["data_type"] = "time"
    elif isinstance(sql_t, sqltypes.JSON):
        col["data_type"] = "json"
    elif isinstance(sql_t, sqltypes.ARRAY):
        col["data_type"] = "json"
    elif isinstance(sql_t, sqltypes.Boolean):
        col["data_type"] = "bool"
    else:
        logger.warning(
            f"A column with name {sql_col.name} contains unknown data type {sql_t} which cannot be"
            " mapped to `dlt` data type. When using sqlalchemy backend such data will be passed to"
            " the normalizer. In case of `pyarrow` and `pandas` backend, data types are detected"
            " from numpy ndarrays. In case of other backends, the behavior is backend-specific."
        )
    return {key: value for key, value in col.items() if value is not None}  # type: ignore[return-value]


def get_primary_key(table: Table) -> Optional[List[str]]:
    """Create primary key or return None if no key defined"""
    primary_key = [c.name for c in table.primary_key]
    return primary_key if len(primary_key) > 0 else None


def table_to_columns(
    table: Table,
    reflection_level: ReflectionLevel = "full",
    type_conversion_fallback: Optional[TTypeAdapter] = None,
    skip_nested_columns_on_minimal: bool = False,
) -> TTableSchemaColumns:
    """Convert an sqlalchemy table to a dlt table schema."""
    return {
        col["name"]: col
        for col in (
            sqla_col_to_column_schema(
                c, reflection_level, type_conversion_fallback, skip_nested_columns_on_minimal
            )
            for c in table.columns
        )
        if col is not None
    }


def get_table_references(table: Table) -> Optional[List[TTableReference]]:
    """Resolve table references from SQLAlchemy foreign key constraints in the table"""
    ref_tables: Dict[str, TTableReference] = {}
    for fk_constraint in table.foreign_key_constraints:
        try:
            referenced_table = fk_constraint.referred_table.name
        except NoReferencedTableError as e:
            logger.warning(
                "Foreign key constraint from table %s could not be resolved to a referenced table,"
                " error message: %s",
                table.name,
                e,
            )
            continue

        if fk_constraint.referred_table.schema != table.schema:
            continue

        elements = fk_constraint.elements
        referenced_columns = [element.column.name for element in elements]
        columns = [col.name for col in fk_constraint.columns]
        if referenced_table in ref_tables:
            # Merge multiple foreign keys to the same table
            existing_ref = ref_tables[referenced_table]
            existing_ref["columns"].extend(columns)  # type: ignore[attr-defined]
            existing_ref["referenced_columns"].extend(referenced_columns)  # type: ignore[attr-defined]
        else:
            ref_tables[referenced_table] = {
                "referenced_table": referenced_table,
                "referenced_columns": referenced_columns,
                "columns": columns,
            }
    return list(ref_tables.values())


def table_to_resource_hints(
    table: Table,
    reflection_level: ReflectionLevel = "full",
    type_conversion_fallback: Optional[TTypeAdapter] = None,
    skip_nested_columns_on_minimal: bool = False,
    resolve_foreign_keys: bool = False,
) -> TReflectedHints:
    result: TReflectedHints = {
        "columns": table_to_columns(
            table,
            reflection_level,
            type_conversion_fallback,
            skip_nested_columns_on_minimal,
        ),
        "primary_key": get_primary_key(table),
    }
    if resolve_foreign_keys:
        result["references"] = get_table_references(table)
    return result
