from typing import Optional, Union

from dlt.common.schema import Schema
from dlt.common.utils import without_none
from dlt.common.exceptions import MissingDependencyException, TerminalValueError
from dlt.common.schema.typing import (
    ColumnPropInfos,
    TColumnType,
    TDataType,
    TColumnSchema,
    TTableSchema,
    TTableReference,
)

try:
    import sqlglot.expressions as sge
    from sqlglot import maybe_parse
    from sqlglot.expressions import DataType, DATA_TYPE
    from sqlglot.schema import Schema as SQLGlotSchema, ensure_schema
    from sqlglot.dialects.dialect import DialectType
except ModuleNotFoundError:
    raise MissingDependencyException("dlt sqlglot helpers", ["sqlglot"])


SQLGLOT_TO_DLT_TYPE_MAP: dict[DataType.Type, TDataType] = {
    # NESTED_TYPES
    DataType.Type.OBJECT: "json",
    DataType.Type.STRUCT: "json",
    DataType.Type.NESTED: "json",
    DataType.Type.UNION: "json",
    DataType.Type.ARRAY: "json",
    DataType.Type.LIST: "json",
    DataType.Type.JSON: "json",
    DataType.Type.VECTOR: "json",
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
    # UNKNOWN
    DataType.Type.UNKNOWN: None,
}

# NOTE currently, dlt defaults to `bigint`, but sqlglot defaults to `int`
# therefore, both return `None` to generate no `precision` dlt hint and set `data_type="bigint"`
SQLGLOT_INT_PRECISION = {
    DataType.Type.TINYINT: 3,
    DataType.Type.SMALLINT: 5,
    DataType.Type.MEDIUMINT: 8,
    DataType.Type.INT: 10,
    DataType.Type.BIGINT: 19,
    DataType.Type.INT128: 39,
    DataType.Type.INT256: 78,
    DataType.Type.UTINYINT: 3,
    DataType.Type.USMALLINT: 5,
    DataType.Type.UMEDIUMINT: 8,
    DataType.Type.UINT: 10,
    DataType.Type.UBIGINT: 19,
    DataType.Type.UINT128: 39,
    DataType.Type.UINT256: 78,
}

SQLGLOT_DECIMAL_PRECISION_AND_SCALE = {
    DataType.Type.BIGDECIMAL: (38, 10),
    DataType.Type.DECIMAL: (38, 10),
    DataType.Type.DECIMAL32: (7, 2),
    DataType.Type.DECIMAL64: (16, 4),
    DataType.Type.DECIMAL128: (34, 10),
    DataType.Type.DECIMAL256: (76, 20),
    DataType.Type.MONEY: (19, 4),
    DataType.Type.SMALLMONEY: (10, 4),
    DataType.Type.UDECIMAL: (38, 10),
}

SQLGLOT_TEMPORAL_PRECISION = {
    DataType.Type.TIMESTAMP: None,  # default value; default precision varies across DB
    DataType.Type.TIMESTAMP_S: 0,
    DataType.Type.TIMESTAMP_MS: 3,
    DataType.Type.TIMESTAMP_NS: 9,
}

# NOTE in Snowflake, TIMESTAMPNTZ == DATETIME; is this true for dlt?
SQLGLOT_HAS_TIMEZONE = {
    DataType.Type.TIMESTAMP: None,  # default value; False
    DataType.Type.TIMESTAMPNTZ: False,
    DataType.Type.TIMESTAMPLTZ: True,
    DataType.Type.TIMESTAMPTZ: True,
    DataType.Type.TIMESTAMP_MS: False,
    DataType.Type.TIMESTAMP_NS: False,
    DataType.Type.TIMESTAMP_S: False,
    DataType.Type.TIME: False,  # default value; False
    DataType.Type.TIMETZ: True,
}

DLT_TO_SQLGLOT = {
    "json": DataType.Type.JSON,
    "text": DataType.Type.TEXT,
    "double": DataType.Type.DOUBLE,
    "bool": DataType.Type.BOOLEAN,
    "date": DataType.Type.DATE,
    "timestamp": DataType.Type.TIMESTAMP,
    "bigint": DataType.Type.BIGINT,
    "binary": DataType.Type.VARBINARY,
    "time": DataType.Type.TIME,
    "decimal": DataType.Type.DECIMAL,
    "wei": DataType.Type.UINT256,
}


# TODO should we raise errors on type conversion or silently convert to correct values?
# TODO support `text` and `binary` precision
def from_sqlglot_type(sqlglot_type: DATA_TYPE) -> TColumnType:
    """Convert a SQLGlot DataType to dlt column hints.

    reference: https://dlthub.com/docs/general-usage/schema/#tables-and-columns
    """
    if isinstance(sqlglot_type, str):
        sqlglot_type = sge.DataType.build(sqlglot_type)
    elif isinstance(sqlglot_type, sge.DataType.Type):
        sqlglot_type = sge.DataType.build(sqlglot_type)
    assert isinstance(sqlglot_type, sge.DataType)

    # only the `DataType.UNKNOWN` will produce `dlt_type=None`
    dlt_type = SQLGLOT_TO_DLT_TYPE_MAP.get(sqlglot_type.this, "text")

    if dlt_type == "bigint":
        hints = _from_integer_type(sqlglot_type)
    elif dlt_type == "decimal":
        hints = _from_decimal_type(sqlglot_type)
    elif dlt_type == "timestamp":
        hints = _from_timezone_type(sqlglot_type)
    elif dlt_type == "time":
        hints = _from_time_type(sqlglot_type)
    else:
        hints = {}

    # `nullable` is not a required arg on exp.DataType
    nullable = sqlglot_type.args.get("nullable")

    return without_none({"data_type": dlt_type, "nullable": nullable, **hints})  # type: ignore[return-value]


def _from_integer_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    if sqlglot_type.expressions:  # from parameterized type
        assert len(sqlglot_type.expressions) == 1
        assert isinstance(sqlglot_type.expressions[0], sge.DataTypeParam)
        precision = sqlglot_type.expressions[0].this.to_py()

    else:  # from named type
        precision = SQLGLOT_INT_PRECISION.get(sqlglot_type.this)

     # special case `precison=19` maps to BIGINT, which is the default in dlt
    precision = None if precision == 19 else precision
    return {"precision": precision}


def _from_decimal_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    if sqlglot_type.expressions:  # from parameterized type
        assert all(isinstance(e, sge.DataTypeParam) for e in sqlglot_type.expressions)
        assert len(sqlglot_type.expressions) in (1, 2)
        if len(sqlglot_type.expressions) == 1:
            precision = sqlglot_type.expressions[0].this.to_py()
            hints = {"precision": precision}
        elif len(sqlglot_type.expressions) == 2:
            precision = sqlglot_type.expressions[0].this.to_py()
            scale = sqlglot_type.expressions[1].this.to_py()
            hints = {"precision": precision, "scale": scale}
        else:
            # TODO log or raise warning; unexpected to see more than 2 DataTypeParam
            breakpoint()

    else:  # from named type
        precision_and_scale = SQLGLOT_DECIMAL_PRECISION_AND_SCALE.get(sqlglot_type.this)
        if precision_and_scale is not None:
            precision, scale = precision_and_scale
            hints = {"precision": precision, "scale": scale}
        else:
            hints = {}

    return hints  # type: ignore[return-value]


def _from_timezone_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    timezone = SQLGLOT_HAS_TIMEZONE.get(sqlglot_type.this)

    if sqlglot_type.expressions:  # from parameterized type
        assert len(sqlglot_type.expressions) == 1
        assert isinstance(sqlglot_type.expressions[0], sge.DataTypeParam)
        precision = sqlglot_type.expressions[0].this.to_py()
    else:  # from named type
        precision = SQLGLOT_TEMPORAL_PRECISION.get(sqlglot_type.this)

    return {"precision": precision, "timezone": timezone}


def _from_time_type(sqlglot_type: sge.DataType) -> TColumnSchema:
    timezone = SQLGLOT_HAS_TIMEZONE.get(sqlglot_type.this)

    if sqlglot_type.expressions:  # from parameterized type
        assert len(sqlglot_type.expressions) == 1
        assert isinstance(sqlglot_type.expressions[0], sge.DataTypeParam)
        precision = sqlglot_type.expressions[0].this.to_py()
        hints = {"precision": precision, "timezone": timezone}
    else:  # from named type
        hints = {"timezone": timezone}

    return hints  # type: ignore[return-value]


# NOTE nullable seems to default to True compared to other hints
# TODO support `text` and `binary` precision
def to_sqlglot_type(
    dlt_type: TDataType,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[bool] = None,
    nullable: Optional[bool] = None,
    use_named_types: bool = False,
) -> DataType:
    """Convert the dlt `data_type` and column hints to a SQLGlot DataType expression.

    `if use_named_type is True:` use "named" types with fallback on "parameterized" types.
    `else:` use "parameterized" types everywhere.

    Named types:
    - have some set attributes, e.g., `DataType.Type.DECIMAL64` has `precision=16` and `scale=4`.
    - can be referenced directly in SQL (table definition, CAST(), which could make SQLGlot transpiling more reliable.
    - may not exist in all dialects and will be casted automatically during transpiling
    - can have limited expressivity, e.g., named types for timestamps only included precision 0, 3, 9

    Parameterized types:
    - instead of DECIMAL64, a generic `DataType.Type.DECIMAL` is used with `DataTypeParam` expressions attached
      to represent `precision` and `scale`. SQLGlot is responsible for properly compiling and transpiling them.
    - parameters might be handled differently across dialects and would require greater testing on the dlt side.

    reference: https://dlthub.com/docs/general-usage/schema/#tables-and-columns
    """
    if use_named_types is True:
        try:
            sqlglot_type = _build_named_sqlglot_type(
                dlt_type=dlt_type,
                nullable=nullable,
                precision=precision,
                scale=scale,
                timezone=timezone,
            )
        except TerminalValueError:
            sqlglot_type = _build_parameterized_sqlglot_type(
                dlt_type=dlt_type,
                nullable=nullable,
                precision=precision,
                scale=scale,
                timezone=timezone,
            )
    else:
        sqlglot_type = _build_parameterized_sqlglot_type(
            dlt_type=dlt_type,
            nullable=nullable,
            precision=precision,
            scale=scale,
            timezone=timezone,
        )

    return sqlglot_type


# NOTE Let's ignore named types for now
def _build_named_sqlglot_type(
    dlt_type: TDataType,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[bool] = None,
    nullable: Optional[bool] = None,
) -> DataType:
    hints = {}
    if nullable is not None:
        hints["nullable"] = nullable

    if dlt_type == "bigint" and precision is not None:
        sqlglot_type = _to_named_integer_type(precision=precision)
    elif dlt_type == "decimal" and precision is not None:
        sqlglot_type = _to_named_decimal_type(precision=precision, scale=scale)
    elif dlt_type == "timestamp" and (precision is not None or timezone is not None):
        sqlglot_type = _to_named_timestamp_type(precision=precision, timezone=timezone)
    elif dlt_type == "time" and timezone is not None:
        sqlglot_type = _to_named_time_type(timezone=timezone)
    else:
        raise TerminalValueError()

    return DataType.build(dtype=sqlglot_type, nested=False, **hints)  # type: ignore[arg-type]


def _build_parameterized_sqlglot_type(
    dlt_type: TDataType,
    nullable: Optional[bool] = None,
    precision: Optional[int] = None,
    scale: Optional[int] = None,
    timezone: Optional[bool] = None,
) -> DataType:
    hints: TColumnType = {}
    # `nullable` should always be added to DataType
    if nullable is not None:
        hints["nullable"] = nullable

    if dlt_type == "bigint" and precision is not None:
        # special case `!=10` because `precison=10` maps to INT, which is the default in SQLGlot
        if precision == 10:
            sqlglot_type = sge.DataType.build("INT", nested=False, **hints)
        else:
            # NOTE INT precision is not supported by many backend.
            # For instance, INT(5) in MySQL should be DECIMAL(5,0) in DuckDB
            # Currently, SQLGLot can't transpile from INT(5) to DECIMAL(5,0).
            # Using non-parameterized type would be more reliable in these cases.
            sqlglot_type = sge.DataType.build(f"INT({precision})", nested=False, **hints)

    elif dlt_type == "decimal" and precision is not None:
        # this code is inspired from sqlglot.dialects.dialect.build_default_decimal_type
        params = f"({precision}{f', {scale}' if scale is not None else ''})"
        sqlglot_type = sge.DataType.build(f"DECIMAL{params}", nested=False, **hints)

    elif dlt_type == "timestamp" and (precision is not None or timezone is not None):
        base_sqlglot_type = _to_named_timestamp_type(precision=None, timezone=timezone)
        params = f"({precision})" if precision is not None else ""
        sqlglot_type = sge.DataType.build(
            f"{base_sqlglot_type.value}{params}", nested=False, **hints
        )

    elif dlt_type == "time" and (precision is not None or timezone is not None):
        base_sqlglot_type = _to_named_time_type(timezone=timezone)
        params = f"({precision})" if precision is not None else ""
        sqlglot_type = sge.DataType.build(
            f"{base_sqlglot_type.value}{params}", nested=False, **hints
        )

    else:
        sqlglot_type = sge.DataType.build(DLT_TO_SQLGLOT[dlt_type], nested=False, **hints)

    return sqlglot_type


def _to_named_integer_type(precision: Optional[int]) -> DataType.Type:
    """Return the SQLGlot INTEGER DataType with precision `>= precision`."""
    if precision is None:
        return DataType.Type.BIGINT
    elif precision <= 0:
        # raise TerminalValueError(f"Precision must be positive, got {precision}")
        sqlglot_type = DataType.Type.BIGINT  # default value
    elif precision <= 3:
        sqlglot_type = DataType.Type.TINYINT
    elif precision <= 5:
        sqlglot_type = DataType.Type.SMALLINT
    elif precision <= 8:
        sqlglot_type = DataType.Type.MEDIUMINT
    elif precision <= 10:
        sqlglot_type = DataType.Type.INT
    elif precision <= 19:
        sqlglot_type = DataType.Type.BIGINT
    elif precision <= 39:
        sqlglot_type = DataType.Type.INT128
    elif precision <= 78:
        sqlglot_type = DataType.Type.INT256
    else:
        raise TerminalValueError(
            f"Precision must be less than maximum precision of 78, got {precision}"
        )

    return sqlglot_type


# TODO
def _to_named_decimal_type(precision: Optional[int], scale: Optional[int]) -> DataType.Type:
    sqlglot_type = DataType.Type.DECIMAL
    # if precision is None:
    #     sqlglot_type = DataType.Type.DECIMAL
    # elif precision <= 0:
    #     raise TerminalValueError(f"Precision must be positive, got {precision}")
    # else

    return sqlglot_type


def _to_named_timestamp_type(precision: Optional[int], timezone: Optional[bool]) -> DataType.Type:
    if precision is None:
        if timezone is True:
            sqlglot_type = DataType.Type.TIMESTAMPTZ
        elif timezone is False:
            sqlglot_type = DataType.Type.TIMESTAMPNTZ
        elif timezone is None:
            sqlglot_type = DataType.Type.TIMESTAMP
        else:
            raise TerminalValueError(
                f"Received `timezone` value not in (True, False, None), got {timezone}"
            )

    else:
        if timezone is None and precision == 0:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_S
        elif timezone is None and precision <= 3:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_MS
        elif timezone is None and precision <= 9:
            sqlglot_type = sge.DataType.Type.TIMESTAMP_NS
        else:
            raise TerminalValueError(
                f"Precision must be less than maximum precision of 9, got {precision}"
            )

    return sqlglot_type


def _to_named_time_type(timezone: Optional[bool]) -> DataType.Type:
    if timezone is None:
        sqlglot_type = DataType.Type.TIME
    elif timezone is True:
        sqlglot_type = DataType.Type.TIMETZ
    elif timezone is False:
        sqlglot_type = DataType.Type.TIME
    else:
        raise TerminalValueError(
            f"Received `timezone` value not in (True, False, None), got {timezone}"
        )

    return sqlglot_type


def _filter_dlt_hints(hints: TColumnSchema) -> TColumnSchema:
    """Filter the metadata dictionary to only include dlt hints."""
    DATA_TYPE_HINTS = ("data_type", "nullable", "precision", "scale", "timezone")
    # allow original name to be propagated, so we can track it if aliases were used
    ALLOWED_HINTS = ("name", "hard_delete", "incremental")
    # only propagate specially designated hints
    # TODO review all x- hints we use and decide which to propagate
    CUSTOM_HINT_PREFIX = "x-annotation"

    final_hints = {}
    for k, v in hints.items():
        # those are directly expressed via the DataType and Column
        if k in DATA_TYPE_HINTS:
            continue

        if k in ALLOWED_HINTS:
            final_hints[k] = v
        elif k.startswith(CUSTOM_HINT_PREFIX):
            final_hints[k] = v

    return final_hints  # type: ignore[return-value]


# TODO implement an SQLGlot annotator for setting dlt hints instead of relying on types.
# ref: https://github.com/tobymao/sqlglot/blob/17f7eaff564790b1fe7faa414143accf362f550e/sqlglot/optimizer/annotate_types.py#L135
def set_metadata(sqlglot_type: sge.DataType, metadata: TColumnSchema) -> sge.DataType:
    """Set a metadata dictionary on the SQGLot DataType object.

    By attaching dlt hints to a DataType object, they will be propagated
    until the DataType is modified.
    """
    sqlglot_type._meta = _filter_dlt_hints(metadata)  # type: ignore[assignment]
    return sqlglot_type


def get_metadata(sqlglot_type: sge.DataType) -> TColumnSchema:
    """Get a metadata dictionary from the SQLGlot DataType object."""
    metadata = sqlglot_type.meta
    return _filter_dlt_hints(metadata)  # type: ignore[arg-type]


def dlt_schema_to_sqlglot_schema(
    schema: Schema,
    dataset_name: str,
    dialect: Optional[DialectType] = "duckdb",
) -> SQLGlotSchema:
    """Create an SQLGlot schema using a dlt Schema and the destination dialect.

    The SQLGlot schema automatically scopes the tables to the `dataset_name`.
    This can allow cross-dataset transformations on the same physical location.
    No name translation nor case folding is performing. All identifiers correspond
    to identifiers in dlt schema.
    """
    sqlglot_schema = {}
    for table_name in schema.tables.keys():
        column_mapping = {}
        # skip not materialized columns
        for column_name, column in schema.get_table_columns(
            table_name, include_incomplete=False
        ).items():
            sqlglot_type = to_sqlglot_type(
                dlt_type=column["data_type"],
                nullable=column.get("nullable"),
                precision=column.get("precision"),
                scale=column.get("scale"),
                timezone=column.get("timezone"),
            )
            sqlglot_type = set_metadata(sqlglot_type, column)
            column_mapping[column_name] = sqlglot_type

        # tables without columns are ignored
        if column_mapping:
            sqlglot_schema[table_name] = column_mapping

    return ensure_schema({dataset_name: sqlglot_schema}, dialect=dialect, normalize=False)


# def sqlglot_schema_to_dlt_schema(sqlglot_schema: SQLGlotSchema) -> Schema:
#     dlt_table_schema: dict[str, TColumnSchema] = {}
#     dataset_name = ...

#     for table in sqlglot_schema:
#         column_hints = {}
#         for column_name, sqlglot_type in table.items():
#             data_type_hints = from_sqlglot_type(sqlglot_type)
#             metadata_hints = get_metadata(sqlglot_type)
#             hints = {
#                 "name": column_name,
#                 **metadata_hints,
#                 **data_type_hints,
#             }
#             column_hints[column_name] = hints

#         if column_hints:
#             dlt_table_schema[column_name] = column_hints

#     return dlt_table_schema


def _from_sqlglot_constraint(constraint: sge.ColumnConstraint) -> TColumnSchema:
    hint: TColumnSchema = {}

    if isinstance(constraint.kind, sge.NotNullColumnConstraint):
        hint["nullable"] = False
    elif isinstance(constraint.kind, sge.UniqueColumnConstraint):
        hint["unique"] = True
    elif isinstance(constraint.kind, sge.PrimaryKeyColumnConstraint):
        hint["primary_key"] = True
    elif isinstance(constraint.kind, sge.CommentColumnConstraint):
        hint["description"] = str(constraint.this)
    elif isinstance(constraint.kind, sge.ClusteredColumnConstraint):
        hint["cluster"] = True
    elif isinstance(constraint.kind, sge.NonClusteredColumnConstraint):
        hint["cluster"] = False

    return hint


def _to_sqlglot_constraint(
    hint_key: str,
    hint_value: Union[str, bool, None],
) -> Optional[sge.ColumnConstraint]:
    constraint_kind: Optional[sge.ColumnConstraintKind] = None
    constraint_kwargs = {}

    if hint_key == "nullable":
        constraint_kind = sge.NotNullColumnConstraint() if hint_value is False else None
    elif hint_key == "unique":
        constraint_kind = sge.UniqueColumnConstraint() if hint_value is True else None
    elif hint_key == "primary_key":
        constraint_kind = sge.PrimaryKeyColumnConstraint() if hint_value is True else None
    elif hint_key == "description":
        if hint_value is None:
            constraint_kind = None
        elif not isinstance(hint_value, str):
            raise TypeError(
                f"Received `description={hint_value}`, but `description` should be a `str`."
            )
        else:
            constraint_kind = sge.CommentColumnConstraint()
            constraint_kwargs["this"] = hint_value
    elif hint_key == "cluster":
        if hint_value is True:
            constraint_kind = sge.ClusteredColumnConstraint()
        elif hint_value is False:
            constraint_kind = sge.NonClusteredColumnConstraint()
        else:
            constraint_kind = None

    if constraint_kind is None:
        return None

    constraint = sge.ColumnConstraint(**constraint_kwargs, kind=constraint_kind)
    return constraint


def _from_sqlglot_column_def(column_def: sge.ColumnDef) -> TColumnSchema:
    """Convert a SQLGlot column DDL to dlt column hints"""
    hints: TColumnSchema = {}

    hints["name"] = str(column_def.this)

    assert column_def.kind is not None
    data_type_hints = from_sqlglot_type(column_def.kind)

    constraints_hints: TColumnSchema = {}
    for constraint in column_def.constraints:
        assert constraint.kind is not None
        constraint_hint = _from_sqlglot_constraint(constraint)
        constraints_hints.update(**constraint_hint)  # type: ignore[misc]

    # constraint hints override hints retrieved from sqlglot type
    # this is to avoid conflict with our "get_metadata / set_metadata mechanism"
    hints.update(**data_type_hints)  # type: ignore[misc]
    hints.update(**constraints_hints)  # type: ignore[misc]

    return hints


def _to_sqlglot_column_def(hints: TColumnSchema) -> sge.ColumnDef:
    """Convert dlt column hints to a SQLGlot column DDL"""
    column_name = sge.Identifier(this=hints["name"], quoted=False)
    # `nullable` is not included because it's set by `NotNullConstraint()`
    sqlglot_type = to_sqlglot_type(
        dlt_type=hints["data_type"],
        precision=hints.get("precision"),
        scale=hints.get("scale"),
        timezone=hints.get("timezone"),
    )
    constraints: list[sge.ColumnConstraint] = []
    for key, value in hints.items():
        constraint = _to_sqlglot_constraint(key, value)  # type: ignore[arg-type]
        if constraint is not None:
            constraints.append(constraint)

    # NOTE we could set the column position explicitly
    column_def = sge.ColumnDef(
        this=column_name,
        kind=sqlglot_type,
        constraints=constraints,
    )
    return column_def


def _from_sqlglot_reference(foreign_key: sge.ForeignKey) -> TTableReference:
    reference_expr = foreign_key.args["reference"]
    assert isinstance(reference_expr, sge.Reference)

    columns = [str(col) for col in foreign_key.expressions]
    referenced_table = str(reference_expr.this.this.this)
    referenced_columns = [str(col) for col in reference_expr.this.expressions]

    dlt_reference = TTableReference(
        columns=columns,
        referenced_table=referenced_table,
        referenced_columns=referenced_columns,
    )
    return dlt_reference


def _to_sqlglot_reference(reference: TTableReference) -> sge.ForeignKey:
    foreign_key_expr = sge.ForeignKey(
        expressions=[sge.Identifier(this=col, quoted=False) for col in reference["columns"]],
        reference=sge.Reference(
            this=sge.Schema(
                this=sge.Table(
                    this=sge.Identifier(this=reference["referenced_table"], quoted=False)
                ),
                expressions=[
                    sge.Identifier(this=col, quoted=False)
                    for col in reference["referenced_columns"]
                ],
            )
        ),
    )
    return foreign_key_expr


# TODO handle special reference for `root_key`, `parent_key`, `row_key`, `_dlt_load_id`
# TODO accept as input `sge.Create` or `sge.Table`
def _from_sqlglot_table_def(table_def: sge.Create) -> TTableSchema:
    assert table_def.kind == "TABLE"

    # TODO need to strip quotes from `table_def.this.this.this` when the string is quoted
    table_hints: TTableSchema = {
        "columns": {},
        "name": str(table_def.this.this.this),
    }

    for expression in table_def.this.expressions:
        # "columns" hint
        if isinstance(expression, sge.ColumnDef):
            # TODO need to strip quotes from `expression.this` when the string is quoted
            column_name = str(expression.this)
            column_hints = _from_sqlglot_column_def(expression)
            # edge case because default SQLGlot is INT and default dlt is "bigint"
            if column_hints["data_type"] == "bigint" and column_hints.get("precision") == 19:
                del column_hints["precision"]
            table_hints["columns"][column_name] = column_hints

        # "references" hint
        elif isinstance(expression, sge.ForeignKey):
            if table_hints.get("references") is None:
                table_hints["references"] = []
            assert isinstance(table_hints["references"], list)

            table_hints["references"].append(_from_sqlglot_reference(expression))

    return table_hints


# TODO handle special reference for `root_key`, `parent_key`, `row_key`, `_dlt_load_id`
# TODO should this return `sge.Create` or `sge.Table`?
# TODO handle CREATE OR REPLACE directive
def _to_sqlglot_table_def(
    table_schema: TTableSchema, dataset_name: Optional[str] = None
) -> sge.Create:
    table_name = sge.Identifier(this=table_schema["name"], quoted=False)
    table_expr = (
        sge.Table(this=table_name)
        if dataset_name is None
        else sge.Table(this=table_name, db=sge.Identifier(this=dataset_name, quoted=False))
    )

    column_defs = [
        _to_sqlglot_column_def(col) for _, col in table_schema.get("columns", {}).items()
    ]
    references = [_to_sqlglot_reference(ref) for ref in table_schema.get("references", [])]

    create_expr = sge.Create(
        this=sge.Schema(
            this=table_expr,
            expressions=[*column_defs, *references],
        ),
        kind="TABLE",
    )
    return create_expr


# TODO this is a temporary hack that mocks `dlt.Schema._infer_column()`
def _set_default_hints(table_hints: TTableSchema) -> TTableSchema:
    DEFAULT_HINTS = {
        "not_null": ["_dlt_id", "_dlt_root_id", "_dlt_parent_id", "_dlt_list_idx", "_dlt_load_id"],
        "parent_key": ["_dlt_parent_id"],
        "root_key": ["_dlt_root_id"],
        "unique": ["_dlt_id"],
        "row_key": ["_dlt_id"],
    }
    for column_name, column in table_hints["columns"].items():
        # default nullable
        if column.get("nullable") is None:
            column["nullable"] = True

        # "compiled hints"
        for hint_key, column_selection in DEFAULT_HINTS.items():
            if column_name in column_selection:
                hint_key = "nullable" if hint_key == "not_null" else hint_key
                column[hint_key] = not ColumnPropInfos[hint_key].defaults[0]  # type: ignore

    return table_hints


# could handle 'CREATE OR REPLACE' SQL
def ddl_to_table_schema(ddl: Union[str, sge.Create]) -> TTableSchema:
    """Convert a table DDL to a dlt table schema.

    Can be used to discover tables on the destination that aren't managed by dlt.

    Compared to `_from_sqlglot_table_def()`, the output of `_dlt_to_table_schema()`
    should match the stored schemas (which receive post-processing)
    """
    ddl_expr = maybe_parse(ddl)
    # TODO skip or raise
    if not isinstance(ddl_expr, sge.Create) and ddl_expr.kind != "TABLE":
        raise TypeError()

    table_schema = _from_sqlglot_table_def(ddl_expr)
    table_schema = _set_default_hints(table_schema)
    return table_schema


# yes, this is just a wrapper. This allows us to modify private interface `_to_sqlglot_table_def()`
def table_schema_to_ddl(
    table_schema: TTableSchema, dataset_name: Optional[str] = None
) -> sge.Create:
    """Convert a dlt table schema to an SQLGlot DDL query."""
    return _to_sqlglot_table_def(table_schema, dataset_name)
