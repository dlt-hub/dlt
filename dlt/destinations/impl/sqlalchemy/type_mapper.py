from typing import Optional, Dict, Any
import inspect

import sqlalchemy as sa
from sqlalchemy import TypeDecorator
from sqlalchemy.sql import sqltypes

from dlt.common import json
from dlt.common.exceptions import TerminalValueError
from dlt.common.typing import TLoaderFileFormat
from dlt.common.destination.capabilities import DataTypeMapper, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema


# TODO: base type mapper should be a generic class to support TypeEngine instead of str types
class SqlalchemyTypeMapper(DataTypeMapper):
    """Maps dlt data types to SQLAlchemy column types and back.

    to_destination_type dispatches to per-type visitor methods named
    _db_type_from_<dlt_type>_type (e.g. _db_type_from_json_type). Subclasses
    can override individual visitors to customize mapping for a single dlt type
    without reimplementing the full dispatch. Overriding to_destination_type
    directly is still supported for backward compatibility.
    """

    def __init__(
        self,
        capabilities: DestinationCapabilitiesContext,
        dialect: Optional[sa.engine.Dialect] = None,
    ):
        super().__init__(capabilities)
        # Mapper is used to verify supported types without client, dialect is not important for this
        self.dialect = dialect or sa.engine.default.DefaultDialect()

    def _db_integer_type(self, precision: Optional[int]) -> sa.types.TypeEngine:
        if precision is None:
            return sa.BigInteger()
        elif precision <= 16:
            return sa.SmallInteger()
        elif precision <= 32:
            return sa.Integer()
        elif precision <= 64:
            return sa.BigInteger()
        raise TerminalValueError(f"Unsupported `{precision=:}` for integer type.")

    def _create_date_time_type(
        self, sc_t: str, precision: Optional[int], timezone: Optional[bool]
    ) -> sa.types.TypeEngine:
        """Use the dialect specific datetime/time type if possible since the generic type doesn't accept precision argument"""
        precision = precision if precision is not None else self.capabilities.timestamp_precision
        base_type: sa.types.TypeEngine
        timezone = timezone is None or bool(timezone)
        if sc_t == "timestamp":
            base_type = sa.DateTime()
        elif sc_t == "time":
            base_type = sa.Time()

        dialect_type = type(
            self.dialect.type_descriptor(base_type)
        )  # Get the dialect specific subtype

        # Find out whether the dialect type accepts precision or fsp argument
        params = inspect.signature(dialect_type).parameters
        kwargs: Dict[str, Any] = dict(timezone=timezone)
        if "fsp" in params:
            kwargs["fsp"] = precision  # MySQL uses fsp for fractional seconds
        elif "precision" in params:
            kwargs["precision"] = precision
        return dialect_type(**kwargs)  # type: ignore[no-any-return,misc]

    def _create_double_type(self) -> sa.types.TypeEngine:
        if dbl := getattr(sa, "Double", None):
            # Sqlalchemy 2 has generic double type
            return dbl()  # type: ignore[no-any-return]
        return sa.Float(precision=53)  # Otherwise use float

    def _to_db_decimal_type(self, column: TColumnSchema) -> sa.types.TypeEngine:
        precision, scale = column.get("precision"), column.get("scale")
        if precision is None and scale is None:
            precision, scale = self.capabilities.decimal_precision
        return sa.Numeric(precision, scale)

    def db_type_from_text_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "text" — variable-length string, optional precision sets max length."""
        precision = column.get("precision")
        length = precision
        if length is None and column.get("unique"):
            length = 128
        if length is None:
            return sa.Text()
        else:
            return sa.String(length=length)

    def db_type_from_double_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "double" — 64-bit IEEE 754 floating point."""
        return self._create_double_type()

    def db_type_from_bool_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "bool" — true/false."""
        return sa.Boolean()

    def db_type_from_timestamp_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "timestamp" — date and time with optional timezone and fractional seconds."""
        return self._create_date_time_type(
            "timestamp", column.get("precision"), column.get("timezone")
        )

    def db_type_from_bigint_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "bigint" — integer with optional precision (16, 32, 64 bits)."""
        return self._db_integer_type(column.get("precision"))

    def db_type_from_binary_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "binary" — raw bytes, optional precision sets max length."""
        return sa.LargeBinary(length=column.get("precision"))

    def db_type_from_json_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "json" — arbitrary nested JSON data."""
        return sa.JSON(none_as_null=True)

    def db_type_from_decimal_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "decimal" — arbitrary-precision decimal number."""
        return self._to_db_decimal_type(column)

    def db_type_from_wei_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "wei" — large-precision integer for blockchain wei values."""
        wei_precision, wei_scale = self.capabilities.wei_precision
        return sa.Numeric(precision=wei_precision, scale=wei_scale)

    def db_type_from_date_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "date" — calendar date without time component."""
        return sa.Date()

    def db_type_from_time_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        """Converts dlt "time" — time of day with optional timezone and fractional seconds."""
        return self._create_date_time_type("time", column.get("precision"), column.get("timezone"))

    def to_destination_type(  # type: ignore[override]
        self, column: TColumnSchema, table: PreparedTableSchema = None
    ) -> sqltypes.TypeEngine:
        sc_t = column["data_type"]
        method = getattr(self, f"db_type_from_{sc_t}_type", None)
        if method is not None:
            return method(column, table)  # type: ignore[no-any-return]
        raise TerminalValueError(f"Unsupported data type: `{sc_t}`")

    def _from_db_integer_type(self, db_type: sa.Integer) -> TColumnSchema:
        if isinstance(db_type, sa.SmallInteger):
            return dict(data_type="bigint", precision=16)
        elif isinstance(db_type, sa.Integer):
            return dict(data_type="bigint", precision=32)
        elif isinstance(db_type, sa.BigInteger):
            return dict(data_type="bigint")
        return dict(data_type="bigint")

    def _from_db_decimal_type(self, db_type: sa.Numeric) -> TColumnSchema:
        precision, scale = db_type.precision, db_type.scale
        if (precision, scale) == self.capabilities.wei_precision:
            return dict(data_type="wei")

        return dict(data_type="decimal", precision=precision, scale=scale)

    def from_destination_type(  # type: ignore[override]
        self,
        db_type: sqltypes.TypeEngine,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
    ) -> TColumnSchema:
        # TODO: pass the sqla type through dialect.type_descriptor before instance check
        # Possibly need to check both dialect specific and generic types
        if isinstance(db_type, sa.String):
            return dict(data_type="text")
        elif isinstance(db_type, sa.Float):
            return dict(data_type="double")
        elif isinstance(db_type, sa.Boolean):
            return dict(data_type="bool")
        elif isinstance(db_type, sa.DateTime):
            return dict(data_type="timestamp", timezone=db_type.timezone)
        elif isinstance(db_type, sa.Integer):
            return self._from_db_integer_type(db_type)
        elif isinstance(db_type, sqltypes._Binary):
            return dict(data_type="binary", precision=db_type.length)
        elif isinstance(db_type, sa.JSON):
            return dict(data_type="json")
        elif isinstance(db_type, sa.Numeric):
            return self._from_db_decimal_type(db_type)
        elif isinstance(db_type, sa.Date):
            return dict(data_type="date")
        elif isinstance(db_type, sa.Time):
            return dict(data_type="time")
        raise TerminalValueError(f"Unsupported db type: `{db_type}`")

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        pass


class MssqlVariantTypeMapper(SqlalchemyTypeMapper):
    def db_type_from_text_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        dt = super().db_type_from_text_type(column, table)
        precision = column.get("precision")
        length = precision
        if length is None and column.get("unique"):
            length = 4000  # max regular varchar
        if length is None:
            return dt.with_variant(sa.UnicodeText(), "mssql")  # type: ignore[no-any-return]
        else:
            return dt.with_variant(sa.Unicode(length=length), "mssql")  # type: ignore[no-any-return]


class MysqlVariantTypeMapper(SqlalchemyTypeMapper):
    def db_type_from_double_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        from sqlalchemy.dialects.mysql import DOUBLE

        return DOUBLE()

    def db_type_from_timestamp_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        # Special case, type_descriptor does not return the specific datetime type
        from sqlalchemy.dialects.mysql import DATETIME

        precision = column.get("precision")
        precision = precision if precision is not None else self.capabilities.timestamp_precision
        return DATETIME(fsp=precision)


class HexVarBinary(TypeDecorator):
    impl = sa.String

    def bind_processor(self, dialect: sa.engine.Dialect) -> Any:
        import binascii

        return lambda v: None if v is None else binascii.hexlify(v).decode()


class JSONString(sa.TypeDecorator):
    """Custom SQLAlchemy type that stores JSON data as a string in the database.

    Automatically serializes Python objects to JSON strings on write and
    deserializes JSON strings back to Python objects on read.
    """

    impl = sa.String
    cache_ok = True

    def process_bind_param(self, value: Any, dialect: sa.engine.Dialect) -> Any:
        if value is None:
            return None

        if isinstance(value, str):
            return value

        return json.dumps(value)

    def process_result_value(self, value: Any, dialect: sa.engine.Dialect) -> Any:
        if value is None:
            return None

        return json.loads(value)


class TrinoVariantTypeMapper(SqlalchemyTypeMapper):
    def db_type_from_binary_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        dt = super().db_type_from_binary_type(column, table)
        return dt.with_variant(HexVarBinary(), "trino")  # type: ignore[no-any-return]

    def db_type_from_json_type(
        self, column: TColumnSchema, table: PreparedTableSchema
    ) -> sqltypes.TypeEngine:
        return JSONString()
