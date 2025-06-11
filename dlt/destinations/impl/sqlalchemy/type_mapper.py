from typing import Optional, Dict, Any
import inspect

import sqlalchemy as sa
from sqlalchemy.sql import sqltypes

from dlt.common.exceptions import TerminalValueError
from dlt.common.typing import TLoaderFileFormat
from dlt.common.destination.capabilities import DataTypeMapper, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.schema.typing import TColumnSchema


# TODO: base type mapper should be a generic class to support TypeEngine instead of str types
class SqlalchemyTypeMapper(DataTypeMapper):
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
            if self.dialect.name == "mysql":
                # Special case, type_descriptor does not return the specifc datetime type
                from sqlalchemy.dialects.mysql import DATETIME

                return DATETIME(fsp=precision)
        elif sc_t == "time":
            base_type = sa.Time()

        dialect_type = type(
            self.dialect.type_descriptor(base_type)
        )  # Get the dialect specific subtype
        precision = precision if precision is not None else self.capabilities.timestamp_precision

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
        elif self.dialect.name == "mysql":
            # MySQL has a specific double type
            from sqlalchemy.dialects.mysql import DOUBLE

            return DOUBLE()
        return sa.Float(precision=53)  # Otherwise use float

    def _to_db_decimal_type(self, column: TColumnSchema) -> sa.types.TypeEngine:
        precision, scale = column.get("precision"), column.get("scale")
        if precision is None and scale is None:
            precision, scale = self.capabilities.decimal_precision
        return sa.Numeric(precision, scale)

    def to_destination_type(  # type: ignore[override]
        self, column: TColumnSchema, table: PreparedTableSchema = None
    ) -> sqltypes.TypeEngine:
        sc_t = column["data_type"]
        precision = column.get("precision")
        # TODO: Precision and scale for supported types
        if sc_t == "text":
            length = precision
            if length is None and column.get("unique"):
                length = 128
            if length is None:
                return sa.Text()
            return sa.String(length=length)
        elif sc_t == "double":
            return self._create_double_type()
        elif sc_t == "bool":
            return sa.Boolean()
        elif sc_t == "timestamp":
            return self._create_date_time_type(sc_t, precision, column.get("timezone"))
        elif sc_t == "bigint":
            return self._db_integer_type(precision)
        elif sc_t == "binary":
            return sa.LargeBinary(length=precision)
        elif sc_t == "json":
            return sa.JSON(none_as_null=True)
        elif sc_t == "decimal":
            return self._to_db_decimal_type(column)
        elif sc_t == "wei":
            wei_precision, wei_scale = self.capabilities.wei_precision
            return sa.Numeric(precision=wei_precision, scale=wei_scale)
        elif sc_t == "date":
            return sa.Date()
        elif sc_t == "time":
            return self._create_date_time_type(sc_t, precision, column.get("timezone"))
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

        pass

    def ensure_supported_type(
        self,
        column: TColumnSchema,
        table: PreparedTableSchema,
        loader_file_format: TLoaderFileFormat,
    ) -> None:
        pass
