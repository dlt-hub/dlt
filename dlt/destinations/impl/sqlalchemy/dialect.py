"""Extensible dialect capabilities for the SqlAlchemy destination.

Users can register custom DialectCapabilities subclasses to adapt the destination
for dialects that are not built-in. See register_dialect_capabilities.
"""

from typing import Any, Dict, Optional, Type

import sqlalchemy as sa

from dlt.common.destination.capabilities import DataTypeMapper, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema


# generic undefined-relation patterns shared across dialects
# matched against lowercased str(exception)
_GENERIC_UNDEFINED_RELATION_PATTERNS = [
    # MySQL / MariaDB
    "unknown database",
    "doesn't exist",
    "unknown table",
    # SQLite
    "no such table",
    "no such database",
    # PostgreSQL / Trino / Vertica / Exasol
    "does not exist",
    # MSSQL
    "invalid object name",
    # SAP HANA
    "invalid schema name",
    "invalid table name",
    # DB2
    "is an undefined name",
    # Apache Hive
    "table not found",
    "database does not exist",
    # Exasol (broad)
    " not found",
]

# when an exception matches these but not the undefined-relation patterns above,
# it is still terminal (e.g. wrong column name, syntax referring to non-existing object)
GENERIC_TERMINAL_PATTERNS = [
    "no such",
    "not found",
    "not exist",
    "unknown",
]


class DialectCapabilities:
    """Base class defining dialect-specific behavior for the SqlAlchemy destination.

    Subclass this to adapt the destination for a new SqlAlchemy dialect. Each method
    corresponds to an extension point:

    * adjust_capabilities -- tweak destination capabilities (identifier lengths, timestamp
      precision, sqlglot dialect, etc.)
    * type_mapper_class -- return a custom DataTypeMapper subclass for the dialect
    * adapt_table -- modify an sa.Table object before it is materialized
      (e.g. reorder columns for StarRocks)
    * is_undefined_relation -- detect "table/schema not found" errors for the dialect

    The sqlglot_dialect property maps backend names to sqlglot dialect names. Override
    it in subclasses or add entries to SQLGLOT_DIALECTS for non-obvious mappings.
    """

    # backend name → sqlglot dialect name
    # only entries where the two names differ need to be listed; for all others
    # the property falls back to the backend name itself
    SQLGLOT_DIALECTS: Dict[str, str] = {
        "postgresql": "postgres",
        "mssql": "tsql",
        "mariadb": "mysql",
        "awsathena": "athena",
        "teradatasql": "teradata",
    }

    def __init__(self, backend_name: str = "") -> None:
        self._backend_name = backend_name

    @property
    def sqlglot_dialect(self) -> str:
        """The sqlglot dialect name for this backend.

        Looks up SQLGLOT_DIALECTS first, falls back to the backend name itself.
        """
        # use backend name as fallback if mapping not found
        return self.SQLGLOT_DIALECTS.get(self._backend_name, self._backend_name)

    def adjust_capabilities(
        self,
        caps: DestinationCapabilitiesContext,
        dialect: sa.engine.interfaces.Dialect,
    ) -> None:
        """Adjust destination capabilities for this dialect.

        Called during adjust_capabilities on the factory. Modify caps in-place.
        """
        pass

    def type_mapper_class(self) -> Type[DataTypeMapper]:
        """Return the type mapper class for this dialect"""
        from dlt.destinations.impl.sqlalchemy.type_mapper import SqlalchemyTypeMapper

        return SqlalchemyTypeMapper

    def adapt_table(
        self,
        table: sa.Table,
        table_schema: PreparedTableSchema,
    ) -> sa.Table:
        """Modify an sa.Table object before it is created or used for loading.

        Return the (possibly modified) table. The default implementation is a no-op.
        """
        return table

    def is_undefined_relation(self, e: Exception) -> Optional[bool]:
        """Classify an exception as an undefined-relation error (or not).

        The base implementation matches generic patterns that work across many
        databases. Override in subclasses for dialect-specific error detection.

        Returns:
            True if the exception represents a missing table/schema,
            False if it is definitely not such an error, or None to fall
            through to the built-in pattern matching.
        """
        msg = str(e).lower()
        for pat in _GENERIC_UNDEFINED_RELATION_PATTERNS:
            if pat in msg:
                return True
        return None


# registry

DIALECT_CAPS_REGISTRY: Dict[str, Type[DialectCapabilities]] = {}
"""Maps dialect / backend name to the DialectCapabilities class that handles it."""


def register_dialect_capabilities(
    dialect_name: str,
    caps_class: Type[DialectCapabilities],
) -> None:
    """Register a custom DialectCapabilities for a dialect name.

    After registration the capabilities are automatically applied when the
    SqlAlchemy destination connects to a database whose backend name matches
    dialect_name.

    Args:
        dialect_name: Backend name as returned by SqlalchemyCredentials.get_backend_name()
            (e.g. "oracle", "starrocks").
        caps_class: A subclass of DialectCapabilities.

    Raises:
        ValueError: If caps_class is not a subclass of DialectCapabilities.
    """
    if not (isinstance(caps_class, type) and issubclass(caps_class, DialectCapabilities)):
        raise ValueError(
            f"caps_class must be a subclass of DialectCapabilities, got {caps_class!r}"
        )
    DIALECT_CAPS_REGISTRY[dialect_name] = caps_class


def get_dialect_capabilities(dialect_name: str) -> Optional[DialectCapabilities]:
    """Look up previously registered DialectCapabilities instance for a dialect name.
    Returns None of not found
    """
    caps_cls = DIALECT_CAPS_REGISTRY.get(dialect_name)
    if caps_cls is not None:
        return caps_cls(dialect_name)
    return None


# built-in dialect capabilities


class MysqlDialectCapabilities(DialectCapabilities):
    """Capabilities for MySQL / MariaDB."""

    def adjust_capabilities(
        self,
        caps: DestinationCapabilitiesContext,
        dialect: sa.engine.interfaces.Dialect,
    ) -> None:
        # dialect uses 255 (max length for aliases) instead of 64 (max length of identifiers)
        caps.max_identifier_length = 64
        caps.max_column_identifier_length = 64
        caps.format_datetime_literal = _format_mysql_datetime_literal
        caps.enforces_nulls_on_alter = False

    def type_mapper_class(self) -> Type[DataTypeMapper]:
        from dlt.destinations.impl.sqlalchemy.type_mapper import MysqlVariantTypeMapper

        return MysqlVariantTypeMapper


class TrinoDialectCapabilities(DialectCapabilities):
    """Capabilities for Trino."""

    def adjust_capabilities(
        self,
        caps: DestinationCapabilitiesContext,
        dialect: sa.engine.interfaces.Dialect,
    ) -> None:
        caps.timestamp_precision = 3
        caps.max_timestamp_precision = 3

    def type_mapper_class(self) -> Type[DataTypeMapper]:
        from dlt.destinations.impl.sqlalchemy.type_mapper import TrinoVariantTypeMapper

        return TrinoVariantTypeMapper


class MssqlDialectCapabilities(DialectCapabilities):
    """Capabilities for Microsoft SQL Server."""

    def type_mapper_class(self) -> Type[DataTypeMapper]:
        from dlt.destinations.impl.sqlalchemy.type_mapper import MssqlVariantTypeMapper

        return MssqlVariantTypeMapper


class OracleDialectCapabilities(DialectCapabilities):
    """Capabilities for Oracle."""

    def is_undefined_relation(self, e: Exception) -> Optional[bool]:
        msg = str(e).lower()
        # ORA-00942: table or view does not exist
        if "00942" in msg:
            return True
        return super().is_undefined_relation(e)


# helpers


def _format_mysql_datetime_literal(v: Any, precision: int = 6, no_tz: bool = False) -> str:
    from dlt.common.data_writers.escape import format_datetime_literal

    # format without timezone to prevent tz conversion in SELECT
    return format_datetime_literal(v, precision, no_tz=True)


# register built-in dialects

# MySQL / MariaDB
register_dialect_capabilities("mysql", MysqlDialectCapabilities)
register_dialect_capabilities("mariadb", MysqlDialectCapabilities)

# Trino
register_dialect_capabilities("trino", TrinoDialectCapabilities)

# Microsoft SQL Server
register_dialect_capabilities("mssql", MssqlDialectCapabilities)

# Oracle
register_dialect_capabilities("oracle", OracleDialectCapabilities)
