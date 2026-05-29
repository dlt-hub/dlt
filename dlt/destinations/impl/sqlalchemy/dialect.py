"""Extensible dialect capabilities for the SqlAlchemy destination.

Users can register custom DialectCapabilities subclasses to adapt the destination
for dialects that are not built-in. See register_dialect_capabilities.
"""

from typing import Any, Dict, List, Optional, Type, TYPE_CHECKING

import sqlalchemy as sa  # noqa

from dlt.common.destination.capabilities import DataTypeMapper, DestinationCapabilitiesContext
from dlt.common.destination.typing import PreparedTableSchema
from dlt.destinations.exceptions import DatabaseException, DatabaseTerminalException

if TYPE_CHECKING:
    from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient


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
"""Generic undefined-relation patterns shared across dialects, matched against lowercased str(exception)."""

GENERIC_TERMINAL_PATTERNS = [
    "no such",
    "not found",
    "not exist",
    "unknown",
]
"""Terminal patterns for exceptions that don't match undefined-relation but are still non-transient."""


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
    * dataset_exists / create_dataset / drop_dataset -- adapt schema (dataset) lifecycle
      for dialects that do not support bare CREATE SCHEMA (e.g. Oracle)

    The sqlglot_dialect property maps backend names to sqlglot dialect names. Override
    it in subclasses or add entries to SQLGLOT_DIALECTS for non-obvious mappings.
    """

    SQLGLOT_DIALECTS: Dict[str, str] = {
        "postgresql": "postgres",
        "mssql": "tsql",
        "mariadb": "mysql",
        "awsathena": "athena",
        "teradatasql": "teradata",
    }
    """Backend name to sqlglot dialect name. Only entries where the two differ; others fall back to the backend name."""

    def __init__(self, backend_name: str = "") -> None:
        self._backend_name = backend_name

    @property
    def sqlglot_dialect(self) -> str:
        """The sqlglot dialect name for this backend.

        Looks up SQLGLOT_DIALECTS first, falls back to the backend name itself.
        """
        return self.SQLGLOT_DIALECTS.get(self._backend_name, self._backend_name)

    def adjust_capabilities(
        self,
        caps: DestinationCapabilitiesContext,
        dialect: sa.engine.interfaces.Dialect,
    ) -> None:
        """Adjust destination capabilities for this dialect.

        Called during adjust_capabilities on the factory. Modify caps in-place.
        """

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

    def dataset_exists(self, schema_names: List[str], dataset_name: str) -> bool:
        """Return True if the dataset (schema) exists among the schemas reported by the database.

        Args:
            schema_names: Schema names as returned by the dialect's get_schema_names.
            dataset_name: Name of the dataset (schema) dlt is looking for.
        """
        return dataset_name in schema_names

    def create_dataset(self, client: "SqlalchemyClient") -> None:
        """Create the dataset (schema) identified by client.dataset_name."""
        client.execute_sql(sa.schema.CreateSchema(client.dataset_name))

    def drop_dataset(self, client: "SqlalchemyClient") -> None:
        """Drop the dataset (schema) identified by client.dataset_name and all objects it contains."""
        try:
            client.execute_sql(sa.schema.DropSchema(client.dataset_name, cascade=True))
        except DatabaseException:  # Try again in case cascade is not supported
            client.execute_sql(sa.schema.DropSchema(client.dataset_name))


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
    """Capabilities for Oracle.

    In Oracle a schema is owned by a database user and cannot be created with a bare
    `CREATE SCHEMA` statement (that fails with ORA-02420). dlt therefore treats the dataset
    as an existing schema (user) that must be created in advance and only manages the tables
    within it.
    """

    def is_undefined_relation(self, e: Exception) -> Optional[bool]:
        msg = str(e).lower()
        # ORA-00942: table or view does not exist
        if "00942" in msg:
            return True
        return super().is_undefined_relation(e)

    def dataset_exists(self, schema_names: List[str], dataset_name: str) -> bool:
        # Oracle folds unquoted identifiers to upper case, so match case-insensitively
        folded = dataset_name.casefold()
        return any(name.casefold() == folded for name in schema_names)

    def create_dataset(self, client: "SqlalchemyClient") -> None:
        # Oracle has no bare CREATE SCHEMA (schemas are users); the schema must already exist
        if client.has_dataset():
            return
        raise DatabaseTerminalException(
            Exception(
                f"Oracle schema (user) '{client.dataset_name}' does not exist and cannot be"
                " created by dlt. In Oracle a schema is owned by a database user and must be"
                ' created in advance, e.g. `CREATE USER "'
                f"{client.dataset_name}"
                '" IDENTIFIED BY ...` with the appropriate quota and grants (CREATE SESSION,'
                " CREATE TABLE, ...). The staging dataset (named '<dataset_name>_staging') used by"
                " merge and replace write dispositions must be created the same way. Create the"
                " schema(s) manually and run the pipeline again."
            )
        )

    def drop_dataset(self, client: "SqlalchemyClient") -> None:
        # Oracle cannot DROP SCHEMA (that would require DROP USER, a DBA privilege the loader
        # rarely has and which dlt does not own); drop the tables within the schema instead
        table_names = sa.inspect(client.engine).get_table_names(schema=client.dataset_name)
        if table_names:
            client.drop_tables(*table_names)


class DuckdbDialectCapabilities(DialectCapabilities):
    """Capabilities for DuckDB via duckdb_engine."""

    def is_undefined_relation(self, e: Exception) -> Optional[bool]:
        msg = str(e).lower()
        # binder errors (ie. unknown column) also contain "not found" but are terminal
        if "binder error" in msg:
            return False
        return super().is_undefined_relation(e)


def _format_mysql_datetime_literal(v: Any, precision: int = 6, no_tz: bool = False) -> str:
    from dlt.common.data_writers.escape import format_datetime_literal

    return format_datetime_literal(v, precision, no_tz=True)


register_dialect_capabilities("mysql", MysqlDialectCapabilities)
register_dialect_capabilities("mariadb", MysqlDialectCapabilities)
register_dialect_capabilities("trino", TrinoDialectCapabilities)
register_dialect_capabilities("mssql", MssqlDialectCapabilities)
register_dialect_capabilities("oracle", OracleDialectCapabilities)
register_dialect_capabilities("duckdb", DuckdbDialectCapabilities)
