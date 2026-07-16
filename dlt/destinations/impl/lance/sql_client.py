from __future__ import annotations

import re
from typing import Any, Dict, TYPE_CHECKING, Optional, Tuple

import duckdb
from packaging.version import Version

from dlt.common import logger
from dlt.common.destination.exceptions import DestinationTerminalException
from dlt.common.schema.schema import Schema
from dlt.destinations.exceptions import DatabaseUndefinedRelation
from dlt.destinations.sql_client import raise_database_error
from dlt.destinations.impl.duckdb.sql_client import WithTableScanners
from dlt.destinations.impl.lance.configuration import RestCatalogCredentials
from dlt.destinations.impl.lance.exceptions import is_lance_undefined_entity_exception

if TYPE_CHECKING:
    from duckdb import DuckDBPyConnection

    from dlt.common.destination.typing import PreparedTableSchema
    from dlt.destinations.impl.lance.lance_client import LanceClient

# reads via the duckdb `ATTACH ... (TYPE LANCE)` catalog need this duckdb (extension bundled with core)
MIN_ATTACH_DUCKDB_VERSION = Version("1.5.0")


def _install_and_load_lance_duckdb_extension(duckdb_con: DuckDBPyConnection) -> None:
    """Ensure the `lance-duckdb` extension is loaded.

    DuckDB ensures installation is only done once per system.
    Extension loading must be done on every connection.
    """
    duckdb_con.execute("INSTALL lance;")
    duckdb_con.execute("LOAD lance;")


def _prepare_create_lance_secret_statement(
    secret_name: str, scope: str, storage_options: Dict[str, str]
) -> str:
    storage_options_str = "{" + ", ".join(f"'{k}': '{v}'" for k, v in storage_options.items()) + "}"
    # TODO: never_borrowed resets to True after every borrow/return cycle for external connections
    #  (WithTableScanners.memory_db). All open_connection first-time setup must be idempotent.
    return f"""
        CREATE OR REPLACE SECRET {secret_name} (
            TYPE LANCE,
            PROVIDER config,
            SCOPE '{scope}',
            STORAGE_OPTIONS {storage_options_str}
        )"""


class LanceSQLClient(WithTableScanners):
    def __init__(self, lance_client: LanceClient) -> None:
        self.lance_client = lance_client
        # a REST catalog exposes its tables to duckdb through `ATTACH ... (TYPE LANCE)`, which also
        # vends storage credentials. it requires a non-empty namespace id (a `dataset_name`); the
        # root namespace and the `dir` catalog fall back to per-table `__lance_scan()` views.
        self._attach_catalog = lance_client.config.capabilities.duckdb_attach_catalog and bool(
            lance_client.make_namespace_id()
        )
        if self._attach_catalog:
            # the extension exposes the attached namespace tables under the `main` schema
            super().__init__(remote_client=lance_client, dataset_name="main")
            self.database_name = self._attach_catalog_name()
        else:
            # schema-less (no dataset_name): host the read views in the ephemeral duckdb `main` schema
            super().__init__(
                remote_client=lance_client,
                dataset_name=lance_client.dataset_name or "main",
            )

    def catalog_name(self, quote: bool = True, casefold: bool = True) -> Optional[str]:
        if not self.database_name:
            return None
        database_name = (
            self.capabilities.casefold_identifier(self.database_name)
            if casefold
            else self.database_name
        )
        return self.capabilities.escape_identifier(database_name) if quote else database_name

    def open_connection(self) -> DuckDBPyConnection:
        with self.credentials.conn_pool._conn_lock:
            first_connection = self.credentials.conn_pool.never_borrowed
            if self._attach_catalog:
                # skip WithTableScanners dataset-schema creation: reads target the attached catalog
                super(WithTableScanners, self).open_connection()
            else:
                super().open_connection()

        if first_connection:
            _install_and_load_lance_duckdb_extension(self._conn)
            # a configured storage overrides the catalog: secret lets the extension read the bucket
            # when the namespace does not vend credentials
            self._create_lance_secret()
            if self._attach_catalog:
                self._attach_lance_namespace()

        return self._conn

    def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
        return True

    def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
        return not self._attach_catalog and self.lance_client.config.always_refresh_views

    def create_views_for_tables(self, tables: Dict[str, str]) -> None:
        if self._attach_catalog:
            return
        # lance extension caches datasets so new data is not visible
        # automatically, we duplicate connection to clear the cache
        if self.lance_client.config.always_refresh_views:
            self._conn = self.memory_db.duplicate()
        super().create_views_for_tables(tables)

    def create_view_select(
        self, table_schema: PreparedTableSchema, schema: Schema = None
    ) -> Optional[Tuple[str, str]]:
        if self._attach_catalog:
            return None
        table_name = table_schema["name"]
        lance_table_uri = self.lance_client.get_table_uri(table_name)
        # NOTE: direct querying fails with our Lance Directory Namespace Catalog Spec V2 table URIs, but
        # going through __lance_scan() does work
        return lance_table_uri, f"SELECT * FROM __lance_scan('{lance_table_uri}')"

    def _attach_catalog_name(self) -> str:
        """Returns a stable, duckdb-safe catalog alias for the attached namespace."""
        normalized = re.sub(r"[^a-zA-Z0-9_]", "_", self.lance_client.dataset_name)
        return f"lance_{normalized}"

    @raise_database_error
    def _attach_lance_namespace(self) -> None:
        if Version(duckdb.__version__) < MIN_ATTACH_DUCKDB_VERSION:
            raise DestinationTerminalException(
                "Querying a Lance REST catalog needs duckdb >="
                f" {MIN_ATTACH_DUCKDB_VERSION} with a lance extension that supports `ATTACH ..."
                " (TYPE LANCE, ENDPOINT ...)`. Please upgrade duckdb."
            )
        credentials = self.lance_client.config.credentials
        assert isinstance(credentials, RestCatalogCredentials)
        already_attached = self._conn.execute(
            "SELECT count(*) > 0 FROM duckdb_databases() WHERE database_name = ?",
            [self.database_name],
        ).fetchone()[0]
        if not already_attached:
            # namespace id is the delimiter-joined namespace path (single-level for dlt datasets)
            namespace_id = "$".join(self.lance_client.make_namespace_id())
            options = ["TYPE LANCE", f"ENDPOINT '{credentials.uri}'"]
            if header := credentials.to_duckdb_header():
                options.append(f"HEADER '{header}'")
            try:
                self._conn.execute(
                    f"ATTACH '{namespace_id}' AS {self.catalog_name()} ({', '.join(options)})"
                )
            except duckdb.Error as exc:
                raise DestinationTerminalException(
                    f"Could not attach Lance REST namespace '{namespace_id}' at {credentials.uri}."
                    f" Ensure duckdb (>= {MIN_ATTACH_DUCKDB_VERSION}) has a lance extension that"
                    " supports REST catalogs."
                ) from exc
            logger.info(f"Attached Lance REST namespace '{namespace_id}' as {self.catalog_name()}")
        self._conn.execute(f"USE {self.fully_qualified_dataset_name()}")

    @raise_database_error
    def _create_lance_secret(self) -> None:
        storage_options = self.lance_client.config.storage_options
        if not storage_options:
            return
        scope = self.lance_client.config.storage.namespace_uri
        secret_name = self.create_secret_name(scope)
        stmt = _prepare_create_lance_secret_statement(secret_name, scope, storage_options)
        self._conn.execute(stmt)

    @classmethod
    def _make_database_exception(cls, ex: Exception) -> Exception:
        if is_lance_undefined_entity_exception(ex):
            return DatabaseUndefinedRelation(ex)
        return super()._make_database_exception(ex)
