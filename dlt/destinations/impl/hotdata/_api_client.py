from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, List, Optional, TypeVar

from dlt.common import logger
from dlt.destinations.impl.hotdata.errors import (
    HotdataTerminalError,
    HotdataTransientError,
    classify_sdk_error,
)

T = TypeVar("T")

_MAX_BACKOFF_SECONDS = 30.0
_QUERY_TIMEOUT_SECONDS = 300.0
_LISTING_PROPAGATION_ATTEMPTS = 3
_LISTING_PROPAGATION_SLEEP = 1.0


def _make_configuration(api_key: str, workspace_id: str, api_base_url: str) -> Any:
    import hotdata

    return hotdata.Configuration(
        api_key=api_key,
        workspace_id=workspace_id,
        host=api_base_url.rstrip("/"),
    )


class HotdataApiClient:
    """Managed-database client built directly on the hotdata Python SDK."""

    def __init__(
        self,
        *,
        api_key: str,
        workspace_id: str,
        api_base_url: str,
        max_retries: int,
        retry_backoff_seconds: float,
    ) -> None:
        import hotdata

        self._max_retries = max_retries
        self._retry_backoff_seconds = retry_backoff_seconds
        self._api = hotdata.ApiClient(
            configuration=_make_configuration(api_key, workspace_id, api_base_url)
        )

    def close(self) -> None:
        self._api.close()

    # ------------------------------------------------------------------
    # managed database
    # ------------------------------------------------------------------

    def ensure_managed_database(
        self,
        name: str,
        *,
        schema: str,
        tables: List[str],
        create_if_missing: bool,
    ) -> "_DatabaseHandle":
        def operation() -> "_DatabaseHandle":
            from hotdata.api.databases_api import DatabasesApi
            from hotdata.exceptions import NotFoundException

            db_api = DatabasesApi(self._api)
            listing = db_api.list_databases()
            for summary in listing.databases:
                if summary.name == name:
                    try:
                        detail = db_api.get_database(summary.id)
                    except NotFoundException:
                        # stale listing entry after a recent deletion — treat as missing
                        continue
                    db = _DatabaseHandle(id=detail.id, connection_id=detail.default_connection_id)

                    declared = self._list_declared_tables(db.connection_id, schema=schema)
                    missing = set(tables) - declared
                    if not missing:
                        return db

                    # tables can only be declared at creation time — recreate with the union
                    all_tables = sorted(declared | set(tables))
                    logger.info(
                        f"Hotdata managed database {name!r} is missing tables {sorted(missing)!r};"
                        f" recreating with {all_tables!r}"
                    )
                    db_api.delete_database(db.id)
                    return self._create_managed_database(name, schema=schema, tables=all_tables)

            if not create_if_missing:
                raise KeyError(f"Managed database {name!r} not found")

            return self._create_managed_database(name, schema=schema, tables=tables)

        return self._request_with_retry(operation)

    def _list_declared_tables(self, connection_id: str, *, schema: str) -> set:
        from hotdata.api.information_schema_api import InformationSchemaApi

        api = InformationSchemaApi(self._api)
        declared: set = set()
        cursor = None
        while True:
            resp = api.information_schema(
                connection_id=connection_id,
                var_schema=schema,
                cursor=cursor,
            )
            for entry in resp.tables:
                if entry.var_schema == schema:
                    declared.add(entry.table)
            if not resp.has_more:
                break
            cursor = resp.next_cursor
        return declared

    def _create_managed_database(
        self, name: str, *, schema: str, tables: List[str]
    ) -> "_DatabaseHandle":
        from hotdata.api.databases_api import DatabasesApi
        from hotdata.models.create_database_request import CreateDatabaseRequest
        from hotdata.models.database_default_schema_decl import DatabaseDefaultSchemaDecl
        from hotdata.models.database_default_table_decl import DatabaseDefaultTableDecl

        decl = DatabaseDefaultSchemaDecl(
            name=schema,
            tables=[DatabaseDefaultTableDecl(name=t) for t in sorted(set(tables))],
        )
        resp = DatabasesApi(self._api).create_database(
            CreateDatabaseRequest(name=name, schemas=[decl])
        )
        return _DatabaseHandle(id=resp.id, connection_id=resp.default_connection_id)

    def _resolve_database(self, name: str) -> "_DatabaseHandle":
        from hotdata.api.databases_api import DatabasesApi
        from hotdata.exceptions import NotFoundException

        db_api = DatabasesApi(self._api)
        # retry to handle listing propagation delay after creation or deletion
        for attempt in range(_LISTING_PROPAGATION_ATTEMPTS):
            listing = db_api.list_databases()
            for summary in listing.databases:
                if summary.name == name:
                    try:
                        detail = db_api.get_database(summary.id)
                    except NotFoundException:
                        continue
                    return _DatabaseHandle(id=detail.id, connection_id=detail.default_connection_id)
            if attempt < _LISTING_PROPAGATION_ATTEMPTS - 1:
                time.sleep(_LISTING_PROPAGATION_SLEEP)
        raise KeyError(f"Managed database {name!r} not found")

    # ------------------------------------------------------------------
    # table status
    # ------------------------------------------------------------------

    def _table_is_synced(self, connection_id: str, *, schema: str, table: str) -> bool:
        from hotdata.api.information_schema_api import InformationSchemaApi

        resp = InformationSchemaApi(self._api).information_schema(
            connection_id=connection_id,
            var_schema=schema,
            table=table,
        )
        for entry in resp.tables:
            if entry.table == table and entry.var_schema == schema:
                return entry.synced
        return False

    # ------------------------------------------------------------------
    # data fetch
    # ------------------------------------------------------------------

    def fetch_table(self, *, database: str, schema: str, table: str) -> Optional["pyarrow.Table"]:
        """Fetch table contents as an Arrow table, or None if never loaded."""

        def operation() -> Optional[Any]:
            from hotdata.api.query_api import QueryApi
            from hotdata.arrow import ResultsApi as ArrowResultsApi
            from hotdata.models.query_request import QueryRequest

            db = self._resolve_database(database)
            if not self._table_is_synced(db.connection_id, schema=schema, table=table):
                return None

            sql = f'SELECT * FROM "default"."{schema}"."{table}"'
            response = QueryApi(self._api).query(
                QueryRequest(sql=sql, database_id=db.id),
                x_database_id=db.id,
            )

            # a synchronous response carries the result id directly; an async one
            # only exposes a query run id that resolves to the result once it completes
            result_id = getattr(response, "result_id", None)
            if result_id is None:
                result_id = self._poll_query_run(response.query_run_id, database_id=db.id)
            if result_id is None:
                return None

            self._wait_result_ready(result_id, database_id=db.id)
            return ArrowResultsApi(self._api).get_result_arrow(result_id, db.id)

        return self._request_with_retry(operation)

    def _poll_query_run(self, query_run_id: str, *, database_id: str) -> Optional[str]:
        from hotdata.api.query_runs_api import QueryRunsApi

        runs = QueryRunsApi(self._api)
        deadline = time.monotonic() + _QUERY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            run = runs.get_query_run(query_run_id, database_id)
            if run.status == "succeeded":
                return run.result_id
            if run.status in ("failed", "cancelled"):
                raise RuntimeError(run.error_message or f"Query run {run.status}")
            time.sleep(0.5)
        raise TimeoutError(f"Managed database query timed out after {_QUERY_TIMEOUT_SECONDS}s")

    def _wait_result_ready(self, result_id: str, *, database_id: str) -> Optional[str]:
        from hotdata.api.results_api import ResultsApi

        results = ResultsApi(self._api)
        deadline = time.monotonic() + _QUERY_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            r = results.get_result(result_id, database_id)
            if r.status == "ready":
                return result_id
            if r.status in ("failed", "cancelled"):
                raise RuntimeError(r.error_message or f"Result {r.status}")
            time.sleep(0.3)
        raise TimeoutError(f"Result {result_id} not ready after {_QUERY_TIMEOUT_SECONDS}s")

    # ------------------------------------------------------------------
    # upload and load
    # ------------------------------------------------------------------

    def upload_parquet(self, path: str) -> str:
        """Upload a parquet file and return its upload ID."""

        def operation() -> str:
            from hotdata.uploads import UploadsApi

            resp = UploadsApi(self._api).upload_file(path, content_type="application/parquet")
            return resp.upload_id

        return self._request_with_retry(operation)

    def load_managed_table(
        self,
        database: str,
        table: str,
        *,
        schema: str,
        upload_id: str,
    ) -> None:
        def operation() -> None:
            from hotdata.api.connections_api import ConnectionsApi
            from hotdata.models.load_managed_table_request import LoadManagedTableRequest

            db = self._resolve_database(database)
            ConnectionsApi(self._api).load_managed_table(
                connection_id=db.connection_id,
                var_schema=schema,
                table=table,
                load_managed_table_request=LoadManagedTableRequest(
                    mode="replace",
                    format="parquet",
                    upload_id=upload_id,
                ),
            )

        self._request_with_retry(operation)

    # ------------------------------------------------------------------
    # retry
    # ------------------------------------------------------------------

    def _request_with_retry(self, operation: Callable[[], T]) -> T:
        for attempt in range(1, self._max_retries + 1):
            try:
                return operation()
            except Exception as error:
                mapped = classify_sdk_error(error.__cause__ or error)
                if isinstance(mapped, HotdataTransientError) and attempt < self._max_retries:
                    backoff = min(self._retry_backoff_seconds * attempt, _MAX_BACKOFF_SECONDS)
                    time.sleep(backoff)
                    continue
                raise mapped from error


class _DatabaseHandle:
    """Lightweight holder for a resolved managed database's identity."""

    __slots__ = ("id", "connection_id")

    def __init__(self, *, id: str, connection_id: str) -> None:
        self.id = id
        self.connection_id = connection_id
