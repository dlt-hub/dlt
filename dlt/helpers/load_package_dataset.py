"""Read a dlt load package as a `dlt.Dataset`, without loading it to a destination.

A normalized load package is already "a dataset in files": data files named
`{table_name}.{file_id}.{retry_count}.{file_format}[.gz]` living under
`<load_id>/<job_state>/`, plus a `schema.json` in the package root. That is the
same shape the `filesystem` destination reads, so this module reuses the exact
same machinery:

    dlt.Dataset
      └─ LoadPackageClient (WithSqlClient)          # read-only, wraps PackageStorage
           └─ LoadPackageSqlClient(WithTableScanners)   # duckdb views over the job files
                └─ create_view_select() -> read_parquet(...) / read_json(...) / read_csv(...)

Usage::

    pipeline = dlt.pipeline("chess", destination="postgres")
    pipeline.extract(source)
    pipeline.normalize()

    load_id = ni.loads_ids[-1]               # or any other load id you want to inspect
    ds = load_package_dataset(pipeline, load_id)
    ds.tables
    ds.customers.head(10).df()
    ds("select count(*) from customers").arrow()

    pipeline.load()                         # proceed once happy

Everything after `ds =` is the stock `dlt.Dataset` / `dlt.Relation` API.

`load_id` is a required argument: a load package is a snapshot of one run and does not
accumulate data across runs like a destination does, so there is no safe notion of "the latest
package" to default to (see `load_package_dataset` docstring).

Each call opens exactly one state and one load_id, backed by its own private duckdb connection.
`dataset_name` defaults to the package state ("extracted", "normalized", "loaded", ...) so the
duckdb schema holding the views is named after what you are looking at.

NOTE: this module is deliberately self-contained for review. Before merging, the
"native files" branch of `create_view_select` should be factored out of
`dlt.destinations.impl.filesystem.sql_client.FilesystemSqlClient` and shared with
`LoadPackageSqlClient`, so reads before and after the load step stay identical.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Any, Final, TYPE_CHECKING
from collections.abc import Iterable, Iterator, Sequence

import dlt
from dlt.common.configuration import configspec
from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.client import (
    DestinationClientDwhConfiguration,
    JobClientBase,
    LoadJob,
    PreparedTableSchema,
)
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.schema import Schema
from dlt.common.schema.utils import is_nullable_column
from dlt.common.storages import FileStorage
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.storages.load_package import (
    PackageStorage,
    ParsedLoadJobFileName,
    TLoadPackageStatus,
    TPackageJobState,
)
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient

if TYPE_CHECKING:
    from dlt.common.libs.ibis import BaseBackend
    from dlt.pipeline.pipeline import Pipeline


# default duckdb schema name for a dataset is derived from `package_state` ("extracted",
# "normalized", "loaded", ...), see `load_package_dataset`. this constant is only a fallback
# used when neither is available (e.g. a raw `PackageStorage`/path with no state hint)
DEFAULT_DATASET_NAME = "load_package"

# job state folders that hold readable data, in read order. a normalized package keeps its data
# in `new_jobs`, a loaded package in `completed_jobs` (unless `delete_completed_jobs` pruned it)
DEFAULT_JOB_STATES: tuple[TPackageJobState, ...] = ("new_jobs", "completed_jobs", "started_jobs")

# file formats duckdb can scan directly. `insert_values` is handled separately, and
# `model` / `reference` / `sql` jobs carry no local data at all
DUCKDB_NATIVE_FORMATS = ("parquet", "jsonl", "csv")
NO_DATA_FORMATS = ("reference", "sql", "model")


def resolve_job_states(
    job_states: Sequence[TPackageJobState] | None, include_failed: bool
) -> Sequence[TPackageJobState]:
    """Applies defaults and `include_failed` on top of a user-supplied `job_states`."""
    states: list[TPackageJobState] = list(job_states or DEFAULT_JOB_STATES)
    if include_failed and "failed_jobs" not in states:
        states.append("failed_jobs")
    return states


def _tables_with_local_data(
    package_storage: PackageStorage,
    load_id: str,
    job_states: Sequence[TPackageJobState] | None = None,
    include_failed: bool = False,
) -> set[str]:
    """Names of tables that have at least one job file in `load_id`, in the given `job_states`.

    A load package is a snapshot of one run: unlike a destination, it does not accumulate data
    across runs, so a table can be present in the package `Schema` (because it existed in a
    previous run) without holding any local file in this particular `load_id`.
    """
    states = resolve_job_states(job_states, include_failed)
    all_jobs = package_storage.get_load_package_jobs(load_id)
    return {job.table_name for state in states for job in all_jobs.get(state, [])}


class LoadPackageNotNormalized(Exception):
    def __init__(self, load_id: str) -> None:
        self.load_id = load_id
        super().__init__(
            f"Load package `{load_id}` carries no data tables in its schema. Extracted packages"
            " only get their columns inferred during `pipeline.normalize()`, so they cannot be"
            " read as a dataset. Run `pipeline.normalize()` and read the normalized package."
        )


class LoadPackageJobsNotReadable(Exception):
    def __init__(self, table_name: str, file_format: str, load_id: str) -> None:
        self.table_name = table_name
        self.file_format = file_format
        super().__init__(
            f"Table `{table_name}` in load package `{load_id}` is stored as `{file_format}` jobs,"
            " which hold no data in the load package (they only reference data elsewhere or carry"
            " SQL to run at the destination). Only parquet, jsonl, csv and insert_values jobs can"
            " be read locally."
        )


@configspec
class LoadPackageClientConfiguration(DestinationClientDwhConfiguration):
    destination_type: Final[str] = dataclasses.field(  # type: ignore[misc]
        default="load_package", init=False, repr=False, compare=False
    )
    package_path: str | None = None
    """Absolute path of the folder holding load packages (the `PackageStorage` root)"""
    load_id: str | None = None
    """Load id of the package to read"""
    job_states: list[str] | None = None
    """Job state folders to read data from. Defaults to `DEFAULT_JOB_STATES`"""
    include_failed: bool = False
    """Also expose jobs sitting in `failed_jobs`"""
    always_refresh_views: bool = False
    """Re-list job files on every query. Needed only when the package changes while being read"""

    def data_location(self) -> str | None:
        if not self.package_path or not self.load_id:
            self._no_data_location("package_path and load_id are required")
        return os.path.join(self.package_path, self.load_id)


def load_package_destination_capabilities() -> DestinationCapabilitiesContext:
    caps = DestinationCapabilitiesContext.generic_capabilities(preferred_loader_file_format="jsonl")
    caps.has_case_sensitive_identifiers = True
    caps.sqlglot_dialect = "duckdb"
    caps.supports_nested_types = True
    return caps


class load_package(Destination[LoadPackageClientConfiguration, "LoadPackageClient"]):
    """Internal, read-only destination factory that exposes a single load package."""

    @property
    def spec(self) -> type[LoadPackageClientConfiguration]:
        return LoadPackageClientConfiguration

    @property
    def client_class(self) -> type[LoadPackageClient]:
        return LoadPackageClient

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        return load_package_destination_capabilities()

    def create_ibis_backend(
        self, client: LoadPackageClient, read_only: bool = False, schemas: Sequence[Schema] = ()
    ) -> BaseBackend:
        """Maps the package tables as in-memory duckdb views and hands the connection to ibis."""
        from dlt.helpers.ibis import ibis

        sql_client = client.sql_client
        assert isinstance(sql_client, _sql_client_class())
        if schemas:
            sql_client.set_schemas(schemas)
        # do not use a context manager so the cloned connection is not closed
        duckdb_conn = sql_client.open_connection()
        sql_client.create_views_for_all_tables()
        con = ibis.duckdb.from_connection(duckdb_conn)
        # disable the destructor so the connection survives handover to ibis
        client._sql_client = None
        sql_client.memory_db = None
        return con

    def __init__(
        self,
        package_path: str | None = None,
        load_id: str | None = None,
        job_states: Sequence[str] | None = None,
        include_failed: bool = False,
        always_refresh_views: bool = False,
        destination_name: str | None = None,
        environment: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            package_path=package_path,
            load_id=load_id,
            job_states=list(job_states) if job_states else None,
            include_failed=include_failed,
            always_refresh_views=always_refresh_views,
            destination_name=destination_name,
            environment=environment,
            **kwargs,
        )


class LoadPackageClient(JobClientBase, WithSqlClient):
    """A read-only "destination" whose data is a single load package on local disk.

    Implements just enough of `JobClientBase` for `dlt.Dataset` and `WithTableScanners`,
    and delegates every filesystem access to `PackageStorage`.
    """

    def __init__(
        self,
        schema: Schema,
        config: LoadPackageClientConfiguration,
        capabilities: DestinationCapabilitiesContext,
    ) -> None:
        super().__init__(schema, config, capabilities)
        self.config: LoadPackageClientConfiguration = config
        if not config.package_path or not config.load_id:
            raise ValueError("`package_path` and `load_id` are required to read a load package")
        # `initial_state` only labels packages this storage creates; we never create any
        self.package_storage = PackageStorage(FileStorage(config.package_path), "normalized")
        self.load_id: str = config.load_id
        self._sql_client: SqlClientBase[Any] = None

    #
    # the part that actually matters: table -> files
    #

    @property
    def job_states(self) -> Sequence[TPackageJobState]:
        return resolve_job_states(
            self.config.job_states, self.config.include_failed  # type: ignore[arg-type]
        )

    def list_table_jobs(
        self, table_name: str
    ) -> list[tuple[TPackageJobState, ParsedLoadJobFileName]]:
        """All jobs of `table_name` in the readable job state folders, reusing `PackageStorage`."""
        all_jobs = self.package_storage.get_load_package_jobs(self.load_id)
        result: list[tuple[TPackageJobState, ParsedLoadJobFileName]] = []
        for state in self.job_states:
            for job in all_jobs.get(state, []):
                if job.table_name == table_name:
                    result.append((state, job))
        return result

    def list_table_files(self, table_name: str) -> list[str]:
        """Absolute paths of the data files backing `table_name`."""
        paths: list[str] = []
        for state, job in self.list_table_jobs(table_name):
            rel_path = self.package_storage.get_job_file_path(self.load_id, state, job.file_name())
            paths.append(self.package_storage.storage.make_full_path(rel_path))
        return paths

    def get_table_file_format(self, table_name: str) -> tuple[str, list[str], bool]:
        """Returns `(file_format, files, is_compressed)` for `table_name`."""
        jobs = self.list_table_jobs(table_name)
        if not jobs:
            raise DestinationUndefinedEntity(table_name)
        first = jobs[0][1]
        files = self.list_table_files(table_name)
        return first.file_format, files, first.is_compressed

    #
    # sql client
    #

    @property
    def sql_client_class(self) -> type[SqlClientBase[Any]]:
        return _sql_client_class()  # type: ignore[no-any-return]

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        if not self._sql_client:
            self._sql_client = _sql_client_class()(self, self.config.dataset_name)
        return self._sql_client

    @sql_client.setter
    def sql_client(self, client: SqlClientBase[Any]) -> None:
        self._sql_client = client

    #
    # read-only stubs for the JobClientBase contract
    #

    def initialize_storage(self, truncate_tables: Iterable[str] | None = None) -> None:
        raise NotImplementedError("A load package is read-only")

    def is_storage_initialized(self) -> bool:
        return self.package_storage.storage.has_folder(
            self.package_storage.get_package_path(self.load_id)
        )

    def drop_storage(self) -> None:
        raise NotImplementedError("A load package is read-only")

    def create_load_job(
        self, table: PreparedTableSchema, file_path: str, load_id: str, restore: bool = False
    ) -> LoadJob:
        raise NotImplementedError("A load package is read-only")

    def complete_load(self, load_id: str) -> None:
        raise NotImplementedError("A load package is read-only")

    def __enter__(self) -> LoadPackageClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._sql_client:
            self._sql_client.close_connection()


#
# duckdb table scanner over the package files
#


_SQL_CLIENT_CLASS: Any = None


def _sql_client_class() -> Any:
    """Builds `LoadPackageSqlClient` lazily so duckdb stays an optional dependency.

    duckdb is only needed once a dataset is actually opened, mirroring how the filesystem
    destination imports its sql client from inside the `sql_client` property.
    """
    global _SQL_CLIENT_CLASS
    if _SQL_CLIENT_CLASS is not None:
        return _SQL_CLIENT_CLASS

    from dlt.destinations.impl.duckdb.sql_client import WithTableScanners
    from dlt.destinations.impl.duckdb.factory import DuckDbCredentials

    class LoadPackageSqlClient(WithTableScanners):
        """Maps the job files of one load package as duckdb views.

        Only `create_view_select` differs from `FilesystemSqlClient`; that method should be
        factored into a shared helper before merging.
        """

        def __init__(
            self,
            remote_client: LoadPackageClient,
            dataset_name: str = None,
            cache_db: DuckDbCredentials = None,
            persist_secrets: bool = False,
        ) -> None:
            super().__init__(
                remote_client, dataset_name or DEFAULT_DATASET_NAME, cache_db, persist_secrets
            )
            self.remote_client: LoadPackageClient = remote_client

        def can_create_view(self, table_schema: PreparedTableSchema) -> bool:
            # listing job files is cheap (one folder listing), but we stay optimistic and
            # prune in `create_view_select`, exactly like the filesystem client does
            return True

        def should_replace_view(self, view_name: str, table_schema: PreparedTableSchema) -> bool:
            # a package is immutable while the pipeline sits between normalize and load
            return bool(self.remote_client.config.always_refresh_views)

        def _table_location(self, table_name: str) -> str:
            """Physical location key that `WithTableScanners` uses to merge co-located schemas."""
            return os.path.join(
                self.remote_client.config.package_path, self.remote_client.load_id, table_name
            )

        def create_view_select(
            self, table_schema: PreparedTableSchema, schema: Schema = None
        ) -> tuple[str, str] | None:
            schema = schema or self.schema
            table_name = table_schema["name"]

            try:
                file_format, files, _ = self.remote_client.get_table_file_format(table_name)
            except DestinationUndefinedEntity:
                # no jobs for this table in this package, nothing to expose
                return None

            if file_format in NO_DATA_FORMATS:
                raise LoadPackageJobsNotReadable(
                    table_name, file_format, self.remote_client.load_id
                )

            dlt_table_names = schema.dlt_table_names()

            def _escape_column_name(col_name: str) -> str:
                col_name = self.escape_column_name(col_name)
                # dlt tables are stored as json and never normalized
                if table_name in dlt_table_names:
                    col_name = col_name.lower()
                return col_name

            table_columns = table_schema.get("columns", {})
            columns = [_escape_column_name(c) for c in table_columns.keys()]
            files_string = ",".join(f"'{f}'" for f in files)

            if file_format == "parquet":
                from_statement = f"read_parquet([{files_string}], union_by_name=true)"
            elif file_format in ("jsonl", "csv"):
                type_mapper = self.capabilities.get_type_mapper()
                columns_defs = list(table_columns.values())
                column_types = ",".join(
                    f'{_escape_column_name(c["name"])}:'
                    f' "{type_mapper.to_destination_type(c, table_schema)}"'
                    for c in columns_defs
                )
                if file_format == "jsonl":
                    # binary columns are stored base64 encoded
                    for idx, column_def in enumerate(columns_defs):
                        if column_def["data_type"] == "binary":
                            columns[idx] = f"from_base64(decode({columns[idx]})) as {columns[idx]}"
                    from_statement = f"read_json([{files_string}], columns = {{{column_types}}})"
                else:
                    not_null_columns = [
                        _escape_column_name(c["name"])
                        for c in columns_defs
                        if not is_nullable_column(c)
                    ]
                    force_not_null = (
                        f"force_not_null=[{','.join(not_null_columns)}],"
                        if not_null_columns
                        else ""
                    )
                    from_statement = (
                        f"read_csv([{files_string}],{force_not_null} union_by_name=true,"
                        f"header=true,null_padding=true,types= {{{column_types}}})"
                    )
            elif file_format == "insert_values":
                # SQL destinations (postgres, mssql, ...) normalize to INSERT statements, which
                # duckdb cannot scan. rewrite them into VALUES selects instead
                selects = [_insert_values_file_to_select(f, table_name) for f in files]
                union = " UNION ALL BY NAME ".join(selects)
                # the VALUES alias already names the columns
                return (self._table_location(table_name), f"SELECT * FROM ({union})")
            else:
                # unknown or unsupported format, do not create a view
                return None

            select_sql = f"SELECT {', '.join(columns)} FROM {from_statement}"
            return (self._table_location(table_name), select_sql)

    _SQL_CLIENT_CLASS = LoadPackageSqlClient
    return _SQL_CLIENT_CLASS


def _insert_values_file_to_select(file_path: str, table_name: str) -> str:
    """Rewrites an `insert_values` job file into a `SELECT ... FROM (VALUES ...) AS t(cols)`.

    dlt writes `INSERT INTO {}(col, ...) VALUES (...)` with the table name left as `{}`.
    """
    import sqlglot

    with FileStorage.open_zipsafe_ro(file_path, "r", encoding="utf-8") as f:
        insert_statement = f.read()

    # remove the `E` prefix used by postgres style quote escapes
    insert_statement = insert_statement.replace("E'", "'")
    # the INSERT statement carries no table name, take it from the job file name
    insert_statement = insert_statement.replace("{}", f"{table_name} ")
    insert_expr = sqlglot.parse(insert_statement, read="duckdb")[0]
    # `.expression` is the VALUES select, `.this` is `table_name(col, ...)`
    return (
        f"SELECT * FROM ({insert_expr.expression.sql(dialect='duckdb')}) AS"
        f" {insert_expr.this.sql(dialect='duckdb')}"
    )


#
# Entrypoints
#


def load_package_storage(
    pipeline: Pipeline, package_state: TLoadPackageStatus = "normalized"
) -> PackageStorage:
    """Gets the `PackageStorage` of a pipeline holding packages in `package_state`.

    Args:
        pipeline (Pipeline): The pipeline whose local working dir is inspected.
        package_state (TLoadPackageStatus): One of `new`, `normalized`, `loaded`, `extracted`.

    Returns:
        PackageStorage: Storage rooted at the folder holding those packages.
    """
    if package_state == "extracted":
        return pipeline._get_normalize_storage().extracted_packages
    load_storage = pipeline._get_load_storage()
    if package_state == "new":
        return load_storage.new_packages
    if package_state == "normalized":
        return load_storage.normalized_packages
    if package_state == "loaded":
        return load_storage.loaded_packages
    raise ValueError(f"Unknown package state `{package_state}`")


def load_package_dataset(
    storage: FileStorage | PackageStorage | str | Pipeline,
    load_id: str,
    *,
    package_state: TLoadPackageStatus = "normalized",
    job_states: Sequence[TPackageJobState] = None,
    include_failed: bool = False,
    always_refresh_views: bool = False,
    dataset_name: str = None,
    schema: Schema = None,
) -> dlt.Dataset:
    """Opens one specific load package as a `dlt.Dataset`, reading its data files in place.

    `load_id` is required and not defaulted to "the latest package": a load package is a
    snapshot of a single run, and unlike a destination it does not accumulate data across runs.
    A table can appear in the package `Schema` (because a previous run wrote it) while holding no
    local file in a given `load_id`. Picking an implicit "latest" package would silently expose a
    table with a stale/wrong `Schema` shape. Use `list_packages()` /
    `load_package_storage(pipeline, ...).list_packages()` to enumerate load ids, or
    `load_package_datasets(step_info)` to get one dataset per package a pipeline step just ran.

    Args:
        storage: Where the packages live. A `FileStorage` or `PackageStorage` rooted at the
            packages folder, a plain path, or a `Pipeline` (see `package_state`).
        load_id (str): Load id of the package to read.
        package_state (TLoadPackageStatus): Which package folder of a `Pipeline` to read.
            Ignored when `storage` is not a `Pipeline`.
        job_states (Sequence[TPackageJobState]): Job folders to read. Defaults to
            `new_jobs`, `completed_jobs` and `started_jobs`.
        include_failed (bool): Also expose files in `failed_jobs`.
        always_refresh_views (bool): Re-list job files on every query.
        dataset_name (str): Name of the duckdb schema holding the views. Defaults to
            `package_state` ("extracted", "normalized", "loaded", ...).
        schema (Schema): Overrides the schema stored in the package. Defaults to the schema
            persisted inside the package itself (`<load_id>/schema.json`), which is the schema
            as it was known at the time this package was normalized.

    Returns:
        dlt.Dataset: A regular dataset. Use `.tables`, `ds.<table>`, `ds("select ...")`,
            `.df()`, `.arrow()`, `.ibis()` as usual. `.tables` reflects the package schema as-is:
            since dlt schemas only ever grow, a table defined in an earlier run may still be
            listed even though this package wrote no data for it; querying such a table raises
            `DatabaseUndefinedRelation`, exactly as a real destination would for an empty table.
    """
    package_storage = _resolve_package_storage(storage, package_state)

    if not package_storage.storage.has_folder(package_storage.get_package_path(load_id)):
        raise LoadPackageNotFound(load_id)

    schema = schema or package_storage.load_schema(load_id)
    if not schema.data_table_names():
        raise LoadPackageNotNormalized(load_id)

    dataset_name = dataset_name or package_state or DEFAULT_DATASET_NAME

    destination = load_package(
        package_path=package_storage.storage.storage_path,
        load_id=load_id,
        job_states=list(job_states) if job_states else None,
        include_failed=include_failed,
        always_refresh_views=always_refresh_views,
    )
    return dlt.Dataset(destination=destination, dataset_name=dataset_name, schema=schema)


def load_package_datasets(
    step_info: Any,
    storage: FileStorage | PackageStorage | str | Pipeline = None,
    **kwargs: Any,
) -> Iterator[dlt.Dataset]:
    """Yields a `dlt.Dataset` for every load package referenced by an info object.

    Args:
        step_info: An `ExtractInfo`, `NormalizeInfo` or `LoadInfo` (anything with `loads_ids`).
        storage: Where the packages live. Defaults to `step_info.pipeline`.
        **kwargs: Passed through to `load_package_dataset`.

    Yields:
        dlt.Dataset: One dataset per load id, in the order reported by the step.
    """
    source = storage if storage is not None else getattr(step_info, "pipeline", None)
    if source is None:
        raise ValueError("Cannot determine where load packages live. Pass `storage` explicitly.")
    for load_id in step_info.loads_ids:
        yield load_package_dataset(source, load_id, **kwargs)


def _resolve_package_storage(
    storage: FileStorage | PackageStorage | str | Pipeline,
    package_state: TLoadPackageStatus,
) -> PackageStorage:
    if isinstance(storage, PackageStorage):
        return storage
    if isinstance(storage, FileStorage):
        return PackageStorage(storage, package_state)
    if isinstance(storage, str):
        return PackageStorage(FileStorage(storage), package_state)
    # duck-typed to avoid importing Pipeline at module import time
    if hasattr(storage, "_get_load_storage"):
        return load_package_storage(storage, package_state)
    raise TypeError(f"Cannot resolve load package storage from `{type(storage).__name__}`")


if __name__ == "__main__":
    import dlt

    pipeline = dlt.attach("docs_lint")

    package_storage = load_package_storage(pipeline, package_state="loaded")
    # pick the most recent package that actually has data for `lints`, since not every
    # completed package necessarily wrote that table (see `load_package_dataset` docstring)
    load_id = next(
        lid
        for lid in reversed(package_storage.list_packages())
        if "lints" in _tables_with_local_data(package_storage, lid)
    )

    ds = load_package_dataset(pipeline, load_id, package_state="loaded")

    print(ds.tables)  # noqa: T201
    print(ds.table("lints").df())  # noqa: T201
