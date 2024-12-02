from typing import Any, Generator, Sequence, Union, TYPE_CHECKING, Tuple

from contextlib import contextmanager

from dlt import version
from dlt.common.json import json
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination import AnyDestination
from dlt.common.destination.reference import (
    SupportsReadableRelation,
    SupportsReadableDataset,
    TDatasetType,
    TDestinationReferenceArg,
    Destination,
    JobClientBase,
    WithStateSync,
    DestinationClientDwhConfiguration,
    DestinationClientStagingConfiguration,
    DestinationClientConfiguration,
    DestinationClientDwhWithStagingConfiguration,
)

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.common.schema import Schema
from dlt.common.exceptions import DltException

if TYPE_CHECKING:
    try:
        from dlt.common.libs.ibis import BaseBackend as IbisBackend
    except MissingDependencyException:
        IbisBackend = Any
else:
    IbisBackend = Any


class DatasetException(DltException):
    pass


class ReadableRelationHasQueryException(DatasetException):
    def __init__(self, attempted_change: str) -> None:
        msg = (
            "This readable relation was created with a provided sql query. You cannot change"
            f" {attempted_change}. Please change the orignal sql query."
        )
        super().__init__(msg)


class ReadableRelationUnknownColumnException(DatasetException):
    def __init__(self, column_name: str) -> None:
        msg = (
            f"The selected column {column_name} is not known in the dlt schema for this releation."
        )
        super().__init__(msg)


class ReadableDBAPIRelation(SupportsReadableRelation):
    def __init__(
        self,
        *,
        readable_dataset: "ReadableDBAPIDataset",
        provided_query: Any = None,
        table_name: str = None,
        limit: int = None,
        selected_columns: Sequence[str] = None,
    ) -> None:
        """Create a lazy evaluated relation to for the dataset of a destination"""

        # NOTE: we can keep an assertion here, this class will not be created by the user
        assert bool(table_name) != bool(
            provided_query
        ), "Please provide either an sql query OR a table_name"

        self._dataset = readable_dataset

        self._provided_query = provided_query
        self._table_name = table_name
        self._limit = limit
        self._selected_columns = selected_columns

        # wire protocol functions
        self.df = self._wrap_func("df")  # type: ignore
        self.arrow = self._wrap_func("arrow")  # type: ignore
        self.fetchall = self._wrap_func("fetchall")  # type: ignore
        self.fetchmany = self._wrap_func("fetchmany")  # type: ignore
        self.fetchone = self._wrap_func("fetchone")  # type: ignore

        self.iter_df = self._wrap_iter("iter_df")  # type: ignore
        self.iter_arrow = self._wrap_iter("iter_arrow")  # type: ignore
        self.iter_fetch = self._wrap_iter("iter_fetch")  # type: ignore

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        return self._dataset.sql_client

    @property
    def schema(self) -> Schema:
        return self._dataset.schema

    @property
    def query(self) -> Any:
        """build the query"""
        if self._provided_query:
            return self._provided_query

        table_name = self.sql_client.make_qualified_table_name(
            self.schema.naming.normalize_tables_path(self._table_name)
        )

        maybe_limit_clause_1 = ""
        maybe_limit_clause_2 = ""
        if self._limit:
            maybe_limit_clause_1, maybe_limit_clause_2 = self.sql_client._limit_clause_sql(
                self._limit
            )

        selector = "*"
        if self._selected_columns:
            selector = ",".join(
                [
                    self.sql_client.escape_column_name(self.schema.naming.normalize_path(c))
                    for c in self._selected_columns
                ]
            )

        return f"SELECT {maybe_limit_clause_1} {selector} FROM {table_name} {maybe_limit_clause_2}"

    @property
    def columns_schema(self) -> TTableSchemaColumns:
        return self.compute_columns_schema()

    @columns_schema.setter
    def columns_schema(self, new_value: TTableSchemaColumns) -> None:
        raise NotImplementedError("columns schema in ReadableDBAPIRelation can only be computed")

    def compute_columns_schema(self) -> TTableSchemaColumns:
        """provide schema columns for the cursor, may be filtered by selected columns"""

        columns_schema = (
            self.schema.tables.get(self._table_name, {}).get("columns", {}) if self.schema else {}
        )

        if not columns_schema:
            return None
        if not self._selected_columns:
            return columns_schema

        filtered_columns: TTableSchemaColumns = {}
        for sc in self._selected_columns:
            sc = self.schema.naming.normalize_path(sc)
            if sc not in columns_schema.keys():
                raise ReadableRelationUnknownColumnException(sc)
            filtered_columns[sc] = columns_schema[sc]

        return filtered_columns

    @contextmanager
    def cursor(self) -> Generator[SupportsReadableRelation, Any, Any]:
        """Gets a DBApiCursor for the current relation"""
        with self.sql_client as client:
            # this hacky code is needed for mssql to disable autocommit, read iterators
            # will not work otherwise. in the future we should be able to create a readony
            # client which will do this automatically
            if hasattr(self.sql_client, "_conn") and hasattr(self.sql_client._conn, "autocommit"):
                self.sql_client._conn.autocommit = False
            with client.execute_query(self.query) as cursor:
                if columns_schema := self.columns_schema:
                    cursor.columns_schema = columns_schema
                yield cursor

    def _wrap_iter(self, func_name: str) -> Any:
        """wrap SupportsReadableRelation generators in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self.cursor() as cursor:
                yield from getattr(cursor, func_name)(*args, **kwargs)

        return _wrap

    def _wrap_func(self, func_name: str) -> Any:
        """wrap SupportsReadableRelation functions in cursor context"""

        def _wrap(*args: Any, **kwargs: Any) -> Any:
            with self.cursor() as cursor:
                return getattr(cursor, func_name)(*args, **kwargs)

        return _wrap

    def __copy__(self) -> "ReadableDBAPIRelation":
        return self.__class__(
            readable_dataset=self._dataset,
            provided_query=self._provided_query,
            table_name=self._table_name,
            limit=self._limit,
            selected_columns=self._selected_columns,
        )

    def limit(self, limit: int) -> "ReadableDBAPIRelation":
        if self._provided_query:
            raise ReadableRelationHasQueryException("limit")
        rel = self.__copy__()
        rel._limit = limit
        return rel

    def select(self, *columns: str) -> "ReadableDBAPIRelation":
        if self._provided_query:
            raise ReadableRelationHasQueryException("select")
        rel = self.__copy__()
        rel._selected_columns = columns
        # NOTE: the line below will ensure that no unknown columns are selected if
        # schema is known
        rel.compute_columns_schema()
        return rel

    def __getitem__(self, columns: Union[str, Sequence[str]]) -> "SupportsReadableRelation":
        if isinstance(columns, str):
            return self.select(columns)
        elif isinstance(columns, Sequence):
            return self.select(*columns)
        else:
            raise TypeError(f"Invalid argument type: {type(columns).__name__}")

    def head(self, limit: int = 5) -> "ReadableDBAPIRelation":
        return self.limit(limit)


class ReadableDBAPIDataset(SupportsReadableDataset):
    """Access to dataframes and arrowtables in the destination dataset via dbapi"""

    def __init__(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[Schema, str, None] = None,
    ) -> None:
        self._destination = Destination.from_reference(destination)
        self._provided_schema = schema
        self._dataset_name = dataset_name
        self._sql_client: SqlClientBase[Any] = None
        self._schema: Schema = None

    def ibis(self) -> IbisBackend:
        """return a connected ibis backend"""
        from dlt.common.libs.ibis import create_ibis_backend

        self._ensure_client_and_schema()
        return create_ibis_backend(
            self._destination,
            self._destination_client(self.schema),
        )

    @property
    def schema(self) -> Schema:
        self._ensure_client_and_schema()
        return self._schema

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        self._ensure_client_and_schema()
        return self._sql_client

    def _destination_client(self, schema: Schema) -> JobClientBase:
        return get_destination_clients(
            schema, destination=self._destination, destination_dataset_name=self._dataset_name
        )[0]

    def _ensure_client_and_schema(self) -> None:
        """Lazy load schema and client"""

        # full schema given, nothing to do
        if not self._schema and isinstance(self._provided_schema, Schema):
            self._schema = self._provided_schema

        # schema name given, resolve it from destination by name
        elif not self._schema and isinstance(self._provided_schema, str):
            with self._destination_client(Schema(self._provided_schema)) as client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema(self._provided_schema)
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))
                    else:
                        self._schema = Schema(self._provided_schema)

        # no schema name given, load newest schema from destination
        elif not self._schema:
            with self._destination_client(Schema(self._dataset_name)) as client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema()
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))

        # default to empty schema with dataset name
        if not self._schema:
            self._schema = Schema(self._dataset_name)

        # here we create the client bound to the resolved schema
        if not self._sql_client:
            destination_client = self._destination_client(self._schema)
            if isinstance(destination_client, WithSqlClient):
                self._sql_client = destination_client.sql_client
            else:
                raise Exception(
                    f"Destination {destination_client.config.destination_type} does not support"
                    " SqlClient."
                )

    def __call__(self, query: Any) -> ReadableDBAPIRelation:
        return ReadableDBAPIRelation(readable_dataset=self, provided_query=query)  # type: ignore[abstract]

    def table(self, table_name: str) -> SupportsReadableRelation:
        return ReadableDBAPIRelation(
            readable_dataset=self,
            table_name=table_name,
        )  # type: ignore[abstract]

    def __getitem__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via dict notation"""
        return self.table(table_name)

    def __getattr__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via property notation"""
        return self.table(table_name)


def dataset(
    destination: TDestinationReferenceArg,
    dataset_name: str,
    schema: Union[Schema, str, None] = None,
    dataset_type: TDatasetType = "dbapi",
) -> SupportsReadableDataset:
    if dataset_type == "dbapi":
        return ReadableDBAPIDataset(destination, dataset_name, schema)
    raise NotImplementedError(f"Dataset of type {dataset_type} not implemented")


# helpers
def get_destination_client_initial_config(
    destination: AnyDestination,
    default_schema_name: str,
    dataset_name: str,
    as_staging: bool = False,
) -> DestinationClientConfiguration:
    client_spec = destination.spec

    # this client supports many schemas and datasets
    if issubclass(client_spec, DestinationClientDwhConfiguration):
        if issubclass(client_spec, DestinationClientStagingConfiguration):
            spec: DestinationClientDwhConfiguration = client_spec(as_staging_destination=as_staging)
        else:
            spec = client_spec()

        spec._bind_dataset_name(dataset_name, default_schema_name)
        return spec

    return client_spec()


def get_destination_clients(
    schema: Schema,
    destination: AnyDestination = None,
    destination_dataset_name: str = None,
    destination_initial_config: DestinationClientConfiguration = None,
    staging: AnyDestination = None,
    staging_dataset_name: str = None,
    staging_initial_config: DestinationClientConfiguration = None,
    # pipeline specific settings
    default_schema_name: str = None,
) -> Tuple[JobClientBase, JobClientBase]:
    destination = Destination.from_reference(destination) if destination else None
    staging = Destination.from_reference(staging) if staging else None

    try:
        # resolve staging config in order to pass it to destination client config
        staging_client = None
        if staging:
            if not staging_initial_config:
                # this is just initial config - without user configuration injected
                staging_initial_config = get_destination_client_initial_config(
                    staging,
                    dataset_name=staging_dataset_name,
                    default_schema_name=default_schema_name,
                    as_staging=True,
                )
            # create the client - that will also resolve the config
            staging_client = staging.client(schema, staging_initial_config)

        if not destination_initial_config:
            # config is not provided then get it with injected credentials
            initial_config = get_destination_client_initial_config(
                destination,
                dataset_name=destination_dataset_name,
                default_schema_name=default_schema_name,
            )

        # attach the staging client config to destination client config - if its type supports it
        if (
            staging_client
            and isinstance(initial_config, DestinationClientDwhWithStagingConfiguration)
            and isinstance(staging_client.config, DestinationClientStagingConfiguration)
        ):
            initial_config.staging_config = staging_client.config
        # create instance with initial_config properly set
        client = destination.client(schema, initial_config)
        return client, staging_client
    except ModuleNotFoundError:
        client_spec = destination.spec()
        raise MissingDependencyException(
            f"{client_spec.destination_type} destination",
            [f"{version.DLT_PKG_NAME}[{client_spec.destination_type}]"],
            "Dependencies for specific destinations are available as extras of dlt",
        )
