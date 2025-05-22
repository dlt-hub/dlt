from types import TracebackType, MethodType
from typing import Any, Type, Union, TYPE_CHECKING, List

from dlt.common.destination.exceptions import OpenTableClientNotAvailable
from dlt.common.json import json
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination.reference import TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase, SupportsOpenTables, WithStateSync
from dlt.common.destination.dataset import (
    SupportsReadableRelation,
    SupportsReadableDataset,
)
from dlt.common.destination.typing import TDatasetType
from dlt.common.schema import Schema
from dlt.common.typing import Self
from dlt.common.schema.typing import C_DLT_LOAD_ID

from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation
from dlt.destinations.dataset.relation import ReadableDBAPIRelation
from dlt.destinations.dataset.utils import get_destination_clients

if TYPE_CHECKING:
    from dlt.helpers.ibis import BaseBackend as IbisBackend
else:
    IbisBackend = Any


class ReadableDBAPIDataset(SupportsReadableDataset[ReadableIbisRelation]):
    """Access to dataframes and arrow tables in the destination dataset via dbapi"""

    def __init__(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[Schema, str, None] = None,
        dataset_type: TDatasetType = "auto",
    ) -> None:
        self._destination = Destination.from_reference(destination)
        self._provided_schema = schema
        self._dataset_name = dataset_name
        self._schema: Schema = None
        self._sql_client: SqlClientBase[Any] = None
        self._opened_sql_client: SqlClientBase[Any] = None
        self._table_client: SupportsOpenTables = None
        # resolve dataset type
        if dataset_type in ("auto", "ibis"):
            try:
                from dlt.helpers.ibis import ibis

                dataset_type = "ibis"
            except MissingDependencyException:
                # if ibis is explicitly requested, reraise
                if dataset_type == "ibis":
                    raise
        self._dataset_type: TDatasetType = dataset_type

    def ibis(self) -> IbisBackend:
        """return a connected ibis backend"""
        from dlt.helpers.ibis import create_ibis_backend

        return create_ibis_backend(
            self._destination,
            self._get_destination_client(self.schema),
        )

    @property
    def schema(self) -> Schema:
        # NOTE: if this property raises AttributeError, __getattr__ will get called 🤯
        #   this leads to infinite recursion as __getattr_ calls this property
        if not self._schema:
            self._ensure_schema()
        return self._schema

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @property
    def sql_client(self) -> SqlClientBase[Any]:
        # return the opened sql client if it exists
        if self._opened_sql_client:
            return self._opened_sql_client
        if not self._sql_client:
            self._sql_client = self._get_sql_client(self.schema)
        return self._sql_client

    @property
    def sql_client_class(self) -> Type[SqlClientBase[Any]]:
        return self.sql_client.__class__

    @property
    def destination_client(self) -> JobClientBase:
        return self._get_destination_client(self.schema)

    @property
    def open_table_client(self) -> SupportsOpenTables:
        if not self._table_client:
            client = self._get_destination_client(self.schema)
            if isinstance(client, SupportsOpenTables):
                self._table_client = client
            else:
                raise OpenTableClientNotAvailable(
                    self._dataset_name, self._destination.destination_name
                )
        return self._table_client

    def _get_destination_client(self, schema: Schema) -> JobClientBase:
        return get_destination_clients(
            schema, destination=self._destination, destination_dataset_name=self._dataset_name
        )[0]

    def _get_sql_client(self, schema: Schema) -> SqlClientBase[Any]:
        client = self._get_destination_client(self.schema)
        if isinstance(client, WithSqlClient):
            return client.sql_client
        else:
            raise Exception(
                f"Destination {client.config.destination_type} does not support SqlClient."
            )

    def _ensure_schema(self) -> None:
        """Lazy load the schema on request"""

        # full schema given, nothing to do
        if not self._schema and isinstance(self._provided_schema, Schema):
            self._schema = self._provided_schema

        # schema name given, resolve it from destination by name
        elif not self._schema and isinstance(self._provided_schema, str):
            with self._get_destination_client(Schema(self._provided_schema)) as client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema(self._provided_schema)
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))
                    else:
                        self._schema = Schema(self._provided_schema)

        # no schema name given, load newest schema from destination
        elif not self._schema:
            with self._get_destination_client(Schema(self._dataset_name)) as client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema()
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))

        # default to empty schema with dataset name
        if not self._schema:
            self._schema = Schema(self._dataset_name)

    def __call__(self, query: Any) -> ReadableDBAPIRelation:
        # TODO: accept other query types and return a right relation: sqlglot (DBAPI) and ibis (Expr)
        return ReadableDBAPIRelation(readable_dataset=self, provided_query=query)  # type: ignore[abstract]

    def table(self, table_name: str) -> ReadableIbisRelation:
        # we can create an ibis powered relation if ibis is available
        if self._dataset_type == "ibis":
            from dlt.helpers.ibis import create_unbound_ibis_table
            from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation

            unbound_table = create_unbound_ibis_table(self.sql_client, self.schema, table_name)
            return ReadableIbisRelation(  # type: ignore[abstract]
                readable_dataset=self,
                ibis_object=unbound_table,
                columns_schema=self.schema.get_table(table_name)["columns"],
            )

        # fallback to the standard dbapi relation
        return ReadableDBAPIRelation(  # type: ignore[abstract,return-value]
            readable_dataset=self,
            table_name=table_name,
        )

    def row_counts(
        self,
        *,
        data_tables: bool = True,
        dlt_tables: bool = False,
        table_names: List[str] = None,
        load_id: str = None,
    ) -> SupportsReadableRelation:
        """Returns a dictionary of table names and their row counts, returns counts of all data tables by default"""
        """If table_names is provided, only the tables in the list are returned regardless of the data_tables and dlt_tables flags"""

        selected_tables = table_names or []
        if not selected_tables:
            if data_tables:
                selected_tables += self.schema.data_table_names(seen_data_only=True)
            if dlt_tables:
                selected_tables += self.schema.dlt_table_names()

        # filter tables so only ones with dlt_load_id column are included
        if load_id:
            dlt_load_id_col = self.schema.naming.normalize_identifier(C_DLT_LOAD_ID)
            selected_tables = [
                table
                for table in selected_tables
                if dlt_load_id_col in self.schema.tables[table]["columns"].keys()
            ]

        # Build UNION ALL query to get row counts for all selected tables
        queries = []
        for table in selected_tables:
            query = (
                f"SELECT '{table}' as table_name, COUNT(*) as row_count FROM"
                f" {self.sql_client.make_qualified_table_name(table)}"
            )
            queries.append(query)
            if load_id:
                query += f" WHERE {dlt_load_id_col} = '{load_id}'"

        query = " UNION ALL ".join(queries)

        # Execute query and build result dict
        return self(query)

    def __getitem__(self, table_name: str) -> ReadableIbisRelation:
        """access of table via dict notation"""
        return self.table(table_name)

    def __getattr__(self, table_name: str) -> ReadableIbisRelation:
        """access of table via property notation"""
        return self.table(table_name)

    def __enter__(self) -> Self:
        """Context manager used to open and close sql client and internal connection"""
        assert (
            not self._opened_sql_client
        ), "context manager can't be used when sql client is initialized"
        # return sql_client wrapped so it will not call close on __exit__ as dataset is managing connections
        # use internal class

        # create a new sql client
        self._opened_sql_client = self._get_sql_client(self.schema)

        NoCloseClient = type(
            "NoCloseClient",
            (self._opened_sql_client.__class__,),
            {
                "__exit__": (
                    lambda self, exc_type, exc_val, exc_tb: None
                )  # No-operation: do not close the connection
            },
        )

        self._opened_sql_client.__class__ = NoCloseClient

        self._opened_sql_client.open_connection()
        return self

    def __exit__(
        self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType
    ) -> None:
        self._opened_sql_client.close_connection()
        self._opened_sql_client = None
