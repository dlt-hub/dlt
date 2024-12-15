from typing import Any, Union, TYPE_CHECKING, List

from dlt.common.json import json

from dlt.common.exceptions import MissingDependencyException

from dlt.common.destination.reference import (
    SupportsReadableRelation,
    SupportsReadableDataset,
    TDestinationReferenceArg,
    Destination,
    JobClientBase,
    WithStateSync,
)

from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.common.schema import Schema
from dlt.destinations.dataset.relation import ReadableDBAPIRelation
from dlt.destinations.dataset.utils import get_destination_clients
from dlt.common.destination.reference import TDatasetType

if TYPE_CHECKING:
    try:
        from dlt.helpers.ibis import BaseBackend as IbisBackend
    except MissingDependencyException:
        IbisBackend = Any
else:
    IbisBackend = Any


class ReadableDBAPIDataset(SupportsReadableDataset):
    """Access to dataframes and arrowtables in the destination dataset via dbapi"""

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
        self._sql_client: SqlClientBase[Any] = None
        self._schema: Schema = None
        self._dataset_type = dataset_type

    def ibis(self) -> IbisBackend:
        """return a connected ibis backend"""
        from dlt.helpers.ibis import create_ibis_backend

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
        # we can create an ibis powered relation if ibis is available
        if table_name in self.schema.tables and self._dataset_type in ("auto", "ibis"):
            try:
                from dlt.helpers.ibis import create_unbound_ibis_table
                from dlt.destinations.dataset.ibis_relation import ReadableIbisRelation

                unbound_table = create_unbound_ibis_table(self.sql_client, self.schema, table_name)
                return ReadableIbisRelation(readable_dataset=self, ibis_object=unbound_table, columns_schema=self.schema.tables[table_name]["columns"])  # type: ignore[abstract]
            except MissingDependencyException:
                # if ibis is explicitly requested, reraise
                if self._dataset_type == "ibis":
                    raise

        # fallback to the standard dbapi relation
        return ReadableDBAPIRelation(
            readable_dataset=self,
            table_name=table_name,
        )  # type: ignore[abstract]

    def row_counts(
        self, *, data_tables: bool = True, dlt_tables: bool = False, table_names: List[str] = None
    ) -> SupportsReadableRelation:
        """Returns a dictionary of table names and their row counts, returns counts of all data tables by default"""
        """If table_names is provided, only the tables in the list are returned regardless of the data_tables and dlt_tables flags"""

        selected_tables = table_names or []
        if not selected_tables:
            if data_tables:
                selected_tables += self.schema.data_table_names(seen_data_only=True)
            if dlt_tables:
                selected_tables += self.schema.dlt_table_names()

        # Build UNION ALL query to get row counts for all selected tables
        queries = []
        for table in selected_tables:
            queries.append(
                f"SELECT '{table}' as table_name, COUNT(*) as row_count FROM"
                f" {self.sql_client.make_qualified_table_name(table)}"
            )

        query = " UNION ALL ".join(queries)

        # Execute query and build result dict
        return self(query)

    def __getitem__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via dict notation"""
        return self.table(table_name)

    def __getattr__(self, table_name: str) -> SupportsReadableRelation:
        """access of table via property notation"""
        return self.table(table_name)
