from typing import Any, Union, TYPE_CHECKING

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

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.common.schema import Schema
from dlt.destinations.dataset.relation import ReadableDBAPIRelation
from dlt.destinations.dataset.utils import get_destination_clients

if TYPE_CHECKING:
    try:
        from dlt.common.libs.ibis import BaseBackend as IbisBackend
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
