from contextlib import contextmanager
from types import TracebackType
from typing import Any, Generator, Literal, Optional, Type, Union, TYPE_CHECKING, List


from sqlglot.schema import Schema as SQLGlotSchema

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
from dlt.destinations.dataset.relation import ReadableDBAPIRelation, BaseReadableDBAPIRelation
from dlt.destinations.dataset.utils import get_destination_clients

from dlt.transformations import lineage


if TYPE_CHECKING:
    from dlt.helpers.ibis import BaseBackend as IbisBackend
else:
    IbisBackend = Any


class ReadableDBAPIDataset(SupportsReadableDataset):
    """Access to dataframes and arrow tables in the destination dataset via dbapi"""

    def __init__(
        self,
        destination: TDestinationReferenceArg,
        dataset_name: str,
        schema: Union[Schema, str, None] = None,
    ) -> None:
        # provided properties
        self._destination = Destination.from_reference(destination)
        self._provided_schema = schema
        self._name = dataset_name

        # derived / cached properties
        self._schema: Schema = None
        # self._sqlglot_schema: SQLGlotSchema = None
        self._sql_client: SqlClientBase[Any] = None
        self._opened_sql_client: SqlClientBase[Any] = None
        self._table_client: SupportsOpenTables = None


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
    def sqlglot_schema(self) -> SQLGlotSchema:
        # NOTE: no cache for now, it is probably more expensive to compute the current schema hash
        # to see wether this is stale than to compute a new sqlglot schema
        dialect: str = self.sql_client.capabilities.sqlglot_dialect
        return lineage.create_sqlglot_schema(self.schema, self.dataset_name, dialect=dialect)

    @property
    def name(self) -> str:
        return self._name

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
                    self._name, self._destination.destination_name
                )
        return self._table_client

    def is_same_physical_destination(self, other: "ReadableDBAPIDataset") -> bool:
        """
        Returns true if the other dataset is on the same physical destination
        helpful if we want to run sql queries without extracting the data
        """
        return str(self.destination_client.config) == str(other.destination_client.config)

    def _get_destination_client(self, schema: Schema) -> JobClientBase:
        return get_destination_clients(
            schema, destination=self._destination, destination_dataset_name=self._name
        )[0]

    def _get_sql_client(self, schema: Schema) -> SqlClientBase[Any]:
        client = self._get_destination_client(self.schema)
        if isinstance(client, WithSqlClient):
            return client.sql_client
        else:
            raise Exception(
                f"Destination `{client.config.destination_type}` does not support `SqlClient`."
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
            with self._get_destination_client(Schema(self._name)) as client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema()
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))

        # default to empty schema with dataset name
        if not self._schema:
            self._schema = Schema(self._name)

    # TODO use `@overload` with the `type_` field set to configure the return type
    # could be `sqlglot.expressions.Table`, `ibis.expr.types.Table`, `dlt.Relation`
    # TODO maybe this should return a `Relation` and relation provides different
    # access methods for different types
    def table(
        self,
        table_name: str,
        type_: Literal["ibis", "sqlglot", "relation", "ibis_narwhals", "pandas", "polars_narwhals"] = "relation",
    ) -> Any:
        # dataset only provides access to tables known in dlt schema, direct query may cirumvent this
        if table_name not in self.schema.tables.keys():
            raise KeyError(
                f"Table {table_name} not found in schema {self.schema.name} of dataset"
                f" {self.dataset_name}. Avaible tables are: {self.schema.tables.keys()}"
            )
        
        if type_ == "relation":
            # TODO no circular relationship between dataset and relation
            return ReadableDBAPIRelation(table_name=table_name)
        elif type_ == "sqlglot":
            return ...
        elif type_ == "ibis":
            from dlt.helpers.ibis import create_unbound_ibis_table
            return create_unbound_ibis_table(self.schema, self.dataset_name, table_name)
        elif type_ == "ibis_narwhals":
            import narwhals as nw
            from dlt.helpers.ibis import create_unbound_ibis_table

            unbound_table = create_unbound_ibis_table(self.schema, self.dataset_name, table_name)
            return nw.from_native(unbound_table)
        # TODO clean up eager retrieval `with cursor()` bit
        elif type_ == "pandas":
            with self.cursor(table_name) as cursor:
                data = cursor.df()
            return data
        elif type_ == "pyarrow":
            with self.cursor(table_name) as cursor:
                data = cursor.arrow()
            return data
        elif type_ == "polars_narwhals":
            import narwhals as nw

            with self.cursor(table_name) as cursor:
                data = cursor.arrow()
            
            # same approach for `pandas`, `pyarrow`, `modin`, `cudf```
            return nw.from_arrow(data, backend="polars")
        else:
            raise ValueError(f"Invalid `type_`. Received `{type_}`. Expected one of `['relation', 'ibis', 'sqlglot']")

    # TODO refactor BaseReadableDBAPIRelation to here
    # NOTE this only makes sense for eager transformations
    def iter_table(self, table_name: str, *, type_: Literal["pandas", "polars_narwhals", "pyarrow"], chunk_size: Optional[int] = None) -> Generator:
        if type_ == "pandas":
            with self.cursor(table_name) as cursor:
                yield from cursor.iter_df(chunk_size=chunk_size)
        elif type_ == "pyarrow":
            with self.cursor(table_name) as cursor:
                yield from cursor.iter_arrow(chunk_size=chunk_size)
        elif type_ == "polars_narwhals":
            import narwhals as nw
            with self.cursor(table_name) as cursor:
                for chunk in cursor.iter_arrow(chunk_size=chunk_size):
                    yield nw.from_arrow(chunk, backend="polars")

    # TODO improve this API and split into separate methods
    @contextmanager
    def cursor(self, table_name: str) -> Generator:
        """Gets a DBApiCursor for the current relation"""
        try:
            self._opened_sql_client = self.sql_client

            # case 1: client is already opened and managed from outside
            if self.sql_client.native_connection:
                with self.sql_client.execute_query(f"SELECT * FROM {table_name}") as cursor:
                    yield cursor
            # case 2: client is not opened, we need to manage it
            else:
                with self.sql_client as client:
                    with client.execute_query(f"SELECT * FROM {table_name}") as cursor:
                        yield cursor
        finally:
            self._opened_sql_client = None

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
