from types import TracebackType
from typing import Any, Type, Union, TYPE_CHECKING, List, Literal, overload


from sqlglot.schema import Schema as SQLGlotSchema
import sqlglot.expressions as sge

from dlt.common.destination.exceptions import (
    OpenTableClientNotAvailable,
)
from dlt.common.libs.sqlglot import TSqlGlotDialect
from dlt.common.json import json
from dlt.common.destination.reference import TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase, SupportsOpenTables, WithStateSync
from dlt.common.destination.dataset import (
    Relation,
    Dataset,
)
from dlt.common.schema import Schema
from dlt.common.typing import Self
from dlt.common.schema.typing import C_DLT_LOAD_ID
from dlt.common.utils import simple_repr, without_none
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.dataset.relation import ReadableDBAPIRelation
from dlt.destinations.dataset.utils import get_destination_clients
from dlt.destinations.queries import build_row_counts_expr

from dlt.transformations import lineage


if TYPE_CHECKING:
    from dlt.helpers.ibis import BaseBackend as IbisBackend
    from dlt.helpers.ibis import Table as IbisTable
    from dlt.helpers.ibis import Expr as IbisExpr
else:
    IbisBackend = Any
    IbisTable = Any
    IbisExpr = Any


class ReadableDBAPIDataset(Dataset):
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
        self._dataset_name = dataset_name

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
    def tables(self) -> list[str]:
        # return only completed tables
        return self.schema.data_table_names() + self.schema.dlt_table_names()

    def _ipython_key_completions_(self) -> list[str]:
        return self.tables

    @property
    def sqlglot_schema(self) -> SQLGlotSchema:
        # NOTE: no cache for now, it is probably more expensive to compute the current schema hash
        # to see wether this is stale than to compute a new sqlglot schema
        return lineage.create_sqlglot_schema(
            self.schema, self.dataset_name, dialect=self.sqlglot_dialect
        )

    @property
    def sqlglot_dialect(self) -> TSqlGlotDialect:
        return self.sql_client.capabilities.sqlglot_dialect

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

    def is_same_physical_destination(self, other: "ReadableDBAPIDataset") -> bool:
        """
        Returns true if the other dataset is on the same physical destination
        helpful if we want to run sql queries without extracting the data
        """
        return str(self.destination_client.config) == str(other.destination_client.config)

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
            with self._get_destination_client(Schema(self._dataset_name)) as client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema()
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))

        # default to empty schema with dataset name
        if not self._schema:
            self._schema = Schema(self._dataset_name)

    def query(
        self,
        query: Union[str, sge.Select, IbisExpr],
        query_dialect: TSqlGlotDialect = None,
        _execute_raw_query: bool = False,
    ) -> Relation:
        return ReadableDBAPIRelation(
            readable_dataset=self,
            query=query,
            query_dialect=query_dialect,
            _execute_raw_query=_execute_raw_query,
        )

    def __call__(
        self,
        query: Union[str, sge.Select, IbisExpr],
        query_dialect: TSqlGlotDialect = None,
        _execute_raw_query: bool = False,
    ) -> Relation:
        return self.query(query, query_dialect, _execute_raw_query)

    @overload
    def table(self, table_name: str) -> Relation: ...

    @overload
    def table(self, table_name: str, table_type: Literal["relation"]) -> Relation: ...

    @overload
    def table(self, table_name: str, table_type: Literal["ibis"]) -> IbisTable: ...

    def table(self, table_name: str, table_type: Literal["relation", "ibis"] = "relation") -> Any:
        # dataset only provides access to tables known in dlt schema, direct query may circumvent this
        available_tables = self.tables
        if table_name not in available_tables:
            # TODO: raise TableNotFound
            raise ValueError(
                f"Table `{table_name}` not found in schema `{self.schema.name}` of dataset"
                f" `{self.dataset_name}`. Available table(s):"
                f" {', '.join(available_tables)}"
            )

        if table_type == "ibis":
            from dlt.helpers.ibis import create_unbound_ibis_table

            return create_unbound_ibis_table(self.schema, self.dataset_name, table_name)

        # fallback to the standard dbapi relation
        return ReadableDBAPIRelation(
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
    ) -> Relation:
        """Returns a dictionary of table names and their row counts, returns counts of all data tables by default"""
        """If table_names is provided, only the tables in the list are returned regardless of the data_tables and dlt_tables flags"""

        selected_tables = table_names or []
        if not selected_tables:
            if data_tables:
                selected_tables += self.schema.data_table_names(seen_data_only=True)
            if dlt_tables:
                selected_tables += self.schema.dlt_table_names()

        # filter tables so only ones with dlt_load_id column are included
        dlt_load_id_col = None
        if load_id:
            dlt_load_id_col = self.schema.naming.normalize_identifier(C_DLT_LOAD_ID)
            selected_tables = [
                table
                for table in selected_tables
                if dlt_load_id_col in self.schema.tables[table]["columns"].keys()
            ]

        union_all_expr = None

        for table_name in selected_tables:
            counts_expr = build_row_counts_expr(
                table_name=table_name,
                dlt_load_id_col=dlt_load_id_col,
                load_id=load_id,
            )
            if union_all_expr is None:
                union_all_expr = counts_expr
            else:
                union_all_expr = union_all_expr.union(counts_expr, distinct=False)

        return self.query(query=union_all_expr)

    def __getitem__(self, table_name: str) -> Relation:
        """access of table via dict notation"""
        if table_name not in self.tables:
            raise KeyError(
                f"Table `{table_name}` not found on dataset. Available tables: `{self.tables}`"
            )
        try:
            return self.table(table_name)
        # TODO: expect TableNotFound in the future
        except ValueError as exc:
            raise KeyError(table_name, str(exc))

    def __getattr__(self, name: str) -> Any:
        """Retrieve a `Relation` with `name` and raise `AttributeError` when not found"""
        try:
            return self.table(name)
        # TODO: expect TableNotFound in the future
        except ValueError as exc:
            raise AttributeError(name, str(exc))

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

    def __repr__(self) -> str:
        # schema may not be set
        schema_ = self._schema or self._provided_schema
        schema_repr = repr(schema_) if schema_ else None
        kwargs = {
            "dataset_name": self._dataset_name,
            "destination": repr(self._destination),
            "schema": schema_repr,
        }
        return simple_repr("dlt.dataset", **without_none(kwargs))

    def __str__(self) -> str:
        # obtain schema. this eventually loads it from destination (if only name was provided)
        _schema = self.schema
        _client = self.destination_client
        # str(config) provides displayable physical location
        destination_info = f"{self._destination.destination_name}[{str(_client.config)}]"
        # new schema is created if dataset is not found or schema.name is not materialized yet
        if _schema.is_new:
            schema_info = "\nDataset is not available at the destination.\n"
        else:
            schema_info = f"with tables in dlt schema `{self.schema.name}`:\n"

        msg = f"Dataset `{self._dataset_name}` at `{destination_info}` {schema_info}"
        if _schema:
            tables = [name for name in self.schema.data_table_names()]
            if tables:
                msg += ", ".join(tables)
            else:
                msg += "No data tables found in schema."
        return msg
