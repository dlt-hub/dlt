from __future__ import annotations

from contextlib import contextmanager
from types import TracebackType
from typing import Any, Generator, Literal, Optional, Sequence, Type, Union, TYPE_CHECKING, overload


from sqlglot.schema import Schema as SQLGlotSchema

from dlt.common.destination.exceptions import OpenTableClientNotAvailable
from dlt.common.json import json
from dlt.common.destination.reference import TDestinationReferenceArg, Destination
from dlt.common.destination.client import JobClientBase, SupportsOpenTables, WithStateSync
from dlt.common.schema import Schema
from dlt.common.typing import Self
from dlt.destinations.sql_client import SqlClientBase, WithSqlClient
from dlt.destinations.dataset.relation import ReadableDBAPIRelation
from dlt.destinations.dataset.utils import get_destination_clients

from dlt.transformations import lineage


if TYPE_CHECKING:
    import pandas as pd
    import narwhals as nw
    from ibis import ir
    from sqlglot import exp

    from dlt.helpers.ibis import BaseBackend as IbisBackend


class Dataset:
    """Access data stored on a destination"""
    def __init__(
        self,
        name: str,
        *,
        destination: TDestinationReferenceArg,
        schema: Union[Schema, str, None] = None,
    ) -> None:
        # provided properties
        self._destination = Destination.from_reference(destination)
        self._provided_schema = schema
        self._name = name

        # derived / cached properties
        self._schema: Schema = None
        # self._sqlglot_schema: SQLGlotSchema = None
        self._sql_client: SqlClientBase[Any] = None
        self._opened_sql_client: SqlClientBase[Any] = None
        self._table_client: SupportsOpenTables = None

    @property
    def dialect(self) -> str:
        return self.sql_client.capabilities.sqlglot_dialect

    @property
    def schema(self) -> Schema:
        """Returns the schema of the dataset, will fetch the schema from the destination

        Returns:
            Schema: The schema of the dataset
        """
        if not self._schema:
            self._ensure_schema()
        return self._schema

    @property
    def sqlglot_schema(self) -> SQLGlotSchema:
        """Returns the computed and cached sqlglot schema of the dataset

        Returns:
            SQLGlotSchema: The sqlglot schema of the dataset
        """
        # NOTE: no cache for now, it is probably more expensive to compute the current schema hash
        # to see wether this is stale than to compute a new sqlglot schema
        return lineage.create_sqlglot_schema(self.schema, self.name, dialect=self.dialect)

    @property
    def name(self) -> str:
        """Returns the name of the dataset

        Returns:
            str: The name of the dataset
        """
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
    def destination_client(self) -> JobClientBase:
        return get_destination_clients(
            self.schema,
            destination=self._destination,
            destination_dataset_name=self._name
        )[0]

    @property
    def open_table_client(self) -> SupportsOpenTables:
        if not self._table_client:
            client = self.destination_client
            if isinstance(client, SupportsOpenTables):
                self._table_client = client
            else:
                raise OpenTableClientNotAvailable(self._name, self._destination.destination_name)
        return self._table_client
    
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

    def get_ibis_backend(self) -> IbisBackend:
        """Returns a connected ibis backend for the dataset. Not implemented for all destinations.

        Returns:
            IbisBackend: The ibis backend for the dataset
        """
        from dlt.helpers.ibis import create_ibis_backend

        return create_ibis_backend(self._destination, self.destination_client)

    def _get_sql_client(self, schema: Schema) -> SqlClientBase[Any]:
        client = self.destination_client
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
            client = get_destination_clients(
                Schema(self._provided_schema),
                destination=self._destination,
                destination_dataset_name=self._name
            )[0]
            with client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema(self._provided_schema)
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))
                    else:
                        self._schema = Schema(self._provided_schema)

        # no schema name given, load newest schema from destination
        elif not self._schema:
            client = get_destination_clients(
                Schema(self._name),
                destination=self._destination,
                destination_dataset_name=self._name
            )[0]
            with client:
                if isinstance(client, WithStateSync):
                    stored_schema = client.get_stored_schema()
                    if stored_schema:
                        self._schema = Schema.from_stored_schema(json.loads(stored_schema.schema))

        # default to empty schema with dataset name
        if not self._schema:
            self._schema = Schema(self._name)

    def table(
        self,
        table_name: str,
        type_: Literal["ibis", "narwhals_ibis", "pandas", "narwhals_polars"] = "ibis",
    ) -> Any:
        # dataset only provides access to tables known in dlt schema, direct query may cirumvent this
        return get_dataset_table(self, table_name, type_=type_)

    # TODO refactor BaseReadableDBAPIRelation to here
    # NOTE this only makes sense for eager transformations
    def iter_table(
        self,
        table_name: str,
        *,
        type_: Literal["pandas", "polars_narwhals", "pyarrow"],
        chunk_size: Optional[int] = None,
    ) -> Generator:
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


@overload
def get_dataset_table(dataset: Dataset, table_name: str, *, type_: Literal["ibis"]) -> ir.Table: ...

@overload
def get_dataset_table(dataset: Dataset, table_name: str, *, type_: Literal["pandas"]) -> pd.DataFrame: ...

@overload
def get_dataset_table(dataset: Dataset, table_name: str, *, type_: Literal["narwhals_ibis"]) -> nw.DataFrame: ...

@overload
def get_dataset_table(dataset: Dataset, table_name: str, *, type_: Literal["narwhals_polars"]) -> nw.DataFrame: ...


def get_dataset_table(
    dataset: Dataset,
    table_name: str,
    *,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    filter_: Any = None,
    type_: Literal[
        "ibis", "narwhals_ibis", "pandas", "narwhals_polars"
    ] = "ibis",
) -> Any:
    # dataset only provides access to tables known in dlt schema, direct query may cirumvent this
    if table_name not in dataset.schema.tables.keys():
        raise KeyError(
            f"Table {table_name} not found in schema {dataset.schema.name} of dataset"
            f" {dataset.name}. Avaible tables are: {dataset.schema.tables.keys()}"
        )
    
    if type_ == "ibis":
        return _to_ibis(
            table_name=table_name,
            dataset=dataset,
            columns=columns,
            limit=limit,
            filter_=filter_,
        )
    
    elif type_ == "ibis_narwhals":
        return _to_narwhals_ibis(
            table_name=table_name,
            dataset=dataset,
            columns=columns,
            limit=limit,
            filter_=filter_,
        )
    
    # TODO clean up eager retrieval `with cursor()` bit
    elif type_ == "pandas":
        return _to_pandas(
            table_name=table_name,
            dataset=dataset,
            columns=columns,
            limit=limit,
            filter_=filter_,
        )

    elif type_ == "pyarrow":
        with dataset.cursor(table_name) as cursor:
            data = cursor.arrow()
        return data

    elif type_ == "polars_narwhals":
        import narwhals as nw
        with dataset.cursor(table_name) as cursor:
            data = cursor.arrow()
        # same approach for `pandas`, `pyarrow`, `modin`, `cudf```
        return nw.from_arrow(data, backend="polars")
    else:
        raise ValueError(f"Invalid `type_`. Received `{type_}`.")




def _to_ibis(
    table_name: str,
    dataset: Dataset,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    filter_: Any = None,
) -> ir.Table: 
    from dlt.helpers.ibis import create_unbound_ibis_table
    unbound_table = create_unbound_ibis_table(schema=dataset.schema, dataset_name=dataset.name, table_name=table_name)
    unbound_table = _apply_ibis_selection(unbound_table, columns=columns, limit=limit, filter_=filter_)
    return unbound_table


# TODO support limit, columns
def _to_narwhals_ibis(
    table_name: str,
    dataset: Dataset,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    filter_: Any = None,
) -> nw.LazyFrame[Any]:
    import narwhals as nw
    unbound_table = _to_ibis(
        table_name=table_name,
        dataset=dataset,
        columns=columns,
        limit=limit,
        filter_=filter_,
    )
    return nw.from_native(unbound_table)


def _to_pandas(
    table_name: str,
    dataset: Dataset,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    filter_: Any = None,
) -> pd.DataFrame:
    import pandas as pd
    with dataset.cursor(table_name) as cursor:
        data = cursor.df()

    return ...



def _eager_retrieval_from_dataset(
    table_name: str,
    dataset: Dataset,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    filter_: Any = None,   
):
    query_expr: exp.Select = _apply_sqlglot_selection(table_name=table_name, columns=columns, limit=limit, filter_=filter_)
    query = query_expr.sql(dialect=dataset.dialect)
    

    if dataset.sql_client.native_connection:
        with dataset.sql_client.execute_query(query) as cursor:
            yield cursor
    # case 2: client is not opened, we need to manage it
    else:
        with dataset.sql_client as client:
            with client.execute_query(query) as cursor:
                yield cursor




def _apply_sqlglot_selection(
    table_name: str,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    filter_: Union[str, exp.Where, None] = None,
) -> exp.Select:
    if columns:
        query = exp.select(*columns).from_(table_name)
    else:
        query = exp.select("*").from_(table_name)

    if limit:
        query = query.limit(limit)
    
    if filter_:
        query = query.where(filter_)

    return query


# TODO what type of filters are accepted? Any lazy language should work (SQL string, SQLGLot, Ibis, Narwhals/Polars)
def _apply_ibis_selection(
    table: ir.Table,
    columns: Optional[Sequence[str]] = None,
    limit: Optional[int] = None,
    filter_: Union[ir.BooleanValue, Sequence[ir.BooleanValue], ir.IfAnyAll, None] = None,
) -> ir.Table:
    if columns:
        table = table.select(columns)

    if limit:
        table = table.limit(limit)

    if isinstance(filter_, ir.Expr):
        table = table.filter(filter_)

    return table
