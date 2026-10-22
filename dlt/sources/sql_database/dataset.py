"""Dataset over the database of a `sql_database` source, read with the source table loader"""

import copy
import itertools
from contextlib import contextmanager
from functools import partial
from typing import (
    Any,
    AnyStr,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import dlt
from dlt.common.destination import Destination
from dlt.common.destination.capabilities import DestinationCapabilitiesContext
from dlt.common.libs.sql_alchemy import sa
from dlt.common.normalizers.naming import NamingConvention
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TDataItem
from dlt.dataset import Dataset
from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyClientConfiguration,
    SqlalchemyCredentials,
)
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.destinations.impl.sqlalchemy.factory import sqlalchemy
from dlt.destinations.impl.sqlalchemy.sqlalchemy_job_client import SqlalchemyJobClient
from dlt.destinations.sql_client import DBApiCursorImpl
from dlt.extract import DltResource, DltSource

from dlt.sources.sql_database.arrow_helpers import row_tuples_to_arrow
from dlt.sources.sql_database.helpers import BaseTableLoader, SqlTableReader, get_table_reader


class TableLoaderCursor(DBApiCursorImpl):
    """Cursor that executes its query with a table loader when data is fetched."""

    def __init__(
        self,
        table_loader: BaseTableLoader,
        query: str,
        backend_kwargs: Optional[Dict[str, Any]],
    ) -> None:
        self.native_cursor = None
        self.table_loader = table_loader
        self.query = query
        self.backend_kwargs = backend_kwargs
        self.columns_schema: TTableSchemaColumns = {}
        self._rows: Iterator[Tuple[Any, ...]] = None

    def _load_items(self, chunk_size: Optional[int]) -> Iterator[TDataItem]:
        loader = self.table_loader
        if chunk_size:
            loader = copy.copy(loader)
            loader.chunk_size = chunk_size
        # sqlalchemy reports lowercase result names for case-insensitive identifiers ie. on Oracle
        dialect: Any = loader.engine.dialect
        if not dialect.requires_name_normalize:
            yield from loader.load_query(
                sa.text(self.query), self.backend_kwargs, columns=self.columns_schema
            )
            return
        to_dataset = {dialect.normalize_name(name): name for name in self.columns_schema}
        columns: TTableSchemaColumns = {
            result_name: {**self.columns_schema[name], "name": result_name}
            for result_name, name in to_dataset.items()
        }
        for item in loader.load_query(sa.text(self.query), self.backend_kwargs, columns=columns):
            yield self._rename_columns(item, to_dataset)

    @staticmethod
    def _rename_columns(item: TDataItem, names: Dict[str, str]) -> TDataItem:
        from dlt.common.libs.pyarrow import pyarrow as pa
        from dlt.common.libs.pandas import pandas as pd

        if isinstance(item, pa.Table):
            return item.rename_columns([names.get(n, n) for n in item.column_names])
        if isinstance(item, pd.DataFrame):
            return item.rename(columns=names)
        return [{names.get(k, k): v for k, v in row.items()} for row in item]

    def _to_arrow(self, item: TDataItem) -> Any:
        from dlt.common.libs.pyarrow import pyarrow as pa
        from dlt.common.libs.pandas import pandas as pd

        if isinstance(item, pa.Table):
            return item
        if isinstance(item, pd.DataFrame):
            return pa.Table.from_pandas(item, preserve_index=False)
        # sqlalchemy backend yields lists of dicts in cursor column order
        names = list(item[0].keys()) if item else list(self.columns_schema)
        columns = {n: self.columns_schema.get(n, {"name": n}) for n in names}
        return row_tuples_to_arrow([tuple(row.values()) for row in item], columns, tz="UTC")

    def _to_df(self, item: TDataItem) -> Any:
        from dlt.common.libs.pandas import pandas as pd

        if isinstance(item, pd.DataFrame):
            return item
        return self._to_arrow(item).to_pandas()

    def _iter_chunks(
        self,
        chunk_size: Optional[int],
        convert: Callable[[TDataItem], Any],
        concat: Callable[[List[Any]], Any],
    ) -> Iterator[Any]:
        chunks = (convert(item) for item in self._load_items(chunk_size))
        if chunk_size:
            yield from chunks
            return
        # without chunk size, the full result is returned at once
        chunks_list = list(chunks)
        yield concat(chunks_list) if chunks_list else convert([])

    def iter_arrow(self, chunk_size: int) -> Generator[Any, None, None]:
        from dlt.common.libs.pyarrow import pyarrow as pa

        yield from self._iter_chunks(
            chunk_size,
            self._to_arrow,
            partial(pa.concat_tables, promote_options="default"),
        )

    def iter_df(self, chunk_size: int) -> Generator[Any, None, None]:
        from dlt.common.libs.pandas import pandas as pd

        yield from self._iter_chunks(chunk_size, self._to_df, partial(pd.concat, ignore_index=True))

    def _iter_rows(self) -> Iterator[Tuple[Any, ...]]:
        from dlt.common.libs.pyarrow import pyarrow as pa
        from dlt.common.libs.pandas import pandas as pd

        for item in self._load_items(None):
            if isinstance(item, pa.Table):
                rows: Any = item.to_pylist()
            elif isinstance(item, pd.DataFrame):
                yield from item.itertuples(index=False, name=None)
                continue
            else:
                rows = item
            yield from (tuple(row.values()) for row in rows)

    def _rows_iterator(self) -> Iterator[Tuple[Any, ...]]:
        if self._rows is None:
            self._rows = self._iter_rows()
        return self._rows

    def fetchall(self, *args: Any, **kwargs: Any) -> List[Tuple[Any, ...]]:
        return list(self._rows_iterator())

    def fetchmany(self, chunk_size: int = 1, *args: Any, **kwargs: Any) -> List[Tuple[Any, ...]]:
        return list(itertools.islice(self._rows_iterator(), chunk_size))

    def fetchone(self, *args: Any, **kwargs: Any) -> Optional[Tuple[Any, ...]]:
        return next(self._rows_iterator(), None)

    def execute(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError("Use `execute_query` of the sql client")

    def close(self, *args: Any, **kwargs: Any) -> None:
        self._rows = None


class TableLoaderSqlClient(SqlalchemyClient):
    """Reads query results with a table loader, holds no database connection."""

    def __init__(
        self,
        dataset_name: str,
        staging_dataset_name: str,
        credentials: SqlalchemyCredentials,
        capabilities: DestinationCapabilitiesContext,
        table_loader: BaseTableLoader,
        backend_kwargs: Optional[Dict[str, Any]],
    ) -> None:
        super().__init__(dataset_name, staging_dataset_name, credentials, capabilities)
        self.table_loader = table_loader
        self.backend_kwargs = backend_kwargs

    def open_connection(self) -> None:
        return None

    def close_connection(self) -> None:
        pass

    @contextmanager
    def execute_query(  # type: ignore[override]
        self, query: AnyStr, *args: Any, **kwargs: Any
    ) -> Iterator[TableLoaderCursor]:
        if args or kwargs:
            raise NotImplementedError("Query parameters are not supported")
        yield TableLoaderCursor(self.table_loader, str(query), self.backend_kwargs)

    def execute_sql(self, sql: Any, *args: Any, **kwargs: Any) -> Optional[Sequence[Sequence[Any]]]:
        with self.execute_query(sql, *args, **kwargs) as cursor:
            return cursor.fetchall()


class sql_database_reader(sqlalchemy):
    """Read-only destination that executes queries with a `sql_database` table loader."""

    def __init__(
        self,
        table_loader: BaseTableLoader,
        backend_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        # skip sqlalchemy init: its explicit None defaults would override the config
        Destination.__init__(
            self,
            credentials=table_loader.engine,
            enable_dataset_name_normalization=False,
            **kwargs,
        )
        self.table_loader = table_loader
        self.backend_kwargs = backend_kwargs

    def capabilities(
        self,
        config: Optional[SqlalchemyClientConfiguration] = None,
        naming: Optional[NamingConvention] = None,
    ) -> DestinationCapabilitiesContext:
        caps = super().capabilities(config, naming)
        # dataset schema has physical names so they are never case folded
        caps.casefold_identifier = str
        caps.has_case_sensitive_identifiers = True
        return caps

    def client(
        self, schema: Schema, initial_config: SqlalchemyClientConfiguration = None
    ) -> SqlalchemyJobClient:
        job_client = super().client(schema, initial_config)
        job_client.sql_client = TableLoaderSqlClient(
            job_client.sql_client.dataset_name,
            job_client.sql_client.staging_dataset_name,
            job_client.config.credentials,
            job_client.capabilities,
            self.table_loader,
            self.backend_kwargs,
        )
        return job_client


def _denormalize_schema(schema: Schema, denormalize: Callable[[str], str]) -> Schema:
    """Returns a copy of `schema` with physical names of tables reflected in lowercase by
    dialects with case-insensitive identifiers ie. Oracle.
    """
    physical = Schema(schema.name, normalizers={"names": "direct"})
    for table in schema.data_tables():
        table = copy.deepcopy(table)
        table["name"] = denormalize(table["name"])
        table["columns"] = {
            denormalize(name): {**column, "name": denormalize(name)}
            for name, column in table["columns"].items()
        }
        for reference in table.get("references", []):
            reference["referenced_table"] = denormalize(reference["referenced_table"])
            reference["columns"] = [denormalize(c) for c in reference["columns"]]
            reference["referenced_columns"] = [
                denormalize(c) for c in reference["referenced_columns"]
            ]
        physical.update_table(table)
    return physical


def sql_database_dataset(
    source: Union[DltSource, DltResource], dataset_name: Optional[str] = None
) -> Dataset:
    """Creates a read-only dataset over the tables of a `sql_database` source.

    The dataset schema is reflected from the selected resources and keeps the database
    identifiers as is (`direct` naming convention). Queries are executed with the table loader
    and backend configured on the source, ie. `connectorx` or `pyarrow`.

    Args:
        source (Union[DltSource, DltResource]): A `sql_database` source, a source with
            `sql_table` resources or a single `sql_table` resource. Tables must be reflected,
            so `defer_table_reflect` is not supported.
        dataset_name (Optional[str]): Name of the dataset. Defaults to the database schema of
            the tables or the default schema of the database.

    Returns:
        Dataset: A dataset that reads data from the source database.

    Raises:
        ValueError: If a selected resource is not a reflected `sql_table` or the selected
            tables are in different databases or schemas.
    """
    if isinstance(source, DltResource):
        source = DltSource(Schema(source.name), source.section, [source])

    readers: Dict[str, SqlTableReader] = {}
    for name, resource in source.selected_resources.items():
        reader = get_table_reader(resource)
        if reader is None:
            raise ValueError(
                f"Resource `{name}` is not a `sql_table` with a reflected table. Note that"
                " `defer_table_reflect` is not supported."
            )
        readers[name] = reader
    if not readers:
        raise ValueError(f"Source `{source.name}` has no selected resources")

    reader = next(iter(readers.values()))
    for name, other in readers.items():
        if (
            other.engine.url != reader.engine.url
            or other.table.schema != reader.table.schema
            or other.backend != reader.backend
        ):
            raise ValueError(
                f"Resource `{name}` reads from a different database, schema or backend than"
                " other resources in the source"
            )

    dialect: Any = reader.engine.dialect
    if dataset_name is None:
        dataset_name = reader.table.schema or sa.inspect(reader.engine).default_schema_name
        if dialect.requires_name_normalize:
            dataset_name = dialect.denormalize_name(dataset_name)

    # keep database identifiers so dataset names bind to physical tables and columns
    source = source.clone()
    source.schema = Schema(source.schema.name, normalizers={"names": "direct"})
    schema = source.discover_schema()
    if dialect.requires_name_normalize:
        schema = _denormalize_schema(schema, dialect.denormalize_name)
    # reflected tables exist in the database
    for table in schema.data_tables():
        table.setdefault("x-normalizer", {})["seen-data"] = True

    destination = sql_database_reader(reader.create_table_loader(), reader.backend_kwargs)
    return dlt.dataset(destination, dataset_name, schema=schema)
