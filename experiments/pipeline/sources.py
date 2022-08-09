from collections import abc
from typing import Iterable, Iterator, List, Union, Awaitable, Callable, Sequence, TypeVar, cast

from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import TDataItem


TDirectDataItem = Union[TDataItem, Sequence[TDataItem]]
TDeferredDataItem = Callable[[], TDirectDataItem]
TAwaitableDataItem = Awaitable[TDirectDataItem]
TPendingDataItem = Union[TDirectDataItem, TDeferredDataItem, TAwaitableDataItem]

# TBoundItem = TypeVar("TBoundItem", bound=TDataItem)
# TDeferreBoundItem = Callable[[], TBoundItem]


class TableMetadataMixin:
    def __init__(self, table_schema: TTableSchema, schema: Schema = None, selected_tables: List[str] = None):
        self._table_schema = table_schema
        self.schema = schema
        self._table_name = table_schema["name"]
        self.__name__ = self._table_name
        self.selected_tables = selected_tables

    @property
    def table_schema(self):
        # TODO: returns unified table schema by merging _schema and _table with table taking precedence
        return self._table_schema


_i_info: TableMetadataMixin = None


def extractor_resolver(i: Union[Iterator[TPendingDataItem], Iterable[TPendingDataItem]], selected_tables: List[str] = None) -> Iterator[TDataItem]:

    if not isinstance(i, abc.Iterator):
        i = iter(i)

    for item in i:



class TableIterable(abc.Iterable, TableMetadataMixin):
    def __init__(self, i, table, schema = None, selected_tables: List[str] = None):
        self._data = i
        super().__init__(table, schema, selected_tables)

    def __iter__(self):
        # TODO: this should resolve the _data like we do in the extract method: all awaitables and deferred items are resolved
        #       possibly in parallel.
        resolved_data = extractor_resolver(self._data)
        return TableIterator(resolved_data, self._table_schema, self.schema, self.selected_tables)



class TableIterator(abc.Iterator, TableMetadataMixin):
    def __init__(self, i, table, schema = None, selected_tables: List[str] = None):
        self.i = i
        super().__init__(table, schema, selected_tables)

    def __next__(self):
        # export metadata to global variable so it can be read by extractor
        # TODO: remove this hack if possible
        global _i_info
        _i_info = cast(self, TableMetadataMixin)

        if callable(self._table_name):
            else:
        # if no table filter selected
        return next(self.i)
        while True:
        ni = next(self.i)
        if callable(self._table_name):
            # table name is a lambda, so resolve table name
            t_n = self._table_name(ni)
        return

    def __iter__(self):
        return self


class SourceTables(list, List[TableIterable]):

