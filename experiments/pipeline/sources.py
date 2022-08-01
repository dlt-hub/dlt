from collections import abc
from typing import Union, Awaitable, Callable, Sequence, TypeVar, cast

from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchema
from dlt.common.typing import DictStrAny


TDirectDataItem = Union[DictStrAny, Sequence[DictStrAny]]
TDeferredDataItem = Callable[[], TDirectDataItem]
TAwaitableDataItem = Awaitable[TDirectDataItem]
TDataItem = Union[TDirectDataItem, TDeferredDataItem, TAwaitableDataItem]

# TBoundItem = TypeVar("TBoundItem", bound=TDataItem)
# TDeferreBoundItem = Callable[[], TBoundItem]


class TableMetadataMixin:
    def __init__(self, table: TTableSchema, schema: Schema = None):
        self._table = table
        self._schema = schema
        self._table_name = table["name"]
        self.__name__ = self._table_name

    @property
    def table_schema(self):
        # TODO: returns unified table schema by merging _schema and _table with table taking precedence
        return self._table


class TableIterable(abc.Iterable, TableMetadataMixin):
    def __init__(self, i, table, schema = None):
        self._data = i
        super().__init__(table, schema)

    def __iter__(self):
        # TODO: this should resolve the _data like we do in the extract method: all awaitables and deferred items are resolved
        #       possibly in parallel.
        if isinstance(self._data, abc.Iterator):
            return TableIterator(self._data, self._table, self._schema)
        return TableIterator(iter(self._data), self._table, self._schema)


class TableIterator(abc.Iterator, TableMetadataMixin):
    def __init__(self, i, table, schema = None):
        self.i = i
        super().__init__(table, schema)

    def __next__(self):
        # export metadata to global variable so it can be read by extractor
        # TODO: remove this hack if possible
        global _i_info
        _i_info = cast(self, TableMetadataMixin)

        return next(self.i)

    def __iter__(self):
        return self


class SourceTables(list):
    pass
