import asyncio
from collections import abc
from copy import deepcopy
from functools import wraps
from itertools import chain

import inspect
import itertools
import os
import sys
from typing import Any, Coroutine, Dict, Iterator, List, NamedTuple, Sequence, cast

from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TTableSchema, TTableSchemaColumns
from dlt.common.schema.utils import new_table
from dlt.common.sources import with_retry, with_table_name, get_table_name

# from examples.sources.rasa_tracker_store


_meta = {}

_i_schema:  Schema = None
_i_info = None

#abc.Iterator


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


class TableGenerator(abc.Generator, TableMetadataMixin):
    def __init__(self, g, table, schema = None):
        self.g = g
        super().__init__(table, schema)

    def send(self, value):
        return self.g.send(value)

    def throw(self, typ, val=None, tb=None):
        return self.g.throw(typ, val, tb)


class SourceList(abc.Sequence):
    def __init__(self, s, schema):
        self.s: abc.Sequence = s
        self.schema = schema

    # Sized
    def __len__(self) -> int:
        return self.s.__len__()

    # Iterator
    def __next__(self):
        return next(self.s)

    def __iter__(self):
        return self

    # Container
    def __contains__(self, value: object) -> bool:
        return self.s.__contains__(value)

    # Reversible
    def __reversed__(self):
        return self.s.__reversed__()

    # Sequence
    def __getitem__(self, index):
        return self.s.__getitem__(index)

    def index(self, value: Any, start: int = ..., stop: int = ...) -> int:
        return self.s.index(value, start, stop)

    def count(self, value: Any) -> int:
        return self.s.count(value)

class SourceTable(NamedTuple):
    table_name: str
    data: Iterator[Any]


def source(schema=None):
    """This is source decorator"""
    def _dec(f: callable):
        print(f"calling source on {f.__name__}")
        global _i_schema

        __dlt_schema = Schema(f.__name__) if not schema else schema
        sig = inspect.signature(f)

        @wraps(f)
        def _wrap(*args, **kwargs):
            global _i_schema

            inner_schema: Schema = None
            # if "schema" in kwargs and isinstance(kwargs["schema"], Schema):
            #     inner_schema = kwargs["schema"]
            #     # remove if not in sig
            #     if "schema" not in sig.parameters:
            #         del kwargs["schema"]

            _i_schema = inner_schema or __dlt_schema
            rv = f(*args, **kwargs)

            if not isinstance(rv, (abc.Iterator, abc.Iterable)) or isinstance(rv, (dict, str)):
                raise ValueError(f"Expected iterator/iterable containing tables {type(rv)}")

            # assume that source contain iterator of TableIterable
            tables = []
            for table in rv:
                # if not isinstance(rv, abc.Iterator) or isinstance(rv, (dict, str):
                if not isinstance(table, TableIterable):
                    raise ValueError(f"Please use @table or as_table: {type(table)}")
                tables.append(table)
            # iterator consumed - clear schema
            _i_schema = None
                # if hasattr(rv, "__name__"):
                #     s.a
                # source with single table
            #     return SourceList([rv], _i_schema)
            # elif isinstance(rv, abc.Sequence):
            #     # peek what is inside
            #     item = None if len(rv) == 0 else rv[1]
            #     # if this is list, iterator or empty
            #     if isinstance(item, (NoneType, TableMetadataMixin, abc.Iterator)):
            #         return SourceList(rv, _i_schema)
            #     else:
            #         return SourceList([rv], _i_schema)
            # else:
            #    raise ValueError(f"Unsupported table type {type(rv)}")

            return tables
            # if isinstance(rv, abc.Iterable) or inspect(rv, abc.Iterator):
            #     yield from rv
            # else:
            #     yield rv
        print(f.__doc__)
        _wrap.__doc__ = f.__doc__ + """This is source decorator"""
        return _wrap

    # if isinstance(obj, callable):
    #     return _wrap
    # else:
    #     return obj
    return _dec


def table(name = None, write_disposition = None, parent = None, columns: Sequence[TColumnSchema] = None, schema = None):
    """Create a table .

    Args:
        name ([type], optional): [description]. Defaults to None.
        write_disposition ([type], optional): [description]. Defaults to None.
        parent ([type], optional): [description]. Defaults to None.
        columns (Sequence[TColumn], optional): [description]. Defaults to None.
        schema ([type], optional): [description]. Defaults to None.
    """
    def _dec(f: callable):
        global _i_schema

        if _i_schema and schema:
            raise Exception("Do not set explicit schema for a table in source context")

        l_schema = schema or _i_schema
        table = new_table(name or f.__name__, parent, write_disposition, columns)
        print(f"calling TABLE on {f.__name__}: {l_schema}")

        # @wraps(f, updated=('__dict__','__doc__'))
        def _wrap(*args, **kwargs):
            rv = f(*args, **kwargs)
            return TableIterable(rv, table, l_schema)
            # assert _i_info == None

            # def _yield_inner() :
            #     global _i_info
            #     print(f"TABLE: setting _i_info on {f.__name__} {l_schema}")
            #     _i_info = (table, l_schema)

            #     if isinstance(rv, abc.Sequence):
            #         yield rv
            #         # return TableIterator(iter(rv), _i_info)
            #     elif isinstance(rv, abc.Generator):
            #         # return TableGenerator(rv, _i_info)
            #         yield from rv
            #     else:
            #         yield from rv
            #     _i_info = None
            #     # must clean up in extract
            #     # assert _i_info == None

            # gen_inner = _yield_inner()
            # # generator name is a table name
            # gen_inner.__name__ = "__dlt_meta:" + "*" if callable(table["name"]) else table["name"]
            # # return generator
            # return gen_inner

            # _i_info = None
            # yield from map(lambda i: with_table_name(i, id(rv)), rv)

        return _wrap

    return _dec


def as_table(obj, name, write_disposition = None, parent = None, columns = None):
    global _i_schema
    l_schema = _i_schema

    # for i, f in sys._current_frames():
    #     print(i, f)

    # print(sys._current_frames())

    # try:
    #     for d in range(0, 10):
    #         c_f = sys._getframe(d)
    #         print(c_f.f_code.co_varnames)
    #         print("------------")
    #         if "__dlt_schema" in c_f.f_locals:
    #             l_schema = c_f.f_locals["__dlt_schema"]
    #             break
    # except ValueError:
    #     # stack too deep
    #     pass

    # def inner():
    #     # global _i_info

    #     # assert _i_info == None
    #     print(f"AS_TABLE: setting _i_info on {name} {l_schema}")
    #     table = new_table(name, parent, write_disposition, columns)
    #     _i_info = (table, l_schema)
    #     if isinstance(obj, abc.Sequence):
    #         return TableIterator(iter(obj), _i_info)
    #     elif isinstance(obj, abc.Generator):
    #         return TableGenerator(obj, _i_info)
    #     else:
    #         return TableIterator(obj, _i_info)
    #     # if isinstance(obj, abc.Sequence):
    #     #     yield obj
    #     # else:
    #     #     yield from obj
    #     # _i_info = None

    table = new_table(name, parent, write_disposition, columns)
    print(f"calling AS TABLE on {name}: {l_schema}")
    return TableIterable(obj, table, l_schema)
    # def _yield_inner() :
    #     global _i_info
    #     print(f"AS_TABLE: setting _i_info on {name} {l_schema}")
    #     _i_info = (table, l_schema)

    #     if isinstance(obj, abc.Sequence):
    #         yield obj
    #         # return TableIterator(iter(obj), _i_info)
    #     elif isinstance(obj, abc.Generator):
    #         # return TableGenerator(obj, _i_info)
    #         yield from obj
    #     else:
    #         yield from obj

    #     # must clean up in extract
    #     # assert _i_info == None

    # gen_inner = _yield_inner()
    # # generator name is a table name
    # gen_inner.__name__ = "__dlt_meta:" + "*" if callable(table["name"]) else table["name"]
    # # return generator
    # return gen_inner

    # return inner()

# def async_table(write_disposition = None, parent = None, columns = None):

#     def _dec(f: callable):

#         def _wrap(*args, **kwargs):
#             global _i_info

#             l_info = new_table(f.__name__, parent, write_disposition, columns)
#             rv = f(*args, **kwargs)

#             for i in rv:
#                 # assert _i_info == None
#                 # print("set info")
#                 _i_info = l_info
#                 # print(f"what: {i}")
#                 yield i
#                 _i_info = None
#                 # print("reset info")

#             # else:
#             #     yield from rv
#             # yield from map(lambda i: with_table_name(i, id(rv)), rv)

#         return _wrap

#     return _dec


# takes name from decorated function
@source()
def spotify(api_key=None):
    """This is spotify source with several tables"""

    # takes name from decorated function
    @table(write_disposition="append")
    def numbers():
        return [1, 2, 3, 4]

    @table(write_disposition="replace")
    def songs(library):

        # https://github.com/leshchenko1979/reretry
        async def _i(id):
            await asyncio.sleep(0.5)
            # raise Exception("song cannot be taken")
            return {f"song{id}": library}

        for i in range(3):
            yield _i(i)

    @table(write_disposition="replace")
    def albums(library):

        async def _i(id):
            await asyncio.sleep(0.5)
            return {f"album_{id}": library}


        for i in ["X", "Y"]:
            yield _i(i)

    @table(write_disposition="append")
    def history():
        """This is your song history"""
        print("HISTORY yield")
        yield {"name": "dupa"}

    print("spotify returns list")
    return (
        history(),
        numbers(),
        songs("lib_1"),
        as_table(["lib_song"], name="library"),
        albums("lib_2")
    )


@source()
def annotations():
    """Ad hoc annotation source"""
    yield as_table(["ann1", "ann2", "ann3"], "annotate", write_disposition="replace")


# this table exists out of source context and will attach itself to the current default schema in the pipeline
@table(write_disposition="merge", parent="songs_authors")
def songs__copies(song, num):
    return [{"song": song, "copy": True}] * num


event_column_template: List[TColumnSchema] = [{
    "name": "timestamp",
    "data_type": "timestamp",
    "nullable": False,
    "partition": True,
    "sorted": True
    }
]

# this demonstrates the content based naming of the tables for stream based sources
# same rule that applies to `name` could apply to `write_disposition` and `columns`
@table(name=lambda i: "event_" + i["event"], write_disposition="append", columns=event_column_template)
def events():
    from examples.sources.jsonl import get_source as read_jsonl

    sources = [
     read_jsonl(file) for file in os.scandir("examples/data/rasa_trackers")
    ]
    for i in chain(*sources):
        yield {
                "sender_id": i["sender_id"],
                "timestamp": i["timestamp"],
                "event": i["event"]
            }
        yield i


# another standalone source
authors = as_table(["authr"], "songs_authors")


# def source_with_schema_discovery(credentials, sheet_id, tab_id):

#     # discover the schema from actual API
#     schema: Schema = get_schema_from_sheets(credentials, sheet_id)

#     # now declare the source
#     @source(schema=schema)
#     @table(name=schema.schema_name, write_disposition="replace")
#     def sheet():
#         from  examples.sources.google_sheets import get_source

#         yield from get_source(credentials, sheet_id, tab_id)

#     return sheet()


class Pipeline:
    def __init__(self, parallelism = 2, default_schema: Schema = None) -> None:
        self.sem = asyncio.Semaphore(parallelism)
        self.schemas: Dict[str, Schema] = {}
        self.default_schema_name: str = ""
        if default_schema:
            self.default_schema_name = default_schema.name
            self.schemas[default_schema.name] = default_schema

    async def extract(self, items, schema: Schema = None):
        # global _i_info

        l_info = None
        if isinstance(items, TableIterable):
            l_info = (items._table, items._schema)
        print(f"extracting table with name {getattr(items, '__name__', None)} {l_info}")

        # if id(i) in meta:
        #     print(meta[id(i)])

        def _p_i(item, what):
            if l_info:
                info_schema: Schema = l_info[1]
                if info_schema:
                    # if already in pipeline - use the pipeline one
                    info_schema = self.schemas.get(info_schema.name) or info_schema
                # if explicit - use explicit
                eff_schema = schema or info_schema
                if eff_schema is None:
                    # create default schema when needed
                    eff_schema = self.schemas.get(self.default_schema_name) or Schema(self.default_schema_name)
                if eff_schema is not None:
                    table: TTableSchema = l_info[0]
                    # normalize table name
                    if callable(table["name"]):
                        table_name = table["name"](item)
                    else:
                        table_name = eff_schema.normalize_table_name(table["name"])

                    if table_name not in eff_schema._schema_tables:
                        table = deepcopy(table)
                        table["name"] = table_name
                        # TODO: normalize all other names
                        eff_schema.update_schema(table)
                    # TODO: l_info may contain type hints etc.
                    self.schemas[eff_schema.name] = eff_schema
                    if len(self.schemas) == 1:
                        self.default_schema_name = eff_schema.name

                    print(f"{item} of {what} has HINT and will be written as {eff_schema.name}:{table_name}")
            else:
                eff_schema = self.schemas.get(self.default_schema_name) or Schema(self.default_schema_name)
                print(f"{item} of {what} No HINT and will be written as {eff_schema.name}:table")

        # l_info = _i_info
        if isinstance(items, TableIterable):
            items = iter(items._data)
        if isinstance(items, (abc.Sequence)):
            items = iter(items)
            # for item in items:
            #     _p_i(item, "list_item")
        if inspect.isasyncgen(items):
            raise NotImplemented()
        else:
            # context is set immediately
            item = next(items, None)
            if item is None:
                return

            global _i_info

            if l_info is None and isinstance(_i_info, TableMetadataMixin):
                l_info = (_i_info._table, _i_info._schema)
            # l_info = _i_info
            # _i_info = None
            if inspect.iscoroutine(item) or isinstance(item, Coroutine):
                async def _await(a_i):
                    async with self.sem:
                        # print("enter await")
                        a_itm = await a_i
                        _p_i(a_itm, "awaitable")

                items = await asyncio.gather(
                    asyncio.ensure_future(_await(item)), *(asyncio.ensure_future(_await(ii)) for ii in items)
                )
            else:
                _p_i(item, "iterator")
                list(map(_p_i, items, itertools.repeat("iterator")))

            # print("reset info")
            # _i_info = None
        # assert _i_info is None

    def extract_all(self, sources, schema: Schema = None):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*[self.extract(i, schema=schema) for i in sources]))
        # loop.close()


default_schema = Schema("")

print("s iter of iters")
s = spotify(api_key="7")
for items in s:
    print(items.__name__)
s[0] = map(lambda d: {**d, **{"lambda": True}} , s[0])
print("s2 iter of iters")
s2 = annotations()

# for x in s[0]:
#     print(x)
# exit()

# print(list(s2))
# exit(0)

# mod albums

def mapper(a):
    a["mapper"] = True
    return a

# https://asyncstdlib.readthedocs.io/en/latest/#
# s[3] = map(mapper, s[3])


# Pipeline().extract_all([s2])
p = Pipeline(default_schema=Schema("default"))
chained = chain(s, s2, [authors], [songs__copies("abba", 4)], [["raw", "raw"]])
# for items in chained:
#     print(f"{type(items)}: {getattr(items, '__name__', 'NONE')}")
p.extract_all(chained, schema=None)
# p.extract_all([events()], schema=Schema("events"))
p.extract_all([["nein"] * 5])
p.extract_all([as_table([], name="EMPTY")])


for schema in p.schemas.values():
    print(schema.to_pretty_yaml(remove_defaults=True))

print(p.default_schema_name)
# for i in s:
#     await extract(i)
# for i in s:
#     extract(chain(*i, iter([1, "zeta"])))