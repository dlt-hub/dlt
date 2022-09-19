from collections import abc
from typing import AsyncIterable, Coroutine, Dict, Generator, Iterable, Iterator, List, TypedDict, Union, Awaitable, Callable, Sequence, TypeVar, cast, Optional, Any
from dlt.common.exceptions import DltException

from dlt.common.typing import TDataItem
from dlt.common.sources import TFunDataItemDynHint, TDirectDataItem
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TPartialTableSchema, TTableSchema, TTableSchemaColumns, TWriteDisposition

from experiments.pipeline.pipe import FilterItem, Pipe, CreatePipeException, PipeIterator


class TTableSchemaTemplate(TypedDict, total=False):
    name: Union[str, TFunDataItemDynHint]
    description: Union[str, TFunDataItemDynHint]
    write_disposition: Union[TWriteDisposition, TFunDataItemDynHint]
    # table_sealed: Optional[bool]
    parent: Union[str, TFunDataItemDynHint]
    columns: Union[TTableSchemaColumns, TFunDataItemDynHint]

# async def item(value: str) -> TDataItem:
#     return {"str": value}

# import asyncio

# print(asyncio.run(item("a")))
# exit(0)


# reveal_type(item)
# TBoundItem = TypeVar("TBoundItem", bound=TDataItem)
# TDeferreBoundItem = Callable[[], TBoundItem]



class DltResourceSchema:
    def __init__(self, name: str, table_schema_template: TTableSchemaTemplate):
        self.__name__ = name
        self.name = name
        self.table_name_hint_fun: TFunDataItemDynHint = None
        self.table_has_other_dynamic_props: bool = False
        self._table_schema_template: TTableSchemaTemplate = None
        self._set_template(table_schema_template)

    def table_schema(self, item: TDataItem =  None) -> TPartialTableSchema:

        def _resolve_hint(hint: Union[Any, TFunDataItemDynHint]) -> Any:
            if callable(hint):
                return hint(item)
            else:
                return hint

        if self.table_name_hint_fun:
            if item is None:
                raise DataItemRequiredForDynamicTableHints(self.name)
            else:
                return {k: _resolve_hint(v) for k, v in self._table_schema_template.items()}
        else:
            return cast(TPartialTableSchema, self._table_schema_template)

    def _set_template(self, table_schema_template: TTableSchemaTemplate) -> None:
        if callable(table_schema_template.get("name")):
            self.table_name_hint_fun = table_schema_template.get("name")
        self.table_has_other_dynamic_props = any(callable(v) for k, v in table_schema_template.items() if k != "name")
        # check if template contains any functions
        if self.table_has_other_dynamic_props and not self.table_name_hint_fun:
            raise InvalidTableSchemaTemplate("Table name must be a function if any other table hint is a function")
        self._table_schema_template = table_schema_template


class DltResource(Iterable[TDirectDataItem], DltResourceSchema):
    def __init__(self, pipe: Pipe, table_schema_template: TTableSchemaTemplate):
        self.name = pipe.name
        self.pipe = pipe
        super().__init__(self.name, table_schema_template)

    def select(self, *table_names: Iterable[str]) -> "DltResource":
        if not self.table_name_hint_fun:
            raise CreatePipeException("Table name is not dynamic, table selection impossible")

        def _filter(item: TDataItem) -> bool:
            return self.table_name(item) in table_names

        # add filtering function at the end of pipe
        self.pipe.add_step(FilterItem(_filter))
        return self

    def map(self) -> None:
        pass

    def flat_map(self) -> None:
        pass

    def filter(self) -> None:
        pass

    def __iter__(self) -> Iterator[TDirectDataItem]:
        return map(lambda item: item.item, PipeIterator.from_pipe(self.pipe))

class DltSource(Iterable[TDirectDataItem]):
    def __init__(self, schema: Schema, resources: Sequence[DltResource] = None) -> None:
        self._schema = schema
        self._resources: Dict[str, DltResource] = {} if resources is None else {r.name:r for r in resources}
        self._disabled_resources: Sequence[str] = []
        super().__init__(self)

    def __getitem__(self, i: str) -> DltResource:
        return self.resources[i]

    @property
    def resources(self) -> Sequence[DltResource]:
        return [r for r in self._resources if r not in self._disabled_resources]

    @property
    def pipes(self) -> Sequence[Pipe]:
        return [r.pipe for r in self._resources.values() if r.name not in self._disabled_resources]

    @property
    def schema(self) -> Schema:
        return self._schema

    def discover_schema(self) -> Schema:
        # extract tables from all resources and update internal schema
        # names must be normalized here
        return self._schema

    def select(self, *resource_names: Iterable[str]) -> "DltSource":
        pass

    def __iter__(self) -> Iterator[TDirectDataItem]:
        return map(lambda item: item.item, PipeIterator.from_pipe(self.pipes))


class DltSourceException(DltException):
    pass



# class
