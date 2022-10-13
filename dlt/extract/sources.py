import contextlib
from copy import deepcopy
import inspect
from typing import AsyncIterable, AsyncIterator, Coroutine, Dict, Generator, Iterable, Iterator, List, Set, TypedDict, Union, Awaitable, Callable, Sequence, TypeVar, cast, Optional, Any
from dlt.common.exceptions import DltException
from dlt.common.schema.utils import new_table

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


class DltResourceSchema:
    def __init__(self, name: str, table_schema_template: TTableSchemaTemplate = None):
        # self.__name__ = name
        self.name = name
        self._table_name_hint_fun: TFunDataItemDynHint = None
        self._table_has_other_dynamic_hints: bool = False
        self._table_schema_template: TTableSchemaTemplate = None
        self._table_schema: TPartialTableSchema = None
        if table_schema_template:
            self._set_template(table_schema_template)

    def table_schema(self, item: TDataItem =  None) -> TPartialTableSchema:

        if not self._table_schema_template:
            # if table template is not present, generate partial table from name
            if not self._table_schema:
                self._table_schema = new_table(self.name)
            return self._table_schema

        def _resolve_hint(hint: Union[Any, TFunDataItemDynHint]) -> Any:
            if callable(hint):
                return hint(item)
            else:
                return hint

        # if table template present and has dynamic hints, the data item must be provided
        if self._table_name_hint_fun:
            if item is None:
                raise DataItemRequiredForDynamicTableHints(self.name)
            else:
                cloned_template = deepcopy(self._table_schema_template)
                return cast(TPartialTableSchema, {k: _resolve_hint(v) for k, v in cloned_template.items()})
        else:
            return cast(TPartialTableSchema, self._table_schema_template)

    def _set_template(self, table_schema_template: TTableSchemaTemplate) -> None:
        # if "name" is callable in the template then the table schema requires actual data item to be inferred
        name_hint = table_schema_template.get("name")
        if callable(name_hint):
            self._table_name_hint_fun = name_hint
        # check if any other hints in the table template should be inferred from data
        self._table_has_other_dynamic_hints = any(callable(v) for k, v in table_schema_template.items() if k != "name")

        if self._table_has_other_dynamic_hints and not self._table_name_hint_fun:
            raise InvalidTableSchemaTemplate("Table name must be a function if any other table hint is a function")
        self._table_schema_template = table_schema_template


class DltResource(Iterable[TDirectDataItem], DltResourceSchema):
    def __init__(self, pipe: Pipe, table_schema_template: TTableSchemaTemplate):
        self.name = pipe.name
        self._pipe = pipe
        super().__init__(self.name, table_schema_template)

    @classmethod
    def from_data(cls, data: Any, name: str = None, table_schema_template: TTableSchemaTemplate = None) -> "DltResource":
        # call functions assuming that they do not take any parameters, typically they are generator functions
        if callable(data):
            data = data()

        if isinstance(data, DltResource):
            return data

        if isinstance(data, Pipe):
            return cls(data, table_schema_template)

        # several iterable types are not allowed and must be excluded right away
        if isinstance(data, (AsyncIterator, AsyncIterable, str, dict)):
            raise InvalidResourceDataType("Invalid data type for DltResource", type(data))

        # create resource from iterator or iterable
        if isinstance(data, (Iterable, Iterator)):
            if inspect.isgenerator(data):
                name = name or data.__name__
            else:
                name = name or None
            if not name:
                raise ResourceNameRequired("The DltResource name was not provide or could not be inferred.")
            pipe = Pipe.from_iterable(name, data)
            return cls(pipe, table_schema_template)

        # some other data type that is not supported
        raise InvalidResourceDataType("Invalid data type for DltResource", type(data))


    def select(self, *table_names: Iterable[str]) -> "DltResource":
        if not self._table_name_hint_fun:
            raise CreatePipeException("Table name is not dynamic, table selection impossible")

        def _filter(item: TDataItem) -> bool:
            return self._table_name_hint_fun(item) in table_names

        # add filtering function at the end of pipe
        self._pipe.add_step(FilterItem(_filter))
        return self

    def map(self) -> None:
        raise NotImplementedError()

    def flat_map(self) -> None:
        raise NotImplementedError()

    def filter(self) -> None:
        raise NotImplementedError()

    def __iter__(self) -> Iterator[TDirectDataItem]:
        return map(lambda item: item.item, PipeIterator.from_pipe(self._pipe))

    def __repr__(self) -> str:
        return f"DltResource {self.name} ({self._pipe._pipe_id}) at {id(self)}"


class DltSource(Iterable[TDirectDataItem]):
    def __init__(self, schema: Schema, resources: Sequence[DltResource] = None) -> None:
        self.name = schema.name
        self._schema = schema
        self._resources: List[DltResource] = list(resources or [])
        self._enabled_resource_names: Set[str] = set(r.name for r in self._resources)

    @classmethod
    def from_data(cls, schema: Schema, data: Any) -> "DltSource":
        # creates source from various forms of data
        if isinstance(data, DltSource):
            return data

        # several iterable types are not allowed and must be excluded right away
        if isinstance(data, (AsyncIterator, AsyncIterable, str, dict)):
            raise InvalidSourceDataType("Invalid data type for DltSource", type(data))

        # in case of sequence, enumerate items and convert them into resources
        if isinstance(data, Sequence):
            resources = [DltResource.from_data(i) for i in data]
        else:
            resources = [DltResource.from_data(data)]

        return cls(schema, resources)


    def __getitem__(self, name: str) -> List[DltResource]:
        if name not in self._enabled_resource_names:
            raise KeyError(name)
        return [r for r in self._resources if r.name == name]

    def resource_by_pipe(self, pipe: Pipe) -> DltResource:
        # identify pipes by memory pointer
        return next(r for r in self._resources if r._pipe._pipe_id is pipe._pipe_id)

    @property
    def resources(self) -> Sequence[DltResource]:
        return [r for r in self._resources if r.name in self._enabled_resource_names]

    @property
    def pipes(self) -> Sequence[Pipe]:
        return [r._pipe for r in self._resources if r.name in self._enabled_resource_names]

    @property
    def schema(self) -> Schema:
        return self._schema

    def discover_schema(self) -> Schema:
        # extract tables from all resources and update internal schema
        for r in self._resources:
            # names must be normalized here
            with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                partial_table = self._schema.normalize_table_identifiers(r.table_schema())
                self._schema.update_schema(partial_table)
        return self._schema

    def select(self, *resource_names: str) -> "DltSource":
        # make sure all selected resources exist
        for name in resource_names:
            self.__getitem__(name)
        self._enabled_resource_names = set(resource_names)
        return self


    def __iter__(self) -> Iterator[TDirectDataItem]:
        return map(lambda item: item.item, PipeIterator.from_pipes(self.pipes))

    def __repr__(self) -> str:
        return f"DltSource {self.name} at {id(self)}"


class DltSourceException(DltException):
    pass


class DataItemRequiredForDynamicTableHints(DltException):
    def __init__(self, resource_name: str) -> None:
        self.resource_name = resource_name
        super().__init__(f"Instance of Data Item required to generate table schema in resource {resource_name}")



# class
