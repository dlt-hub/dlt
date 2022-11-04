import contextlib
from copy import deepcopy
import inspect
from collections.abc import Mapping as C_Mapping
from typing import AsyncIterable, AsyncIterator, Dict, Iterable, Iterator, List, Set, Sequence, Union, cast, Any
from typing_extensions import Self

from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.schema.typing import TColumnSchema, TPartialTableSchema, TTableSchemaColumns, TWriteDisposition
from dlt.common.typing import AnyFun, TDataItem, TDataItems
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext

from dlt.extract.typing import TFunHintTemplate, TTableHintTemplate, TTableSchemaTemplate
from dlt.extract.pipe import FilterItem, Pipe, PipeIterator
from dlt.extract.exceptions import (
    DependentResourceIsNotCallable, InvalidDependentResourceDataTypeGeneratorFunctionRequired, InvalidParentResourceDataType, InvalidParentResourceIsAFunction, InvalidResourceDataType, InvalidResourceDataTypeFunctionNotAGenerator,
    ResourceNotFoundError, CreatePipeException, DataItemRequiredForDynamicTableHints, InconsistentTableTemplate, InvalidResourceDataTypeAsync, InvalidResourceDataTypeBasic,
    InvalidResourceDataTypeMultiplePipes, ResourceNameMissing, TableNameMissing)


class DltResourceSchema:
    def __init__(self, name: str, table_schema_template: TTableSchemaTemplate = None):
        # self.__name__ = name
        self.name = name
        self._table_name_hint_fun: TFunHintTemplate[str] = None
        self._table_has_other_dynamic_hints: bool = False
        self._table_schema_template: TTableSchemaTemplate = None
        self._table_schema: TPartialTableSchema = None
        if table_schema_template:
            self.set_template(table_schema_template)

    def table_schema(self, item: TDataItem =  None) -> TPartialTableSchema:
        if not self._table_schema_template:
            # if table template is not present, generate partial table from name
            if not self._table_schema:
                self._table_schema = new_table(self.name)
            return self._table_schema

        def _resolve_hint(hint: TTableHintTemplate[Any]) -> Any:
            if callable(hint):
                return hint(item)
            else:
                return hint

        # if table template present and has dynamic hints, the data item must be provided
        if self._table_name_hint_fun:
            if item is None:
                raise DataItemRequiredForDynamicTableHints(self.name)
            else:
                # cloned_template = deepcopy(self._table_schema_template)
                return cast(TPartialTableSchema, {k: _resolve_hint(v) for k, v in self._table_schema_template.items()})
        else:
            return cast(TPartialTableSchema, self._table_schema_template)

    def apply_hints(
        self,
        table_name: TTableHintTemplate[str] = None,
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TTableSchemaColumns] = None,
    ) -> None:
        t = None
        if not self._table_schema_template:
            # if there's no template yet, create and set new one
            t = self.new_table_template(table_name, parent_table_name, write_disposition, columns)
        else:
            # set single hints
            t = deepcopy(self._table_schema_template)
            if table_name:
                t["name"] = table_name
            if parent_table_name:
                t["parent"] = parent_table_name
            if write_disposition:
                t["write_disposition"] = write_disposition
            if columns:
                t["columns"] = columns
        self.set_template(t)

    def set_template(self, table_schema_template: TTableSchemaTemplate) -> None:
        # if "name" is callable in the template then the table schema requires actual data item to be inferred
        name_hint = table_schema_template["name"]
        if callable(name_hint):
            self._table_name_hint_fun = name_hint
        else:
            self._table_name_hint_fun = None
        # check if any other hints in the table template should be inferred from data
        self._table_has_other_dynamic_hints = any(callable(v) for k, v in table_schema_template.items() if k != "name")
        self._table_schema_template = table_schema_template

    @staticmethod
    def new_table_template(
        table_name: TTableHintTemplate[str],
        parent_table_name: TTableHintTemplate[str] = None,
        write_disposition: TTableHintTemplate[TWriteDisposition] = None,
        columns: TTableHintTemplate[TTableSchemaColumns] = None,
        ) -> TTableSchemaTemplate:
        if not table_name:
            raise TableNameMissing()
        # create a table schema template where hints can be functions taking TDataItem
        if isinstance(columns, C_Mapping):
            # new_table accepts a sequence
            column_list: List[TColumnSchema] = []
            for name, column in columns.items():
                column["name"] = name
                column_list.append(column)
            columns = column_list  # type: ignore

        new_template: TTableSchemaTemplate = new_table(table_name, parent_table_name, write_disposition=write_disposition, columns=columns)  # type: ignore
        # if any of the hints is a function then name must be as well
        if any(callable(v) for k, v in new_template.items() if k != "name") and not callable(table_name):
            raise InconsistentTableTemplate("Table name must be a function if any other table hint is a function")
        return new_template


class DltResource(Iterable[TDataItems], DltResourceSchema):
    def __init__(self, pipe: Pipe, table_schema_template: TTableSchemaTemplate, selected: bool):
        # TODO: allow resource to take name independent from pipe name
        self.name = pipe.name
        self.selected = selected
        self._pipe = pipe
        super().__init__(self.name, table_schema_template)

    @classmethod
    def from_data(cls, data: Any, name: str = None, table_schema_template: TTableSchemaTemplate = None, selected: bool = True, depends_on: Union["DltResource", Pipe] = None) -> "DltResource":

        if isinstance(data, DltResource):
            return data

        if isinstance(data, Pipe):
            return cls(data, table_schema_template, selected)

        if callable(data):
            name = name or data.__name__
            # function must be a generator
            # if not inspect.isgeneratorfunction(inspect.unwrap(data)):
            #     raise InvalidResourceDataTypeFunctionNotAGenerator(name, data, type(data))

        # if generator, take name from it
        if inspect.isgenerator(data):
            name = name or data.__name__

        # name is mandatory
        if not name:
            raise ResourceNameMissing()

        # several iterable types are not allowed and must be excluded right away
        if isinstance(data, (AsyncIterator, AsyncIterable)):
            raise InvalidResourceDataTypeAsync(name, data, type(data))
        if isinstance(data, (str, dict)):
            raise InvalidResourceDataTypeBasic(name, data, type(data))

        # check if depends_on is a valid resource
        parent_pipe: Pipe = None
        if depends_on:
            # must be a callable with single argument
            if not callable(data):
                raise InvalidDependentResourceDataTypeGeneratorFunctionRequired(name, data, type(data))
            else:
                if cls.is_valid_dependent_generator_function(data):
                    raise InvalidDependentResourceDataTypeGeneratorFunctionRequired(name, data, type(data))
            # parent resource
            if isinstance(depends_on, Pipe):
                parent_pipe = depends_on
            elif isinstance(depends_on, DltResource):
                parent_pipe = depends_on._pipe
            else:
                # if this is generator function provide nicer exception
                if callable(depends_on):
                    raise InvalidParentResourceIsAFunction(name, depends_on.__name__)
                else:
                    raise InvalidParentResourceDataType(name, depends_on, type(depends_on))

        # create resource from iterator, iterable or generator function
        if isinstance(data, (Iterable, Iterator)):
            pipe = Pipe.from_iterable(name, data, parent=parent_pipe)
        elif callable(data):
            pipe = Pipe(name, [data], parent_pipe)
        if pipe:
            return cls(pipe, table_schema_template, selected)
        else:
            # some other data type that is not supported
            raise InvalidResourceDataType(name, data, type(data), f"The data type is {type(data).__name__}")


    def add_pipe(self, data: Any) -> None:
        """Creates additional pipe for the resource from the specified data"""
        # TODO: (1) self resource cannot be a dependent one (2) if data is resource both self must and it must be selected/unselected + cannot be dependent
        raise InvalidResourceDataTypeMultiplePipes(self.name, data, type(data))


    def select(self, *table_names: Iterable[str]) -> "DltResource":
        if not self._table_name_hint_fun:
            raise CreatePipeException("Table name is not dynamic, table selection impossible")

        def _filter(item: TDataItem) -> bool:
            return self._table_name_hint_fun(item) in table_names

        # add filtering function at the end of pipe
        self._pipe.add_step(FilterItem(_filter))
        return self

    def map(self) -> None:  # noqa: A003
        raise NotImplementedError()

    def flat_map(self) -> None:
        raise NotImplementedError()

    def filter(self) -> None:  # noqa: A003
        raise NotImplementedError()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # make resource callable to support parametrized resources which are functions taking arguments
        if self._pipe.parent:
            raise DependentResourceIsNotCallable(self.name)
        # pass the call parameters to the pipe's head
        _data = self._pipe.head(*args, **kwargs)  # type: ignore
        # if f is not a generator (does not yield) raise Exception
        if not inspect.isgenerator(_data):
            # the only exception is if resource is returned, then return it instead
            if isinstance(_data, DltResource):
                return _data
            raise InvalidResourceDataTypeFunctionNotAGenerator(self.name, self._pipe.head, type(self._pipe.head))
        # create new resource from extracted data
        return DltResource.from_data(_data, self.name, self._table_schema_template, self.selected, self._pipe.parent)

    def __iter__(self) -> Iterator[TDataItems]:
        return map(lambda item: item.item, PipeIterator.from_pipe(self._pipe))

    def __repr__(self) -> str:
        return f"DltResource {self.name} ({self._pipe._pipe_id}) at {id(self)}"

    @staticmethod
    def is_valid_dependent_generator_function(f: AnyFun) -> bool:
        sig = inspect.signature(f)
        return len(sig.parameters) == 0


class DltResourceDict(Dict[str, DltResource]):
    @property
    def selected(self) -> Dict[str, DltResource]:
        return {k:v for k,v in self.items() if v.selected}

    @property
    def pipes(self) -> List[Pipe]:
        # TODO: many resources may share the same pipe so return ordered set
        return [r._pipe for r in self.values()]

    @property
    def selected_pipes(self) -> Sequence[Pipe]:
        # TODO: many resources may share the same pipe so return ordered set
        return [r._pipe for r in self.values() if r.selected]

    def select(self, *resource_names: str) -> Dict[str, DltResource]:
        # checks if keys are present
        for name in resource_names:
            try:
                self.__getitem__(name)
            except KeyError:
                raise ResourceNotFoundError(name, "Requested resource could not be selected because it is not present in the source.")
        # set the selected flags
        for resource in self.values():
            self[resource.name].selected = resource.name in resource_names
        return self.selected

    def find_by_pipe(self, pipe: Pipe) -> DltResource:
        # TODO: many resources may share the same pipe so return a list and also filter the resources by self._enabled_resource_names
        # identify pipes by memory pointer
        return next(r for r in self.values() if r._pipe._pipe_id is pipe._pipe_id)


class DltSource(Iterable[TDataItems]):
    def __init__(self, schema: Schema, resources: Sequence[DltResource] = None) -> None:
        self.name = schema.name
        self._schema = schema
        self._resources: DltResourceDict = DltResourceDict()
        if resources:
            for resource in resources:
                self._add_resource(resource)

    @classmethod
    def from_data(cls, schema: Schema, data: Any) -> "DltSource":
        # creates source from various forms of data
        if isinstance(data, DltSource):
            return data

        # in case of sequence, enumerate items and convert them into resources
        if isinstance(data, Sequence):
            resources = [DltResource.from_data(i) for i in data]
        else:
            resources = [DltResource.from_data(data)]

        return cls(schema, resources)


    @property
    def resources(self) -> DltResourceDict:
        return self._resources

    @property
    def selected_resources(self) -> Dict[str, DltResource]:
        return self._resources.selected

    @property
    def schema(self) -> Schema:
        return self._schema

    @schema.setter
    def schema(self, value: Schema) -> None:
        self._schema = value

    def discover_schema(self) -> Schema:
        # print(self._schema.tables)
        # extract tables from all resources and update internal schema
        for r in self.selected_resources.values():
            # names must be normalized here
            with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                partial_table = self._schema.normalize_table_identifiers(r.table_schema())
                # print(partial_table)
                self._schema.update_schema(partial_table)
        return self._schema

    def with_resources(self, *resource_names: str) -> "DltSource":
        self._resources.select(*resource_names)
        return self

    def run(self, destination: Any) -> Any:
        return Container()[PipelineContext].pipeline().run(self, destination=destination)

    def _add_resource(self, resource: DltResource) -> None:
        if resource.name in self._resources:
            # for resources with the same name try to add the resource as an another pipe
            self._resources[resource.name].add_pipe(resource)
        else:
            self._resources[resource.name] = resource

    def __iter__(self) -> Iterator[TDataItems]:
        return map(lambda item: item.item, PipeIterator.from_pipes(self._resources.selected_pipes))

    def __repr__(self) -> str:
        return f"DltSource {self.name} at {id(self)}"
