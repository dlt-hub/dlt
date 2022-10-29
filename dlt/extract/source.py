import contextlib
from copy import deepcopy
import inspect
from collections.abc import Mapping as C_Mapping
from typing import AsyncIterable, AsyncIterator, Iterable, Iterator, List, Set, Sequence, Union, cast, Any

from dlt.common.schema import Schema
from dlt.common.schema.utils import new_table
from dlt.common.schema.typing import TPartialTableSchema, TTableSchemaColumns, TWriteDisposition
from dlt.common.typing import TDataItem, TDataItems
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext

from dlt.extract.typing import TFunHintTemplate, TTableHintTemplate, TTableSchemaTemplate
from dlt.extract.pipe import FilterItem, Pipe, PipeIterator
from dlt.extract.exceptions import CreatePipeException, DataItemRequiredForDynamicTableHints, GeneratorFunctionNotAllowedAsParentResource, InconsistentTableTemplate, InvalidResourceAsyncDataType, InvalidResourceBasicDataType, ResourceNameMissing, TableNameMissing


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
            columns = columns.values()  # type: ignore

        new_template: TTableSchemaTemplate = new_table(table_name, parent_table_name, write_disposition=write_disposition, columns=columns)  # type: ignore
        # if any of the hints is a function then name must be as well
        if any(callable(v) for k, v in new_template.items() if k != "name") and not callable(table_name):
            raise InconsistentTableTemplate("Table name must be a function if any other table hint is a function")
        return new_template


class DltResource(Iterable[TDataItems], DltResourceSchema):
    def __init__(self, pipe: Pipe, table_schema_template: TTableSchemaTemplate, selected: bool):
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
            if not inspect.isgeneratorfunction(inspect.unwrap(data)):
                raise ResourceFunctionNotAGenerator(name)

        # if generator, take name from it
        if inspect.isgenerator(data):
            name = name or data.__name__

        # name is mandatory
        if not name:
            raise ResourceNameMissing()

        # several iterable types are not allowed and must be excluded right away
        if isinstance(data, (AsyncIterator, AsyncIterable)):
            raise InvalidResourceAsyncDataType(name, data, type(data))
        if isinstance(data, (str, dict)):
            raise InvalidResourceBasicDataType(name, data, type(data))

        # check if depends_on is a valid resource
        parent_pipe: Pipe = None
        if depends_on:
            if not callable(data):
                raise DependentResourceMustBeAGeneratorFunction()
            else:
                pass
                # TODO: check sig if takes just one argument
                # if sig_valid():
                #     raise DependentResourceMustTakeDataItemArgument()
            if isinstance(depends_on, Pipe):
                parent_pipe = depends_on
            elif isinstance(depends_on, DltResource):
                parent_pipe = depends_on._pipe
            else:
                # if this is generator function provide nicer exception
                if callable(depends_on):
                    raise GeneratorFunctionNotAllowedAsParentResource(depends_on.__name__)
                else:
                    raise ParentNotAResource()


        # create resource from iterator, iterable or generator function
        if isinstance(data, (Iterable, Iterator)):
            pipe = Pipe.from_iterable(name, data, parent=parent_pipe)
        elif callable(data):
            pipe = Pipe(name, [data], parent_pipe)
        if pipe:
            return cls(pipe, table_schema_template, selected)
        else:
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

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        # make resource callable to support parametrized resources which are functions taking arguments
        _data = self._pipe.head(*args, **kwargs)
        # create new resource from extracted data
        return DltResource.from_data(_data, self.name, self._table_schema_template, self.selected, self._pipe.parent)

    def __iter__(self) -> Iterator[TDataItems]:
        return map(lambda item: item.item, PipeIterator.from_pipe(self._pipe))

    def __repr__(self) -> str:
        return f"DltResource {self.name} ({self._pipe._pipe_id}) at {id(self)}"


class DltSource(Iterable[TDataItems]):
    def __init__(self, schema: Schema, resources: Sequence[DltResource] = None) -> None:
        self.name = schema.name
        self._schema = schema
        self._resources: List[DltResource] = list(resources or [])
        self._enabled_resource_names: Set[str] = set(r.name for r in self._resources if r.selected)

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

    @schema.setter
    def schema(self, value: Schema) -> None:
        self._schema = value

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


    def run(self, destination: Any) -> Any:
        return Container()[PipelineContext].pipeline().run(source=self, destination=destination)

    def __iter__(self) -> Iterator[TDataItems]:
        return map(lambda item: item.item, PipeIterator.from_pipes(self.pipes))

    def __repr__(self) -> str:
        return f"DltSource {self.name} at {id(self)}"
