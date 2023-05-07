import contextlib
from copy import copy
import makefun
import inspect
from typing import AsyncIterable, AsyncIterator, ClassVar, Callable, ContextManager, Dict, Iterable, Iterator, List, Sequence, Union, Any

from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer, RelationalNormalizerConfigPropagation
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnName
from dlt.common.typing import AnyFun, StrAny, TDataItem, TDataItems, NoneType
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext, StateInjectableContext, SupportsPipelineRun, _resource_state, source_state, pipeline_state
from dlt.common.utils import flatten_list_or_items, get_callable_name, multi_context_manager, uniq_id

from dlt.extract.typing import DataItemWithMeta, ItemTransformFunc, ItemTransformFunctionWithMeta, TableNameMeta, FilterItem, MapItem, YieldMapItem
from dlt.extract.pipe import Pipe, ManagedPipeIterator, TPipeStep
from dlt.extract.schema import DltResourceSchema, TTableSchemaTemplate
from dlt.extract.incremental import Incremental, IncrementalResourceWrapper
from dlt.extract.exceptions import (
    InvalidTransformerDataTypeGeneratorFunctionRequired, InvalidParentResourceDataType, InvalidParentResourceIsAFunction, InvalidResourceDataType, InvalidResourceDataTypeFunctionNotAGenerator, InvalidResourceDataTypeIsNone, InvalidTransformerGeneratorFunction,
    DataItemRequiredForDynamicTableHints, InvalidResourceDataTypeAsync, InvalidResourceDataTypeBasic,
    InvalidResourceDataTypeMultiplePipes, ParametrizedResourceUnbound, ResourceNameMissing, ResourceNotATransformer, ResourcesNotFoundError, SourceExhausted, DeletingResourcesNotSupported)


def with_table_name(item: TDataItems, table_name: str) -> DataItemWithMeta:
    """Marks `item` to be dispatched to table `table_name` when yielded from resource function."""
    return DataItemWithMeta(TableNameMeta(table_name), item)


class DltResource(Iterable[TDataItem], DltResourceSchema):

    Empty: ClassVar["DltResource"] = None

    def __init__(
        self,
        pipe: Pipe,
        table_schema_template: TTableSchemaTemplate,
        selected: bool,
        incremental: IncrementalResourceWrapper = None,
        section: str = None
    ) -> None:
        self._name = pipe.name
        self.section = section
        self.selected = selected
        self._pipe = pipe
        self._bound = False
        if incremental and not self.incremental:
            self.add_step(incremental)
        super().__init__(self._name, table_schema_template)

    @classmethod
    def from_data(
        cls,
        data: Any,
        name: str = None,
        section: str = None,
        table_schema_template: TTableSchemaTemplate = None,
        selected: bool = True,
        depends_on: Union["DltResource", Pipe] = None,
        incremental: IncrementalResourceWrapper = None
    ) -> "DltResource":
        if data is None:
            raise InvalidResourceDataTypeIsNone(name, data, NoneType)  # type: ignore

        if isinstance(data, DltResource):
            return data

        if isinstance(data, Pipe):
            return cls(data, table_schema_template, selected, incremental=incremental, section=section)

        if callable(data):
            name = name or get_callable_name(data)

        # if generator, take name from it
        if inspect.isgenerator(data):
            name = name or get_callable_name(data)  # type: ignore

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
        if depends_on is not None:
            DltResource._ensure_valid_transformer_resource(name, data)
            parent_pipe = DltResource._get_parent_pipe(name, depends_on)

        # create resource from iterator, iterable or generator function
        if isinstance(data, (Iterable, Iterator)) or callable(data):
            pipe = Pipe.from_data(name, data, parent=parent_pipe)
            return cls(pipe, table_schema_template, selected, incremental=incremental, section=section)
        else:
            # some other data type that is not supported
            raise InvalidResourceDataType(name, data, type(data), f"The data type is {type(data).__name__}")

    @property
    def name(self) -> str:
        """Resource name inherited from the pipe"""
        return self._name

    @property
    def is_transformer(self) -> bool:
        """Checks if the resource is a transformer that takes data from another resource"""
        return self._pipe.has_parent

    @property
    def requires_binding(self) -> bool:
        """Checks if resource has unbound parameters"""
        try:
            self._pipe.ensure_gen_bound()
            return False
        except (TypeError, ParametrizedResourceUnbound):
            return True

    @property
    def incremental(self) -> IncrementalResourceWrapper:
        """Gets incremental transform if it is in the pipe"""
        incremental: IncrementalResourceWrapper = None
        step_no = self._pipe.find(IncrementalResourceWrapper, Incremental)
        if step_no >= 0:
            incremental = self._pipe.steps[step_no]  # type: ignore
        return incremental

    def pipe_data_from(self, data_from: Union["DltResource", Pipe]) -> None:
        """Replaces the parent in the transformer resource pipe from which the data is piped."""
        if self.is_transformer:
            DltResource._ensure_valid_transformer_resource(self._name, self._pipe.gen)
        else:
            raise ResourceNotATransformer(self._name, "Cannot pipe data into resource that is not a transformer.")
        parent_pipe = self._get_parent_pipe(self._name, data_from)
        self._pipe.parent = parent_pipe

    def add_pipe(self, data: Any) -> None:
        """Creates additional pipe for the resource from the specified data"""
        # TODO: (1) self resource cannot be a transformer (2) if data is resource both self must and it must be selected/unselected + cannot be tranformer
        raise InvalidResourceDataTypeMultiplePipes(self._name, data, type(data))

    def select_tables(self, *table_names: Iterable[str]) -> "DltResource":
        """For resources that dynamically dispatch data to several tables allows to select tables that will receive data, effectively filtering out other data items.

            Both `with_table_name` marker and data-based (function) table name hints are supported.
        """
        def _filter(item: TDataItem, meta: Any = None) -> bool:
            is_in_meta = isinstance(meta, TableNameMeta) and meta.table_name in table_names
            is_in_dyn = self._table_name_hint_fun and self._table_name_hint_fun(item) in table_names
            return is_in_meta or is_in_dyn

        # add filtering function at the end of pipe
        self.add_filter(_filter)
        return self

    def add_map(self, item_map: ItemTransformFunc[TDataItem], insert_at: int = None) -> "DltResource":  # noqa: A003
        """Adds mapping function defined in `item_map` to the resource pipe at position `inserted_at`

        `item_map` receives single data items, `dlt` will enumerate any lists of data items automatically

        Args:
            item_map (ItemTransformFunc[TDataItem]): A function taking a single data item and optional meta argument. Returns transformed data item.
            insert_at (int, optional): At which step in pipe to insert the mapping. Defaults to None which inserts after last step

        Returns:
            "DltResource": returns self
        """
        if insert_at is None:
            self._pipe.append_step(MapItem(item_map))
        else:
            self._pipe.insert_step(MapItem(item_map), insert_at)
        return self

    def add_yield_map(self, item_map: ItemTransformFunc[Iterator[TDataItem]], insert_at: int = None) -> "DltResource":  # noqa: A003
        """Adds generating function defined in `item_map` to the resource pipe at position `inserted_at`

        `item_map` receives single data items, `dlt` will enumerate any lists of data items automatically. It may yield 0 or more data items and be used to
        ie. pivot an item into sequence of rows.

        Args:
            item_map (ItemTransformFunc[Iterator[TDataItem]]): A function taking a single data item and optional meta argument. Yields 0 or more data items.
            insert_at (int, optional): At which step in pipe to insert the generator. Defaults to None which inserts after last step

        Returns:
            "DltResource": returns self
        """
        if insert_at is None:
            self._pipe.append_step(YieldMapItem(item_map))
        else:
            self._pipe.insert_step(YieldMapItem(item_map), insert_at)
        return self

    def add_filter(self, item_filter: ItemTransformFunc[bool], insert_at: int = None) -> "DltResource":  # noqa: A003
        """Adds filter defined in `item_filter` to the resource pipe at position `inserted_at`

        `item_filter` receives single data items, `dlt` will enumerate any lists of data items automatically

        Args:
            item_filter (ItemTransformFunc[bool]): A function taking a single data item and optional meta argument. Returns bool. If True, item is kept
            insert_at (int, optional): At which step in pipe to insert the filter. Defaults to None which inserts after last step
        Returns:
            "DltResource": returns self
        """
        if insert_at is None:
            self._pipe.append_step(FilterItem(item_filter))
        else:
            self._pipe.insert_step(FilterItem(item_filter), insert_at)
        return self

    def add_limit(self, max_items: int) -> "DltResource":  # noqa: A003
        """Adds a limit `max_items` to the resource pipe

        This mutates the encapsulated generator to stop after `max_items` items are yielded. This is useful for testing and debugging. It is
        a no-op for transformers. Those should be limited by their input data.

        Args:
            max_items (int): The maximum number of items to yield
        Returns:
            "DltResource": returns self
        """
        def _gen_wrap(gen: TPipeStep) -> TPipeStep:
            """Wrap a generator to take the first `max_items` records"""
            nonlocal max_items
            count = 0
            if inspect.isfunction(gen):
                gen = gen()
            try:
                for i in gen:  # type: ignore # TODO: help me fix this later
                    yield i
                    count += 1
                    if count == max_items:
                        return
            finally:
                if inspect.isgenerator(gen):
                    gen.close()
            return
        # transformers should be limited by their input, so we only limit non-transformers
        if not self.is_transformer:
            self._pipe.replace_gen(_gen_wrap(self._pipe.gen))
        return self

    def add_step(self, item_transform: ItemTransformFunctionWithMeta[TDataItems], insert_at: int = None) -> "DltResource":  # noqa: A003
        if insert_at is None:
            self._pipe.append_step(item_transform)
        else:
            self._pipe.insert_step(item_transform, insert_at)
        return self

    def set_template(self, table_schema_template: TTableSchemaTemplate) -> None:
        super().set_template(table_schema_template)
        incremental = self.incremental
        # try to late assign incremental
        if table_schema_template.get("incremental") is not None:
            if incremental:
                incremental._incremental = table_schema_template["incremental"]
            else:
                # if there's no wrapper add incremental as a transform
                incremental = table_schema_template["incremental"]  # type: ignore
                self.add_step(incremental)

        if incremental:
            primary_key = table_schema_template.get("primary_key", incremental.primary_key)
            if primary_key is not None:
                incremental.primary_key = primary_key

    def bind(self, *args: Any, **kwargs: Any) -> "DltResource":
        """Binds the parametrized resource to passed arguments. Modifies resource pipe in place. Does not evaluate generators or iterators."""
        if self._bound:
            raise TypeError("Bound DltResource object is not callable")
        gen = self._pipe.bind_gen(*args, **kwargs)
        if isinstance(gen, DltResource):
            # replace resource in place
            old_pipe = self._pipe
            self.__dict__.clear()
            self.__dict__.update(gen.__dict__)
            # keep old pipe instance
            self._pipe = old_pipe
            self._pipe.__dict__.clear()
            # write props from new pipe instance
            self._pipe.__dict__.update(gen._pipe.__dict__)
        elif isinstance(gen, Pipe):
            # just replace pipe
            self._pipe.__dict__.clear()
            # write props from new pipe instance
            self._pipe.__dict__.update(gen.__dict__)
        else:
            self._bound = True
        return self

    @property
    def state(self) -> StrAny:
        """Gets resource-scoped state from the existing pipeline context. If pipeline context is not available, PipelineStateNotAvailable is raised"""
        with self._get_config_section_context():
            return _resource_state(self.name)

    def __call__(self, *args: Any, **kwargs: Any) -> "DltResource":
        """Binds the parametrized resources to passed arguments. Creates and returns a bound resource. Generators and iterators are not evaluated."""
        if self._bound:
            raise TypeError("Bound DltResource object is not callable")
        r = DltResource.from_data(self._pipe._clone(keep_pipe_id=False), self._name, self.section, self._table_schema_template, self.selected, self._pipe.parent)
        return r.bind(*args, **kwargs)

    def __or__(self, transform: Union["DltResource", AnyFun]) -> "DltResource":
        """Allows to pipe data from across resources and transform functions with | operator"""
        # print(f"{resource.name} | {self.name} -> {resource.name}[{resource.is_transformer}]")
        if isinstance(transform, DltResource):
            transform.pipe_data_from(self)
            # return transformed resource for chaining
            return transform
        else:
            # map or yield map
            if inspect.isgeneratorfunction(inspect.unwrap(transform)):
                return self.add_yield_map(transform)
            else:
                return self.add_map(transform)

    def __iter__(self) -> Iterator[TDataItem]:
        """Opens iterator that yields the data items from the resources in the same order as in Pipeline class.

            A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.
        """
        # use the same state dict when opening iterator and when iterator is iterated
        container = Container()
        state, _ = pipeline_state(container, {})

        def _get_context() -> List[ContextManager[Any]]:
            return [
                self._get_config_section_context(),
                Container().injectable_context(StateInjectableContext(state=state))
            ]

        # managed pipe iterator will remove injected contexts when closing
        with multi_context_manager(_get_context()):
            pipe_iterator: ManagedPipeIterator = ManagedPipeIterator.from_pipe(self._pipe)  # type: ignore

        pipe_iterator.set_context_manager(multi_context_manager(_get_context()))
        _iter = map(lambda item: item.item, pipe_iterator)
        return flatten_list_or_items(_iter)

    def _get_config_section_context(self) -> ContextManager[ConfigSectionContext]:
        container = Container()
        proxy = container[PipelineContext]
        pipeline_name = None if not proxy.is_active() else proxy.pipeline().pipeline_name
        return inject_section(ConfigSectionContext(pipeline_name=pipeline_name, sections=(known_sections.SOURCES, self.section or pipeline_name or uniq_id(), self._name)))

    def __str__(self) -> str:
        info = f"DltResource {self._name}"
        if self.section:
            info += f" in section {self.section}:"
        else:
            info += ":"
        if self.is_transformer:
            info += f"\nThis resource is a transformer and takes data items from {self._pipe.parent.name}"
        else:
            if self._pipe.is_data_bound:
                if self.requires_binding:
                    head_sig = inspect.signature(self._pipe.gen)  # type: ignore
                    info += f"\nThis resource is parametrized and takes the following arguments {head_sig}. You must call this resource before loading."
                else:
                    info += "\nIf you want to see the data items in the resource you must iterate it or convert to list ie. list(resource). Note that, like any iterator, you can iterate the resource only once."
            else:
                info += "\nThis resource is not bound to the data"
        info += f"\nInstance: info: (data pipe id:{self._pipe._pipe_id}) at {id(self)}"
        return info

    @staticmethod
    def _ensure_valid_transformer_resource(name: str, data: Any) -> None:
        # resource must be a callable with single argument
        if callable(data):
            valid_code = DltResource.validate_transformer_generator_function(data)
            if valid_code != 0:
                raise InvalidTransformerGeneratorFunction(name, get_callable_name(data), inspect.signature(data), valid_code)
        else:
            raise InvalidTransformerDataTypeGeneratorFunctionRequired(name, data, type(data))

    @staticmethod
    def _get_parent_pipe(name: str, data_from: Union["DltResource", Pipe]) -> Pipe:
        # parent resource
        if isinstance(data_from, Pipe):
            return data_from
        elif isinstance(data_from, DltResource):
            return data_from._pipe
        else:
            # if this is generator function provide nicer exception
            if callable(data_from):
                raise InvalidParentResourceIsAFunction(name, get_callable_name(data_from))
            else:
                raise InvalidParentResourceDataType(name, data_from, type(data_from))

    @staticmethod
    def validate_transformer_generator_function(f: AnyFun) -> int:
        sig = inspect.signature(f)
        if len(sig.parameters) == 0:
            return 1
        # transformer may take only one positional only argument
        pos_only_len = sum(1 for p in sig.parameters.values() if p.kind == p.POSITIONAL_ONLY)
        if pos_only_len > 1:
            return 2
        first_ar = next(iter(sig.parameters.values()))
        # and pos only must be first
        if pos_only_len == 1 and first_ar.kind != first_ar.POSITIONAL_ONLY:
            return 2
        # first arg must be positional or kw_pos
        if first_ar.kind not in (first_ar.POSITIONAL_ONLY, first_ar.POSITIONAL_OR_KEYWORD):
            return 3
        return 0


# produce Empty resource singleton
DltResource.Empty = DltResource(Pipe(None), None, False)
TUnboundDltResource = Callable[[], DltResource]


class DltResourceDict(Dict[str, DltResource]):
    def __init__(self, source_name: str, source_section: str) -> None:
        super().__init__()
        self.source_name = source_name
        self.source_section = source_section
        self._recently_added: List[DltResource] = []
        self._known_pipes: Dict[str, DltResource] = {}

    @property
    def selected(self) -> Dict[str, DltResource]:
        """Returns a subset of all resources that will be extracted and loaded to the destination."""
        return {k:v for k,v in self.items() if v.selected}

    @property
    def extracted(self) -> Dict[str, DltResource]:
        """Returns a dictionary of all resources that will be extracted. That includes selected resources and all their dependencies.
        For dependencies that are not added explicitly to the source, a mock resource object is created that holds the parent pipe and derives the table
        schema from the child resource
        """
        extracted = self.selected
        for resource in self.selected.values():
            while (pipe := resource._pipe.parent) is not None:
                if not pipe.is_empty:
                    try:
                        resource = self.find_by_pipe(pipe)
                    except KeyError:
                        # resource for pipe not found: return mock resource
                        mock_template = DltResourceSchema.new_table_template(pipe.name, write_disposition=resource._table_schema_template.get("write_disposition"))
                        resource = DltResource(pipe, mock_template, False, section=resource.section)
                    extracted[resource._name] = resource
                else:
                    break
        return extracted

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
            if name not in self:
                # if any key is missing, display the full info
                raise ResourcesNotFoundError(self.source_name, set(self.keys()), set(resource_names))
        # set the selected flags
        for resource in self.values():
            self[resource._name].selected = resource._name in resource_names
        return self.selected

    def find_by_pipe(self, pipe: Pipe) -> DltResource:
        # TODO: many resources may share the same pipe so return a list and also filter the resources by self._enabled_resource_names
        # identify pipes by _pipe_id
        if pipe._pipe_id in self._known_pipes:
            return self._known_pipes[pipe._pipe_id]
        try:
            return self._known_pipes.setdefault(pipe._pipe_id, next(r for r in self.values() if r._pipe._pipe_id == pipe._pipe_id))
        except StopIteration:
            raise KeyError(pipe)

    def clone_new_pipes(self) -> None:
        cloned_pipes = ManagedPipeIterator.clone_pipes([r._pipe for r in self.values() if r in self._recently_added])
        # replace pipes in resources, the cloned_pipes preserve parent connections
        for cloned in cloned_pipes:
            self.find_by_pipe(cloned)._pipe = cloned
        self._recently_added.clear()

    def __setitem__(self, resource_name: str, resource: DltResource) -> None:
        # make shallow copy of the resource
        resource = copy(resource)
        resource.section = self.source_section
        # now set it in dict
        self._recently_added.append(resource)
        return super().__setitem__(resource_name, resource)

    def __delitem__(self, resource_name: str) -> None:
        raise DeletingResourcesNotSupported(self.source_name, resource_name)


class DltSource(Iterable[TDataItem]):
    """Groups several `dlt resources` under a single schema and allows to perform operations on them.

    ### Summary
    The instance of this class is created whenever you call the `dlt.source` decorated function. It automates several functions for you:
    * You can pass this instance to `dlt` `run` method in order to load all data present in the `dlt resources`.
    * You can select and deselect resources that you want to load via `with_resources` method
    * You can access the resources (which are `DltResource` instances) as source attributes
    * It implements `Iterable` interface so you can get all the data from the resources yourself and without dlt pipeline present.
    * You can get the `schema` for the source and all the resources within it.
    * You can use a `run` method to load the data with a default instance of dlt pipeline.
    """
    def __init__(self, name: str, section: str, schema: Schema, resources: Sequence[DltResource] = None) -> None:
        self.name = name
        self.section = section
        self.exhausted = False
        """Tells if iterator associated with a source is exhausted"""
        self._schema = schema
        self._resources: DltResourceDict = DltResourceDict(self.name, self.section)
        if resources:
            for resource in resources:
                self._add_resource(resource._name, resource)
            self._resources.clone_new_pipes()

    @classmethod
    def from_data(cls, name: str, section: str, schema: Schema, data: Any) -> "DltSource":
        """Converts any `data` supported by `dlt` `run` method into `dlt source` with a name `section`.`name` and `schema` schema."""
        # creates source from various forms of data
        if isinstance(data, DltSource):
            return data

        # in case of sequence, enumerate items and convert them into resources
        if isinstance(data, Sequence):
            resources = [DltResource.from_data(i) for i in data]
        else:
            resources = [DltResource.from_data(data)]

        return cls(name, section, schema, resources)

    # TODO: 4 properties below must go somewhere else ie. into RelationalSchema which is Schema + Relational normalizer.

    @property
    def max_table_nesting(self) -> int:
        """A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON."""
        return RelationalNormalizer.get_normalizer_config(self._schema).get("max_nesting")

    @max_table_nesting.setter
    def max_table_nesting(self, value: int) -> None:
        RelationalNormalizer.update_normalizer_config(self._schema, {"max_nesting": value})

    @property
    def root_key(self) -> bool:
        """Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge"""
        config = RelationalNormalizer.get_normalizer_config(self._schema).get("propagation")
        return config is not None and "root" in config and "_dlt_id" in config["root"] and config["root"]["_dlt_id"] == "_dlt_root_id"


    @root_key.setter
    def root_key(self, value: bool) -> None:
        if value is True:
            propagation_config: RelationalNormalizerConfigPropagation = {
                "root": {
                    "_dlt_id": TColumnName("_dlt_root_id")
                },
                "tables": {}
            }
            RelationalNormalizer.update_normalizer_config(self._schema, {"propagation": propagation_config})
        else:
            if self.root_key:
                propagation_config = RelationalNormalizer.get_normalizer_config(self._schema)["propagation"]
                propagation_config["root"].pop("_dlt_id")  # type: ignore

    @property
    def resources(self) -> DltResourceDict:
        """A dictionary of all resources present in the source, where the key is a resource name."""
        return self._resources

    @property
    def selected_resources(self) -> Dict[str, DltResource]:
        """A dictionary of all the resources that are selected to be loaded."""
        return self._resources.selected

    @property
    def schema(self) -> Schema:
        return self._schema

    @schema.setter
    def schema(self, value: Schema) -> None:
        self._schema = value

    def discover_schema(self, item: TDataItem = None) -> Schema:
        """Computes table schemas for all selected resources in the source and merges them with a copy of current source schema. If `item` is provided,
        dynamic tables will be evaluated, otherwise those tables will be ignored."""
        schema = self._schema.clone()
        for r in self.selected_resources.values():
            # names must be normalized here
            with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                partial_table = self._schema.normalize_table_identifiers(r.table_schema(item))
                schema.update_schema(partial_table)
        return schema

    def with_resources(self, *resource_names: str) -> "DltSource":
        """A convenience method to select one of more resources to be loaded. Returns a source with the specified resources selected."""
        self._resources.select(*resource_names)
        return self


    def add_limit(self, max_items: int) -> "DltSource":  # noqa: A003
        """Adds a limit `max_items` yielded from all selected resources in the source that are not transformers.

        This is useful for testing, debugging and generating sample datasets for experimentation. You can easily get your test dataset in a few minutes, when otherwise
        you'd need to wait hours for the full loading to complete.

        Args:
            max_items (int): The maximum number of items to yield
        Returns:
            "DltSource": returns self
        """
        for resource in self.resources.selected.values():
            resource.add_limit(max_items)
        return self

    @property
    def run(self) -> SupportsPipelineRun:
        """A convenience method that will call `run` run on the currently active `dlt` pipeline. If pipeline instance is not found, one with default settings will be created."""
        self_run: SupportsPipelineRun = makefun.partial(Container()[PipelineContext].pipeline().run, *(), data=self)
        return self_run

    @property
    def state(self) -> StrAny:
        """Gets source-scoped state from the existing pipeline context. If pipeline context is not available, PipelineStateNotAvailable is raised"""
        with self._get_config_section_context():
            return source_state()

    def __iter__(self) -> Iterator[TDataItem]:
        """Opens iterator that yields the data items from all the resources within the source in the same order as in Pipeline class.

            A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.

            A source config section is injected to allow secrets/config injection as during regular extraction.
        """
        # use the same state dict when opening iterator and when iterator is iterated
        mock_state, _ = pipeline_state(Container(), {})

        def _get_context() -> List[ContextManager[Any]]:
            return [
                self._get_config_section_context(),
                Container().injectable_context(StateInjectableContext(state=mock_state))
            ]

        # managed pipe iterator will remove injected contexts when closing
        with multi_context_manager(_get_context()):
            pipe_iterator: ManagedPipeIterator = ManagedPipeIterator.from_pipes(self._resources.selected_pipes)  # type: ignore
        pipe_iterator.set_context_manager(multi_context_manager(_get_context()))
        _iter = map(lambda item: item.item, pipe_iterator)
        self.exhausted = True
        return flatten_list_or_items(_iter)

    def _get_config_section_context(self) -> ContextManager[ConfigSectionContext]:
        proxy = Container()[PipelineContext]
        pipeline_name = None if not proxy.is_active() else proxy.pipeline().pipeline_name
        return inject_section(ConfigSectionContext(pipeline_name=pipeline_name, sections=(known_sections.SOURCES, self.section, self.name)))

    def _add_resource(self, name: str, resource: DltResource) -> None:
        if self.exhausted:
            raise SourceExhausted(self.name)

        if name in self._resources:
            # for resources with the same name try to add the resource as an another pipe
            self._resources[name].add_pipe(resource)
        else:
            self._resources[name] = resource
            # remember that resource got cloned when set into dict
            super().__setattr__(name, self._resources[name])

    def __getattr__(self, resource_name: str) -> DltResource:
        return self._resources[resource_name]

    def __setattr__(self, name: str, value: Any) -> None:
        if isinstance(value, DltResource):
            # TODO: refactor adding resources. 1. resource dict should be read only 2. we should correct the parent pipes after cloning 3. allow replacing existing resources
            self._add_resource(name, value)
        else:
            super().__setattr__(name, value)

    def __str__(self) -> str:
        info = f"DltSource {self.name} section {self.section} contains {len(self.resources)} resource(s) of which {len(self.selected_resources)} are selected"
        for r in self.resources.values():
            selected_info = "selected" if r.selected else "not selected"
            if r.is_transformer:
                info += f"\ntransformer {r._name} is {selected_info} and takes data from {r._pipe.parent.name}"
            else:
                info += f"\nresource {r._name} is {selected_info}"
        if self.exhausted:
            info += "\nSource is already iterated and cannot be used again ie. to display or load data."
        else:
            info += "\nIf you want to see the data items in this source you must iterate it or convert to list ie. list(source)."
        info += " Note that, like any iterator, you can iterate the source only once."
        info += f"\ninstance id: {id(self)}"
        return info
