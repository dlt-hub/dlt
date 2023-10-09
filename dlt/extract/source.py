import warnings
import contextlib
from copy import copy, deepcopy
import makefun
import inspect
from typing import AsyncIterable, AsyncIterator, ClassVar, Callable, ContextManager, Dict, Iterable, Iterator, List, Sequence, Tuple, Union, Any, Optional
import types

from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer, RelationalNormalizerConfigPropagation
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnName
from dlt.common.typing import AnyFun, DictStrAny, StrAny, TDataItem, TDataItems, NoneType
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext, StateInjectableContext, SupportsPipelineRun, resource_state, source_state, pipeline_state
from dlt.common.utils import graph_find_scc_nodes, flatten_list_or_items, get_callable_name, graph_edges_to_nodes, multi_context_manager, uniq_id

from dlt.extract.typing import (DataItemWithMeta, ItemTransformFunc, ItemTransformFunctionWithMeta, TDecompositionStrategy, TableNameMeta,
                                FilterItem, MapItem, YieldMapItem, ValidateItem)
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
    """Implements dlt resource. Contains a data pipe that wraps a generating item and table schema that can be adjusted"""
    Empty: ClassVar["DltResource"] = None
    source_name: str
    """Name of the source that contains this instance of the source, set when added to DltResourcesDict"""
    section: str
    """A config section name"""

    def __init__(
        self,
        pipe: Pipe,
        table_schema_template: TTableSchemaTemplate,
        selected: bool,
        incremental: IncrementalResourceWrapper = None,
        section: str = None,
        args_bound: bool = False
    ) -> None:
        self.section = section
        self.selected = selected
        self._pipe = pipe
        self._args_bound = args_bound
        self._explicit_args: DictStrAny = None
        if incremental and not self.incremental:
            self.add_step(incremental)
        self.source_name = None
        super().__init__(table_schema_template)

    @classmethod
    def from_data(
        cls,
        data: Any,
        name: str = None,
        section: str = None,
        table_schema_template: TTableSchemaTemplate = None,
        selected: bool = True,
        data_from: Union["DltResource", Pipe] = None,
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
        if data_from is not None:
            DltResource._ensure_valid_transformer_resource(name, data)
            parent_pipe = DltResource._get_parent_pipe(name, data_from)

        # create resource from iterator, iterable or generator function
        if isinstance(data, (Iterable, Iterator)) or callable(data):
            pipe = Pipe.from_data(name, data, parent=parent_pipe)
            return cls(pipe, table_schema_template, selected, incremental=incremental, section=section, args_bound=not callable(data))
        else:
            # some other data type that is not supported
            raise InvalidResourceDataType(name, data, type(data), f"The data type is {type(data).__name__}")

    @property
    def name(self) -> str:
        """Resource name inherited from the pipe"""
        return self._pipe.name

    def with_name(self, new_name: str) -> "DltResource":
        """Clones the resource with a new name. Such resource keeps separate state and loads data to `new_name` table by default."""
        return self._clone(new_name=new_name, with_parent=True)

    @property
    def is_transformer(self) -> bool:
        """Checks if the resource is a transformer that takes data from another resource"""
        return self._pipe.has_parent

    @property
    def requires_args(self) -> bool:
        """Checks if resource has unbound arguments"""
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

    @property
    def validator(self) -> Optional[ValidateItem]:
        """Gets validator transform if it is in the pipe"""
        validator: ValidateItem = None
        step_no = self._pipe.find(ValidateItem)
        if step_no >= 0:
            validator = self._pipe.steps[step_no]  # type: ignore[assignment]
        return validator

    @validator.setter
    def validator(self, validator: Optional[ValidateItem]) -> None:
        """Add/remove or replace the validator in pipe"""
        step_no = self._pipe.find(ValidateItem)
        if step_no >= 0:
            self._pipe.remove_step(step_no)
        if validator:
            self.add_step(validator, insert_at=step_no if step_no >= 0 else None)

    def pipe_data_from(self, data_from: Union["DltResource", Pipe]) -> None:
        """Replaces the parent in the transformer resource pipe from which the data is piped."""
        if self.is_transformer:
            DltResource._ensure_valid_transformer_resource(self.name, self._pipe.gen)
        else:
            raise ResourceNotATransformer(self.name, "Cannot pipe data into resource that is not a transformer.")
        parent_pipe = self._get_parent_pipe(self.name, data_from)
        self._pipe.parent = parent_pipe

    def add_pipe(self, data: Any) -> None:
        """Creates additional pipe for the resource from the specified data"""
        # TODO: (1) self resource cannot be a transformer (2) if data is resource both self must and it must be selected/unselected + cannot be tranformer
        raise InvalidResourceDataTypeMultiplePipes(self.name, data, type(data))

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

        if table_schema_template.get('validator') is not None:
            self.validator = table_schema_template['validator']

    def bind(self, *args: Any, **kwargs: Any) -> "DltResource":
        """Binds the parametrized resource to passed arguments. Modifies resource pipe in place. Does not evaluate generators or iterators."""
        if self._args_bound:
            raise TypeError(f"Parametrized resource {self.name} is not callable")
        orig_gen = self._pipe.gen
        gen = self._pipe.bind_gen(*args, **kwargs)
        if isinstance(gen, DltResource):
            # the resource returned resource: update in place
            old_pipe = self._pipe
            self.__dict__.clear()
            self.__dict__.update(gen.__dict__)
            # keep old pipe instance
            self._pipe = old_pipe
            self._pipe.__dict__.clear()
            # write props from new pipe instance
            self._pipe.__dict__.update(gen._pipe.__dict__)
        elif isinstance(gen, Pipe):
            # the resource returned pipe: just replace pipe
            self._pipe.__dict__.clear()
            # write props from new pipe instance
            self._pipe.__dict__.update(gen.__dict__)
        else:
            self._args_bound = True
        self._set_explicit_args(orig_gen, None, *args, **kwargs)  # type: ignore
        return self

    @property
    def explicit_args(self) -> StrAny:
        """Returns a dictionary of arguments used to parametrize the resource. Does not include defaults and injected args."""
        if not self._args_bound:
            raise TypeError(f"Resource {self.name} is not yet parametrized")
        return self._explicit_args

    @property
    def state(self) -> StrAny:
        """Gets resource-scoped state from the active pipeline. PipelineStateNotAvailable is raised if pipeline context is not available"""
        with inject_section(self._get_config_section_context()):
            return resource_state(self.name)

    def __call__(self, *args: Any, **kwargs: Any) -> "DltResource":
        """Binds the parametrized resources to passed arguments. Creates and returns a bound resource. Generators and iterators are not evaluated."""
        if self._args_bound:
            raise TypeError(f"Parametrized resource {self.name} is not callable")
        r = self._clone()
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
        state_context = StateInjectableContext(state=state)
        section_context = self._get_config_section_context()

        # managed pipe iterator will set the context on each call to  __next__
        with inject_section(section_context), Container().injectable_context(state_context):
            pipe_iterator: ManagedPipeIterator = ManagedPipeIterator.from_pipes([self._pipe])  # type: ignore

        pipe_iterator.set_context([state_context, section_context])
        _iter = map(lambda item: item.item, pipe_iterator)
        return flatten_list_or_items(_iter)

    def _set_explicit_args(self, f: AnyFun, sig: inspect.Signature = None, *args: Any, **kwargs: Any) -> None:
        try:
            sig = sig or inspect.signature(f)
            self._explicit_args = sig.bind_partial(*args, **kwargs).arguments
        except Exception:
            pass

    def _clone(self, new_name: str = None, with_parent: bool = False) -> "DltResource":
        """Creates a deep copy of a current resource, optionally renaming the resource. The clone will not be part of the source
        """
        pipe = self._pipe
        if self._pipe and not self._pipe.is_empty:
            pipe = pipe._clone(new_name=new_name, with_parent=with_parent)
        # incremental and parent are already in the pipe (if any)
        return DltResource(
            pipe,
            deepcopy(self._table_schema_template),
            selected=self.selected,
            section=self.section
        )

    def _get_config_section_context(self) -> ConfigSectionContext:
        container = Container()
        proxy = container[PipelineContext]
        pipeline = None if not proxy.is_active() else proxy.pipeline()
        if pipeline:
            pipeline_name = pipeline.pipeline_name
        else:
            pipeline_name = None
        if pipeline:
            default_schema_name = pipeline.default_schema_name
        else:
            default_schema_name = None
        if not default_schema_name and pipeline_name:
            default_schema_name = pipeline._make_schema_with_default_name().name
        return ConfigSectionContext(
            pipeline_name=pipeline_name,
            # do not emit middle config section to not overwrite the resource section
            # only sources emit middle config section
            sections=(known_sections.SOURCES, "", self.source_name or default_schema_name or self.name),
            source_state_key=self.source_name or default_schema_name or self.section or uniq_id()
        )

    def __str__(self) -> str:
        info = f"DltResource [{self.name}]"
        if self.section:
            info += f" in section [{self.section}]"
        if self.source_name:
            info += f" added to source [{self.source_name}]:"
        else:
            info += ":"

        if self.is_transformer:
            info += f"\nThis resource is a transformer and takes data items from {self._pipe.parent.name}"
        else:
            if self._pipe.is_data_bound:
                if self.requires_args:
                    head_sig = inspect.signature(self._pipe.gen)  # type: ignore
                    info += f"\nThis resource is parametrized and takes the following arguments {head_sig}. You must call this resource before loading."
                else:
                    info += "\nIf you want to see the data items in the resource you must iterate it or convert to list ie. list(resource). Note that, like any iterator, you can iterate the resource only once."
            else:
                info += "\nThis resource is not bound to the data"
        info += f"\nInstance: info: (data pipe id:{id(self._pipe)}) at {id(self)}"
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
TUnboundDltResource = Callable[..., DltResource]


class DltResourceDict(Dict[str, DltResource]):
    def __init__(self, source_name: str, source_section: str) -> None:
        super().__init__()
        self.source_name = source_name
        self.source_section = source_section
        self._suppress_clone_on_setitem = False
        # pipes not yet cloned in __setitem__
        self._new_pipes: List[Pipe] = []
        # pipes already cloned by __setitem__ id(original Pipe):cloned(Pipe)
        self._cloned_pairs: Dict[int, Pipe] = {}

    @property
    def selected(self) -> Dict[str, DltResource]:
        """Returns a subset of all resources that will be extracted and loaded to the destination."""
        return {k:v for k,v in self.items() if v.selected}

    @property
    def extracted(self) -> Dict[str, DltResource]:
        """Returns a dictionary of all resources that will be extracted. That includes selected resources and all their parents.
        For parents that are not added explicitly to the source, a mock resource object is created that holds the parent pipe and derives the table
        schema from the child resource
        """
        extracted = self.selected
        for resource in self.selected.values():
            while (pipe := resource._pipe.parent) is not None:
                if not pipe.is_empty:
                    try:
                        resource = self[pipe.name]
                    except KeyError:
                        # resource for pipe not found: return mock resource
                        mock_template = DltResourceSchema.new_table_template(
                            pipe.name,
                            write_disposition=resource.write_disposition
                        )
                        resource = DltResource(pipe, mock_template, False, section=resource.section)
                        resource.source_name = resource.source_name
                    extracted[resource.name] = resource
                else:
                    break
        return extracted

    @property
    def selected_dag(self) -> List[Tuple[str, str]]:
        """Returns a list of edges of directed acyclic graph of pipes and their parents in selected resources"""
        dag: List[Tuple[str, str]] = []
        for pipe in self.selected_pipes:
            selected = pipe
            parent: Pipe = None
            while (parent := pipe.parent) is not None:
                if not parent.is_empty:
                    dag.append((pipe.parent.name, pipe.name))
                    pipe = parent
                else:
                    # do not descend into disconnected pipes
                    break
            if selected is pipe:
                # add isolated element
                dag.append((pipe.name, pipe.name))
        return dag

    @property
    def pipes(self) -> List[Pipe]:
        return [r._pipe for r in self.values()]

    @property
    def selected_pipes(self) -> Sequence[Pipe]:
        return [r._pipe for r in self.values() if r.selected]

    def select(self, *resource_names: str) -> Dict[str, DltResource]:
        # checks if keys are present
        for name in resource_names:
            if name not in self:
                # if any key is missing, display the full info
                raise ResourcesNotFoundError(self.source_name, set(self.keys()), set(resource_names))
        # set the selected flags
        for resource in self.values():
            self[resource.name].selected = resource.name in resource_names
        return self.selected

    def add(self, *resources: DltResource) -> None:
        try:
            # temporarily block cloning when single resource is added
            self._suppress_clone_on_setitem = True
            for resource in resources:
                if resource.name in self:
                    # for resources with the same name try to add the resource as an another pipe
                    self[resource.name].add_pipe(resource)
                else:
                    self[resource.name] = resource
        finally:
            self._suppress_clone_on_setitem = False
        self._clone_new_pipes([r.name for r in resources])

    def _clone_new_pipes(self, resource_names: Sequence[str]) -> None:
        # clone all new pipes and keep
        _, self._cloned_pairs = ManagedPipeIterator.clone_pipes(self._new_pipes, self._cloned_pairs)
        # self._cloned_pairs.update(cloned_pairs)
        # replace pipes in resources, the cloned_pipes preserve parent connections
        for name in resource_names:
            resource = self[name]
            pipe_id = id(resource._pipe)
            if pipe_id in self._cloned_pairs:
                resource._pipe = self._cloned_pairs[pipe_id]
        self._new_pipes.clear()

    def __setitem__(self, resource_name: str, resource: DltResource) -> None:
        if resource_name != resource.name:
            raise ValueError(f"The index name {resource_name} does not correspond to resource name {resource.name}")
        pipe_id = id(resource._pipe)
        # make shallow copy of the resource
        resource = copy(resource)
        # resource.section = self.source_section
        resource.source_name = self.source_name
        if pipe_id in self._cloned_pairs:
            # if resource_name in self:
            #     raise ValueError(f"Resource with name {resource_name} and pipe id {id(pipe_id)} is already present in the source. "
            #                      "Modify the resource pipe directly instead of setting a possibly modified instance.")
            # TODO: instead of replacing pipe with existing one we should clone and replace the existing one in all resources that have it
            resource._pipe = self._cloned_pairs[pipe_id]
        else:
            self._new_pipes.append(resource._pipe)
        # now set it in dict
        super().__setitem__(resource_name, resource)
        # immediately clone pipe if not suppressed
        if not self._suppress_clone_on_setitem:
            self._clone_new_pipes([resource.name])

    def __delitem__(self, resource_name: str) -> None:
        raise DeletingResourcesNotSupported(self.source_name, resource_name)


class DltSource(Iterable[TDataItem]):
    """Groups several `dlt resources` under a single schema and allows to perform operations on them.

    The instance of this class is created whenever you call the `dlt.source` decorated function. It automates several functions for you:
    * You can pass this instance to `dlt` `run` method in order to load all data present in the `dlt resources`.
    * You can select and deselect resources that you want to load via `with_resources` method
    * You can access the resources (which are `DltResource` instances) as source attributes
    * It implements `Iterable` interface so you can get all the data from the resources yourself and without dlt pipeline present.
    * You can get the `schema` for the source and all the resources within it.
    * You can use a `run` method to load the data with a default instance of dlt pipeline.
    * You can get source read only state for the currently active Pipeline instance
    """
    def __init__(self, name: str, section: str, schema: Schema, resources: Sequence[DltResource] = None) -> None:
        self.name = name
        self.section = section
        """Tells if iterator associated with a source is exhausted"""
        self._schema = schema
        self._resources: DltResourceDict = DltResourceDict(self.name, self.section)

        if self.name != schema.name:
            # raise ValueError(f"Schema name {schema.name} differs from source name {name}! The explicit source name argument is deprecated and will be soon removed.")
            warnings.warn(f"Schema name {schema.name} differs from source name {name}! The explicit source name argument is deprecated and will be soon removed.")

        if resources:
            self.resources.add(*resources)

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
    def exhausted(self) -> bool:
        """check all selected pipes wether one of them has started. if so, the source is exhausted."""
        for resource in self._resources.extracted.values():
            item = resource._pipe.gen
            if inspect.isgenerator(item):
                if inspect.getgeneratorstate(item) != "GEN_CREATED":
                    return True
        return False

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
        schema = self._schema.clone(update_normalizers=True)
        for r in self.selected_resources.values():
            # names must be normalized here
            with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                partial_table = self._schema.normalize_table_identifiers(
                    r.compute_table_schema(item)
                )
                schema.update_schema(partial_table)
        return schema

    def with_resources(self, *resource_names: str) -> "DltSource":
        """A convenience method to select one of more resources to be loaded. Returns a clone of the original source with the specified resources selected."""
        source = self.clone()
        source._resources.select(*resource_names)
        return source

    def decompose(self, strategy: TDecompositionStrategy) -> List["DltSource"]:
        """Decomposes source into a list of sources with a given strategy.

            "none" will return source as is
            "scc" will decompose the dag of selected pipes and their parent into strongly connected components
        """
        if strategy == "none":
            return [self]
        elif strategy == "scc":
            dag = self.resources.selected_dag
            scc = graph_find_scc_nodes(graph_edges_to_nodes(dag, directed=False))
            # components contain elements that are not currently selected
            selected_set = set(self.resources.selected.keys())
            return [self.with_resources(*component.intersection(selected_set)) for component in scc]
        else:
            raise ValueError(strategy)

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
        """Gets source-scoped state from the active pipeline. PipelineStateNotAvailable is raised if no pipeline is active"""
        with inject_section(self._get_config_section_context()):
            return source_state()

    def clone(self) -> "DltSource":
        """Creates a deep copy of the source where copies of schema, resources and pipes are created"""
        # mind that resources and pipes are cloned when added to the DltResourcesDict in the source constructor
        return DltSource(self.name, self.section, self.schema.clone(), list(self._resources.values()))

    def __iter__(self) -> Iterator[TDataItem]:
        """Opens iterator that yields the data items from all the resources within the source in the same order as in Pipeline class.

            A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.

            A source config section is injected to allow secrets/config injection as during regular extraction.
        """
        # use the same state dict when opening iterator and when iterator is iterated
        mock_state, _ = pipeline_state(Container(), {})
        state_context = StateInjectableContext(state=mock_state)
        section_context = self._get_config_section_context()

        # managed pipe iterator will set the context on each call to  __next__
        with inject_section(section_context), Container().injectable_context(state_context):
            pipe_iterator: ManagedPipeIterator = ManagedPipeIterator.from_pipes(self._resources.selected_pipes)  # type: ignore
        pipe_iterator.set_context([section_context, state_context])
        _iter = map(lambda item: item.item, pipe_iterator)
        return flatten_list_or_items(_iter)

    def _get_config_section_context(self) -> ConfigSectionContext:
        proxy = Container()[PipelineContext]
        pipeline_name = None if not proxy.is_active() else proxy.pipeline().pipeline_name
        return ConfigSectionContext(
            pipeline_name=pipeline_name,
            sections=(known_sections.SOURCES, self.section, self.name),
            source_state_key=self.name
        )

    def __getattr__(self, resource_name: str) -> DltResource:
        try:
            return self._resources[resource_name]
        except KeyError:
            raise AttributeError(f"Resource with name {resource_name} not found in source {self.name}")

    def __setattr__(self, name: str, value: Any) -> None:
        if isinstance(value, DltResource):
            self.resources[name] = value
        else:
            super().__setattr__(name, value)

    def __str__(self) -> str:
        info = f"DltSource {self.name} section {self.section} contains {len(self.resources)} resource(s) of which {len(self.selected_resources)} are selected"
        for r in self.resources.values():
            selected_info = "selected" if r.selected else "not selected"
            if r.is_transformer:
                info += f"\ntransformer {r.name} is {selected_info} and takes data from {r._pipe.parent.name}"
            else:
                info += f"\nresource {r.name} is {selected_info}"
        if self.exhausted:
            info += "\nSource is already iterated and cannot be used again ie. to display or load data."
        else:
            info += "\nIf you want to see the data items in this source you must iterate it or convert to list ie. list(source)."
        info += " Note that, like any iterator, you can iterate the source only once."
        info += f"\ninstance id: {id(self)}"
        return info
