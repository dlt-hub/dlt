from copy import deepcopy
import inspect
from typing import (
    AsyncIterable,
    AsyncIterator,
    ClassVar,
    Callable,
    Iterable,
    Iterator,
    Union,
    Any,
    Optional,
)

from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.typing import AnyFun, DictStrAny, StrAny, TDataItem, TDataItems, NoneType
from dlt.common.configuration.container import Container
from dlt.common.pipeline import (
    PipelineContext,
    StateInjectableContext,
    resource_state,
    pipeline_state,
)
from dlt.common.utils import flatten_list_or_items, get_callable_name, uniq_id

from dlt.extract.typing import (
    DataItemWithMeta,
    ItemTransformFunc,
    ItemTransformFunctionWithMeta,
    TableNameMeta,
    FilterItem,
    MapItem,
    YieldMapItem,
    ValidateItem,
)
from dlt.extract.pipe import Pipe, ManagedPipeIterator, TPipeStep
from dlt.extract.hints import DltResourceHints, TResourceHints
from dlt.extract.incremental import Incremental, IncrementalResourceWrapper
from dlt.extract.exceptions import (
    InvalidTransformerDataTypeGeneratorFunctionRequired,
    InvalidParentResourceDataType,
    InvalidParentResourceIsAFunction,
    InvalidResourceDataType,
    InvalidResourceDataTypeIsNone,
    InvalidTransformerGeneratorFunction,
    InvalidResourceDataTypeAsync,
    InvalidResourceDataTypeBasic,
    InvalidResourceDataTypeMultiplePipes,
    ParametrizedResourceUnbound,
    ResourceNameMissing,
    ResourceNotATransformer,
)
from dlt.extract.wrappers import wrap_additional_type


def with_table_name(item: TDataItems, table_name: str) -> DataItemWithMeta:
    """Marks `item` to be dispatched to table `table_name` when yielded from resource function."""
    return DataItemWithMeta(TableNameMeta(table_name), item)


class DltResource(Iterable[TDataItem], DltResourceHints):
    """Implements dlt resource. Contains a data pipe that wraps a generating item and table schema that can be adjusted"""

    Empty: ClassVar["DltResource"] = None
    source_name: str
    """Name of the source that contains this instance of the source, set when added to DltResourcesDict"""
    section: str
    """A config section name"""

    def __init__(
        self,
        pipe: Pipe,
        hints: TResourceHints,
        selected: bool,
        incremental: IncrementalResourceWrapper = None,
        section: str = None,
        args_bound: bool = False,
    ) -> None:
        self.section = section
        self.selected = selected
        self._pipe = pipe
        self._args_bound = args_bound
        self._explicit_args: DictStrAny = None
        if incremental and not self.incremental:
            self.add_step(incremental)
        self.source_name = None
        super().__init__(hints)

    @classmethod
    def from_data(
        cls,
        data: Any,
        name: str = None,
        section: str = None,
        hints: TResourceHints = None,
        selected: bool = True,
        data_from: Union["DltResource", Pipe] = None,
        incremental: IncrementalResourceWrapper = None,
    ) -> "DltResource":
        if data is None:
            raise InvalidResourceDataTypeIsNone(name, data, NoneType)  # type: ignore

        if isinstance(data, DltResource):
            return data

        if isinstance(data, Pipe):
            return cls(data, hints, selected, incremental=incremental, section=section)

        if callable(data):
            name = name or get_callable_name(data)

        # if generator, take name from it
        if inspect.isgenerator(data):
            name = name or get_callable_name(data)  # type: ignore

        # name is mandatory
        if not name:
            raise ResourceNameMissing()

        # wrap additional types
        data = wrap_additional_type(data)

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
            return cls(
                pipe,
                hints,
                selected,
                incremental=incremental,
                section=section,
                args_bound=not callable(data),
            )
        else:
            # some other data type that is not supported
            raise InvalidResourceDataType(
                name, data, type(data), f"The data type of supplied type is {type(data).__name__}"
            )

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
            raise ResourceNotATransformer(
                self.name, "Cannot pipe data into resource that is not a transformer."
            )
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

    def add_map(
        self, item_map: ItemTransformFunc[TDataItem], insert_at: int = None
    ) -> "DltResource":  # noqa: A003
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

    def add_yield_map(
        self, item_map: ItemTransformFunc[Iterator[TDataItem]], insert_at: int = None
    ) -> "DltResource":  # noqa: A003
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

    def add_filter(
        self, item_filter: ItemTransformFunc[bool], insert_at: int = None
    ) -> "DltResource":  # noqa: A003
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

    def add_step(
        self, item_transform: ItemTransformFunctionWithMeta[TDataItems], insert_at: int = None
    ) -> "DltResource":  # noqa: A003
        if insert_at is None:
            self._pipe.append_step(item_transform)
        else:
            self._pipe.insert_step(item_transform, insert_at)
        return self

    def set_hints(self, table_schema_template: TResourceHints) -> None:
        super().set_hints(table_schema_template)
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

        if table_schema_template.get("validator") is not None:
            self.validator = table_schema_template["validator"]

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

    def _set_explicit_args(
        self, f: AnyFun, sig: inspect.Signature = None, *args: Any, **kwargs: Any
    ) -> None:
        try:
            sig = sig or inspect.signature(f)
            self._explicit_args = sig.bind_partial(*args, **kwargs).arguments
        except Exception:
            pass

    def _clone(self, new_name: str = None, with_parent: bool = False) -> "DltResource":
        """Creates a deep copy of a current resource, optionally renaming the resource. The clone will not be part of the source"""
        pipe = self._pipe
        if self._pipe and not self._pipe.is_empty:
            pipe = pipe._clone(new_name=new_name, with_parent=with_parent)
        # incremental and parent are already in the pipe (if any)
        return DltResource(
            pipe,
            deepcopy(self._hints),
            selected=self.selected,
            section=self.section,
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
            sections=(
                known_sections.SOURCES,
                "",
                self.source_name or default_schema_name or self.name,
            ),
            source_state_key=self.source_name or default_schema_name or self.section or uniq_id(),
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
            info += (
                "\nThis resource is a transformer and takes data items from"
                f" {self._pipe.parent.name}"
            )
        else:
            if self._pipe.is_data_bound:
                if self.requires_args:
                    head_sig = inspect.signature(self._pipe.gen)  # type: ignore
                    info += (
                        "\nThis resource is parametrized and takes the following arguments"
                        f" {head_sig}. You must call this resource before loading."
                    )
                else:
                    info += (
                        "\nIf you want to see the data items in the resource you must iterate it or"
                        " convert to list ie. list(resource). Note that, like any iterator, you can"
                        " iterate the resource only once."
                    )
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
                raise InvalidTransformerGeneratorFunction(
                    name, get_callable_name(data), inspect.signature(data), valid_code
                )
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
