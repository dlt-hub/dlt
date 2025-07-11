from functools import update_wrapper
import inspect
from typing import (
    AsyncIterable,
    cast,
    ClassVar,
    Callable,
    Iterable,
    Iterator,
    Type,
    Union,
    Any,
    Optional,
    Mapping,
    List,
    Tuple,
)

from dlt.common import logger
from dlt.common.configuration.inject import get_fun_spec, with_config
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import BaseConfiguration, known_sections
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.reflection.inspect import isgeneratorfunction
from dlt.common.typing import (
    AnyFun,
    DictStrAny,
    StrAny,
    TDataItem,
    TDataItems,
    NoneType,
    Self,
    TypeVar,
)
from dlt.common.configuration.container import Container
from dlt.common.pipeline import (
    PipelineContext,
    StateInjectableContext,
    pipeline_state,
)
from dlt.common.utils import (
    flatten_list_or_items,
    get_callable_name,
    uniq_id,
    without_none,
    simple_repr,
)

from dlt.common.schema.typing import TTableSchema

from dlt.extract.utils import (
    make_schema_with_default_name,
    wrap_parallel_iterator,
    dynstr,
)
from dlt.extract.items import (
    DataItemWithMeta,
    TableNameMeta,
)
from dlt.extract.items_transform import (
    FilterItem,
    MapItem,
    YieldMapItem,
    ValidateItem,
    LimitItem,
    ItemTransformFunc,
    ItemTransformFunctionWithMeta,
)
from dlt.extract.state import resource_state
from dlt.extract.pipe_iterator import ManagedPipeIterator
from dlt.extract.pipe import Pipe
from dlt.extract.hints import DltResourceHints, HintsMeta, TResourceHints
from dlt.extract.incremental import Incremental, IncrementalResourceWrapper
from dlt.extract.exceptions import (
    InvalidTransformerDataTypeGeneratorFunctionRequired,
    InvalidParentResourceDataType,
    InvalidParentResourceIsAFunction,
    InvalidResourceDataType,
    InvalidResourceDataTypeIsNone,
    InvalidTransformerGeneratorFunction,
    InvalidResourceDataTypeBasic,
    InvalidResourceDataTypeMultiplePipes,
    InvalidParallelResourceDataType,
    ParametrizedResourceUnbound,
    ResourceNameMissing,
    ResourceNotATransformer,
)
from dlt.extract.wrappers import wrap_additional_type


def with_table_name(item: TDataItems, table_name: str) -> DataItemWithMeta:
    """Marks `item` to be dispatched to table `table_name` when yielded from resource function."""
    return DataItemWithMeta(TableNameMeta(table_name), item)


def with_hints(
    item: TDataItems,
    hints: TResourceHints = None,
    create_table_variant: bool = False,
) -> DataItemWithMeta:
    """Marks `item` to update the resource with specified `hints`.

    Will create a separate variant of hints for a table if `name` is provided in `hints` and `create_table_variant` is set.

    Create `TResourceHints` with `make_hints`.
    Setting `table_name` will dispatch the `item` to a specified table, like `with_table_name`
    """
    return DataItemWithMeta(HintsMeta(hints, create_table_variant), item)


TDltResourceImpl = TypeVar("TDltResourceImpl", bound="DltResource", default="DltResource")


class DltResource(Iterable[TDataItem], DltResourceHints):
    """Implements dlt resource. Contains a data pipe that wraps a generating item and table schema that can be adjusted"""

    Empty: ClassVar["DltResource"] = None
    source_name: str
    """Name of the source that contains this instance of the source, set when added to DltResourcesDict"""
    section: str
    """A config section name"""
    SPEC: Type[BaseConfiguration]
    """A SPEC that defines signature of callable(parametrized) resource/transformer"""

    def __init__(
        self,
        pipe: Pipe,
        hints: TResourceHints,
        selected: bool,
        *,
        section: str = None,
        args_bound: bool = False,
    ) -> None:
        self.section = section
        self.selected = selected
        self._pipe = pipe
        self._args_bound = args_bound
        self._explicit_args: DictStrAny = None
        self.source_name = None
        super().__init__(hints)
        self._update_wrapper()

    @classmethod
    def from_data(
        cls,
        data: Any,
        name: str = None,
        section: str = None,
        hints: TResourceHints = None,
        selected: bool = True,
        data_from: Union["DltResource", Pipe] = None,
        inject_config: bool = False,
    ) -> Self:
        """Creates an instance of DltResource from compatible `data` with a given `name` and `section`.

        Internally (in the most common case) a new instance of Pipe with `name` is created from `data` and
        optionally connected to an existing pipe `from_data` to form a transformer (dependent resource).

        If `inject_config` is set to True and data is a callable, the callable is wrapped in incremental and config
        injection wrappers.
        """
        if data is None:
            raise InvalidResourceDataTypeIsNone(name, data, NoneType)

        if isinstance(data, DltResource):
            return data  # type: ignore[return-value]

        if isinstance(data, Pipe):
            r_ = cls(data, hints, selected, section=section)
            if inject_config:
                r_._inject_config()
            return r_

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
        if isinstance(data, (str, dict)):
            raise InvalidResourceDataTypeBasic(name, data, type(data))

        # check if depends_on is a valid resource
        parent_pipe: Pipe = None
        if data_from is not None:
            DltResource._ensure_valid_transformer_resource(name, data)
            parent_pipe = DltResource._get_parent_pipe(name, data_from)

        # create resource from iterator, iterable or generator function
        if isinstance(data, (Iterable, Iterator, AsyncIterable)) or callable(data):
            pipe = Pipe.from_data(name, data, parent=parent_pipe)
            r_ = cls(
                pipe,
                hints,
                selected,
                section=section,
                args_bound=not callable(data),
            )
            if inject_config:
                r_._inject_config()
            return r_
        else:
            # some other data type that is not supported
            raise InvalidResourceDataType(
                name, data, type(data), f"The data type of supplied type is {type(data).__name__}"
            )

    @property
    def name(self) -> str:
        """Resource name inherited from the pipe"""
        return self._pipe.name

    def with_name(self, new_name: str, new_section: str = None) -> Self:
        """Clones the resource with a new name. Such resource keeps separate state and loads data to `new_name` table by default."""
        return self._clone(new_name=new_name, new_section=new_section, with_parent=True)

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
    def incremental(self) -> Optional[IncrementalResourceWrapper]:
        """Gets incremental transform if it is in the pipe"""
        return cast(
            Optional[IncrementalResourceWrapper],
            self._pipe.get_by_type(IncrementalResourceWrapper, Incremental),
        )

    @property
    def validator(self) -> Optional[ValidateItem]:
        """Gets validator transform if it is in the pipe"""
        return cast(Optional[ValidateItem], self._pipe.get_by_type(ValidateItem))

    @validator.setter
    def validator(self, validator: Optional[ValidateItem]) -> None:
        """Add/remove or replace the validator in pipe"""
        step_no = self._pipe.remove_by_type(ValidateItem)
        if validator:
            self.add_step(validator, insert_at=step_no if step_no >= 0 else None)

    @property
    def max_table_nesting(self) -> Optional[int]:
        """A schema hint for resource that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON."""
        max_nesting = self._hints.get("x-normalizer", {}).get("max_nesting")  # type: ignore[attr-defined]
        return max_nesting if isinstance(max_nesting, int) else None

    @max_table_nesting.setter
    def max_table_nesting(self, value: Optional[int]) -> None:
        normalizer = self._hints.setdefault("x-normalizer", {})  # type: ignore[typeddict-item]
        if value is None:
            normalizer.pop("max_nesting", None)
        else:
            normalizer["max_nesting"] = value

    def pipe_data_from(self, data_from: Union[TDltResourceImpl, Pipe]) -> None:
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

    def select_tables(self, *table_names: Iterable[str]) -> Self:
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
    ) -> Self:  # noqa: A003
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
        self,
        item_map: ItemTransformFunc[Iterator[TDataItem]],
        insert_at: int = None,
    ) -> Self:  # noqa: A003
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
    ) -> Self:  # noqa: A003
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

    def add_limit(
        self,
        max_items: Optional[int] = None,
        max_time: Optional[float] = None,
    ) -> Self:  # noqa: A003
        """Adds a limit `max_items` to the resource pipe.

         This mutates the encapsulated generator to stop after `max_items` items are yielded. This is useful for testing and debugging.

         Notes:
             1. Transformers won't be limited. They should process all the data they receive fully to avoid inconsistencies in generated datasets.
             2. Each yielded item may contain several records. `add_limit` only limits the "number of yields", not the total number of records.
             3. Async resources with a limit added may occasionally produce one item more than the limit on some runs. This behavior is not deterministic.

        Args:
             max_items (int): The maximum number of items to yield, set to None for no limit
             max_time (float): The maximum number of seconds for this generator to run after it was opened, set to None for no limit
         Returns:
             "DltResource": returns self
        """

        if self.is_transformer:
            logger.warning(
                f"Setting add_limit to a transformer {self.name} has no effect. Set the limit on"
                " the top level resource."
            )
        else:
            # remove existing limit if any
            self._pipe.remove_by_type(LimitItem)
            self.add_step(LimitItem(max_items=max_items, max_time=max_time))

        return self

    def parallelize(self) -> Self:
        """Wraps the resource to execute each item in a threadpool to allow multiple resources to extract in parallel.

        The resource must be a generator or generator function or a transformer function.
        """
        if (
            not inspect.isgenerator(self._pipe.gen)
            and not (callable(self._pipe.gen) and isgeneratorfunction(self._pipe.gen))
            and not (callable(self._pipe.gen) and self.is_transformer)
        ):
            raise InvalidParallelResourceDataType(self.name, self._pipe.gen, type(self._pipe.gen))

        self._pipe.replace_gen(wrap_parallel_iterator(self._pipe.gen))  # type: ignore  # TODO
        return self

    def add_step(
        self,
        item_transform: ItemTransformFunctionWithMeta[TDataItems],
        insert_at: int = None,
    ) -> Self:  # noqa: A003
        if insert_at is None:
            self._pipe.append_step(item_transform)
        else:
            self._pipe.insert_step(item_transform, insert_at)
        return self

    def _remove_incremental_step(self) -> None:
        self._pipe.remove_by_type(Incremental, IncrementalResourceWrapper)

    def set_incremental(
        self,
        new_incremental: Union[Incremental[Any], IncrementalResourceWrapper],
        from_hints: bool = False,
    ) -> Optional[Union[Incremental[Any], IncrementalResourceWrapper]]:
        """Set/replace the incremental transform for the resource.

        Args:
            new_incremental: The Incremental instance/hint to set or replace
            from_hints: If the incremental is set from hints. Defaults to False.
        """
        if new_incremental is Incremental.EMPTY:
            new_incremental = None
        incremental = self.incremental
        if incremental is not None:
            # if isinstance(new_incremental, Mapping):
            #     new_incremental = Incremental.ensure_instance(new_incremental)

            if isinstance(new_incremental, IncrementalResourceWrapper):
                # Completely replace the wrapper
                self._remove_incremental_step()
                self.add_step(new_incremental)
            elif isinstance(incremental, IncrementalResourceWrapper):
                incremental.set_incremental(new_incremental, from_hints=from_hints)
            else:
                self._remove_incremental_step()
                # re-add the step
                incremental = None
        if incremental is None:
            # if there's no wrapper add incremental as a transform
            if new_incremental:
                if not isinstance(new_incremental, IncrementalResourceWrapper):
                    new_incremental = Incremental.ensure_instance(new_incremental)
                self.add_step(new_incremental)
        return new_incremental

    def _set_hints(
        self, table_schema_template: TResourceHints, create_table_variant: bool = False
    ) -> None:
        super()._set_hints(table_schema_template, create_table_variant)
        # validators and incremental apply only to resource hints
        if not create_table_variant:
            # try to late assign incremental
            if table_schema_template.get("incremental") is not None:
                incremental = self.set_incremental(
                    table_schema_template["incremental"], from_hints=True
                )
            else:
                incremental = self.incremental

            if incremental:
                primary_key = table_schema_template.get("primary_key", incremental.primary_key)
                if primary_key is not None:
                    incremental.primary_key = primary_key

            if table_schema_template.get("validator") is not None:
                self.validator = table_schema_template["validator"]

    def compute_table_schema(self, item: TDataItem = None, meta: Any = None) -> TTableSchema:
        incremental: Optional[Union[Incremental[Any], IncrementalResourceWrapper]] = (
            self.incremental
        )
        if incremental and "incremental" not in self._hints:
            if isinstance(incremental, IncrementalResourceWrapper):
                incremental = incremental.incremental
                if incremental:
                    self._hints["incremental"] = incremental

        table_schema = super().compute_table_schema(item, meta)

        return table_schema

    def bind(self, *args: Any, **kwargs: Any) -> Self:
        """Binds the parametrized resource to passed arguments. Modifies resource pipe in place. Does not evaluate generators or iterators."""
        if self._args_bound:
            raise TypeError(
                f"Parametrized resource `{self.name}` is not callable. You can call and pass"
                " arguments to a parametrized resource only once. Make sure you didn't call this"
                " resource before."
            )

        orig_gen = self._pipe.gen
        gen = self._pipe.bind_gen(*args, **kwargs)
        if isinstance(gen, DltResource):
            # the resource returned resource: update in place
            # TODO: find a better way to modify resource when called
            old_pipe = self._pipe
            self.__dict__.clear()
            self.__dict__.update(gen.__dict__)
            # keep old pipe instance
            self._pipe = old_pipe
            self._pipe.__dict__.clear()
            # write props from new pipe instance
            self._pipe.__dict__.update(gen._pipe.__dict__)
        # elif isinstance(gen, Pipe):
        #     # the resource returned pipe: just replace pipe
        #     self._pipe.__dict__.clear()
        #     # write props from new pipe instance
        #     self._pipe.__dict__.update(gen.__dict__)
        else:
            self._args_bound = True
        self._set_explicit_args(orig_gen, None, *args, **kwargs)  # type: ignore
        return self

    @property
    def args_bound(self) -> bool:
        """Returns true if resource the parameters are bound to values. Such resource cannot be further called.
        Note that resources are lazily evaluated and arguments are only formally checked. Configuration
        was not yet injected as well.
        """
        return self._args_bound

    @property
    def explicit_args(self) -> StrAny:
        """Returns a dictionary of arguments used to parametrize the resource. Does not include defaults and injected args."""
        if not self._args_bound:
            raise TypeError(f"Resource `{self.name}` is not yet parametrized")
        return self._explicit_args

    @property
    def state(self) -> StrAny:
        """Gets resource-scoped state from the active pipeline. PipelineStateNotAvailable is raised if pipeline context is not available"""
        with inject_section(self._get_config_section_context()):
            return resource_state(self.name)

    def __call__(self: TDltResourceImpl, *args: Any, **kwargs: Any) -> TDltResourceImpl:
        """Binds the parametrized resources to passed arguments. Creates and returns a bound resource. Generators and iterators are not evaluated."""
        if self._args_bound:
            raise TypeError(
                f"Parametrized resource `{self.name}` is not callable. You can call and pass"
                " arguments to a parametrized resource only once. Make sure you didn't call this"
                " resource before."
            )
        # detect if name should be renamed
        bound_args = None
        if isinstance(self.name, dynstr) or isinstance(self.section, dynstr):
            _, _, bound_args = self._pipe.sim_gen(*args, **kwargs)
        new_name = self.name(bound_args.arguments) if isinstance(self.name, dynstr) else None
        new_section = (
            self.section(bound_args.arguments) if isinstance(self.section, dynstr) else None
        )

        r = self._clone(new_name=new_name, new_section=new_section)
        return r.bind(*args, **kwargs)

    def __or__(self, transform: Union["DltResource", AnyFun]) -> "DltResource":
        """Allows to pipe data from across resources and transform functions with | operator
        This is the LEFT side OR so the self may be resource or transformer
        """
        # print(f"{resource.name} | {self.name} -> {resource.name}[{resource.is_transformer}]")
        if isinstance(transform, DltResource):
            transform.pipe_data_from(self)
            # return transformed resource for chaining
            return transform
        else:
            # map or yield map
            if isgeneratorfunction(transform):
                return self.add_yield_map(transform)
            else:
                return self.add_map(transform)

    def __ror__(self, data: Union[Iterable[Any], Iterator[Any]]) -> Self:
        """Allows to pipe data from across resources and transform functions with | operator
        This is the RIGHT side OR so the self may not be a resource and the LEFT must be an object
        that does not implement | ie. a list
        """
        self.pipe_data_from(self.from_data(data, name="iter_" + uniq_id(4)))
        return self

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

    def _eject_config(self) -> bool:
        """Unwraps the pipe generator step from config injection and incremental wrappers by restoring the original step.

        Removes the step with incremental wrapper. Should be used before a subsequent _inject_config is called on the
        same pipe to successfully wrap it with new incremental and config injection.
        Note that resources with bound arguments cannot be ejected.

        """
        if not self._pipe.is_empty and not self._args_bound:
            orig_gen = getattr(self._pipe.gen, "__GEN__", None)
            if orig_gen:
                self._remove_incremental_step()
                self._pipe.replace_gen(orig_gen)
                return True
        return False

    def _inject_config(self, incremental_from_hints_override: Optional[bool] = None) -> Self:
        """Wraps the pipe generation step in incremental and config injection wrappers and adds pipe step with
        Incremental transform.
        """
        gen = self._pipe.gen
        if not callable(gen):
            return self

        incremental: IncrementalResourceWrapper = None
        sig = inspect.signature(gen)
        if IncrementalResourceWrapper.should_wrap(sig):
            incremental = IncrementalResourceWrapper(self._hints.get("primary_key"))
            if incr_hint := self._hints.get("incremental"):
                incremental.set_incremental(
                    incr_hint,
                    from_hints=(
                        incremental_from_hints_override
                        if incremental_from_hints_override is not None
                        else True
                    ),
                )
            incr_f = incremental.wrap(sig, gen)
            self.set_incremental(incremental)
        else:
            incr_f = gen
        resource_sections = (known_sections.SOURCES, self.section, self.name)
        # function should have associated SPEC
        spec = get_fun_spec(gen)
        # standalone resource will prefer existing section context when resolving config values
        # this lets the source to override those values and provide common section for all config values for resources present in that source
        # for autogenerated spec do not include defaults
        conf_f = with_config(
            incr_f,
            spec=spec,
            sections=resource_sections,
            sections_merge_style=ConfigSectionContext.resource_merge_style,
        )
        if conf_f != gen:
            self._pipe.replace_gen(conf_f)
            # storage the original generator to be able to eject config and incremental wrapper
            # when resource is cloned
            setattr(conf_f, "__GEN__", gen)  # noqa: B010
        return self

    def _clone(
        self, *, new_name: str = None, new_section: str = None, with_parent: bool = False
    ) -> Self:
        """Creates a deep copy of a current resource, optionally renaming the resource. The clone will not be part of the source."""
        pipe = self._pipe
        if self._pipe and not self._pipe.is_empty:
            pipe = pipe._clone(new_name=new_name, with_parent=with_parent)
        # incremental and parent are already in the pipe (if any)

        incremental = self.incremental
        if isinstance(incremental, IncrementalResourceWrapper):
            incremental_from_hints: Optional[bool] = incremental._from_hints
        else:
            incremental_from_hints = None
        r_ = self.__class__(
            pipe,
            self._clone_hints(self._hints),
            selected=self.selected,
            section=new_section or self.section,
            args_bound=self._args_bound,
        )
        # try to eject and then inject configuration and incremental wrapper when resource is cloned
        # this makes sure that a take config values from a right section and wrapper has a separated
        # instance in the pipeline
        if r_._eject_config():
            r_._inject_config(incremental_from_hints_override=incremental_from_hints)
        return r_

    def _update_wrapper(self) -> None:
        """Update wrapper from callable pipe gen to behave as functools wrapper"""
        if self._pipe.is_empty:
            return
        gen = self._pipe.gen
        if not callable(gen):
            return
        update_wrapper(self, gen)
        # also change the signature return annotation to dlt source class
        sig = inspect.signature(gen).replace(return_annotation=self.__class__)
        self.__signature__ = sig

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
            default_schema_name = make_schema_with_default_name(pipeline.pipeline_name)
        return ConfigSectionContext(
            pipeline_name=pipeline_name,
            # do not emit middle config section to not overwrite the resource section
            # only sources emit middle config section
            sections=(
                known_sections.SOURCES,
                self.section or pipeline_name or "",
                self.name,
            ),
            source_state_key=self.source_name or default_schema_name or self.section or uniq_id(),
        )

    def __repr__(self) -> str:
        # TODO add a mechanism to truncate the repr of some hints
        # TODO expand information about steps
        # TODO ensure consistent ordering between object __repr__
        # TODO verify that attributes are inexpensive to compute
        # TODO add a toggle for kwargs that are not valid kwargs, but
        # helpful for debugging

        limit = None
        for step in self._pipe.steps:
            if isinstance(step, LimitItem):
                limit = step.max_items
                break

        kwargs = {
            "name": self.name,
            #  "section": self.section,  should this be explicitly passed?
            "table_name": self._hints.get("table_name"),
            "primary_key": self._hints.get("primary_key"),
            "merge_key": self._hints.get("merge_key"),
            "columns": "{...}" if self._hints.get("columns") else None,
            "parent_table_name": self._hints.get("parent_table_name"),
            "references": "{...}" if self._hints.get("references") else None,
            "nested_hints": "{...}" if self._hints.get("nested_hints") else None,
            "limit": limit,  # NOTE not a valid kwarg for `@dlt.resource`
            "max_table_nesting": self._hints.get("max_table_nesting"),
            "write_disposition": self._hints.get("write_disposition"),
            "table_format": self._hints.get("table_format"),
            "file_format": self._hints.get("file_format"),
            "schema_contract": "{...}" if self._hints.get("schema_contract") else None,
            "incremental": self.incremental,
            "validator": self.validator,
        }
        if len(self._pipe.steps) > 1:
            # NOTE both are not valid kwargs for `@dlt.resource`
            kwargs["n_steps"] = len(self._pipe.steps)
            kwargs["steps"] = [type(step).__name__ for step in self._pipe.steps]
        # the name isn't `DltResource` because it's not the main entrypoint
        # to create a resource
        if self.is_transformer:
            return simple_repr("@dlt.transformer", **without_none(kwargs))
        else:
            return simple_repr("@dlt.resource", **without_none(kwargs))

    def __str__(self) -> str:
        info = f"DltResource `{self.name}`"
        if self.section:
            info += f" in section `{self.section}`"
        if self.source_name:
            info += f" added to source `{self.source_name}`:"
        else:
            info += ":"

        if self.is_transformer:
            info += (
                "\nThis resource is a transformer and takes data items from"
                f" `{self._pipe.parent.name}`"
            )
        else:
            if self._pipe.is_data_bound:
                if self.requires_args:
                    head_sig = inspect.signature(self._pipe.gen)  # type: ignore
                    info += (
                        "\nThis resource is parametrized and takes the following arguments"
                        f" `{head_sig}`. You must call this resource before loading."
                    )
                else:
                    info += (
                        "\nIf you want to see the data items in the resource you must iterate it or"
                        " convert to list ie. `list(resource)`. Note that, like any iterator, you"
                        " can iterate the resource only once."
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
