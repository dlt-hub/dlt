import contextlib
from copy import copy
from functools import partial
import inspect
from typing import (
    ClassVar,
    Dict,
    Iterable,
    Generator,
    List,
    Optional,
    Sequence,
    Tuple,
    Any,
)
from typing_extensions import Self

from dlt.common import logger
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.base_configuration import ContainerInjectableContext, configspec
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.normalizers.json.typing import RelationalNormalizerConfig
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnName, TSchemaContract
from dlt.common.schema.utils import normalize_table_identifiers
from dlt.common.typing import StrAny, TDataItem
from dlt.common.configuration.container import Container
from dlt.common.pipeline import (
    PipelineContext,
    StateInjectableContext,
    SupportsPipelineRun,
    pipeline_state,
)
from dlt.common.utils import (
    graph_find_scc_nodes,
    flatten_list_or_items,
    graph_edges_to_nodes,
    simple_repr,
    without_none,
)

from dlt.extract.items import SupportsPipe, TDecompositionStrategy
from dlt.extract.state import source_state
from dlt.extract.pipe_iterator import ManagedPipeIterator
from dlt.extract.pipe import Pipe
from dlt.extract.resource import DltResource
from dlt.extract.exceptions import (
    DataItemRequiredForDynamicTableHints,
    ResourceNotFoundError,
    ResourcesNotFoundError,
    DeletingResourcesNotSupported,
    InvalidParallelResourceDataType,
)


class DltResourceDict(Dict[str, DltResource]):
    def __init__(self, source_name: str, source_section: str) -> None:
        super().__init__()
        self.source_name = source_name
        self.source_section = source_section

        self._suppress_clone_on_setitem = False
        self._new_pipes: List[Pipe] = []
        """Collects pipes added via __setitem__"""
        self._cloned_pairs: Dict[str, Pipe] = {}
        """Maps pipe instance_id to pipe instance (contains full pipe tree)"""
        self._resource_pipes: Dict[str, DltResource] = {}
        """Maps pipe instance_id to resource (contains explicit and implicit resources)"""

    @property
    def selected(self) -> Dict[str, DltResource]:
        """Returns a subset of all resources that will be extracted and loaded to the destination."""
        return {k: v for k, v in self.items() if v.selected}

    # NOTE the name `extracted` suggests that the resources were ran and data was extracted
    # this is not what the docstring implies. Could rename to "`to_extract`". Or `selected` vs. `user_selected`
    @property
    def extracted(self) -> List[DltResource]:
        """Returns a all resources that will be extracted. That includes selected resources and all their parents.
        Note that several resources with the same name may be included if there were several parent instances of the
        same resource.
        """
        extracted = []
        for pipe in self.extracted_pipes:
            with contextlib.suppress(ResourceNotFoundError):
                extracted.append(self.with_pipe(pipe))
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
                # NOTE a node pointing to itself creates a cycle / is not a DAG
                # the implementation doesn't match the function name and docstring
                # add isolated element
                # TODO: it is not node pointing to itself. change to (pipe.name, None)
                #   to indicate isolated node
                dag.append((pipe.name, pipe.name))
        return dag

    @property
    def pipes(self) -> List[Pipe]:
        return [r._pipe for r in self.values()]

    @property
    def selected_pipes(self) -> Sequence[Pipe]:
        return [r._pipe for r in self.values() if r.selected]

    @property
    def extracted_pipes(self) -> Sequence[Pipe]:
        """Returns all execution pipes in the source including all parent pipes"""
        extracted = []
        for resource in self.selected.values():
            pipe = resource._pipe
            while pipe is not None:
                extracted.append(pipe)
                pipe = pipe.parent
        return extracted

    def select(self, *resource_names: str) -> Dict[str, DltResource]:
        """Selects `resource_name` to be extracted, and unselects remaining resources."""
        for name in resource_names:
            if name not in self:
                # if any key is missing, display the full info
                raise ResourcesNotFoundError(
                    self.source_name, set(self.keys()), set(resource_names)
                )
        # set the selected flags
        for resource in self.values():
            self[resource.name].selected = resource.name in resource_names
        return self.selected

    def with_pipe(self, pipe: SupportsPipe) -> DltResource:
        """Gets resource with given execution pipe matched by pipe instance id.
        Note that we do not use names to match pipe to resource as many resources
        with the same name may be extracted at the same time: ie. when transformers
        with different names depend on several instances of the same resource.
        Note that ad hoc resources for parent pipes without resource won't be created
        """
        try:
            return self._resource_pipes[pipe.instance_id]
        except KeyError:
            assert pipe.name not in self, (
                f"consistency of search by pipe id failed, {pipe.name} was found,"
                f" {pipe.instance_id} not."
            )
            raise ResourceNotFoundError(
                pipe.name,
                f"Resource with pipe with `{pipe.instance_id} could not be found in source",
            )

    def add(self, *resources: DltResource) -> None:
        """Add `resources` to the source. Adding multiple resources with the same name is not
        supported.
        """
        with self._add_multiple_resources():
            for resource in resources:
                if resource.name in self:
                    # for resources with the same name try to add the resource as an another pipe
                    self[resource.name].add_pipe(resource)
                else:
                    self[resource.name] = resource

        self._clone_new_pipes([r.name for r in resources])
        self._adjust_instances()

    def detach(self, resource_name: str = None) -> DltResource:
        """Clones `resource_name` (including parent resource pipes) and removes source contexts.
        Defaults to the first resource in the source if `resource_name` is None.
        """
        return (self[resource_name] if resource_name else list(self.values())[0])._clone(
            with_parent=True
        )

    @contextlib.contextmanager
    def _add_multiple_resources(self) -> Generator[TDataItem, None, None]:
        # temporarily block cloning when single resource is added
        try:
            self._suppress_clone_on_setitem = True
            yield
        finally:
            self._suppress_clone_on_setitem = False

    def _clone_new_pipes(self, resource_names: Sequence[str]) -> None:
        # get tree of new pipes and updated
        _, self._cloned_pairs = ManagedPipeIterator.clone_pipes(self._new_pipes, self._cloned_pairs)
        # replace pipes in resources, the cloned_pipes preserve parent connections
        for name in resource_names:
            resource = self[name]
            pipe_id = resource._pipe.instance_id
            resource._pipe = self._cloned_pairs[pipe_id]
            # store pipes of newly added resources
            self._resource_pipes[pipe_id] = resource

        self._new_pipes.clear()

    def _adjust_instances(self) -> None:
        """Walks resource tree starting from explicit resources and adjusts parent resource instances
        and pipe instances based on pipe instance_id. This is necessary to keep consistent
        pipe and resource tree. Also detects implicitly added parents and adds those resources
        to resource_pipes.
        Dangling resources and pipes are removed at the end
        """
        # collect all pipes and resources
        collected_pipes = set()
        collected_resources = set()
        # descend into parents and align _parent instances
        for resource in self.values():
            parent = resource._parent
            assert resource._pipe is self._cloned_pairs[resource._pipe.instance_id]
            collected_pipes.add(resource._pipe)
            collected_resources.add(resource)
            while parent and not parent._pipe.is_empty:
                # also index pipes of parents that are not part of the source
                if parent._pipe.instance_id not in self._resource_pipes:
                    # clone parent when adding
                    parent = copy(parent)
                    # correct pipe
                    self._resource_pipes[parent._pipe.instance_id] = parent
                # correct resource parent
                parent = self.with_pipe(resource._pipe.parent)
                parent._pipe = self._cloned_pairs[parent._pipe.instance_id]
                resource._parent = parent
                # recur
                collected_pipes.add(parent._pipe)
                collected_resources.add(parent)
                resource = parent
                parent = parent._parent

        # drop all resource instances and pipe instances that were not collected in tree walk
        # those are no longer used and will not be extracted. note that both explicitly
        # added and implicitly (ie. transformer parent) are collected
        dropped_resources = set(self._resource_pipes.values()).difference(collected_resources)
        if dropped_resources:
            # all collected resources must be known
            assert dropped_resources == set(self._resource_pipes.values()).symmetric_difference(
                collected_resources
            )
            for resource in list(self._resource_pipes.values()):
                if resource in dropped_resources:
                    del self._resource_pipes[resource._pipe.instance_id]
        dropped_pipes = set(self._cloned_pairs.values()).difference(collected_pipes)
        if dropped_pipes:
            # all collected pipes must be known
            assert dropped_pipes == set(self._cloned_pairs.values()).symmetric_difference(
                collected_pipes
            )
            for pipe in list(self._cloned_pairs.values()):
                if pipe in dropped_pipes:
                    del self._cloned_pairs[pipe.instance_id]

        self._assert_source_consistency()

    def _assert_source_consistency(self) -> None:
        """Makes sure that all resource and pipe references in extracted resource tree
        point to the same Python instances.
        """
        pipe_ids = list(self._cloned_pairs.values())
        for resource in self.values():
            if not resource._pipe.is_empty:
                assert resource._pipe in pipe_ids
                assert resource is self.with_pipe(resource._pipe)
                # check parent
                parent = resource._parent
                while parent:
                    # also pipe has parent
                    assert resource._pipe.has_parent
                    # instances must match if known
                    if parent.name in self:
                        if parent is not self[parent.name]:
                            logger.warning(
                                f"Source `{self.source_name}` contains resource `{resource.name}`"
                                f" which has a parent named `{parent.name}` which refers to a"
                                " different instance than an explicitly added resource with the"
                                " same name. This may happen if you added resource and transformer"
                                " to your source and later replaced the resource (parent) with"
                                f" another resource. In that case transformer `{resource.name}`"
                                " still points to original parent which will be separately"
                                " extracted. If this is not what you want, replace transformer as"
                                " well or rename your resources and transformers."
                            )

                    if parent._pipe.is_empty:
                        assert parent._pipe not in pipe_ids
                    else:
                        # knows pipes
                        assert parent._pipe in pipe_ids
                        assert parent is self.with_pipe(parent._pipe)
                        # pipes must match
                        assert parent._pipe is resource._pipe.parent

                    resource = parent
                    parent = resource._parent

    def __setitem__(self, resource_name: str, resource: DltResource) -> None:
        if resource_name != resource.name:
            raise ValueError(
                f"The index name `{resource_name}` does not correspond to resource name"
                f" `{resource.name}`"
            )
        pipe_instance_id = resource._pipe.instance_id
        # make shallow copy of the resource
        resource = copy(resource)
        # resource.section = self.source_section
        resource.source_name = self.source_name
        if pipe_instance_id not in self._cloned_pairs:
            self._new_pipes.append(resource._pipe)
        # now set it in dict
        super().__setitem__(resource_name, resource)

        # immediately clone pipe if not suppressed
        if not self._suppress_clone_on_setitem:
            self._clone_new_pipes([resource.name])
            self._adjust_instances()

    def __delitem__(self, resource_name: str) -> None:
        raise DeletingResourcesNotSupported(self.source_name, resource_name)

    def __repr__(self) -> str:
        if not self:
            return "{}"

        s = "{\n  'selected': {"
        for name, r in self.selected.items():
            s += f"\n    '{name}': {r.__repr__()}, "
        s += "\n  }"

        s += "\n  'extracted': {"
        for r in self.extracted:
            s += f"\n    '{r.name}': {r.__repr__()}, "
        s += "\n  }"
        s += "\n}"

        return s


class DltSource(Iterable[TDataItem]):
    """Groups several `dlt resources` under a single schema and allows to perform operations on them.

    The instance of this class is created whenever you call the `dlt.source` decorated function. It automates several functions for you:
    * You can pass this instance to `dlt` `run` method in order to load all data present in the `dlt resources`.
    * You can select and deselect resources that you want to load via `with_resources` method
    * You can access the resources (which are `DltResource` instances) as source attributes
    * It implements `Iterable` interface so you can get all the data from the resources yourself and without dlt pipeline present.
    * It will create a DAG from resources and transformers and optimize the extraction so parent resources are extracted only once
    * You can get the `schema` for the source and all the resources within it.
    * You can use a `run` method to load the data with a default instance of dlt pipeline.
    * You can get source read only state for the currently active Pipeline instance
    """

    def __init__(
        self, schema: Schema, section: str, resources: Sequence[DltResource] = None
    ) -> None:
        self.section = section
        """Tells if iterator associated with a source is exhausted"""
        self._schema = schema
        self._resources: DltResourceDict = DltResourceDict(self.name, self.section)

        if resources:
            self.resources.add(*resources)

    @classmethod
    def from_data(cls, schema: Schema, section: str, data: Any) -> Self:
        """Converts any `data` supported by `dlt` `run` method into `dlt source` with a name `section`.`name` and `schema` schema."""
        # creates source from various forms of data
        if isinstance(data, DltSource):
            return data  # type: ignore[return-value]

        # in case of sequence, enumerate items and convert them into resources
        if isinstance(data, Sequence):
            resources = [DltResource.from_data(i) for i in data]
        else:
            resources = [DltResource.from_data(data)]

        return cls(schema, section, resources)

    @property
    def name(self) -> str:
        return self._schema.name

    @property
    def max_table_nesting(self) -> int:
        """A schema hint that sets the maximum depth of nested table above which the remaining nodes are loaded as structs or JSON."""
        return RelationalNormalizer.get_normalizer_config(self._schema).get("max_nesting")

    @max_table_nesting.setter
    def max_table_nesting(self, value: int) -> None:
        if value is None:
            # this also check the normalizer type
            config = RelationalNormalizer.get_normalizer_config(self._schema)
            config.pop("max_nesting", None)
        else:
            RelationalNormalizer.update_normalizer_config(self._schema, {"max_nesting": value})

    @property
    def root_key(self) -> Optional[bool]:
        """Enables merging on all resources by propagating root foreign key to nested tables.
        This option is most useful if you plan to change write disposition of a resource to disable/enable merge.

        """
        # this also check the normalizer type
        config = RelationalNormalizer.get_normalizer_config(self._schema)
        is_root_key = config.get("root_key_propagation")
        if is_root_key is None:
            # if not found get legacy value
            is_root_key = self._get_root_key_legacy(config)
            if is_root_key:
                # set the root key if legacy value set
                self.root_key = True
        return is_root_key

    @root_key.setter
    def root_key(self, value: bool) -> None:
        # this also check the normalizer type
        config = RelationalNormalizer.get_normalizer_config(self._schema)
        if value is None:
            value = self._get_root_key_legacy(config)
        if value is not None:
            RelationalNormalizer.update_normalizer_config(
                self._schema,
                {"root_key_propagation": value},
            )

    def _get_root_key_legacy(self, config: RelationalNormalizerConfig) -> Optional[bool]:
        data_normalizer = self._schema.data_item_normalizer
        assert isinstance(data_normalizer, RelationalNormalizer)
        # we must remove root key propagation
        with contextlib.suppress(KeyError):
            propagation_config = config["propagation"]
            propagation_config["root"].pop(data_normalizer.c_dlt_id)
            # and set the value below
            return True
        return None

    @property
    def schema_contract(self) -> TSchemaContract:
        return self.schema.settings.get("schema_contract")

    @schema_contract.setter
    def schema_contract(self, settings: TSchemaContract) -> None:
        self.schema.set_schema_contract(settings)

    @property
    def exhausted(self) -> bool:
        """Check all selected pipes whether one of them has started. if so, the source is exhausted."""
        for pipe in self._resources.selected_pipes:
            item = pipe.gen
            if inspect.isgenerator(item):
                if inspect.getgeneratorstate(item) != "GEN_CREATED":
                    return True
        return False

    @property
    def resources(self) -> DltResourceDict:
        """A dictionary of all resources present in the source, where the key is a resource name.

        Returns:
            DltResourceDict: A dictionary of all resources present in the source, where the key is a resource name.
        """
        return self._resources

    @property
    def selected_resources(self) -> Dict[str, DltResource]:
        """A dictionary of all the resources that are selected to be loaded.

        Returns:
            Dict[str, DltResource]: A dictionary of all the resources that are selected to be loaded.
        """
        return self._resources.selected

    @property
    def schema(self) -> Schema:
        return self._schema

    @schema.setter
    def schema(self, value: Schema) -> None:
        self._schema = value

    def discover_schema(self, item: TDataItem = None, meta: Any = None) -> Schema:
        """Computes table schemas for all selected resources in the source and merges them with a copy of current source schema. If `item` is provided,
        dynamic tables will be evaluated, otherwise those tables will be ignored."""
        schema = self._schema.clone(update_normalizers=True)
        for r in self.selected_resources.values():
            # names must be normalized here
            with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                root_table_schema = r.compute_table_schema(item, meta)
                nested_tables_schema = r.compute_nested_table_schemas(
                    root_table_schema["name"], schema.naming, item, meta
                )
                # NOTE must ensure that `schema.update_table()` is called in an order that respect parent-child relationships
                for table_schema in (root_table_schema, *nested_tables_schema):
                    partial_table = normalize_table_identifiers(table_schema, self._schema.naming)
                    schema.update_table(partial_table)
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

    def add_limit(
        self,
        max_items: Optional[int] = None,
        max_time: Optional[float] = None,
        count_rows: Optional[bool] = False,
    ) -> "DltSource":  # noqa: A003
        """Limits the items processed in all selected resources in the source that are not transformers: by count or time.

        This is useful for testing, debugging and generating sample datasets for experimentation. You can easily get your test dataset in a few minutes, when otherwise
        you'd need to wait hours for the full loading to complete.

        For incremental resources that return rows in deterministic order, you can use this function to process large load
        in batches.

        Notes:
            1. Transformers resources won't be limited. They should process all the data they receive fully to avoid inconsistencies in generated datasets.
            2. Each yielded item may contain several records. `add_limit` only limits the "number of yields", not the total number of records.
            3. Empty pages/yields are also counted. Use `count_rows` to skip empty pages.

        Args:
            max_items (Optional[int]): The maximum number of items (not rows!) to yield, set to None for no limit
            max_time (Optional[float]): The maximum number of seconds for this generator to run after it was opened, set to None for no limit
            count_rows (Optional[bool]): Default: false.
                Count rows instead of pages. Note that if resource yields pages of rows, last page will not be trimmed and more rows that expected will be received.
        Returns:
            "DltSource": returns self
        """
        for resource in self.resources.selected.values():
            resource.add_limit(max_items, max_time=max_time, count_rows=count_rows)
        return self

    def parallelize(self) -> "DltSource":
        """Mark all resources in the source to run in parallel.

        Only transformers and resources based on generators and generator functions are supported, unsupported resources will be skipped.
        """
        for resource in self.resources.selected.values():
            try:
                resource.parallelize()
            except InvalidParallelResourceDataType:
                pass
        return self

    @property
    def run(self) -> SupportsPipelineRun:
        """A convenience method that will call `run` run on the currently active `dlt` pipeline. If pipeline instance is not found, one with default settings will be created."""
        self_run: SupportsPipelineRun = partial(
            Container()[PipelineContext].pipeline().run, data=self
        )
        return self_run

    @property
    def state(self) -> StrAny:
        """Gets source-scoped state from the active pipeline. PipelineStateNotAvailable is raised if no pipeline is active"""
        with inject_section(self._get_config_section_context()):
            return source_state()

    def clone(self, with_name: str = None) -> "DltSource":
        """Creates a deep copy of the source where copies of schema, resources and pipes are created.

        If `with_name` is provided, a schema is cloned with a changed name
        """
        # mind that resources and pipes are cloned when added to the DltResourcesDict in the source constructor
        return DltSource(
            self.schema.clone(with_name=with_name), self.section, list(self._resources.values())
        )

    def __iter__(self) -> Generator[TDataItem, None, None]:
        """Opens iterator that yields the data items from all the resources within the source in the same order as in Pipeline class.

        A read-only state is provided, initialized from active pipeline state. The state is discarded after the iterator is closed.

        A source config section is injected to allow secrets/config injection as during regular extraction.
        """
        # use the same state dict when opening iterator and when iterator is iterated
        mock_state, _ = pipeline_state(Container(), {})
        state_context = StateInjectableContext(state=mock_state)
        section_context = self._get_config_section_context()
        source_context = SourceInjectableContext(self)
        schema_context = SourceSchemaInjectableContext(self.schema)

        # managed pipe iterator will set the context on each call to  __next__
        with (
            inject_section(section_context),
            Container().injectable_context(state_context),
            Container().injectable_context(source_context),
            Container().injectable_context(schema_context),
        ):
            pipe_iterator: ManagedPipeIterator = ManagedPipeIterator.from_pipes(self._resources.selected_pipes)  # type: ignore
        pipe_iterator.set_context([section_context, state_context, schema_context, source_context])
        _iter = map(lambda item: item.item, pipe_iterator)
        return flatten_list_or_items(_iter)

    def _get_config_section_context(self) -> ConfigSectionContext:
        proxy = Container()[PipelineContext]
        pipeline_name = None if not proxy.is_active() else proxy.pipeline().pipeline_name
        return ConfigSectionContext(
            pipeline_name=pipeline_name,
            sections=(known_sections.SOURCES, self.section, self.name),
            source_state_key=self.name,
        )

    def __getattr__(self, resource_name: str) -> DltResource:
        try:
            return self._resources[resource_name]
        except KeyError:
            all_resources = ", ".join(self._resources.keys())
            raise AttributeError(
                f"Resource `{resource_name}` not found in source `{self.name}`. Available"
                f" resources: {all_resources}"
            )

    def __setattr__(self, name: str, value: Any) -> None:
        if isinstance(value, DltResource):
            self.resources[name] = value
        else:
            super().__setattr__(name, value)

    def __repr__(self) -> str:
        kwargs = {
            "name": self.name,
            "section": self.section,
            "schema_contract": "{...}" if self.schema_contract else None,
            "exhausted": self.exhausted if self.exhausted else None,
            "n_resources": len(self.resources),
            "resources": list(self.resources.keys()),
        }
        return simple_repr("@dlt.source", **without_none(kwargs))

    def __str__(self) -> str:
        info = (
            f"DltSource `{self.name}` section `{self.section}` contains"
            f" {len(self.resources)} resource(s) of which {len(self.selected_resources)} are"
            " selected"
        )
        for r in self.resources.values():
            selected_info = "selected" if r.selected else "not selected"
            if r.is_transformer:
                info += (
                    f"\ntransformer `{r.name}` is {selected_info} and takes data from"
                    f" `{r._pipe.parent.name}`"
                )
            else:
                info += f"\nresource `{r.name}` is {selected_info}"
        if self.exhausted:
            info += (
                "\nSource is already iterated and cannot be used again ie. to display or load data."
            )
        else:
            info += (
                "\nIf you want to see the data items in this source you must iterate it or convert"
                " to list ie. `list(source)`."
            )
        info += " Note that, like any iterator, you can iterate the source only once."
        info += f"\ninstance id: {id(self)}"
        return info


@configspec
class SourceSchemaInjectableContext(ContainerInjectableContext):
    """A context containing the source schema, present when dlt.source/resource decorated function is executed"""

    schema: Schema = None

    can_create_default: ClassVar[bool] = False


@configspec
class SourceInjectableContext(ContainerInjectableContext):
    """A context containing the source schema, present when dlt.resource decorated function is executed"""

    source: DltSource = None

    can_create_default: ClassVar[bool] = False


class _DltSingleSource(DltSource):
    """Used to register standalone (non-inner) resources"""

    @property
    def single_resource(self) -> DltResource:
        return list(self.resources.values())[0]
