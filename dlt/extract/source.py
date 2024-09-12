import contextlib
from copy import copy
import makefun
import inspect
from typing import Dict, Iterable, Iterator, List, Sequence, Tuple, Any
from typing_extensions import Self

from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnName, TSchemaContract
from dlt.common.schema.utils import normalize_table_identifiers
from dlt.common.typing import StrAny, TDataItem
from dlt.common.configuration.container import Container
from dlt.common.pipeline import (
    PipelineContext,
    StateInjectableContext,
    SupportsPipelineRun,
    source_state,
    pipeline_state,
)
from dlt.common.utils import graph_find_scc_nodes, flatten_list_or_items, graph_edges_to_nodes

from dlt.extract.items import TDecompositionStrategy
from dlt.extract.pipe_iterator import ManagedPipeIterator
from dlt.extract.pipe import Pipe
from dlt.extract.hints import DltResourceHints, make_hints
from dlt.extract.resource import DltResource
from dlt.extract.exceptions import (
    DataItemRequiredForDynamicTableHints,
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
        # pipes not yet cloned in __setitem__
        self._new_pipes: List[Pipe] = []
        # pipes already cloned by __setitem__ id(original Pipe):cloned(Pipe)
        self._cloned_pairs: Dict[int, Pipe] = {}

    @property
    def selected(self) -> Dict[str, DltResource]:
        """Returns a subset of all resources that will be extracted and loaded to the destination."""
        return {k: v for k, v in self.items() if v.selected}

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
                        mock_template = make_hints(
                            pipe.name, write_disposition=resource.write_disposition
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
                raise ResourcesNotFoundError(
                    self.source_name, set(self.keys()), set(resource_names)
                )
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
            raise ValueError(
                f"The index name {resource_name} does not correspond to resource name"
                f" {resource.name}"
            )
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

    # TODO: max_table_nesting/root_key below must go somewhere else ie. into RelationalSchema which is Schema + Relational normalizer.
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
    def root_key(self) -> bool:
        """Enables merging on all resources by propagating root foreign key to child tables. This option is most useful if you plan to change write disposition of a resource to disable/enable merge"""
        # this also check the normalizer type
        config = RelationalNormalizer.get_normalizer_config(self._schema).get("propagation")
        data_normalizer = self._schema.data_item_normalizer
        assert isinstance(data_normalizer, RelationalNormalizer)
        return (
            config is not None
            and "root" in config
            and data_normalizer.c_dlt_id in config["root"]
            and config["root"][data_normalizer.c_dlt_id] == data_normalizer.c_dlt_root_id
        )

    @root_key.setter
    def root_key(self, value: bool) -> None:
        # this also check the normalizer type
        config = RelationalNormalizer.get_normalizer_config(self._schema)
        data_normalizer = self._schema.data_item_normalizer
        assert isinstance(data_normalizer, RelationalNormalizer)

        if value is True:
            RelationalNormalizer.update_normalizer_config(
                self._schema,
                {
                    "propagation": {
                        "root": {
                            data_normalizer.c_dlt_id: TColumnName(data_normalizer.c_dlt_root_id)
                        }
                    }
                },
            )
        else:
            if self.root_key:
                propagation_config = config["propagation"]
                propagation_config["root"].pop(data_normalizer.c_dlt_id)

    @property
    def schema_contract(self) -> TSchemaContract:
        return self.schema.settings.get("schema_contract")

    @schema_contract.setter
    def schema_contract(self, settings: TSchemaContract) -> None:
        self.schema.set_schema_contract(settings)

    @property
    def exhausted(self) -> bool:
        """Check all selected pipes whether one of them has started. if so, the source is exhausted."""
        for resource in self._resources.extracted.values():
            item = resource._pipe.gen
            if inspect.isgenerator(item):
                if inspect.getgeneratorstate(item) != "GEN_CREATED":
                    return True
        return False

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
                partial_table = normalize_table_identifiers(
                    r.compute_table_schema(item), self._schema.naming
                )
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

    def add_limit(self, max_items: int) -> "DltSource":  # noqa: A003
        """Adds a limit `max_items` yielded from all selected resources in the source that are not transformers.

        This is useful for testing, debugging and generating sample datasets for experimentation. You can easily get your test dataset in a few minutes, when otherwise
        you'd need to wait hours for the full loading to complete.

        Notes:
            1. Transformers resources won't be limited. They should process all the data they receive fully to avoid inconsistencies in generated datasets.
            2. Each yielded item may contain several records. `add_limit` only limits the "number of yields", not the total number of records.

        Args:
            max_items (int): The maximum number of items to yield
        Returns:
            "DltSource": returns self
        """
        for resource in self.resources.selected.values():
            resource.add_limit(max_items)
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
        self_run: SupportsPipelineRun = makefun.partial(
            Container()[PipelineContext].pipeline().run, *(), data=self
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
            source_state_key=self.name,
        )

    def __getattr__(self, resource_name: str) -> DltResource:
        try:
            return self._resources[resource_name]
        except KeyError:
            raise AttributeError(
                f"Resource with name {resource_name} not found in source {self.name}"
            )

    def __setattr__(self, name: str, value: Any) -> None:
        if isinstance(value, DltResource):
            self.resources[name] = value
        else:
            super().__setattr__(name, value)

    def __str__(self) -> str:
        info = (
            f"DltSource {self.name} section {self.section} contains"
            f" {len(self.resources)} resource(s) of which {len(self.selected_resources)} are"
            " selected"
        )
        for r in self.resources.values():
            selected_info = "selected" if r.selected else "not selected"
            if r.is_transformer:
                info += (
                    f"\ntransformer {r.name} is {selected_info} and takes data from"
                    f" {r._pipe.parent.name}"
                )
            else:
                info += f"\nresource {r.name} is {selected_info}"
        if self.exhausted:
            info += (
                "\nSource is already iterated and cannot be used again ie. to display or load data."
            )
        else:
            info += (
                "\nIf you want to see the data items in this source you must iterate it or convert"
                " to list ie. list(source)."
            )
        info += " Note that, like any iterator, you can iterate the source only once."
        info += f"\ninstance id: {id(self)}"
        return info
