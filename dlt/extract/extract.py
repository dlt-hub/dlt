import contextlib
from collections.abc import Sequence as C_Sequence
from copy import copy
import itertools
from typing import List, Set, Dict, Optional, Set, Any
import yaml

from dlt.common.configuration.container import Container
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs import ConfigSectionContext, known_sections
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.data_writers.writers import EMPTY_DATA_WRITER_METRICS
from dlt.common.pipeline import (
    ExtractDataInfo,
    ExtractInfo,
    ExtractMetrics,
    SupportsPipeline,
    WithStepInfo,
    reset_resource_state,
)
from dlt.common.runtime import signals
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.schema import Schema, utils
from dlt.common.schema.typing import (
    TAnySchemaColumns,
    TColumnNames,
    TSchemaContract,
    TWriteDisposition,
)
from dlt.common.storages import NormalizeStorageConfiguration, LoadPackageInfo, SchemaStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import get_callable_name, get_full_class_name

from dlt.extract.decorators import SourceInjectableContext, SourceSchemaInjectableContext
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints
from dlt.extract.incremental import IncrementalResourceWrapper
from dlt.extract.pipe_iterator import PipeIterator
from dlt.extract.source import DltSource
from dlt.extract.resource import DltResource
from dlt.extract.storage import ExtractStorage
from dlt.extract.extractors import JsonLExtractor, ArrowExtractor, Extractor


def data_to_sources(
    data: Any,
    pipeline: SupportsPipeline,
    schema: Schema = None,
    table_name: str = None,
    parent_table_name: str = None,
    write_disposition: TWriteDisposition = None,
    columns: TAnySchemaColumns = None,
    primary_key: TColumnNames = None,
    schema_contract: TSchemaContract = None,
) -> List[DltSource]:
    """Creates a list of sources for data items present in `data` and applies specified hints to all resources.

    `data` may be a DltSource, DltResource, a list of those or any other data type accepted by pipeline.run
    """

    def apply_hint_args(resource: DltResource) -> None:
        resource.apply_hints(
            table_name,
            parent_table_name,
            write_disposition,
            columns,
            primary_key,
            schema_contract=schema_contract,
        )

    def apply_settings(source_: DltSource) -> None:
        # apply schema contract settings
        if schema_contract:
            source_.schema_contract = schema_contract

    def choose_schema() -> Schema:
        """Except of explicitly passed schema, use a clone that will get discarded if extraction fails"""
        if schema:
            schema_ = schema
        # TODO: We should start with a new schema of the same name here ideally, but many tests fail
        # because of this. So some investigation is needed.
        elif pipeline.default_schema_name:
            schema_ = pipeline.schemas[pipeline.default_schema_name].clone()
        else:
            schema_ = pipeline._make_schema_with_default_name()
        return schema_

    effective_schema = choose_schema()

    # a list of sources or a list of resources may be passed as data
    sources: List[DltSource] = []
    resources: List[DltResource] = []

    def append_data(data_item: Any) -> None:
        if isinstance(data_item, DltSource):
            # if schema is explicit then override source schema
            if schema:
                data_item.schema = schema
            sources.append(data_item)
        elif isinstance(data_item, DltResource):
            # do not set section to prevent source that represent a standalone resource
            # to overwrite other standalone resources (ie. parents) in that source
            sources.append(DltSource(effective_schema, "", [data_item]))
        else:
            # iterator/iterable/generator
            # create resource first without table template
            resources.append(
                DltResource.from_data(data_item, name=table_name, section=pipeline.pipeline_name)
            )

    if isinstance(data, C_Sequence) and len(data) > 0:
        # if first element is source or resource
        if isinstance(data[0], (DltResource, DltSource)):
            for item in data:
                append_data(item)
        else:
            append_data(data)
    else:
        append_data(data)

    # add all the appended resources in one source
    if resources:
        sources.append(DltSource(effective_schema, pipeline.pipeline_name, resources))

    # apply hints and settings
    for source in sources:
        apply_settings(source)
        for resource in source.selected_resources.values():
            apply_hint_args(resource)

    return sources


def describe_extract_data(data: Any) -> List[ExtractDataInfo]:
    """Extract source and resource names from data passed to extract"""
    data_info: List[ExtractDataInfo] = []

    def add_item(item: Any) -> bool:
        if isinstance(item, (DltResource, DltSource)):
            # record names of sources/resources
            data_info.append(
                {
                    "name": item.name,
                    "data_type": "resource" if isinstance(item, DltResource) else "source",
                }
            )
            return False
        else:
            # skip None
            if data is not None:
                # any other data type does not have a name - just type
                data_info.append({"name": "", "data_type": type(item).__name__})
            return True

    item: Any = data
    if isinstance(data, C_Sequence) and len(data) > 0:
        for item in data:
            # add_item returns True if non named item was returned. in that case we break
            if add_item(item):
                break
        return data_info

    add_item(item)
    return data_info


class Extract(WithStepInfo[ExtractMetrics, ExtractInfo]):
    def __init__(
        self,
        schema_storage: SchemaStorage,
        normalize_storage_config: NormalizeStorageConfiguration,
        collector: Collector = NULL_COLLECTOR,
        original_data: Any = None,
    ) -> None:
        """optionally saves originally extracted `original_data` to generate extract info"""
        self.collector = collector
        self.schema_storage = schema_storage
        self.extract_storage = ExtractStorage(normalize_storage_config)
        self.original_data: Any = original_data
        super().__init__()

    def _compute_metrics(self, load_id: str, source: DltSource) -> ExtractMetrics:
        # map by job id
        job_metrics = {
            ParsedLoadJobFileName.parse(m.file_path): m
            for m in self.extract_storage.closed_files(load_id)
        }
        # aggregate by table name
        table_metrics = {
            table_name: sum(map(lambda pair: pair[1], metrics), EMPTY_DATA_WRITER_METRICS)
            for table_name, metrics in itertools.groupby(
                job_metrics.items(), lambda pair: pair[0].table_name
            )
        }
        # aggregate by resource name
        resource_metrics = {
            resource_name: sum(map(lambda pair: pair[1], metrics), EMPTY_DATA_WRITER_METRICS)
            for resource_name, metrics in itertools.groupby(
                table_metrics.items(), lambda pair: source.schema.get_table(pair[0])["resource"]
            )
        }
        # collect resource hints
        clean_hints: Dict[str, Dict[str, Any]] = {}
        for resource in source.selected_resources.values():
            # cleanup the hints
            hints = clean_hints[resource.name] = {}
            resource_hints = copy(resource._hints) or resource.compute_table_schema()
            if resource.incremental and "incremental" not in resource_hints:
                resource_hints["incremental"] = resource.incremental  # type: ignore

            for name, hint in resource_hints.items():
                if hint is None or name in ["validator"]:
                    continue
                if name == "incremental":
                    # represent incremental as dictionary (it derives from BaseConfiguration)
                    if isinstance(hint, IncrementalResourceWrapper):
                        hint = hint._incremental
                    # sometimes internal incremental is not bound
                    if hint:
                        hints[name] = dict(hint)  # type: ignore[call-overload]
                    continue
                if name == "original_columns":
                    # this is original type of the columns ie. Pydantic model
                    hints[name] = get_full_class_name(hint)
                    continue
                if callable(hint):
                    hints[name] = get_callable_name(hint)
                    continue
                if name == "columns":
                    if hint:
                        hints[name] = yaml.dump(
                            hint, allow_unicode=True, default_flow_style=False, sort_keys=False
                        )
                    continue
                hints[name] = hint

        return {
            "started_at": None,
            "finished_at": None,
            "schema_name": source.schema.name,
            "job_metrics": {job.job_id(): metrics for job, metrics in job_metrics.items()},
            "table_metrics": table_metrics,
            "resource_metrics": resource_metrics,
            "dag": source.resources.selected_dag,
            "hints": clean_hints,
        }

    def _extract_single_source(
        self,
        load_id: str,
        source: DltSource,
        *,
        max_parallel_items: int = None,
        workers: int = None,
        futures_poll_interval: float = None,
    ) -> None:
        schema = source.schema
        collector = self.collector
        resources_with_items: Set[str] = set()
        extractors: Dict[TLoaderFileFormat, Extractor] = {
            "puae-jsonl": JsonLExtractor(
                load_id, self.extract_storage, schema, resources_with_items, collector=collector
            ),
            "arrow": ArrowExtractor(
                load_id, self.extract_storage, schema, resources_with_items, collector=collector
            ),
        }
        last_item_format: Optional[TLoaderFileFormat] = None

        with collector(f"Extract {source.name}"):
            self._step_info_start_load_id(load_id)
            # yield from all selected pipes
            with PipeIterator.from_pipes(
                source.resources.selected_pipes,
                max_parallel_items=max_parallel_items,
                workers=workers,
                futures_poll_interval=futures_poll_interval,
            ) as pipes:
                left_gens = total_gens = len(pipes._sources)
                collector.update("Resources", 0, total_gens)
                for pipe_item in pipes:
                    curr_gens = len(pipes._sources)
                    if left_gens > curr_gens:
                        delta = left_gens - curr_gens
                        left_gens -= delta
                        collector.update("Resources", delta)

                    signals.raise_if_signalled()

                    resource = source.resources[pipe_item.pipe.name]
                    # Fallback to last item's format or default (puae-jsonl) if the current item is an empty list
                    item_format = (
                        Extractor.item_format(pipe_item.item) or last_item_format or "puae-jsonl"
                    )
                    extractors[item_format].write_items(resource, pipe_item.item, pipe_item.meta)
                    last_item_format = item_format

                # find defined resources that did not yield any pipeitems and create empty jobs for them
                # NOTE: do not include incomplete tables. those tables have never seen data so we do not need to reset them
                data_tables = {t["name"]: t for t in schema.data_tables(include_incomplete=False)}
                tables_by_resources = utils.group_tables_by_resource(data_tables)
                for resource in source.resources.selected.values():
                    if (
                        resource.write_disposition != "replace"
                        or resource.name in resources_with_items
                    ):
                        continue
                    if resource.name not in tables_by_resources:
                        continue
                    for table in tables_by_resources[resource.name]:
                        # we only need to write empty files for the top tables
                        if not table.get("parent", None):
                            extractors["puae-jsonl"].write_empty_items_file(table["name"])

                if left_gens > 0:
                    # go to 100%
                    collector.update("Resources", left_gens)

            # flush all buffered writers
            self.extract_storage.close_writers(load_id)
            # gather metrics
            self._step_info_complete_load_id(load_id, self._compute_metrics(load_id, source))
            # remove the metrics of files processed in this extract run
            # NOTE: there may be more than one extract run per load id: ie. the resource and then dlt state
            self.extract_storage.remove_closed_files(load_id)

    def extract(
        self,
        source: DltSource,
        max_parallel_items: int,
        workers: int,
    ) -> str:
        # generate load package to be able to commit all the sources together later
        load_id = self.extract_storage.create_load_package(source.discover_schema())
        with Container().injectable_context(
            SourceSchemaInjectableContext(source.schema)
        ), Container().injectable_context(SourceInjectableContext(source)):
            # inject the config section with the current source name
            with inject_section(
                ConfigSectionContext(
                    sections=(known_sections.SOURCES, source.section, source.name),
                    source_state_key=source.name,
                )
            ):
                # reset resource states, the `extracted` list contains all the explicit resources and all their parents
                for resource in source.resources.extracted.values():
                    with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                        if resource.write_disposition == "replace":
                            reset_resource_state(resource.name)

                self._extract_single_source(
                    load_id,
                    source,
                    max_parallel_items=max_parallel_items,
                    workers=workers,
                )
        return load_id

    def commit_packages(self) -> None:
        """Commits all extracted packages to normalize storage"""
        # commit load packages
        for load_id, metrics in self._load_id_metrics.items():
            self.extract_storage.commit_new_load_package(
                load_id, self.schema_storage[metrics[0]["schema_name"]]
            )
        # all load ids got processed, cleanup empty folder
        self.extract_storage.delete_empty_extract_folder()

    def get_step_info(self, pipeline: SupportsPipeline) -> ExtractInfo:
        load_ids = list(self._load_id_metrics.keys())
        load_packages: List[LoadPackageInfo] = []
        metrics: Dict[str, List[ExtractMetrics]] = {}
        for load_id in self._load_id_metrics.keys():
            load_package = self.extract_storage.get_load_package_info(load_id)
            load_packages.append(load_package)
            metrics[load_id] = self._step_info_metrics(load_id)
        return ExtractInfo(
            pipeline,
            metrics,
            describe_extract_data(self.original_data),
            load_ids,
            load_packages,
            pipeline.first_run,
        )
