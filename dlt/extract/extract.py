import contextlib
import os
from typing import ClassVar, List, Set

from dlt.common.configuration.container import Container
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.pipeline import reset_resource_state

from dlt.common.runtime import signals
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.utils import uniq_id
from dlt.common.typing import TDataItems, TDataItem
from dlt.common.schema import Schema, utils, TSchemaUpdate
from dlt.common.storages import NormalizeStorageConfiguration, NormalizeStorage, DataItemStorage
from dlt.common.configuration.specs import known_sections

from dlt.extract.decorators import SourceSchemaInjectableContext
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints
from dlt.extract.pipe import PipeIterator
from dlt.extract.source import DltResource, DltSource
from dlt.extract.typing import TableNameMeta


class ExtractorStorage(DataItemStorage, NormalizeStorage):
    EXTRACT_FOLDER: ClassVar[str] = "extract"

    def __init__(self, C: NormalizeStorageConfiguration) -> None:
        # data item storage with jsonl with pua encoding
        super().__init__("puae-jsonl", True, C)
        self.storage.create_folder(ExtractorStorage.EXTRACT_FOLDER, exists_ok=True)

    def create_extract_id(self) -> str:
        extract_id = uniq_id()
        self.storage.create_folder(self._get_extract_path(extract_id))
        return extract_id

    def commit_extract_files(self, extract_id: str, with_delete: bool = True) -> None:
        extract_path = self._get_extract_path(extract_id)
        for file in self.storage.list_folder_files(extract_path, to_root=False):
            from_file = os.path.join(extract_path, file)
            to_file = os.path.join(NormalizeStorage.EXTRACTED_FOLDER, file)
            if with_delete:
                self.storage.atomic_rename(from_file, to_file)
            else:
                # create hardlink which will act as a copy
                self.storage.link_hard(from_file, to_file)
        if with_delete:
            self.storage.delete_folder(extract_path, recursively=True)

    def _get_data_item_path_template(self, load_id: str, schema_name: str, table_name: str) -> str:
        template = NormalizeStorage.build_extracted_file_stem(schema_name, table_name, "%s")
        return self.storage.make_full_path(os.path.join(self._get_extract_path(load_id), template))

    def _get_extract_path(self, extract_id: str) -> str:
        return os.path.join(ExtractorStorage.EXTRACT_FOLDER, extract_id)


def extract(
    extract_id: str,
    source: DltSource,
    storage: ExtractorStorage,
    collector: Collector = NULL_COLLECTOR,
    *,
    max_parallel_items: int = None,
    workers: int = None,
    futures_poll_interval: float = None
) -> TSchemaUpdate:

    dynamic_tables: TSchemaUpdate = {}
    schema = source.schema
    resources_with_items: Set[str] = set()

    with collector(f"Extract {source.name}"):

        def _write_empty_file(table_name: str) -> None:
            table_name = schema.naming.normalize_table_identifier(table_name)
            storage.write_empty_file(extract_id, schema.name, table_name, None)

        def _write_item(table_name: str, resource_name: str, item: TDataItems) -> None:
            # normalize table name before writing so the name match the name in schema
            # note: normalize function should be cached so there's almost no penalty on frequent calling
            # note: column schema is not required for jsonl writer used here
            table_name = schema.naming.normalize_table_identifier(table_name)
            collector.update(table_name)
            resources_with_items.add(resource_name)
            storage.write_data_item(extract_id, schema.name, table_name, item, None)

        def _write_dynamic_table(resource: DltResource, item: TDataItem) -> None:
            table_name = resource._table_name_hint_fun(item)
            existing_table = dynamic_tables.get(table_name)
            if existing_table is None:
                dynamic_tables[table_name] = [resource.compute_table_schema(item)]
            else:
                # quick check if deep table merge is required
                if resource._table_has_other_dynamic_hints:
                    new_table = resource.compute_table_schema(item)
                    # this merges into existing table in place
                    utils.merge_tables(existing_table[0], new_table)
                else:
                    # if there are no other dynamic hints besides name then we just leave the existing partial table
                    pass
            # write to storage with inferred table name
            _write_item(table_name, resource.name, item)

        def _write_static_table(resource: DltResource, table_name: str) -> None:
            existing_table = dynamic_tables.get(table_name)
            if existing_table is None:
                static_table = resource.compute_table_schema()
                static_table["name"] = table_name
                dynamic_tables[table_name] = [static_table]

        # yield from all selected pipes
        with PipeIterator.from_pipes(source.resources.selected_pipes, max_parallel_items=max_parallel_items, workers=workers, futures_poll_interval=futures_poll_interval) as pipes:
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
                table_name: str = None
                if isinstance(pipe_item.meta, TableNameMeta):
                    table_name = pipe_item.meta.table_name
                    _write_static_table(resource, table_name)
                    _write_item(table_name, resource.name, pipe_item.item)
                else:
                    # get partial table from table template
                    if resource._table_name_hint_fun:
                        if isinstance(pipe_item.item, List):
                            for item in pipe_item.item:
                                _write_dynamic_table(resource, item)
                        else:
                            _write_dynamic_table(resource, pipe_item.item)
                    else:
                        # write item belonging to table with static name
                        table_name = resource.table_name  # type: ignore
                        _write_static_table(resource, table_name)
                        _write_item(table_name, resource.name, pipe_item.item)

            # find defined resources that did not yield any pipeitems and create empty jobs for them
            data_tables = {t["name"]: t for t in schema.data_tables()}
            tables_by_resources = utils.group_tables_by_resource(data_tables)
            for resource in source.resources.selected.values():
                if resource.write_disposition != "replace" or resource.name in resources_with_items:
                    continue
                if resource.name not in tables_by_resources:
                    continue
                for table in tables_by_resources[resource.name]:
                    # we only need to write empty files for the top tables
                    if not table.get("parent", None):
                        _write_empty_file(table["name"])

            if left_gens > 0:
                # go to 100%
                collector.update("Resources", left_gens)

        # flush all buffered writers
        storage.close_writers(extract_id)

    # returns set of partial tables
    return dynamic_tables


def extract_with_schema(
    storage: ExtractorStorage,
    source: DltSource,
    schema: Schema,
    collector: Collector,
    max_parallel_items: int,
    workers: int
) -> str:
    # generate extract_id to be able to commit all the sources together later
    extract_id = storage.create_extract_id()
    with Container().injectable_context(SourceSchemaInjectableContext(schema)):
        # inject the config section with the current source name
        with inject_section(ConfigSectionContext(sections=(known_sections.SOURCES, source.section, source.name), source_state_key=source.name)):
            # reset resource states, the `extracted` list contains all the explicit resources and all their parents
            for resource in source.resources.extracted.values():
                with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                    if resource.write_disposition == "replace":
                        reset_resource_state(resource.name)

            extractor = extract(extract_id, source, storage, collector, max_parallel_items=max_parallel_items, workers=workers)
            # iterate over all items in the pipeline and update the schema if dynamic table hints were present
            for _, partials in extractor.items():
                for partial in partials:
                    schema.update_schema(schema.normalize_table_identifiers(partial))

    return extract_id

