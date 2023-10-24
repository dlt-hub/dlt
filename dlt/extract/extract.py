import contextlib
import os
from typing import ClassVar, List, Set, Dict, Type, Any, Sequence, Optional
from collections import defaultdict

from dlt.common.configuration.container import Container
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.pipeline import reset_resource_state
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.exceptions import MissingDependencyException

from dlt.common.runtime import signals
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.utils import uniq_id
from dlt.common.typing import TDataItems, TDataItem
from dlt.common.schema import Schema, utils, TSchemaUpdate
from dlt.common.schema.typing import TColumnSchema, TTableSchemaColumns
from dlt.common.storages import NormalizeStorageConfiguration, NormalizeStorage, DataItemStorage, FileStorage
from dlt.common.configuration.specs import known_sections

from dlt.extract.decorators import SourceSchemaInjectableContext
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints
from dlt.extract.pipe import PipeIterator
from dlt.extract.source import DltResource, DltSource
from dlt.extract.typing import TableNameMeta
try:
    from dlt.common.libs import pyarrow
    from dlt.common.libs.pyarrow import pyarrow as pa
except MissingDependencyException:
    pyarrow = None
try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None


class ExtractorItemStorage(DataItemStorage):
    load_file_type: TLoaderFileFormat

    def __init__(self, storage: FileStorage, extract_folder: str="extract") -> None:
        # data item storage with jsonl with pua encoding
        super().__init__(self.load_file_type)
        self.extract_folder = extract_folder
        self.storage = storage


    def _get_data_item_path_template(self, load_id: str, schema_name: str, table_name: str) -> str:
        template = NormalizeStorage.build_extracted_file_stem(schema_name, table_name, "%s")
        return self.storage.make_full_path(os.path.join(self._get_extract_path(load_id), template))

    def _get_extract_path(self, extract_id: str) -> str:
        return os.path.join(self.extract_folder, extract_id)


class JsonLExtractorStorage(ExtractorItemStorage):
    load_file_type: TLoaderFileFormat = "puae-jsonl"


class ArrowExtractorStorage(ExtractorItemStorage):
    load_file_type: TLoaderFileFormat = "arrow"


class ExtractorStorage(NormalizeStorage):
    EXTRACT_FOLDER: ClassVar[str] = "extract"

    """Wrapper around multiple extractor storages with different file formats"""
    def __init__(self, C: NormalizeStorageConfiguration) -> None:
        super().__init__(True, C)
        self._item_storages: Dict[TLoaderFileFormat, ExtractorItemStorage] = {
            "puae-jsonl": JsonLExtractorStorage(self.storage, extract_folder=self.EXTRACT_FOLDER),
            "arrow": ArrowExtractorStorage(self.storage, extract_folder=self.EXTRACT_FOLDER)
        }

    def _get_extract_path(self, extract_id: str) -> str:
        return os.path.join(self.EXTRACT_FOLDER, extract_id)

    def create_extract_id(self) -> str:
        extract_id = uniq_id()
        self.storage.create_folder(self._get_extract_path(extract_id))
        return extract_id

    def get_storage(self, loader_file_format: TLoaderFileFormat) -> ExtractorItemStorage:
        return self._item_storages[loader_file_format]

    def close_writers(self, extract_id: str) -> None:
        for storage in self._item_storages.values():
            storage.close_writers(extract_id)

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

    def write_data_item(self, file_format: TLoaderFileFormat, load_id: str, schema_name: str, table_name: str, item: TDataItems, columns: TTableSchemaColumns) -> None:
        self.get_storage(file_format).write_data_item(load_id, schema_name, table_name, item, columns)



class Extractor:
    file_format: TLoaderFileFormat
    dynamic_tables: TSchemaUpdate
    def __init__(
            self,
            extract_id: str,
            storage: ExtractorStorage,
            schema: Schema,
            resources_with_items: Set[str],
            dynamic_tables: TSchemaUpdate,
            collector: Collector = NULL_COLLECTOR
    ) -> None:
        self._storage = storage
        self.schema = schema
        self.dynamic_tables = dynamic_tables
        self.collector = collector
        self.resources_with_items = resources_with_items
        self.extract_id = extract_id

    @property
    def storage(self) -> ExtractorItemStorage:
        return self._storage.get_storage(self.file_format)

    @staticmethod
    def item_format(items: TDataItems) -> Optional[TLoaderFileFormat]:
        """Detect the loader file format of the data items based on type.
        Currently this is either 'arrow' or 'puae-jsonl'

        Returns:
            The loader file format or `None` if if can't be detected.
        """
        for item in items if isinstance(items, list) else [items]:
            # Assume all items in list are the same type
            if (pyarrow and pyarrow.is_arrow_item(item)) or (pd and isinstance(item, pd.DataFrame)):
                return "arrow"
            return "puae-jsonl"
        return None # Empty list is unknown format

    def write_table(self, resource: DltResource, items: TDataItems, meta: Any) -> None:
        if isinstance(meta, TableNameMeta):
            table_name = meta.table_name
            self._write_static_table(resource, table_name, items)
            self._write_item(table_name, resource.name, items)
        else:
            if resource._table_name_hint_fun:
                if isinstance(items, list):
                    for item in items:
                        self._write_dynamic_table(resource, item)
                else:
                    self._write_dynamic_table(resource, items)
            else:
                # write item belonging to table with static name
                table_name = resource.table_name  # type: ignore[assignment]
                self._write_static_table(resource, table_name, items)
                self._write_item(table_name, resource.name, items)

    def write_empty_file(self, table_name: str) -> None:
        table_name = self.schema.naming.normalize_table_identifier(table_name)
        self.storage.write_empty_file(self.extract_id, self.schema.name, table_name, None)

    def _write_item(self, table_name: str, resource_name: str, items: TDataItems, columns: TTableSchemaColumns = None) -> None:
        # normalize table name before writing so the name match the name in schema
        # note: normalize function should be cached so there's almost no penalty on frequent calling
        # note: column schema is not required for jsonl writer used here
        table_name = self.schema.naming.normalize_identifier(table_name)
        self.collector.update(table_name)
        self.resources_with_items.add(resource_name)
        self.storage.write_data_item(self.extract_id, self.schema.name, table_name, items, columns)

    def _write_dynamic_table(self, resource: DltResource, item: TDataItem) -> None:
        table_name = resource._table_name_hint_fun(item)
        existing_table = self.dynamic_tables.get(table_name)
        if existing_table is None:
            self.dynamic_tables[table_name] = [resource.compute_table_schema(item)]
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
        self._write_item(table_name, resource.name, item)

    def _write_static_table(self, resource: DltResource, table_name: str, items: TDataItems) -> None:
        existing_table = self.dynamic_tables.get(table_name)
        if existing_table is None:
            static_table = resource.compute_table_schema()
            static_table["name"] = table_name
            self.dynamic_tables[table_name] = [static_table]


class JsonLExtractor(Extractor):
    file_format = "puae-jsonl"


class ArrowExtractor(Extractor):
    file_format = "arrow"

    def _rename_columns(self, items: List[TDataItem], new_column_names: List[str]) -> List[TDataItem]:
        """Rename arrow columns to normalized schema column names"""
        if not items:
            return items
        if items[0].schema.names == new_column_names:
            # No need to rename
            return items
        if isinstance(items[0], pyarrow.pyarrow.Table):
            return [item.rename_columns(new_column_names) for item in items]
        elif isinstance(items[0], pyarrow.pyarrow.RecordBatch):
            # Convert the batches to table -> rename -> then back to batches
            return pa.Table.from_batches(items).rename_columns(new_column_names).to_batches()  # type: ignore[no-any-return]
        else:
            raise TypeError(f"Unsupported data item type {type(items[0])}")

    def write_table(self, resource: DltResource, items: TDataItems, meta: Any) -> None:
        items = [
            pyarrow.pyarrow.Table.from_pandas(item) if (pd and isinstance(item, pd.DataFrame)) else item
            for item in (items if isinstance(items, list) else [items])
        ]
        super().write_table(resource, items, meta)

    def _write_item(self, table_name: str, resource_name: str, items: TDataItems, columns: TTableSchemaColumns = None) -> None:
        # Note: `items` is always a list here due to the conversion in `write_table`
        new_columns = list(self.dynamic_tables[table_name][0]["columns"].keys())
        super()._write_item(table_name, resource_name, self._rename_columns(items, new_columns), self.dynamic_tables[table_name][0]["columns"])

    def _write_static_table(self, resource: DltResource, table_name: str, items: TDataItems) -> None:
        existing_table = self.dynamic_tables.get(table_name)
        if existing_table is not None:
            return
        static_table = resource.compute_table_schema()
        if isinstance(items, list):
            item = items[0]
        else:
            item = items
        # Merge the columns to include primary_key and other hints that may be set on the resource
        arrow_columns = pyarrow.py_arrow_to_table_schema_columns(item.schema)
        for key, value in static_table["columns"].items():
            arrow_columns[key] = utils.merge_columns(value, arrow_columns.get(key, {}))
        static_table["columns"] = arrow_columns
        static_table["name"] = table_name
        self.dynamic_tables[table_name] = [self.schema.normalize_table_identifiers(static_table)]


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
    extractors: Dict[TLoaderFileFormat, Extractor] = {
        "puae-jsonl": JsonLExtractor(
            extract_id, storage, schema, resources_with_items, dynamic_tables, collector=collector
        ),
        "arrow": ArrowExtractor(
            extract_id, storage, schema, resources_with_items, dynamic_tables, collector=collector
        )
    }
    last_item_format: Optional[TLoaderFileFormat] = None

    with collector(f"Extract {source.name}"):
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
                # Fallback to last item's format or default (puae-jsonl) if the current item is an empty list
                item_format = Extractor.item_format(pipe_item.item) or last_item_format or "puae-jsonl"
                extractors[item_format].write_table(resource, pipe_item.item, pipe_item.meta)
                last_item_format = item_format

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
                        extractors[last_item_format or "puae-jsonl"].write_empty_file(table["name"])

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
                    schema.update_table(schema.normalize_table_identifiers(partial))

    return extract_id
