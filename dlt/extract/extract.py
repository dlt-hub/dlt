import contextlib
from copy import copy
import os
from typing import ClassVar, Set, Dict, Any, Optional, Set

from dlt.common.configuration.container import Container
from dlt.common.configuration.resolve import inject_section
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.libs.pyarrow import TAnyArrowItem
from dlt.common.pipeline import reset_resource_state
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.exceptions import MissingDependencyException

from dlt.common.runtime import signals
from dlt.common.runtime.collector import Collector, NULL_COLLECTOR
from dlt.common.utils import uniq_id, update_dict_nested
from dlt.common.typing import StrStr, TDataItems, TDataItem
from dlt.common.schema import Schema, utils
from dlt.common.schema.typing import TSchemaContractDict, TSchemaEvolutionMode, TTableSchema, TTableSchemaColumns
from dlt.common.storages import NormalizeStorageConfiguration, NormalizeStorage, DataItemStorage, FileStorage
from dlt.common.configuration.specs import known_sections
from dlt.common.schema.typing import TPartialTableSchema

from dlt.extract.decorators import SourceSchemaInjectableContext
from dlt.extract.exceptions import DataItemRequiredForDynamicTableHints, NameNormalizationClash
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
    def __init__(
            self,
            extract_id: str,
            storage: ExtractorStorage,
            schema: Schema,
            resources_with_items: Set[str],
            collector: Collector = NULL_COLLECTOR
    ) -> None:
        self.schema = schema
        self.collector = collector
        self.resources_with_items = resources_with_items
        self.extract_id = extract_id
        self._table_contracts: Dict[str, TSchemaContractDict] = {}
        self._filtered_tables: Set[str] = set()
        self._filtered_columns: Dict[str, Dict[str, TSchemaEvolutionMode]]
        self._storage = storage

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

    def write_items(self, resource: DltResource, items: TDataItems, meta: Any) -> None:
        """Write `items` to `resource` optionally computing table schemas and revalidating/filtering data"""
        if isinstance(meta, TableNameMeta):
            # write item belonging to table with static name
            self._write_to_static_table(resource, meta.table_name, items)
        else:
            if resource._table_name_hint_fun:
                # table has name or other hints depending on data items
                self._write_to_dynamic_table(resource, items)
            else:
                # write item belonging to table with static name
                self._write_to_static_table(resource, resource.table_name, items)  # type: ignore[arg-type]

    def write_empty_file(self, table_name: str) -> None:
        table_name = self.schema.naming.normalize_table_identifier(table_name)
        self.storage.write_empty_file(self.extract_id, self.schema.name, table_name, None)

    def _write_item(self, table_name: str, resource_name: str, items: TDataItems, columns: TTableSchemaColumns = None) -> None:
        new_rows_count = self.storage.write_data_item(self.extract_id, self.schema.name, table_name, items, columns)
        self.collector.update(table_name, inc=new_rows_count)
        self.resources_with_items.add(resource_name)

    def _write_to_dynamic_table(self, resource: DltResource, items: TDataItems) -> None:
        if not isinstance(items, list):
            items = [items]
        for item in items:
            table_name = self.schema.naming.normalize_table_identifier(resource._table_name_hint_fun(item))
            if table_name in self._filtered_tables:
                continue
            if table_name not in self._table_contracts or resource._table_has_other_dynamic_hints:
                self._compute_and_update_table(resource, table_name, item)
            # write to storage with inferred table name
            if table_name not in self._filtered_tables:
                self._write_item(table_name, resource.name, item)

    def _write_to_static_table(self, resource: DltResource, table_name: str, items: TDataItems) -> None:
        table_name = self.schema.naming.normalize_table_identifier(table_name)
        if table_name not in self._table_contracts:
            self._compute_and_update_table(resource, table_name, items)
        if table_name not in self._filtered_tables:
            self._write_item(table_name, resource.name, items)

    def _compute_table(self, resource: DltResource, data_item: TDataItem) -> TTableSchema:
        """Computes a schema for a new or dynamic table and normalizes identifiers"""
        return self.schema.normalize_table_identifiers(
            resource.compute_table_schema(data_item)
        )

    def _compute_and_update_table(self, resource: DltResource, table_name: str, data_item: TDataItem) -> None:
        """
        Computes new table and does contract checks, if false is returned, the table may not be created and not items should be written
        """
        computed_table = self._compute_table(resource, data_item)
        # overwrite table name (if coming from meta)
        computed_table["name"] = table_name
        # get or compute contract
        schema_contract = self._table_contracts.setdefault(
            table_name,
            self.schema.resolve_contract_settings_for_table(table_name)
        )

        # this is a new table so allow evolve once
        if schema_contract["columns"] != "evolve" and self.schema.is_new_table(table_name):
            computed_table["x-normalizer"] = {"evolve-columns-once": True}  # type: ignore[typeddict-unknown-key]
        existing_table = self.schema._schema_tables.get(table_name, None)
        if existing_table:
            diff_table = utils.merge_tables(existing_table, computed_table)
        else:
            diff_table = computed_table

        # apply contracts
        diff_table, filters = self.schema.apply_schema_contract(schema_contract, diff_table)

        # merge with schema table
        if diff_table:
            self.schema.update_table(diff_table)

        # process filters
        if filters:
            for entity, name, mode in filters:
                if entity == "tables":
                    self._filtered_tables.add(name)
                elif entity == "columns":
                    filtered_columns = self._filtered_columns.setdefault(table_name, {})
                    filtered_columns[name] = mode


class JsonLExtractor(Extractor):
    file_format = "puae-jsonl"


class ArrowExtractor(Extractor):
    file_format = "arrow"



    def write_items(self, resource: DltResource, items: TDataItems, meta: Any) -> None:
        items = [
            # 3. remove columns and rows in data contract filters
            # 2. Remove null-type columns from the table(s) as they can't be loaded
            self._apply_contract_filters(pyarrow.remove_null_columns(tbl)) for tbl in (
                # 1. Convert pandas frame(s) to arrow Table
                pa.Table.from_pandas(item) if (pd and isinstance(item, pd.DataFrame)) else item
                for item in (items if isinstance(items, list) else [items])
            )
        ]
        super().write_items(resource, items, meta)

    def _apply_contract_filters(self, item: TAnyArrowItem) -> TAnyArrowItem:
        # convert arrow schema names into normalized names
        # find matching columns and delete by original name
        return item

    def _get_normalized_arrow_fields(self, resource_name: str, item: TAnyArrowItem) -> StrStr:
        """Normalizes schema field names and returns mapping from original to normalized name. Raises on name clashes"""
        norm_f = self.schema.naming.normalize_identifier
        name_mapping = {n.name: norm_f(n.name) for n in item.schema}
        # verify if names uniquely normalize
        normalized_names = set(name_mapping.values())
        if len(name_mapping) != len(normalized_names):
            raise NameNormalizationClash(resource_name, f"Arrow schema fields normalized from {list(name_mapping.keys())} to {list(normalized_names)}")
        return name_mapping

    def _write_item(self, table_name: str, resource_name: str, items: TDataItems, columns: TTableSchemaColumns = None) -> None:
        # Note: `items` is always a list here due to the conversion in `write_table`
        items = [pyarrow.rename_columns(
            item,
            list(self._get_normalized_arrow_fields(resource_name, item).values())
        )
        for item in items]
        super()._write_item(table_name, resource_name, items, self.schema.tables[table_name]["columns"])

    def _compute_table(self, resource: DltResource, data_item: TDataItem) -> TPartialTableSchema:
        data_item = data_item[0]
        computed_table = super()._compute_table(resource, data_item)

        # Merge the columns to include primary_key and other hints that may be set on the resource
        arrow_table = copy(computed_table)
        arrow_table["columns"] = pyarrow.py_arrow_to_table_schema_columns(data_item.schema)
        # normalize arrow table before merging
        arrow_table = self.schema.normalize_table_identifiers(arrow_table)
        # we must override the columns to preserve the order in arrow table
        arrow_table["columns"] = update_dict_nested(arrow_table["columns"], computed_table["columns"])

        return arrow_table

def extract(
    extract_id: str,
    source: DltSource,
    storage: ExtractorStorage,
    collector: Collector = NULL_COLLECTOR,
    *,
    max_parallel_items: int = None,
    workers: int = None,
    futures_poll_interval: float = None
) -> None:
    schema = source.schema
    resources_with_items: Set[str] = set()
    extractors: Dict[TLoaderFileFormat, Extractor] = {
        "puae-jsonl": JsonLExtractor(
            extract_id, storage, schema, resources_with_items, collector=collector
        ),
        "arrow": ArrowExtractor(
            extract_id, storage, schema, resources_with_items, collector=collector
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
                extractors[item_format].write_items(resource, pipe_item.item, pipe_item.meta)
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
                        extractors["puae-jsonl"].write_empty_file(table["name"])

            if left_gens > 0:
                # go to 100%
                collector.update("Resources", left_gens)

        # flush all buffered writers
        storage.close_writers(extract_id)


def extract_with_schema(
    storage: ExtractorStorage,
    source: DltSource,
    collector: Collector,
    max_parallel_items: int,
    workers: int,
) -> str:
    # generate extract_id to be able to commit all the sources together later
    extract_id = storage.create_extract_id()
    with Container().injectable_context(SourceSchemaInjectableContext(source.schema)):
        # inject the config section with the current source name
        with inject_section(ConfigSectionContext(sections=(known_sections.SOURCES, source.section, source.name), source_state_key=source.name)):
            # reset resource states, the `extracted` list contains all the explicit resources and all their parents
            for resource in source.resources.extracted.values():
                with contextlib.suppress(DataItemRequiredForDynamicTableHints):
                    if resource.write_disposition == "replace":
                        reset_resource_state(resource.name)
            extract(extract_id, source, storage, collector, max_parallel_items=max_parallel_items, workers=workers)

    return extract_id
