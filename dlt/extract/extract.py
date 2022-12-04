import os
from typing import ClassVar, List
from dlt.common import signals

from dlt.common.utils import uniq_id
from dlt.common.typing import TDataItems, TDataItem
from dlt.common.schema import utils, TSchemaUpdate
from dlt.common.storages import NormalizeStorage, DataItemStorage
from dlt.common.configuration.specs import NormalizeVolumeConfiguration


from dlt.extract.pipe import PipeIterator
from dlt.extract.source import DltResource, DltSource
from dlt.extract.typing import TableNameMeta


class ExtractorStorage(DataItemStorage, NormalizeStorage):
    EXTRACT_FOLDER: ClassVar[str] = "extract"

    def __init__(self, C: NormalizeVolumeConfiguration) -> None:
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


def extract(extract_id: str, source: DltSource, storage: ExtractorStorage, *, max_parallel_items: int = 100, workers: int = 5, futures_poll_interval: float = 0.01) -> TSchemaUpdate:
    # TODO: add metrics: number of items processed, also per resource and table
    dynamic_tables: TSchemaUpdate = {}
    schema = source.schema

    def _write_item(table_name: str, item: TDataItems) -> None:
        # normalize table name before writing so the name match the name in schema
        # note: normalize function should be cached so there's almost no penalty on frequent calling
        # note: column schema is not required for jsonl writer used here
        # event.pop(DLT_METADATA_FIELD, None)  # type: ignore
        storage.write_data_item(extract_id, schema.name, schema.normalize_table_name(table_name), item, None)

    def _write_dynamic_table(resource: DltResource, item: TDataItem) -> None:
        table_name = resource._table_name_hint_fun(item)
        existing_table = dynamic_tables.get(table_name)
        if existing_table is None:
            dynamic_tables[table_name] = [resource.table_schema(item)]
        else:
            # quick check if deep table merge is required
            if resource._table_has_other_dynamic_hints:
                new_table = resource.table_schema(item)
                # this merges into existing table in place
                utils.merge_tables(existing_table[0], new_table)
            else:
                # if there are no other dynamic hints besides name then we just leave the existing partial table
                pass
        # write to storage with inferred table name
        _write_item(table_name, item)

    # yield from all selected pipes
    for pipe_item in PipeIterator.from_pipes(source.resources.selected_pipes, max_parallel_items=max_parallel_items, workers=workers, futures_poll_interval=futures_poll_interval):
        # TODO: many resources may be returned. if that happens the item meta must be present with table name and this name must match one of resources
        # TDataItemMeta(table_name, requires_resource, write_disposition, columns, parent etc.)
        signals.raise_if_signalled()
        # if meta contains table name
        if isinstance(pipe_item.meta, TableNameMeta):
            table_name = pipe_item.meta.table_name
            existing_table = dynamic_tables.get(table_name)
            if not existing_table:
                dynamic_tables[table_name] = [utils.new_table(table_name)]
            _write_item(table_name, pipe_item.item)
        else:
            resource = source.resources.find_by_pipe(pipe_item.pipe)
            # get partial table from table template
            if resource._table_name_hint_fun:
                if isinstance(pipe_item.item, List):
                    for item in pipe_item.item:
                        _write_dynamic_table(resource, item)
                else:
                    _write_dynamic_table(resource, pipe_item.item)
            else:
                # write item belonging to table with static name
                table_name = resource.table_name
                _write_item(table_name, pipe_item.item)

    # flush all buffered writers
    storage.close_writers(extract_id)

    # returns set of partial tables
    return dynamic_tables

