import os
from typing import Dict, List, Sequence, Type

from dlt.common import logger
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.utils import uniq_id
from dlt.common.sources import TDirectDataItem, TDataItem
from dlt.common.schema import utils, TSchemaUpdate
from dlt.common.data_writers import BufferedDataWriter
from dlt.common.storages import NormalizeStorage
from dlt.common.configuration import NormalizeVolumeConfiguration


from experiments.pipeline.pipe import PipeIterator
from experiments.pipeline.sources import DltResource, DltSource


class ExtractorStorage(NormalizeStorage):
    EXTRACT_FOLDER = "extract"
    EXTRACT_FILE_NAME_TEMPLATE = ""

    def __init__(self, C: Type[NormalizeVolumeConfiguration]) -> None:
        super().__init__(False, C)
        self.initialize_storage()
        self.buffered_writers: Dict[str, BufferedDataWriter] = {}

    def initialize_storage(self) -> None:
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

    def write_data_item(self, extract_id: str, schema_name: str, table_name: str, item: TDirectDataItem, columns: TTableSchemaColumns) -> None:
        # unique writer id
        writer_id = f"{extract_id}.{schema_name}.{table_name}"
        writer = self.buffered_writers.get(writer_id, None)
        if not writer:
            # assign a jsonl writer with pua encoding for each table, use %s for file id to create required template
            template = NormalizeStorage.build_extracted_file_stem(schema_name, table_name, "%s")
            path = self.storage.make_full_path(os.path.join(self._get_extract_path(extract_id), template))
            writer = BufferedDataWriter("puae-jsonl", path)
            self.buffered_writers[writer_id] = writer
        # write item(s)
        writer.write_data_item(item, columns)

    def close_writers(self, extract_id: str) -> None:
        # flush and close all files
        for name, writer in self.buffered_writers.items():
            if name.startswith(extract_id):
                logger.debug(f"Closing writer for {name} with file {writer._file} and actual name {writer._file_name}")
                writer.close_writer()

    def _get_extract_path(self, extract_id: str) -> str:
        return os.path.join(ExtractorStorage.EXTRACT_FOLDER, extract_id)


def extract(source: DltSource, storage: ExtractorStorage) -> TSchemaUpdate:
    dynamic_tables: TSchemaUpdate = {}
    schema = source.schema
    extract_id = storage.create_extract_id()

    def _write_item(table_name: str, item: TDirectDataItem) -> None:
        # normalize table name before writing so the name match the name in schema
        # note: normalize function should be cached so there's almost no penalty on frequent calling
        # note: column schema is not required for jsonl writer used here
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
    for pipe_item in PipeIterator.from_pipes(source.pipes):
        # get partial table from table template
        print(pipe_item)
        resource = source.resource_by_pipe(pipe_item.pipe)
        if resource._table_name_hint_fun:
            if isinstance(pipe_item.item, List):
                for item in pipe_item.item:
                    _write_dynamic_table(resource, item)
            else:
                _write_dynamic_table(resource, pipe_item.item)
        else:
            # write item belonging to table with static name
            _write_item(resource.name, pipe_item.item)

    # flush all buffered writers
    storage.close_writers(extract_id)
    storage.commit_extract_files(extract_id)

    # returns set of partial tables
    return dynamic_tables

