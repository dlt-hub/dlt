from typing import Dict, Any, List, Generic
from abc import ABC, abstractmethod

from dlt.common import logger
from dlt.common.schema import TTableSchemaColumns
from dlt.common.typing import TDataItems
from dlt.common.data_writers import TLoaderFileFormat, BufferedDataWriter, DataWriter


class DataItemStorage(ABC):
    def __init__(self, load_file_type: TLoaderFileFormat, *args: Any) -> None:
        self.loader_file_format = load_file_type
        self.buffered_writers: Dict[str, BufferedDataWriter[DataWriter]] = {}
        super().__init__(*args)

    def get_writer(self, load_id: str, schema_name: str, table_name: str) -> BufferedDataWriter[DataWriter]:
        # unique writer id
        writer_id = f"{load_id}.{schema_name}.{table_name}"
        writer = self.buffered_writers.get(writer_id, None)
        if not writer:
            # assign a jsonl writer for each table
            path = self._get_data_item_path_template(load_id, schema_name, table_name)
            writer = BufferedDataWriter(self.loader_file_format, path)
            self.buffered_writers[writer_id] = writer
        return writer

    def write_data_item(self, load_id: str, schema_name: str, table_name: str, item: TDataItems, columns: TTableSchemaColumns) -> None:
        writer = self.get_writer(load_id, schema_name, table_name)
        # write item(s)
        writer.write_data_item(item, columns)

    def write_empty_file(self, load_id: str, schema_name: str, table_name: str, columns: TTableSchemaColumns) -> None:
        writer = self.get_writer(load_id, schema_name, table_name)
        writer.write_empty_file(columns)

    def close_writers(self, extract_id: str) -> None:
        # flush and close all files
        for name, writer in self.buffered_writers.items():
            if name.startswith(extract_id):
                logger.debug(f"Closing writer for {name} with file {writer._file} and actual name {writer._file_name}")
                writer.close()

    def closed_files(self) -> List[str]:
        files: List[str] = []
        for writer in self.buffered_writers.values():
            files.extend(writer.closed_files)

        return files

    @abstractmethod
    def _get_data_item_path_template(self, load_id: str, schema_name: str, table_name: str) -> str:
        # note: use %s for file id to create required template format
        pass
