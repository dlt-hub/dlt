from pathlib import Path
from typing import Dict, Any, List, Sequence
from abc import ABC, abstractmethod

from dlt.common import logger
from dlt.common.destination import TLoaderFileFormat
from dlt.common.schema import TTableSchemaColumns
from dlt.common.typing import StrAny, TDataItems
from dlt.common.data_writers import BufferedDataWriter, DataWriter, DataWriterMetrics


class DataItemStorage(ABC):
    def __init__(self, load_file_type: TLoaderFileFormat, *args: Any) -> None:
        self.loader_file_format = load_file_type
        self.buffered_writers: Dict[str, BufferedDataWriter[DataWriter]] = {}
        super().__init__(*args)

    def get_writer(
        self, load_id: str, schema_name: str, table_name: str
    ) -> BufferedDataWriter[DataWriter]:
        # unique writer id
        writer_id = f"{load_id}.{schema_name}.{table_name}"
        writer = self.buffered_writers.get(writer_id, None)
        if not writer:
            # assign a writer for each table
            path = self._get_data_item_path_template(load_id, schema_name, table_name)
            writer = BufferedDataWriter(self.loader_file_format, path)
            self.buffered_writers[writer_id] = writer
        return writer

    def write_data_item(
        self,
        load_id: str,
        schema_name: str,
        table_name: str,
        item: TDataItems,
        columns: TTableSchemaColumns,
    ) -> int:
        writer = self.get_writer(load_id, schema_name, table_name)
        # write item(s)
        return writer.write_data_item(item, columns)

    def write_empty_file(
        self, load_id: str, schema_name: str, table_name: str, columns: TTableSchemaColumns
    ) -> None:
        writer = self.get_writer(load_id, schema_name, table_name)
        writer.write_empty_file(columns)

    def close_writers(self, load_id: str) -> None:
        # flush and close all files
        for name, writer in self.buffered_writers.items():
            if name.startswith(load_id) and not writer.closed:
                logger.debug(
                    f"Closing writer for {name} with file {writer._file} and actual name"
                    f" {writer._file_name}"
                )
                writer.close()

    def closed_files(self) -> List[DataWriterMetrics]:
        files: List[DataWriterMetrics] = []
        for writer in self.buffered_writers.values():
            files.extend(writer.closed_files)

        return files

    def _write_temp_job_file(
        self,
        load_id: str,
        table_name: str,
        table: TTableSchemaColumns,
        file_id: str,
        rows: Sequence[StrAny],
    ) -> str:
        """Writes new file into new packages "new_jobs". Intended for testing"""
        file_name = (
            self._get_data_item_path_template(load_id, None, table_name) % file_id
            + "."
            + self.loader_file_format
        )
        format_spec = DataWriter.data_format_from_file_format(self.loader_file_format)
        mode = "wb" if format_spec.is_binary_format else "w"
        with self.storage.open_file(file_name, mode=mode) as f:  # type: ignore[attr-defined]
            writer = DataWriter.from_file_format(self.loader_file_format, f)
            writer.write_all(table, rows)
        return Path(file_name).name

    @abstractmethod
    def _get_data_item_path_template(self, load_id: str, schema_name: str, table_name: str) -> str:
        """Returns a file template for item writer. note: use %s for file id to create required template format"""
        pass
