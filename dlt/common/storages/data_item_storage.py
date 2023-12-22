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

    def _get_writer(
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
        writer = self._get_writer(load_id, schema_name, table_name)
        # write item(s)
        return writer.write_data_item(item, columns)

    def write_empty_items_file(
        self, load_id: str, schema_name: str, table_name: str, columns: TTableSchemaColumns
    ) -> DataWriterMetrics:
        """Writes empty file: only header and footer without actual items. Closed the
        empty file and returns metrics. Mind that header and footer will be written."""
        writer = self._get_writer(load_id, schema_name, table_name)
        return writer.write_empty_file(columns)

    def import_items_file(
        self,
        load_id: str,
        schema_name: str,
        table_name: str,
        file_path: str,
        metrics: DataWriterMetrics,
    ) -> DataWriterMetrics:
        """Import a file from `file_path` into items storage under a new file name. Does not check
        the imported file format. Uses counts from `metrics` as a base. Logically closes the imported file

        The preferred import method is a hard link to avoid copying the data. If current filesystem does not
        support it, a regular copy is used.
        """
        writer = self._get_writer(load_id, schema_name, table_name)
        return writer.import_file(file_path, metrics)

    def close_writers(self, load_id: str) -> None:
        # flush and close all files
        for name, writer in self.buffered_writers.items():
            if name.startswith(load_id) and not writer.closed:
                logger.debug(
                    f"Closing writer for {name} with file {writer._file} and actual name"
                    f" {writer._file_name}"
                )
                writer.close()

    def closed_files(self, load_id: str) -> List[DataWriterMetrics]:
        """Return metrics for all fully processed (closed) files"""
        files: List[DataWriterMetrics] = []
        for name, writer in self.buffered_writers.items():
            if name.startswith(load_id):
                files.extend(writer.closed_files)

        return files

    def remove_closed_files(self, load_id: str) -> None:
        """Remove metrics for closed files in a given `load_id`"""
        for name, writer in self.buffered_writers.items():
            if name.startswith(load_id):
                writer.closed_files.clear()

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
