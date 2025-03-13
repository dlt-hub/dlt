from typing import Dict, Any, List
from abc import ABC, abstractmethod

from dlt.common import logger
from dlt.common.metrics import DataWriterMetrics
from dlt.common.schema import TTableSchemaColumns
from dlt.common.typing import TDataItems
from dlt.common.data_writers import (
    BufferedDataWriter,
    DataWriter,
    FileWriterSpec,
)


class DataItemStorage(ABC):
    def __init__(self, writer_spec: FileWriterSpec, *args: Any) -> None:
        self.writer_spec = writer_spec
        self.writer_cls = DataWriter.writer_class_from_spec(writer_spec)
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
            kwargs = {}
            if self.writer_spec.file_max_items:
                kwargs["file_max_items"] = self.writer_spec.file_max_items
            path = self._get_data_item_path_template(load_id, schema_name, table_name)
            writer = BufferedDataWriter(self.writer_spec, path, **kwargs)
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
        with_extension: str = None,
    ) -> DataWriterMetrics:
        """Import a file from `file_path` into items storage under a new file name. Does not check
        the imported file format. Uses counts from `metrics` as a base. Logically closes the imported file

        The preferred import method is a hard link to avoid copying the data. If current filesystem does not
        support it, a regular copy is used.

        Alternative extension may be provided via `with_extension` so various file formats may be imported into the same folder.
        """
        writer = self._get_writer(load_id, schema_name, table_name)
        return writer.import_file(file_path, metrics, with_extension)

    def close_writers(self, load_id: str, skip_flush: bool = False) -> None:
        """Flush, write footers (skip_flush), write metrics and close files in all
        writers belonging to `load_id` package
        """
        for name, writer in self.buffered_writers.items():
            if name.startswith(load_id) and not writer.closed:
                logger.debug(
                    f"Closing writer for {name} with file {writer._file} and actual name"
                    f" {writer._file_name}"
                )
                writer.close(skip_flush=skip_flush)

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

    @abstractmethod
    def _get_data_item_path_template(self, load_id: str, schema_name: str, table_name: str) -> str:
        """Returns a file template for item writer. note: use %s for file id to create required template format"""
        pass
