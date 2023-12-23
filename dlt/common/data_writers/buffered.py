import gzip
import time
from typing import List, IO, Any, Optional, Type, TypeVar, Generic

from dlt.common.typing import TDataItem, TDataItems
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.data_writers.exceptions import (
    BufferedDataWriterClosed,
    DestinationCapabilitiesRequired,
    InvalidFileNameTemplateException,
)
from dlt.common.data_writers.writers import DataWriter, DataWriterMetrics
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.configuration import with_config, known_sections, configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.utils import uniq_id

TWriter = TypeVar("TWriter", bound=DataWriter)


def new_file_id() -> str:
    """Creates new file id which is globally unique within table_name scope"""
    return uniq_id(5)


class BufferedDataWriter(Generic[TWriter]):
    @configspec
    class BufferedDataWriterConfiguration(BaseConfiguration):
        buffer_max_items: int = 5000
        file_max_items: Optional[int] = None
        file_max_bytes: Optional[int] = None
        disable_compression: bool = False
        _caps: Optional[DestinationCapabilitiesContext] = None

        __section__ = known_sections.DATA_WRITER

    @with_config(spec=BufferedDataWriterConfiguration)
    def __init__(
        self,
        file_format: TLoaderFileFormat,
        file_name_template: str,
        *,
        buffer_max_items: int = 5000,
        file_max_items: int = None,
        file_max_bytes: int = None,
        disable_compression: bool = False,
        _caps: DestinationCapabilitiesContext = None
    ):
        self.file_format = file_format
        self._file_format_spec = DataWriter.data_format_from_file_format(self.file_format)
        if self._file_format_spec.requires_destination_capabilities and not _caps:
            raise DestinationCapabilitiesRequired(file_format)
        self._caps = _caps
        # validate if template has correct placeholders
        self.file_name_template = file_name_template
        self.closed_files: List[DataWriterMetrics] = []  # all fully processed files
        # buffered items must be less than max items in file
        self.buffer_max_items = min(buffer_max_items, file_max_items or buffer_max_items)
        self.file_max_bytes = file_max_bytes
        self.file_max_items = file_max_items
        # the open function is either gzip.open or open
        self.open = (
            gzip.open
            if self._file_format_spec.supports_compression and not disable_compression
            else open
        )

        self._current_columns: TTableSchemaColumns = None
        self._file_name: str = None
        self._buffered_items: List[TDataItem] = []
        self._buffered_items_count: int = 0
        self._writer: TWriter = None
        self._file: IO[Any] = None
        self._created: float = None
        self._last_modified: float = None
        self._closed = False
        try:
            self._rotate_file()
        except TypeError:
            raise InvalidFileNameTemplateException(file_name_template)

    def write_data_item(self, item: TDataItems, columns: TTableSchemaColumns) -> int:
        if self._closed:
            self._rotate_file()
            self._closed = False
        # rotate file if columns changed and writer does not allow for that
        # as the only allowed change is to add new column (no updates/deletes), we detect the change by comparing lengths
        if (
            self._writer
            and not self._writer.data_format().supports_schema_changes
            and len(columns) != len(self._current_columns)
        ):
            assert len(columns) > len(self._current_columns)
            self._rotate_file()
        # until the first chunk is written we can change the columns schema freely
        if columns is not None:
            self._current_columns = dict(columns)

        new_rows_count: int
        if isinstance(item, List):
            # items coming in single list will be written together, not matter how many are there
            self._buffered_items.extend(item)
            # update row count, if item supports "num_rows" it will be used to count items
            if len(item) > 0 and hasattr(item[0], "num_rows"):
                new_rows_count = sum(tbl.num_rows for tbl in item)
            else:
                new_rows_count = len(item)
        else:
            self._buffered_items.append(item)
            # update row count, if item supports "num_rows" it will be used to count items
            if hasattr(item, "num_rows"):
                new_rows_count = item.num_rows
            else:
                new_rows_count = 1
        self._buffered_items_count += new_rows_count
        # flush if max buffer exceeded
        if self._buffered_items_count >= self.buffer_max_items:
            self._flush_items()
        # set last modification date
        self._last_modified = time.time()
        # rotate the file if max_bytes exceeded
        if self._file:
            # rotate on max file size
            if self.file_max_bytes and self._file.tell() >= self.file_max_bytes:
                self._rotate_file()
            # rotate on max items
            elif self.file_max_items and self._writer.items_count >= self.file_max_items:
                self._rotate_file()
        return new_rows_count

    def write_empty_file(self, columns: TTableSchemaColumns) -> DataWriterMetrics:
        """Writes empty file: only header and footer without actual items. Closed the
        empty file and returns metrics. Mind that header and footer will be written."""
        self._rotate_file()
        if columns is not None:
            self._current_columns = dict(columns)
        self._last_modified = time.time()
        return self._rotate_file(allow_empty_file=True)

    def import_file(self, file_path: str, metrics: DataWriterMetrics) -> DataWriterMetrics:
        """Import a file from `file_path` into items storage under a new file name. Does not check
        the imported file format. Uses counts from `metrics` as a base. Logically closes the imported file

        The preferred import method is a hard link to avoid copying the data. If current filesystem does not
        support it, a regular copy is used.
        """
        # TODO: we should separate file storage from other storages. this creates circular deps
        from dlt.common.storages import FileStorage

        self._rotate_file()
        FileStorage.link_hard_with_fallback(file_path, self._file_name)
        self._last_modified = time.time()
        metrics = metrics._replace(
            file_path=self._file_name,
            created=self._created,
            last_modified=self._last_modified or self._created,
        )
        self.closed_files.append(metrics)
        # reset current file
        self._file_name = None
        self._last_modified = None
        self._created = None
        # get ready for a next one
        self._rotate_file()
        return metrics

    def close(self) -> None:
        self._ensure_open()
        self._flush_and_close_file()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> "BufferedDataWriter[TWriter]":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: Any) -> None:
        self.close()

    def _rotate_file(self, allow_empty_file: bool = False) -> DataWriterMetrics:
        metrics = self._flush_and_close_file(allow_empty_file)
        self._file_name = (
            self.file_name_template % new_file_id() + "." + self._file_format_spec.file_extension
        )
        self._created = time.time()
        return metrics

    def _flush_items(self, allow_empty_file: bool = False) -> None:
        if self._buffered_items_count > 0 or allow_empty_file:
            # we only open a writer when there are any items in the buffer and first flush is requested
            if not self._writer:
                # create new writer and write header
                if self._file_format_spec.is_binary_format:
                    self._file = self.open(self._file_name, "wb")  # type: ignore
                else:
                    self._file = self.open(self._file_name, "wt", encoding="utf-8")  # type: ignore
                self._writer = DataWriter.from_file_format(self.file_format, self._file, caps=self._caps)  # type: ignore[assignment]
                self._writer.write_header(self._current_columns)
            # write buffer
            if self._buffered_items:
                self._writer.write_data(self._buffered_items)
            # reset buffer and counter
            self._buffered_items.clear()
            self._buffered_items_count = 0

    def _flush_and_close_file(self, allow_empty_file: bool = False) -> DataWriterMetrics:
        # if any buffered items exist, flush them
        self._flush_items(allow_empty_file)
        # if writer exists then close it
        if not self._writer:
            return None
        # write the footer of a file
        self._writer.write_footer()
        self._file.flush()
        # add file written to the list so we can commit all the files later
        metrics = DataWriterMetrics(
            self._file_name,
            self._writer.items_count,
            self._file.tell(),
            self._created,
            self._last_modified,
        )
        self.closed_files.append(metrics)
        self._file.close()
        self._writer = None
        self._file = None
        self._file_name = None
        self._created = None
        self._last_modified = None
        return metrics

    def _ensure_open(self) -> None:
        if self._closed:
            raise BufferedDataWriterClosed(self._file_name)
