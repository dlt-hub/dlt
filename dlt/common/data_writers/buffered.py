import gzip
import time
import contextlib
from typing import ClassVar, Iterator, List, IO, Any, Optional, Type, Generic

from dlt.common.metrics import DataWriterMetrics
from dlt.common.typing import TDataItem, TDataItems
from dlt.common.data_writers.exceptions import (
    BufferedDataWriterClosed,
    DestinationCapabilitiesRequired,
    FileImportNotFound,
    InvalidFileNameTemplateException,
)
from dlt.common.data_writers.writers import TWriter, DataWriter, FileWriterSpec
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.configuration import with_config, known_sections, configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.utils import uniq_id


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

        __section__: ClassVar[str] = known_sections.DATA_WRITER

    @with_config(spec=BufferedDataWriterConfiguration)
    def __init__(
        self,
        writer_spec: FileWriterSpec,
        file_name_template: str,
        *,
        buffer_max_items: int = 5000,
        file_max_items: int = None,
        file_max_bytes: int = None,
        disable_compression: bool = False,
        _caps: DestinationCapabilitiesContext = None,
    ):
        self.writer_spec = writer_spec
        if self.writer_spec.requires_destination_capabilities and not _caps:
            raise DestinationCapabilitiesRequired(self.writer_spec.file_format)
        self.writer_cls = DataWriter.writer_class_from_spec(writer_spec)
        self._supports_schema_changes = self.writer_spec.supports_schema_changes
        self._caps = _caps
        # validate if template has correct placeholders
        self.file_name_template = file_name_template
        self.closed_files: List[DataWriterMetrics] = []  # all fully processed files
        # buffered items must be less than max items in file
        self.buffer_max_items = min(buffer_max_items, file_max_items or buffer_max_items)
        # Explicitly configured max size supersedes destination limit
        self.file_max_bytes = file_max_bytes
        if self.file_max_bytes is None and _caps:
            self.file_max_bytes = _caps.recommended_file_size
        self.file_max_items = file_max_items
        self.should_compress = self.writer_spec.supports_compression and not disable_compression
        # the open function is either gzip.open or open
        self.open = gzip.open if self.should_compress else open

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
            self._current_columns is not None
            and (self._writer or self._supports_schema_changes == "False")
            and self._supports_schema_changes != "True"
            and len(columns) != len(self._current_columns)
        ):
            assert len(columns) > len(self._current_columns)
            self._rotate_file()
        # until the first chunk is written we can change the columns schema freely
        if columns is not None:
            self._current_columns = dict(columns)
        # add item to buffer and count new rows
        new_rows_count = self._buffer_items_with_row_count(item)
        self._buffered_items_count += new_rows_count
        # set last modification date
        self._last_modified = time.time()
        # flush if max buffer exceeded, the second path of the expression prevents empty data frames to pile up in the buffer
        if (
            self._buffered_items_count >= self.buffer_max_items
            or len(self._buffered_items) >= self.buffer_max_items
        ):
            self._flush_items()
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

    def import_file(
        self, file_path: str, metrics: DataWriterMetrics, with_extension: str = None
    ) -> DataWriterMetrics:
        """Import a file from `file_path` into items storage under a new file name. Does not check
        the imported file format. Uses counts from `metrics` as a base. Logically closes the imported file

        The preferred import method is a hard link to avoid copying the data. If current filesystem does not
        support it, a regular copy is used.

        Alternative extension may be provided via `with_extension` so various file formats may be imported into the same folder.
        """
        # TODO: we should separate file storage from other storages. this creates circular deps
        from dlt.common.storages import FileStorage

        # import file with alternative extension
        spec = self.writer_spec
        if with_extension:
            spec = self.writer_spec._replace(file_extension=with_extension)
        with self.alternative_spec(spec):
            (
                self._rotate_file(mark_compressed=True)
                if FileStorage.is_gzipped(file_path)
                else self._rotate_file(mark_compressed=False)
            )
        try:
            FileStorage.link_hard_with_fallback(file_path, self._file_name)
        except FileNotFoundError as f_ex:
            raise FileImportNotFound(file_path, self._file_name) from f_ex

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

    def close(self, skip_flush: bool = False) -> None:
        """Flushes the data, writes footer (skip_flush is True), collects metrics and closes the underlying file."""
        # like regular files, we do not except on double close
        if not self._closed:
            self._flush_and_close_file(skip_flush=skip_flush)
            self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    @contextlib.contextmanager
    def alternative_spec(self, spec: FileWriterSpec) -> Iterator[FileWriterSpec]:
        """Temporarily changes the writer spec ie. for the moment file is rotated"""
        old_spec = self.writer_spec
        try:
            self.writer_spec = spec
            yield spec
        finally:
            self.writer_spec = old_spec

    def __enter__(self) -> "BufferedDataWriter[TWriter]":
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: Any) -> None:
        # skip flush if we had exception
        in_exception = exc_val is not None
        try:
            self.close(skip_flush=in_exception)
        except Exception:
            if not in_exception:
                # close again but without flush
                self.close(skip_flush=True)
            # raise the on close exception if we are not handling another exception
            if not in_exception:
                raise

    def _buffer_items_with_row_count(self, item: TDataItems) -> int:
        """Adds `item` to in-memory buffer and counts new rows, depending in item type"""
        new_rows_count: int
        if isinstance(item, List):
            # update row count, if item supports "num_rows" it will be used to count items
            if len(item) > 0 and hasattr(item[0], "num_rows"):
                new_rows_count = sum(tbl.num_rows for tbl in item)
            else:
                new_rows_count = len(item)
            # items coming in a single list will be written together, no matter how many there are
            self._buffered_items.extend(item)
        else:
            self._buffered_items.append(item)
            # update row count, if item supports "num_rows" it will be used to count items
            if hasattr(item, "num_rows"):
                new_rows_count = item.num_rows
            else:
                new_rows_count = 1
        return new_rows_count

    def _rotate_file(
        self,
        allow_empty_file: bool = False,
        mark_compressed: Optional[bool] = None,
    ) -> DataWriterMetrics:
        metrics = self._flush_and_close_file(allow_empty_file)

        base_filename = self.file_name_template % new_file_id()
        file_extension = self.writer_spec.file_extension

        # Add .gz extension if marked as a compressed imported file or
        # or no imported file mark is provided and configs instruct to compress
        if mark_compressed or (mark_compressed is None and self.should_compress):
            self._file_name = f"{base_filename}.{file_extension}.gz"
        else:
            self._file_name = f"{base_filename}.{file_extension}"

        self._created = time.time()
        return metrics

    def _flush_items(self, allow_empty_file: bool = False) -> None:
        if self._buffered_items or allow_empty_file:
            # we only open a writer when there are any items in the buffer and first flush is requested
            if not self._writer:
                # create new writer and write header
                if self.writer_spec.is_binary_format:
                    self._file = self.open(self._file_name, "wb")  # type: ignore
                else:
                    self._file = self.open(self._file_name, "wt", encoding="utf-8", newline="")
                self._writer = self.writer_cls(self._file, caps=self._caps)  # type: ignore[assignment]
                self._writer.write_header(self._current_columns)
            # write buffer
            if self._buffered_items:
                self._writer.write_data(self._buffered_items)
            # reset buffer and counter
            self._buffered_items.clear()
            self._buffered_items_count = 0

    def _flush_and_close_file(
        self, allow_empty_file: bool = False, skip_flush: bool = False
    ) -> DataWriterMetrics:
        if not skip_flush:
            # if any buffered items exist, flush them
            self._flush_items(allow_empty_file)
            # if writer exists then close it
            if not self._writer:
                return None
            # write the footer of a file
            self._writer.write_footer()
            self._file.flush()
        else:
            if not self._writer:
                return None
        self._writer.close()
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
