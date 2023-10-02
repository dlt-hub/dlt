import gzip
from typing import List, IO, Any, Optional, Type, TypeVar, Generic

from dlt.common.utils import uniq_id
from dlt.common.typing import TDataItem, TDataItems
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.data_writers.exceptions import BufferedDataWriterClosed, DestinationCapabilitiesRequired, InvalidFileNameTemplateException
from dlt.common.data_writers.writers import DataWriter
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.configuration import with_config, known_sections, configspec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.destination import DestinationCapabilitiesContext


TWriter = TypeVar("TWriter", bound=DataWriter)


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
        self.closed_files: List[str] = []  # all fully processed files
        # buffered items must be less than max items in file
        self.buffer_max_items = min(buffer_max_items, file_max_items or buffer_max_items)
        self.file_max_bytes = file_max_bytes
        self.file_max_items = file_max_items
        # the open function is either gzip.open or open
        self.open = gzip.open if self._file_format_spec.supports_compression and not disable_compression else open

        self._current_columns: TTableSchemaColumns = None
        self._file_name: str = None
        self._buffered_items: List[TDataItem] = []
        self._writer: TWriter = None
        self._file: IO[Any] = None
        self._closed = False
        try:
            self._rotate_file()
        except TypeError:
            raise InvalidFileNameTemplateException(file_name_template)

    def write_data_item(self, item: TDataItems, columns: TTableSchemaColumns) -> None:
        self._ensure_open()
        # rotate file if columns changed and writer does not allow for that
        # as the only allowed change is to add new column (no updates/deletes), we detect the change by comparing lengths
        if self._writer and not self._writer.data_format().supports_schema_changes and len(columns) != len(self._current_columns):
            assert len(columns) > len(self._current_columns)
            self._rotate_file()
        # until the first chunk is written we can change the columns schema freely
        if columns is not None:
            self._current_columns = dict(columns)
        if isinstance(item, List):
            # items coming in single list will be written together, not matter how many are there
            self._buffered_items.extend(item)
        else:
            self._buffered_items.append(item)
        # flush if max buffer exceeded
        if len(self._buffered_items) >= self.buffer_max_items:
            self._flush_items()
        # rotate the file if max_bytes exceeded
        if self._file:
            # rotate on max file size
            if self.file_max_bytes and self._file.tell() >= self.file_max_bytes:
                self._rotate_file()
            # rotate on max items
            elif self.file_max_items and self._writer.items_count >= self.file_max_items:
                self._rotate_file()

    def write_empty_file(self, columns: TTableSchemaColumns) -> None:
        if columns is not None:
            self._current_columns = dict(columns)
        self._flush_items(allow_empty_file=True)

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

    def _rotate_file(self) -> None:
        self._flush_and_close_file()
        self._file_name = self.file_name_template % uniq_id(5) + "." + self._file_format_spec.file_extension

    def _flush_items(self, allow_empty_file: bool = False) -> None:
        if len(self._buffered_items) > 0 or allow_empty_file:
            # we only open a writer when there are any items in the buffer and first flush is requested
            if not self._writer:
                # create new writer and write header
                if self._file_format_spec.is_binary_format:
                    self._file = self.open(self._file_name, "wb") # type: ignore
                else:
                    self._file = self.open(self._file_name, "wt", encoding="utf-8") # type: ignore
                self._writer = DataWriter.from_file_format(self.file_format, self._file, caps=self._caps)  # type: ignore[assignment]
                self._writer.write_header(self._current_columns)
            # write buffer
            if self._buffered_items:
                self._writer.write_data(self._buffered_items)
            self._buffered_items.clear()

    def _flush_and_close_file(self) -> None:
        # if any buffered items exist, flush them
        self._flush_items()
        # if writer exists then close it
        if self._writer:
            # write the footer of a file
            self._writer.write_footer()
            self._file.close()
            # add file written to the list so we can commit all the files later
            self.closed_files.append(self._file_name)
            self._writer = None
            self._file = None

    def _ensure_open(self) -> None:
        if self._closed:
            raise BufferedDataWriterClosed(self._file_name)
