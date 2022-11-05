from typing import List, IO, Any

from dlt.common.utils import uniq_id
from dlt.common.typing import TDataItem, TDataItems
from dlt.common.data_writers import TLoaderFileFormat
from dlt.common.data_writers.exceptions import BufferedDataWriterClosed, DestinationCapabilitiesRequired, InvalidFileNameTemplateException
from dlt.common.data_writers.writers import DataWriter
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.configuration import with_config
from dlt.common.destination import DestinationCapabilitiesContext


class BufferedDataWriter:

    @with_config(only_kw=True, namespaces=("data_writer",))
    def __init__(
        self,
        file_format: TLoaderFileFormat,
        file_name_template: str,
        *,
        buffer_max_items: int = 5000,
        file_max_items: int = None,
        file_max_bytes: int = None,
        _caps: DestinationCapabilitiesContext = None
    ):
        self.file_format = file_format
        self._file_format_spec = DataWriter.data_format_from_file_format(self.file_format)
        if self._file_format_spec.requires_destination_capabilities and not _caps:
            raise DestinationCapabilitiesRequired(file_format)
        self._caps = _caps
        # validate if template has correct placeholders
        self.file_name_template = file_name_template
        self.all_files: List[str] = []
        # buffered items must be less than max items in file
        self.buffer_max_items = min(buffer_max_items, file_max_items or buffer_max_items)
        self.file_max_bytes = file_max_bytes
        self.file_max_items = file_max_items

        self._current_columns: TTableSchemaColumns = None
        self._file_name: str = None
        self._buffered_items: List[TDataItem] = []
        self._writer: DataWriter = None
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
            self._rotate_file()
        # until the first chunk is written we can change the columns schema freely
        self._current_columns = columns
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
            if self.file_max_items and self._writer.items_count >= self.file_max_items:
                self._rotate_file()

    def close_writer(self) -> None:
        self._ensure_open()
        self._flush_and_close_file()
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def _rotate_file(self) -> None:
        self._flush_and_close_file()
        self._file_name = self.file_name_template % uniq_id(5) + "." + self._file_format_spec.file_extension

    def _flush_items(self) -> None:
        if len(self._buffered_items) > 0:
            # we only open a writer when there are any files in the buffer and first flush is requested
            if not self._writer:
                # create new writer and write header
                if self._file_format_spec.is_binary_format:
                    self._file = open(self._file_name, "wb")
                else:
                    self._file = open(self._file_name, "wt", encoding="utf-8")
                self._writer = DataWriter.from_file_format(self.file_format, self._file, caps=self._caps)
                self._writer.write_header(self._current_columns)
            # write buffer
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
            self.all_files.append(self._file_name)
            self._writer = None
            self._file = None

    def _ensure_open(self) -> None:
        if self._closed:
            raise BufferedDataWriterClosed(self._file_name)