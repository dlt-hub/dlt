from typing import Any, NamedTuple, Sequence

from dlt.common.destination import TLoaderFileFormat
from dlt.common.exceptions import DltException
from dlt.common.typing import TDataItem


class DataWriterException(DltException):
    pass


class InvalidFileNameTemplateException(DataWriterException, ValueError):
    def __init__(self, file_name_template: str):
        self.file_name_template = file_name_template
        super().__init__(
            f"Wrong file name template `{file_name_template}`. File name template must contain"
            " exactly one %s formatter"
        )


class InvalidEncoding(DataWriterException, ValueError):
    def __init__(self, encoding: str):
        self.encoding = encoding
        super().__init__(
            f"Unknown encoding `{encoding}` in csv format configuration. Use a Python codec name,"
            " e.g. `utf-8`, `utf-8-sig`, `latin-1` or `cp1252`."
        )


class InvalidEncodingErrors(DataWriterException, ValueError):
    def __init__(self, encoding_errors: str):
        self.encoding_errors = encoding_errors
        super().__init__(
            f"Unknown error handler `{encoding_errors}` in `encoding_errors` of csv format"
            " configuration. Use a Python error handler name, e.g. `strict`, `replace`,"
            " `backslashreplace` or `ignore`."
        )


class BufferedDataWriterClosed(DataWriterException):
    def __init__(self, file_name: str):
        self.file_name = file_name
        super().__init__(f"Writer with recent file name `{file_name}` is already closed")


class FileImportNotFound(DataWriterException, FileNotFoundError):
    def __init__(self, import_file_path: str, local_file_path: str) -> None:
        self.import_file_path = import_file_path
        self.local_file_path = local_file_path
        super().__init__(
            f"Attempt to import non existing file `{import_file_path}` into extract storage file"
            f" `{local_file_path}`"
        )


class DestinationCapabilitiesRequired(DataWriterException, ValueError):
    def __init__(self, file_format: TLoaderFileFormat):
        self.file_format = file_format
        super().__init__(
            f"Writer for `{file_format=:}` requires destination capabilities which were not"
            " provided."
        )


class DataWriterNotFound(DataWriterException):
    pass


class FileFormatForItemFormatNotFound(DataWriterNotFound):
    def __init__(self, file_format: TLoaderFileFormat, data_item_format: str):
        self.file_format = file_format
        self.data_item_format = data_item_format
        super().__init__(
            f"Can't find a file writer for `{file_format=:}` and item format `{data_item_format=:}`"
        )


class FileSpecNotFound(KeyError, DataWriterNotFound):
    def __init__(self, file_format: TLoaderFileFormat, data_item_format: str, spec: NamedTuple):
        self.file_format = file_format
        self.data_item_format = data_item_format
        super().__init__(
            f"Can't find a file writer for spec with `{file_format=:}` and `{data_item_format=:}`"
            f" where the full spec is `{spec}`"
        )


class SpecLookupFailed(DataWriterNotFound):
    def __init__(
        self,
        data_item_format: str,
        possible_file_formats: Sequence[TLoaderFileFormat],
        file_format: TLoaderFileFormat,
    ):
        self.file_format = file_format
        self.possible_file_formats = possible_file_formats
        self.data_item_format = data_item_format
        super().__init__(
            f"Failed to find file writer for {data_item_format=:} among file formats"
            f" {possible_file_formats=:}. The preferred file format was `{file_format=:}`."
        )


class InvalidDataItem(DataWriterException):
    def __init__(self, file_format: TLoaderFileFormat, data_item_format: str, details: str):
        self.file_format = file_format
        self.data_item_format = data_item_format
        super().__init__(
            f"A data item of type {data_item_format=:} cannot be written as `{file_format}:"
            f" {details}`"
        )


class FileRotationRequired(DataWriterException):
    """Signals that buffered items cannot be written to the current file because their schema
    does not fit schema of the writer file
    """

    def __init__(self, items: Sequence[TDataItem]) -> None:
        self.items = items
        super().__init__(
            "Data items could not be written to the current file because their schema widens the"
            " schema already locked on the file. The file must be rotated and the items rewritten."
        )


class IncompatibleArrowSchema(DataWriterException):
    """Arrow data cannot be combined into a single parquet schema while type promotion is off."""

    def __init__(self, reason: str, details: str) -> None:
        self.reason = reason
        super().__init__(
            f"{reason} while arrow type promotion is disabled"
            " (`arrow_concat_promote_options='none'`)."
            " Set `data_writer.arrow_concat_promote_options` (env"
            " variable `DATA_WRITER__ARROW_CONCAT_PROMOTE_OPTIONS`) to one of:\n"
            "  - `default`: lossless. Safe differences are merged and incompatible batches are"
            " written to a separate file; data is never altered.\n"
            "  - `permissive`: applies full type promotion which MAY alter data or lose precision"
            " (e.g. `int64` -> `double` or decimal rescale).\n"
            f"Detailed message\n{details}"
        )
