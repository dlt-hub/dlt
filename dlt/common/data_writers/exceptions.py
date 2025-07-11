from typing import NamedTuple, Sequence

from dlt.common.destination import TLoaderFileFormat
from dlt.common.exceptions import DltException


class DataWriterException(DltException):
    pass


class InvalidFileNameTemplateException(DataWriterException, ValueError):
    def __init__(self, file_name_template: str):
        self.file_name_template = file_name_template
        super().__init__(
            f"Wrong file name template `{file_name_template}`. File name template must contain"
            " exactly one %s formatter"
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


class CompressionConfigMismatchException(DataWriterException):
    def __init__(self, file_path: str, is_file_compressed: bool, should_compress: bool):
        self.file_path = file_path
        file_state = "compressed" if is_file_compressed else "uncompressed"
        writer_state = "compressed" if should_compress else "uncompressed"
        suggested_value = "False" if is_file_compressed else "True"

        super().__init__(
            f"Compression-configuration mismatch while importing '{file_path}'.\n"
            f"• Source file: {file_state}\n"
            f"• Writer expects: {writer_state}\n\n"
            "Fix one of the following:\n"
            f"  • In your `config.toml`, set `disable_compression = {suggested_value}` "
            "under the [data_writer] section, **or**\n"
            "  • Set the environment variable "
            f"`DATA_WRITER__DISABLE_COMPRESSION={suggested_value}`.\n"
        )
