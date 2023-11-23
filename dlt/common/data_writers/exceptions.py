from dlt.common.destination import TLoaderFileFormat
from dlt.common.exceptions import DltException


class DataWriterException(DltException):
    pass


class InvalidFileNameTemplateException(DataWriterException, ValueError):
    def __init__(self, file_name_template: str):
        self.file_name_template = file_name_template
        super().__init__(
            f"Wrong file name template {file_name_template}. File name template must contain"
            " exactly one %s formatter"
        )


class BufferedDataWriterClosed(DataWriterException):
    def __init__(self, file_name: str):
        self.file_name = file_name
        super().__init__(f"Writer with recent file name {file_name} is already closed")


class DestinationCapabilitiesRequired(DataWriterException, ValueError):
    def __init__(self, file_format: TLoaderFileFormat):
        self.file_format = file_format
        super().__init__(
            f"Writer for {file_format} requires destination capabilities which were not provided."
        )
