from dlt.common.exceptions import DltException


class DataWriterException(DltException):
    pass


class InvalidFileNameTemplateException(DataWriterException, ValueError):
    def __init__(self, file_name_template: str):
        self.file_name_template = file_name_template
        super().__init__(f"Wrong file name template {file_name_template}. File name template must contain exactly one %s formatter")


class BufferedDataWriterClosed(DataWriterException):
    def __init__(self, file_name: str):
        self.file_name = file_name
        super().__init__(f"Writer with recent file name {file_name} is already closed")
