from dlt.common.exceptions import DltException


class DataWriterException(DltException):
    pass


class InvalidFileNameTemplateException(DataWriterException, ValueError):
    def __init__(self, file_name_template: str):
        self.file_name_template = file_name_template
        super().__init__(f"Wrong file name template {file_name_template}. File name template must contain exactly one %s formatter")
