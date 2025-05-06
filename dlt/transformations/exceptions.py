from dlt.common.exceptions import DltException
from dlt.extract.exceptions import DltResourceException


class TransformationException(DltResourceException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class TransformationTypeMismatch(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class LineageFailedException(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class UnknownColumnTypesException(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class IncompatibleDatasetsException(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class TransformationInvalidReturnTypeException(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)
