from typing import Optional

from dlt.common.exceptions import DltException
from dlt.extract.exceptions import DltResourceException


class TransformationException(DltResourceException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class TransformationTypeMismatch(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class LineageFailedException(DltException):
    def __init__(self, msg: Optional[str] = None, *, resource_name: Optional[str] = None):
        super().__init__(msg)
        self.resource_name = resource_name


class UnknownColumnTypesException(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class IncompatibleDatasetsException(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)


class TransformationInvalidReturnTypeException(TransformationException):
    def __init__(self, resource_name: str, msg: str):
        super().__init__(resource_name, msg)
