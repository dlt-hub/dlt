from dlt.common.exceptions import DltException


class TransformException(DltException):
    def __init__(self, msg: str):
        super().__init__(msg)


class TransformationTypeMismatch(TransformException):
    def __init__(self, msg: str):
        super().__init__(msg)


class LineageFailedException(TransformException):
    def __init__(self, msg: str):
        super().__init__(msg)


class UnknownColumnTypesException(TransformException):
    def __init__(self, msg: str):
        super().__init__(msg)


class IncompatibleDatasetsException(TransformException):
    def __init__(self, msg: str):
        super().__init__(msg)


class TransformationInvalidReturnTypeException(TransformException):
    def __init__(self, msg: str):
        super().__init__(msg)
