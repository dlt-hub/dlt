from dlt_plus.common.exceptions import DltPlusException


class TransformException(DltPlusException):
    def __init__(self, msg: str):
        super().__init__(msg)


class TransformationTypeMismatch(TransformException):
    def __init__(self, msg: str):
        super().__init__(msg)


class MaterializationTypeMismatch(TransformException):
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
