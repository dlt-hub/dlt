from dlt.common.exceptions import DltException


class NormalizersException(DltException):
    pass


class UnknownNamingModule(NormalizersException):
    def __init__(self, naming_module: str) -> None:
        self.naming_module = naming_module
        if "." in naming_module:
            msg = f"Naming module {naming_module} could not be found and imported"
        else:
            msg = f"Naming module {naming_module} is not one of the standard dlt naming convention"
        super().__init__(msg)


class InvalidNamingModule(NormalizersException):
    def __init__(self, naming_module: str) -> None:
        self.naming_module = naming_module
        msg = (
            f"Naming module {naming_module} does not implement required SupportsNamingConvention"
            " protocol"
        )
        super().__init__(msg)
