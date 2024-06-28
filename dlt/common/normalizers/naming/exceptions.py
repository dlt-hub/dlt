from dlt.common.exceptions import DltException


class NormalizersException(DltException):
    pass


class UnknownNamingModule(ImportError, NormalizersException):
    def __init__(self, naming_module: str) -> None:
        self.naming_module = naming_module
        if "." in naming_module:
            msg = f"Naming module {naming_module} could not be found and imported"
        else:
            msg = f"Naming module {naming_module} is not one of the standard dlt naming conventions"
        super().__init__(msg)


class InvalidNamingModule(NormalizersException):
    def __init__(self, naming_module: str, naming_class: str) -> None:
        self.naming_module = naming_module
        self.naming_class = naming_class
        msg = (
            f"In naming module '{naming_module}' the class '{naming_class}' is not a"
            " NamingConvention"
        )
        super().__init__(msg)
