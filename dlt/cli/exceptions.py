from dlt.common.exceptions import DltException


class CliCommandInnerException(DltException):
    def __init__(self, cmd: str, msg: str, inner_exc: Exception = None) -> None:
        self.cmd = cmd
        self.inner_exc = inner_exc
        super().__init__(msg)


class CliCommandException(DltException):
    """
    Exception that can be thrown inside a cli command and can change the
    error code or docs url presented to the user. Will always be caught.
    """

    def __init__(
        self, error_code: int = -1, docs_url: str = None, raiseable_exception: Exception = None
    ) -> None:
        self.error_code = error_code
        self.docs_url = docs_url
        self.raiseable_exception = raiseable_exception


class VerifiedSourceRepoError(DltException):
    def __init__(self, msg: str, source_name: str) -> None:
        self.source_name = source_name
        super().__init__(msg)
