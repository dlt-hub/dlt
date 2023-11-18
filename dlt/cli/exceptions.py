from dlt.common.exceptions import DltException


class CliCommandException(DltException):
    def __init__(self, cmd: str, msg: str, inner_exc: Exception = None) -> None:
        self.cmd = cmd
        self.inner_exc = inner_exc
        super().__init__(msg)


class VerifiedSourceRepoError(DltException):
    def __init__(self, msg: str, source_name: str) -> None:
        self.source_name = source_name
        super().__init__(msg)
