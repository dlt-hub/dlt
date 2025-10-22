from dlt.common.exceptions import DltException


class RuntimeException(DltException):
    pass


class RunContextNotAvailable(RuntimeException):
    def __init__(self, run_dir: str, msg: str):
        super().__init__(run_dir, msg)
