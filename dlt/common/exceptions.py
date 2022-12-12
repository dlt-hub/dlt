from typing import Any, AnyStr, Sequence


class DltException(Exception):
    pass


class SignalReceivedException(BaseException):
    """Raises when signal comes. Derives from `BaseException` to not be caught in regular exception handlers."""
    def __init__(self, signal_code: int) -> None:
        self.signal_code = signal_code
        super().__init__(f"Signal {signal_code} received")


# class PoolException(DltException):
#     """
#     Thrown by worker pool to pass information when thrown during processing an item
#     """
#     def __init__(self, pool_name: str = None, item: str = None, internal_exception: Exception = None) -> None:
#         # we need it to make it pickle compatible
#         if pool_name:
#             self.pool_name = pool_name
#             self.item = item
#             self.internal_exception = internal_exception
#             super().__init__(f"Pool {pool_name} raised on item {item} with {str(internal_exception)}")


class UnsupportedProcessStartMethodException(DltException):
    def __init__(self, method: str) -> None:
        self.method = method
        super().__init__(f"Process pool supports only fork start method, {method} not supported. Switch the pool type to threading")


class CannotInstallDependency(DltException):
    def __init__(self, dependency: str, interpreter: str, output: AnyStr) -> None:
        self.dependency = dependency
        self.interpreter = interpreter
        if isinstance(output, bytes):
            str_output = output.decode("utf-8")
        else:
            str_output = output
        super().__init__(f"Cannot install dependency {dependency} with {interpreter} and pip:\n{str_output}\n")


class VenvNotFound(DltException):
    def __init__(self, interpreter: str) -> None:
        self.interpreter = interpreter
        super().__init__(f"Venv with interpreter {interpreter} not found in path")


class TerminalException(Exception):
    """
    Marks an exception that cannot be recovered from, should be mixed in into concrete exception class
    """


class TransientException(Exception):
    """
    Marks an exception in operation that can be retried, should be mixed in into concrete exception class
    """


class TerminalValueError(ValueError, TerminalException):
    """
    ValueError that is unrecoverable
    """


class TimeRangeExhaustedException(DltException):
    """
    Raised when backfilling complete and no more time ranges can be generated
    """
    def __init__(self, start_ts: float, end_ts: float) -> None:
        self.start_ts = start_ts
        self.end_ts = end_ts
        super().__init__(f"Timerange ({start_ts} to {end_ts}> exhausted")


class DictValidationException(DltException):
    def __init__(self, msg: str, path: str, field: str = None, value: Any = None) -> None:
        self.path = path
        self.field = field
        self.value = value
        super().__init__(msg)


class ArgumentsOverloadException(DltException):
    def __init__(self, msg: str, func_name: str, *args: str) -> None:
        self.func_name = func_name
        msg = f"Arguments combination not allowed when calling function {func_name}: {msg}"
        msg = "\n".join((msg, *args))
        super().__init__(msg)


class MissingDependencyException(DltException):
    def __init__(self, caller: str, dependencies: Sequence[str], appendix: str = "") -> None:
        self.caller = caller
        self.dependencies = dependencies
        super().__init__(self._get_msg(appendix))

    def _get_msg(self, appendix: str) -> str:
        msg = f"""
You must install additional dependencies to run {self.caller}. If you use pip you may do the following:

{self._to_pip_install()}
"""
        if appendix:
            msg = msg + "\n" + appendix
        return msg

    def _to_pip_install(self) -> str:
        return "\n".join([f"pip install {d}" for d in self.dependencies])


class DestinationException(DltException):
    pass


class UnknownDestinationModule(DestinationException):
    def __init__(self, destination_module: str) -> None:
        self.destination_module = destination_module
        if "." in destination_module:
            msg = f"Destination module {destination_module} could not be found and imported"
        else:
            msg = f"Destination {destination_module} is not one of the standard dlt destinations"
        super().__init__(msg)


class InvalidDestinationReference(DestinationException):
    def __init__(self, destination_module: Any) -> None:
        self.destination_module = destination_module
        msg = f"Destination {destination_module} is not a valid destination module."
        super().__init__(msg)
