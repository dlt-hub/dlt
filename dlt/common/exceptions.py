from typing import Any, AnyStr, Sequence, Optional


class DltException(Exception):
    def __reduce__(self) -> Any:
        """Enables exceptions with parametrized constructor to be pickled"""
        return type(self).__new__, (type(self), *self.args), self.__dict__

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


class TerminalException(BaseException):
    """
    Marks an exception that cannot be recovered from, should be mixed in into concrete exception class
    """


class TransientException(BaseException):
    """
    Marks an exception in operation that can be retried, should be mixed in into concrete exception class
    """


class TerminalValueError(ValueError, TerminalException):
    """
    ValueError that is unrecoverable
    """


class SignalReceivedException(KeyboardInterrupt, TerminalException):
    """Raises when signal comes. Derives from `BaseException` to not be caught in regular exception handlers."""
    def __init__(self, signal_code: int) -> None:
        self.signal_code = signal_code
        super().__init__(f"Signal {signal_code} received")


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


class DestinationTerminalException(DestinationException, TerminalException):
    pass


class DestinationTransientException(DestinationException, TransientException):
    pass


class IdentifierTooLongException(DestinationTerminalException):
    def __init__(self, destination_name: str, identifier_type: str, identifier_name: str, max_identifier_length: int) -> None:
        self.destination_name = destination_name
        self.identifier_type = identifier_type
        self.identifier_name = identifier_name
        self.max_identifier_length = max_identifier_length
        super().__init__(f"The length of {identifier_type} {identifier_name} exceeds {max_identifier_length} allowed for {destination_name}")


class DestinationHasFailedJobs(DestinationTerminalException):
    def __init__(self, destination_name: str, load_id: str) -> None:
        self.destination_name = destination_name
        self.load_id = load_id
        super().__init__(f"Destination {destination_name} has failed jobs in load package {load_id}")



class PipelineException(DltException):
    def __init__(self, pipeline_name: str, msg: str) -> None:
        """Base class for all pipeline exceptions. Should not be raised."""
        self.pipeline_name = pipeline_name
        super().__init__(msg)


class PipelineStateNotAvailable(PipelineException):
    def __init__(self, source_name: Optional[str] = None) -> None:
        if source_name:
            msg = f"The source {source_name} requested the access to pipeline state but no pipeline is active right now."
        else:
            msg = "The resource you called requested the access to pipeline state but no pipeline is active right now."
        msg += " Call dlt.pipeline(...) before you call the @dlt.source or  @dlt.resource decorated function."
        self.source_name = source_name
        super().__init__(None, msg)


class SourceSectionNotAvailable(PipelineException):
    def __init__(self) -> None:
        msg = "Access to state was requested without source section active. State should be requested from within the @dlt.source and @dlt.resource decorated function."
        super().__init__(None, msg)
