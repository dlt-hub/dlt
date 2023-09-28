from typing import Any, AnyStr, List, Sequence, Optional, Iterable


class DltException(Exception):
    def __reduce__(self) -> Any:
        """Enables exceptions with parametrized constructor to be pickled"""
        return type(self).__new__, (type(self), *self.args), self.__dict__

class UnsupportedProcessStartMethodException(DltException):
    def __init__(self, method: str) -> None:
        self.method = method
        super().__init__(f"Process pool supports only fork start method, {method} not supported. Switch the pool type to threading")


class CannotInstallDependencies(DltException):
    def __init__(self, dependencies: Sequence[str], interpreter: str, output: AnyStr) -> None:
        self.dependencies = dependencies
        self.interpreter = interpreter
        if isinstance(output, bytes):
            str_output = output.decode("utf-8")
        else:
            str_output = output
        super().__init__(f"Cannot install dependencies {', '.join(dependencies)} with {interpreter} and pip:\n{str_output}\n")


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
        return "\n".join([f"pip install \"{d}\"" for d in self.dependencies])


class SystemConfigurationException(DltException):
    pass


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


class DestinationUndefinedEntity(DestinationTerminalException):
    pass


class DestinationTransientException(DestinationException, TransientException):
    pass


class DestinationLoadingViaStagingNotSupported(DestinationTerminalException):
    def __init__(self, destination: str) -> None:
        self.destination = destination
        super().__init__(f"Destination {destination} does not support loading via staging.")

class DestinationLoadingWithoutStagingNotSupported(DestinationTerminalException):
    def __init__(self, destination: str) -> None:
        self.destination = destination
        super().__init__(f"Destination {destination} does not support loading without staging.")

class DestinationNoStagingMode(DestinationTerminalException):
    def __init__(self, destination: str) -> None:
        self.destination = destination
        super().__init__(f"Destination {destination} cannot be used as a staging")


class DestinationIncompatibleLoaderFileFormatException(DestinationTerminalException):
    def __init__(self, destination: str, staging: str, file_format: str, supported_formats: Iterable[str]) -> None:
        self.destination = destination
        self.staging = staging
        self.file_format = file_format
        self.supported_formats = supported_formats
        supported_formats_str = ", ".join(supported_formats)
        if self.staging:
            if not supported_formats:
                msg = f"Staging {staging} cannot be used with destination {destination} because they have no file formats in common."
            else:
                msg = f"Unsupported file format {file_format} for destination {destination} in combination with staging destination {staging}. Supported formats: {supported_formats_str}"
        else:
            msg = f"Unsupported file format {file_format} destination {destination}. Supported formats: {supported_formats_str}. Check the staging option in the dlt.pipeline for additional formats."
        super().__init__(msg)


class IdentifierTooLongException(DestinationTerminalException):
    def __init__(self, destination_name: str, identifier_type: str, identifier_name: str, max_identifier_length: int) -> None:
        self.destination_name = destination_name
        self.identifier_type = identifier_type
        self.identifier_name = identifier_name
        self.max_identifier_length = max_identifier_length
        super().__init__(f"The length of {identifier_type} {identifier_name} exceeds {max_identifier_length} allowed for {destination_name}")


class DestinationHasFailedJobs(DestinationTerminalException):
    def __init__(self, destination_name: str, load_id: str, failed_jobs: List[Any]) -> None:
        self.destination_name = destination_name
        self.load_id = load_id
        self.failed_jobs = failed_jobs
        super().__init__(f"Destination {destination_name} has failed jobs in load package {load_id}")


class PipelineException(DltException):
    def __init__(self, pipeline_name: str, msg: str) -> None:
        """Base class for all pipeline exceptions. Should not be raised."""
        self.pipeline_name = pipeline_name
        super().__init__(msg)


class PipelineStateNotAvailable(PipelineException):
    def __init__(self, source_state_key: Optional[str] = None) -> None:
        if source_state_key:
            msg = f"The source {source_state_key} requested the access to pipeline state but no pipeline is active right now."
        else:
            msg = "The resource you called requested the access to pipeline state but no pipeline is active right now."
        msg += " Call dlt.pipeline(...) before you call the @dlt.source or  @dlt.resource decorated function."
        self.source_state_key = source_state_key
        super().__init__(None, msg)


class ResourceNameNotAvailable(PipelineException):
    def __init__(self) -> None:
        super().__init__(None,
            "A resource state was requested but no active extract pipe context was found. Resource state may be only requested from @dlt.resource decorated function or with explicit resource name.")


class SourceSectionNotAvailable(PipelineException):
    def __init__(self) -> None:
        msg = "Access to state was requested without source section active. State should be requested from within the @dlt.source and @dlt.resource decorated function."
        super().__init__(None, msg)
