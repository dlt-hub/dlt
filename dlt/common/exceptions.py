from typing import Any, AnyStr, Dict, List, Sequence, Optional, Iterable, Type, TypedDict


class ExceptionTrace(TypedDict, total=False):
    """Exception trace. NOTE: we intend to change it with an extended line by line trace with code snippets"""

    message: str
    exception_type: str
    docstring: str
    stack_trace: List[str]
    is_terminal: bool
    """Says if exception is terminal if happened to a job during load step"""
    exception_attrs: Dict[str, Any]
    """Public attributes of an exception deriving from DltException (not starting with _)"""
    load_id: str
    """Load id if found in exception attributes"""
    pipeline_name: str
    """Pipeline name if found in exception attributes or in the active pipeline (Container)"""
    source_name: str
    """Source name if found in exception attributes or in Container"""
    resource_name: str
    """Resource name if found in exception attributes"""
    job_id: str
    """Job id if found in exception attributes"""


class DltException(Exception):
    def __reduce__(self) -> Any:
        """Enables exceptions with parametrized constructor to be pickled"""
        return type(self).__new__, (type(self), *self.args), self.__dict__

    def attrs(self) -> Dict[str, Any]:
        """Returns "public" attributes of the DltException"""
        return {
            k: v
            for k, v in vars(self).items()
            if not k.startswith("_") and not callable(v) and not hasattr(self.__class__, k)
        }


class UnsupportedProcessStartMethodException(DltException):
    def __init__(self, method: str) -> None:
        self.method = method
        super().__init__(
            f"Process pool supports only fork start method, {method} not supported. Switch the pool"
            " type to threading"
        )


class CannotInstallDependencies(DltException):
    def __init__(self, dependencies: Sequence[str], interpreter: str, output: AnyStr) -> None:
        self.dependencies = dependencies
        self.interpreter = interpreter
        if isinstance(output, bytes):
            str_output = output.decode("utf-8")
        else:
            str_output = output
        super().__init__(
            f"Cannot install dependencies {', '.join(dependencies)} with {interpreter} and"
            f" pip:\n{str_output}\n"
        )


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
    def __init__(
        self,
        msg: str,
        path: str,
        expected_type: Type[Any] = None,
        field: str = None,
        value: Any = None,
        nested_exceptions: List["DictValidationException"] = None,
    ) -> None:
        self.path = path
        self.expected_type = expected_type
        self.field = field
        self.value = value
        self.nested_exceptions = nested_exceptions
        self.msg = msg
        super().__init__(msg)

    def __str__(self) -> str:
        return f"In path {self.path}: " + self.msg


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
        return "\n".join([f'pip install "{d}"' for d in self.dependencies])


class DependencyVersionException(DltException):
    def __init__(
        self, pkg_name: str, version_found: str, version_required: str, appendix: str = ""
    ) -> None:
        self.pkg_name = pkg_name
        self.version_found = version_found
        self.version_required = version_required
        super().__init__(self._get_msg(appendix))

    def _get_msg(self, appendix: str) -> str:
        msg = (
            f"Found `{self.pkg_name}=={self.version_found}`, while"
            f" `{self.pkg_name}{self.version_required}` is required."
        )
        if appendix:
            msg = msg + "\n" + appendix
        return msg


class SystemConfigurationException(DltException):
    pass


class PipelineException(DltException):
    def __init__(self, pipeline_name: str, msg: str) -> None:
        """Base class for all pipeline exceptions. Should not be raised."""
        self.pipeline_name = pipeline_name
        super().__init__(msg)


class PipelineStateNotAvailable(PipelineException):
    def __init__(self, source_state_key: Optional[str] = None) -> None:
        if source_state_key:
            msg = (
                f"The source {source_state_key} requested the access to pipeline state but no"
                " pipeline is active right now."
            )
        else:
            msg = (
                "The resource you called requested the access to pipeline state but no pipeline is"
                " active right now."
            )
        msg += (
            " Call dlt.pipeline(...) before you call the @dlt.source or  @dlt.resource decorated"
            " function."
        )
        self.source_state_key = source_state_key
        super().__init__(None, msg)


class ResourceNameNotAvailable(PipelineException):
    def __init__(self) -> None:
        super().__init__(
            None,
            "A resource state was requested but no active extract pipe context was found. Resource"
            " state may be only requested from @dlt.resource decorated function or with explicit"
            " resource name.",
        )


class SourceSectionNotAvailable(PipelineException):
    def __init__(self) -> None:
        msg = (
            "Access to state was requested without source section active. State should be requested"
            " from within the @dlt.source and @dlt.resource decorated function."
        )
        super().__init__(None, msg)
