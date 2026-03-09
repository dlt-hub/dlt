"""Job decorators for marking functions as deployable jobs."""

import inspect
from functools import update_wrapper, wraps
from typing import Any, Callable, Dict, List, Optional, Type, Union, overload

from typing_extensions import TypeVar

from dlt.common.configuration import get_fun_spec, with_config
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.reflection.inspect import iscoroutinefunction
from dlt.common.typing import AnyFun, Generic, ParamSpec
from dlt.common.utils import get_callable_name, get_module_name

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment.triggers import normalize_triggers
from dlt.extract.reference import SourceFactory as AnySourceFactory, SourceReference
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource

from dlt._workspace.deployment.typing import (
    DEFAULT_HTTP_PORT,
    TDeliveryRef,
    TEntryPoint,
    TExecutionSpec,
    TExposeSpec,
    TInterfaceType,
    TJobDefinition,
    TJobType,
    TTimeoutSpec,
    TTrigger,
)

TJobFunParams = ParamSpec("TJobFunParams")
TJobResult = TypeVar("TJobResult", default=Any)

_INTERVAL_MULTIPLIERS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _normalize_timeout(
    value: Union[float, str, TTimeoutSpec],
) -> TTimeoutSpec:
    """Normalize timeout input to TTimeoutSpec for the manifest."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        suffix = value[-1].lower()
        if suffix in _INTERVAL_MULTIPLIERS:
            seconds = float(value[:-1]) * _INTERVAL_MULTIPLIERS[suffix]
        else:
            seconds = float(value)
        return {"timeout": seconds}
    return {"timeout": float(value)}


TDeliverTarget = Union[AnySourceFactory[Any, DltSource], DltSource, DltResource]


def _source_ref_from_deliver(deliver: TDeliverTarget) -> str:
    """Extract source_ref string from a SourceFactory, DltSource, or DltResource."""
    if isinstance(deliver, AnySourceFactory):
        return f"sources.{deliver.ref.section}.{deliver.ref.name}"

    if isinstance(deliver, (DltSource, DltResource)):
        factory = getattr(deliver, "_factory", None)
        if factory is None or not isinstance(factory, AnySourceFactory):
            raise ValueError(
                "only top-level standalone resources can be used as deliver target,"
                f" got inner resource {type(deliver).__name__}"
            )
        return f"sources.{factory.ref.section}.{factory.ref.name}"

    raise ValueError(
        "deliver must be a @dlt.source, standalone @dlt.resource, or a called source instance,"
        f" got {type(deliver).__name__}"
    )


class JobFactory(Generic[TJobFunParams, TJobResult]):
    """Callable wrapper for a decorated job function.

    Stores job metadata and provides config injection, async support,
    and trigger properties for job chaining. Preserves the decorated
    function's parameter types and return type via ParamSpec/TypeVar.
    """

    def __init__(self) -> None:
        self._f: AnyFun = None
        self._deco_f: AnyFun = None
        self._spec: Type[BaseConfiguration] = None
        self._user_spec: Type[BaseConfiguration] = None

        self.name: str = None
        self.section: str = None
        self.job_type: TJobType = "batch"
        self.trigger: List[TTrigger] = []
        self.timeout: Union[None, float, str, TTimeoutSpec] = None
        self.concurrency: int = 1
        self.starred: bool = False
        self.tags: List[str] = None
        self.deliver: Optional[TDeliverTarget] = None
        self.expose: Optional[TExposeSpec] = None

    @property
    def job_ref(self) -> str:
        return f"jobs.{self.section}.{self.name}"

    @property
    def success(self) -> TTrigger:
        return TTrigger(f"job.success:{self.job_ref}")

    @property
    def fail(self) -> TTrigger:
        return TTrigger(f"job.fail:{self.job_ref}")

    @property
    def completed(self) -> tuple[TTrigger, TTrigger]:
        """Tuple of (success, fail) triggers — fires on any outcome."""
        return (self.success, self.fail)

    def __call__(self, *args: TJobFunParams.args, **kwargs: TJobFunParams.kwargs) -> TJobResult:
        return self._deco_f(*args, **kwargs)  # type: ignore[no-any-return]

    def bind(self, f: AnyFun) -> "JobFactory[TJobFunParams, TJobResult]":
        """Binds wrapper to the original function. Called once by the decorator."""
        self._f = f
        self.name = self.name or get_callable_name(f)
        func_module = inspect.getmodule(f)
        self.section = self.section or get_module_name(func_module)
        self._wrap(f)
        self._update_wrapper()
        return self

    def _wrap(self, f: AnyFun) -> None:
        """Wraps function with configuration injection."""
        job_sections = (ws_known_sections.JOBS, self.section, self.name)
        conf_f = with_config(f, spec=self._user_spec, sections=job_sections)
        self._spec = get_fun_spec(conf_f)

        @wraps(conf_f)
        def _call(*args: Any, **kwargs: Any) -> Any:
            return conf_f(*args, **kwargs)

        @wraps(conf_f)
        async def _call_coro(*args: Any, **kwargs: Any) -> Any:
            return await conf_f(*args, **kwargs)

        self._deco_f = _call_coro if iscoroutinefunction(f) else _call

    def _update_wrapper(self) -> None:
        """Preserves signature and module from the original function."""
        if not callable(self._f):
            return
        update_wrapper(self, self._f)
        self.__signature__ = inspect.signature(self._f)

    def to_job_definition(self) -> TJobDefinition:
        """Builds a TJobDefinition manifest dict from this wrapper's metadata."""
        entry_point: TEntryPoint = {
            "module": self._f.__module__,
            "function": get_callable_name(self._f),
            "job_type": self.job_type,
        }
        if self.expose is not None:
            entry_point["expose"] = self.expose

        execution: TExecutionSpec = {"concurrency": self.concurrency}
        if self.timeout is not None:
            execution["timeout"] = _normalize_timeout(self.timeout)

        job_def: TJobDefinition = {
            "job_ref": self.job_ref,
            "entry_point": entry_point,
            "triggers": list(self.trigger),
            "execution": execution,
            "starred": self.starred,
        }

        description = (self._f.__doc__ or "").strip()
        if description:
            job_def["description"] = description

        if self._spec is not None:
            config_keys = list(self._spec.get_resolvable_fields().keys())
            if config_keys:
                job_def["config_keys"] = config_keys

        if self.tags:
            job_def["tags"] = list(self.tags)

        if self.deliver is not None:
            job_def["deliver"] = TDeliveryRef(source_ref=_source_ref_from_deliver(self.deliver))

        return job_def


def _job(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    section: str = None,
    job_type: TJobType = "batch",
    trigger: Union[str, TTrigger, List[Union[str, TTrigger]]] = None,
    timeout: Union[None, float, str, TTimeoutSpec] = None,
    concurrency: int = 1,
    starred: bool = False,
    tags: List[str] = None,
    deliver: Optional[TDeliverTarget] = None,
    expose: Optional[TExposeSpec] = None,
    spec: Type[BaseConfiguration] = None,
) -> Any:
    """Common decorator implementation for all job types."""
    wrapper: JobFactory[Any, Any] = JobFactory()
    wrapper.name = name
    wrapper.section = section
    wrapper.job_type = job_type
    wrapper.trigger = normalize_triggers(trigger)
    wrapper.timeout = timeout
    wrapper.concurrency = concurrency
    wrapper.starred = starred
    wrapper.tags = tags
    wrapper.deliver = deliver
    wrapper.expose = expose
    wrapper._user_spec = spec

    if func is None:
        return wrapper.bind
    return wrapper.bind(func)


@overload
def job(
    func: Callable[TJobFunParams, TJobResult],
    /,
    name: str = None,
    section: str = None,
    trigger: Union[str, TTrigger, List[Union[str, TTrigger]]] = None,
    timeout: Union[None, float, str, TTimeoutSpec] = None,
    concurrency: int = 1,
    starred: bool = False,
    tags: List[str] = None,
    deliver: Optional[TDeliverTarget] = None,
    spec: Type[BaseConfiguration] = None,
) -> JobFactory[TJobFunParams, TJobResult]: ...


@overload
def job(
    func: None = ...,
    /,
    name: str = None,
    section: str = None,
    trigger: Union[str, TTrigger, List[Union[str, TTrigger]]] = None,
    timeout: Union[None, float, str, TTimeoutSpec] = None,
    concurrency: int = 1,
    starred: bool = False,
    tags: List[str] = None,
    deliver: Optional[TDeliverTarget] = None,
    spec: Type[BaseConfiguration] = None,
) -> Callable[[Callable[TJobFunParams, TJobResult]], JobFactory[TJobFunParams, TJobResult]]: ...


def job(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    section: str = None,
    trigger: Union[str, TTrigger, List[Union[str, TTrigger]]] = None,
    timeout: Union[None, float, str, TTimeoutSpec] = None,
    concurrency: int = 1,
    starred: bool = False,
    tags: List[str] = None,
    deliver: Optional[TDeliverTarget] = None,
    spec: Type[BaseConfiguration] = None,
) -> Any:
    """Marks a function as a deployable batch job.

    Args:
        func (Optional[AnyFun]): The function to decorate.
        name (str): Job name. Defaults to the function name.
        section (str): Config section. Defaults to the module name.
        trigger: One or more trigger strings or TTrigger values.
        timeout: Max wall-clock duration as seconds, human string (e.g. `"4h"`),
            or `TTimeoutSpec` dict.
        concurrency (int): Max concurrent runs. Defaults to 1.
        starred (bool): Whether this job is starred in the UI.
        tags (List[str]): Optional list of tags.
        deliver (TDeliverTarget): A `@dlt.source`, standalone `@dlt.resource`, or
            called source instance for delivery association.
        spec (Type[BaseConfiguration]): Optional configuration spec class.

    Returns:
        JobFactory: Preserves the original function's signature and return type.
    """
    return _job(
        func,
        name=name,
        section=section,
        job_type="batch",
        trigger=trigger,
        timeout=timeout,
        concurrency=concurrency,
        starred=starred,
        tags=tags,
        deliver=deliver,
        spec=spec,
    )


@overload
def interactive(
    func: Callable[TJobFunParams, TJobResult],
    /,
    name: str = None,
    section: str = None,
    port: int = DEFAULT_HTTP_PORT,
    interface: TInterfaceType = "gui",
    run_params: Dict[str, Any] = None,
    timeout: Union[None, float, str, TTimeoutSpec] = None,
    concurrency: int = 1,
    starred: bool = False,
    tags: List[str] = None,
    spec: Type[BaseConfiguration] = None,
) -> JobFactory[TJobFunParams, TJobResult]: ...


@overload
def interactive(
    func: None = ...,
    /,
    name: str = None,
    section: str = None,
    port: int = DEFAULT_HTTP_PORT,
    interface: TInterfaceType = "gui",
    run_params: Dict[str, Any] = None,
    timeout: Union[None, float, str, TTimeoutSpec] = None,
    concurrency: int = 1,
    starred: bool = False,
    tags: List[str] = None,
    spec: Type[BaseConfiguration] = None,
) -> Callable[[Callable[TJobFunParams, TJobResult]], JobFactory[TJobFunParams, TJobResult]]: ...


def interactive(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    section: str = None,
    port: int = DEFAULT_HTTP_PORT,
    interface: TInterfaceType = "gui",
    run_params: Dict[str, Any] = None,
    timeout: Union[None, float, str, TTimeoutSpec] = None,
    concurrency: int = 1,
    starred: bool = False,
    tags: List[str] = None,
    spec: Type[BaseConfiguration] = None,
) -> Any:
    """Marks a function as a deployable interactive job.

    Interactive jobs are long-running processes that expose an HTTP endpoint.
    The runtime starts the job and proxies traffic to its port.

    Args:
        func (Optional[AnyFun]): The function to decorate.
        name (str): Job name. Defaults to the function name.
        section (str): Config section. Defaults to the module name.
        port (int): HTTP port the job listens on. Defaults to 5000.
        interface (TInterfaceType): What the job exposes: `"gui"`, `"rest_api"`,
            or `"mcp"`. Defaults to `"gui"`.
        run_params (Dict[str, Any]): Framework-specific parameters for the runtime.
        timeout: Max wall-clock duration as seconds, human string (e.g. `"24h"`),
            or `TTimeoutSpec` dict.
        concurrency (int): Max concurrent runs. Defaults to 1.
        starred (bool): Whether this job is starred in the UI.
        tags (List[str]): Optional list of tags (e.g. `"notebook"`, `"dashboard"`).
        spec (Type[BaseConfiguration]): Optional configuration spec class.

    Returns:
        JobFactory: Preserves the original function's signature and return type.
    """
    expose: TExposeSpec = {"interface": interface, "port": port}
    if run_params:
        expose["run_params"] = run_params

    return _job(
        func,
        name=name,
        section=section,
        job_type="interactive",
        trigger=f"http:{port}",
        timeout=timeout,
        concurrency=concurrency,
        starred=starred,
        tags=tags,
        expose=expose,
        spec=spec,
    )
