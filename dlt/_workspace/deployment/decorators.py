import inspect
from functools import update_wrapper, wraps
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Sequence, Type, Union, overload

from typing_extensions import TypeVar

from dlt.common.configuration import get_fun_spec, with_config
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.reflection.inspect import iscoroutinefunction
from dlt.common.typing import AnyFun, Generic, ParamSpec
from dlt.common.utils import get_callable_name, get_module_name

if TYPE_CHECKING:
    from dlt.common.pipeline import SupportsPipeline

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment import freshness as _freshness
from dlt._workspace.deployment import trigger as _triggers
from dlt._workspace.deployment._trigger_helpers import normalize_triggers, parse_period_seconds
from dlt._workspace.deployment.freshness import normalize_freshness_constraints
from dlt.extract.reference import SourceFactory as AnySourceFactory
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource

from dlt._workspace.deployment._job_ref import make_job_ref
from dlt._workspace.deployment.exceptions import InvalidJobName, InvalidJobSection
from dlt._workspace.deployment.typing import (
    TDeliverSpec,
    TEntryPoint,
    TExecuteSpec,
    TExposeSpec,
    TFreshnessConstraint,
    TInterfaceType,
    TIntervalSpec,
    TJobDefinition,
    TJobExposeSpec,
    TJobRef,
    TJobType,
    TRequireSpec,
    TTimeoutSpec,
    TTrigger,
)

TJobFunParams = ParamSpec("TJobFunParams")
TJobResult = TypeVar("TJobResult", default=Any)


def _normalize_timeout(
    value: Union[float, str, TTimeoutSpec],
) -> TTimeoutSpec:
    """Normalize timeout input to TTimeoutSpec for the manifest."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return {"timeout": parse_period_seconds(value)}
    return {"timeout": float(value)}


def _validate_job_name(name: Optional[str]) -> None:
    """Reject decorator names that are not valid Python identifiers."""
    if name is not None and not name.isidentifier():
        raise InvalidJobName(name)


def _validate_job_section(section: Optional[str]) -> None:
    """Reject decorator sections that are not valid Python identifiers."""
    if section is not None and not section.isidentifier():
        raise InvalidJobSection(section)


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
        self.execute: Optional[TExecuteSpec] = None
        self.expose: Optional[TJobExposeSpec] = None
        self.require: Optional[TRequireSpec] = None
        self.deliver: Optional[TDeliverTarget] = None
        self.interval: Optional[TIntervalSpec] = None
        self.freshness: List[TFreshnessConstraint] = []
        self.allow_external_schedulers: bool = False

    @property
    def job_ref(self) -> TJobRef:
        return make_job_ref(self.section, self.name)

    @property
    def success(self) -> TTrigger:
        return _triggers.job_success(self.job_ref)

    @property
    def fail(self) -> TTrigger:
        return _triggers.job_fail(self.job_ref)

    @property
    def completed(self) -> tuple[TTrigger, TTrigger]:
        """Tuple of (success, fail) triggers — fires on any outcome."""
        return (self.success, self.fail)

    @property
    def is_matching_interval_fresh(self) -> TFreshnessConstraint:
        """Downstream interval must be fully covered by this job's completed intervals."""
        return _freshness.is_matching_interval_fresh(self.job_ref)

    @property
    def is_fresh(self) -> TFreshnessConstraint:
        """This job's overall interval (intersected with downstream's) must be complete."""
        return _freshness.is_fresh(self.job_ref)

    def __call__(self, *args: TJobFunParams.args, **kwargs: TJobFunParams.kwargs) -> TJobResult:
        rv: TJobResult = self._deco_f(*args, **kwargs)
        return rv

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

        job_def: TJobDefinition = {
            "job_ref": self.job_ref,
            "entry_point": entry_point,
            "triggers": list(self.trigger),
            "execute": self.execute or TExecuteSpec(),
        }

        if self.expose:
            job_def["expose"] = self.expose  # type: ignore[typeddict-item]

        description = (self._f.__doc__ or "").strip()
        if description:
            job_def["description"] = description

        if self._spec is not None:
            config_keys = list(self._spec.get_resolvable_fields().keys())
            if config_keys:
                job_def["config_keys"] = config_keys

        if self.interval is not None:
            job_def["interval"] = self.interval
        if self.freshness:
            job_def["freshness"] = list(self.freshness)
        if self.allow_external_schedulers:
            job_def["allow_external_schedulers"] = True

        if self.deliver is not None:
            if isinstance(self.deliver, dict):
                job_def["deliver"] = self.deliver  # type: ignore[typeddict-item]
            else:
                job_def["deliver"] = TDeliverSpec(source_ref=_source_ref_from_deliver(self.deliver))

        if self.require is not None:
            job_def["require"] = self.require

        return job_def


def _job(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    section: str = None,
    job_type: TJobType = "batch",
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    deliver: Optional[TDeliverTarget] = None,
    interval: Optional[TIntervalSpec] = None,
    freshness: Union[
        None, str, TFreshnessConstraint, Sequence[Union[str, TFreshnessConstraint]]
    ] = None,
    allow_external_schedulers: bool = False,
    spec: Type[BaseConfiguration] = None,
) -> Any:
    """Common decorator implementation for all job types."""
    _validate_job_name(name)
    _validate_job_section(section)
    wrapper: JobFactory[Any, Any] = JobFactory()
    wrapper.name = name
    wrapper.section = section
    wrapper.job_type = job_type
    wrapper.trigger = normalize_triggers(trigger)
    wrapper.execute = execute
    wrapper.expose = expose
    wrapper.require = require
    wrapper.deliver = deliver
    wrapper.interval = interval
    wrapper.freshness = normalize_freshness_constraints(freshness)
    wrapper.allow_external_schedulers = allow_external_schedulers
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
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    deliver: Optional[TDeliverTarget] = None,
    interval: Optional[TIntervalSpec] = None,
    freshness: Union[
        None, str, TFreshnessConstraint, Sequence[Union[str, TFreshnessConstraint]]
    ] = None,
    allow_external_schedulers: bool = False,
    spec: Type[BaseConfiguration] = None,
) -> JobFactory[TJobFunParams, TJobResult]: ...


@overload
def job(
    func: None = ...,
    /,
    name: str = None,
    section: str = None,
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    deliver: Optional[TDeliverTarget] = None,
    interval: Optional[TIntervalSpec] = None,
    freshness: Union[
        None, str, TFreshnessConstraint, Sequence[Union[str, TFreshnessConstraint]]
    ] = None,
    allow_external_schedulers: bool = False,
    spec: Type[BaseConfiguration] = None,
) -> Callable[[Callable[TJobFunParams, TJobResult]], JobFactory[TJobFunParams, TJobResult]]: ...


def job(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    section: str = None,
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    deliver: Optional[TDeliverTarget] = None,
    interval: Optional[TIntervalSpec] = None,
    freshness: Union[
        None, str, TFreshnessConstraint, Sequence[Union[str, TFreshnessConstraint]]
    ] = None,
    allow_external_schedulers: bool = False,
    spec: Type[BaseConfiguration] = None,
) -> Any:
    """Marks a function as a deployable batch job.

    Args:
        func: The function to decorate.

        name: Job name. Defaults to the function name.

        section: Config section. Defaults to the module name.

        trigger: One or more trigger strings or TTrigger values.

        execute: Execution constraints. Accepts `TExecuteSpec` with:
            `timeout` (seconds, human string like `"4h"`, or `TTimeoutSpec` dict),
            `concurrency` (max concurrent runs, `None` = no limit).

        expose: UI presentation. Accepts `TJobExposeSpec` with:
            `tags` (grouping labels), `starred` (top-level UI visibility),
            `manual` (`False` to disable manual triggering).

        require: Runtime resource requirements. Accepts `TRequireSpec` with:
            `extras` (pyproject.toml extras for venv), `profile` (workspace profile),
            `machine` (machine spec), `region` (runner placement).

        deliver: A `@dlt.source`, standalone `@dlt.resource`, or called source
            instance for delivery association.

        interval: Overall time range for interval-based scheduling.

        freshness: Upstream freshness constraints. Accepts a single constraint
            string, `TFreshnessConstraint`, or a list of them.

        allow_external_schedulers: When `True`, intervals and state are managed
            by the scheduler rather than the job itself.

        spec: Optional configuration spec class.

    Returns:
        JobFactory: Preserves the original function's signature and return type.
    """
    return _job(
        func,
        name=name,
        section=section,
        job_type="batch",
        trigger=trigger,
        execute=execute,
        expose=expose,
        require=require,
        deliver=deliver,
        interval=interval,
        freshness=freshness,
        allow_external_schedulers=allow_external_schedulers,
        spec=spec,
    )


@overload
def interactive(
    func: Callable[TJobFunParams, TJobResult],
    /,
    name: str = None,
    section: str = None,
    interface: TInterfaceType = "gui",
    idle_timeout: Union[None, float, str] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    spec: Type[BaseConfiguration] = None,
) -> JobFactory[TJobFunParams, TJobResult]: ...


@overload
def interactive(
    func: None = ...,
    /,
    name: str = None,
    section: str = None,
    interface: TInterfaceType = "gui",
    idle_timeout: Union[None, float, str] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    spec: Type[BaseConfiguration] = None,
) -> Callable[[Callable[TJobFunParams, TJobResult]], JobFactory[TJobFunParams, TJobResult]]: ...


def interactive(
    func: Optional[AnyFun] = None,
    /,
    name: str = None,
    section: str = None,
    interface: TInterfaceType = "gui",
    idle_timeout: Union[None, float, str] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    spec: Type[BaseConfiguration] = None,
) -> Any:
    """Marks a function as a deployable interactive job.

    Interactive jobs are long-running processes that expose an HTTP endpoint.
    The runtime assigns a port and proxies traffic to the job.

    Args:
        func: The function to decorate.
        name: Job name. Defaults to the function name.
        section: Config section. Defaults to the module name.
        interface: What the job exposes: `"gui"`, `"rest_api"`, or `"mcp"`.
        idle_timeout: Idle timeout as seconds or human string (e.g. `"24h"`).
            Sets `execute.timeout` with `grace_period=5`.
        execute: Execution constraints. Accepts `TExecuteSpec` with:
            `timeout` and `concurrency`. Concurrency defaults to `1` for
            interactive jobs.
        expose: UI presentation. Accepts `TJobExposeSpec` with:
            `tags`, `starred`, `manual`. The `interface` argument is merged
            into expose automatically.
        require: Runtime resource requirements. Accepts `TRequireSpec` with:
            `extras`, `profile`, `machine`, `region`.
        spec: Optional configuration spec class.

    Returns:
        JobFactory: Preserves the original function's signature and return type.
    """
    # build expose with required interface
    full_expose: TExposeSpec = {"interface": interface}
    if expose:
        full_expose.update(expose)

    # build execute: concurrency=1 default, idle_timeout overrides
    exec_spec: TExecuteSpec = dict(execute) if execute else {}  # type: ignore[assignment]
    exec_spec.setdefault("concurrency", 1)
    if idle_timeout is not None:
        timeout = _normalize_timeout(idle_timeout)
        timeout.setdefault("grace_period", 5.0)
        exec_spec["timeout"] = timeout
    elif "timeout" not in exec_spec:
        exec_spec["timeout"] = {"grace_period": 5.0}

    return _job(
        func,
        name=name,
        section=section,
        job_type="interactive",
        trigger=_triggers.http(),
        execute=exec_spec,
        expose=full_expose,
        require=require,
        spec=spec,
    )


def pipeline_run(
    pipeline: Union[str, "SupportsPipeline"],
    /,
    name: str = None,
    section: str = None,
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    interval: Optional[TIntervalSpec] = None,
    freshness: Union[
        None, str, TFreshnessConstraint, Sequence[Union[str, TFreshnessConstraint]]
    ] = None,
    allow_external_schedulers: bool = False,
    spec: Type[BaseConfiguration] = None,
) -> Callable[[Callable[TJobFunParams, TJobResult]], JobFactory[TJobFunParams, TJobResult]]:
    """Creates a job bound to a specific pipeline.

    The decorated function runs as a batch job that operates on the named
    pipeline. The pipeline association is stored in the manifest's deliver
    spec, and the job is categorized as `"pipeline"` in the UI.

    Args:
        pipeline: Pipeline name (str) or `SupportsPipeline` instance.

        name: Pipeline run name. Defaults to the function name.

        section: Config section for pipeline run. Defaults to the module name.

        trigger: One or more trigger strings or TTrigger values.

        execute: Execution constraints (`TExecuteSpec`): `timeout`, `concurrency`.

        expose: UI presentation (`TJobExposeSpec`): `tags`, `starred`, `manual`.

        require: Resource requirements (`TRequireSpec`): `extras`, `profile`,
            `machine`, `region`.

        interval: Overall time range for interval-based scheduling.

        freshness: Upstream freshness constraints.

        allow_external_schedulers: When `True`, intervals managed by scheduler.

        spec: Optional configuration spec class.

    Returns:
        A decorator that wraps the function in a `JobFactory`.
    """
    _validate_job_name(name)
    _validate_job_section(section)
    pipeline_name = pipeline if isinstance(pipeline, str) else pipeline.pipeline_name

    deliver: TDeliverSpec = {"pipeline_name": pipeline_name}
    full_expose: TExposeSpec = dict(expose) if expose else {}  # type: ignore[assignment]
    full_expose.setdefault("category", "pipeline")

    def decorator(
        func: Callable[TJobFunParams, TJobResult]
    ) -> JobFactory[TJobFunParams, TJobResult]:
        return _job(  # type: ignore[no-any-return]
            func,
            name=name,
            section=section,
            job_type="batch",
            trigger=trigger,
            execute=execute,
            expose=full_expose,
            require=require,
            deliver=deliver,  # type: ignore[arg-type]
            interval=interval,
            freshness=freshness,
            allow_external_schedulers=allow_external_schedulers,
            spec=spec,
        )

    return decorator
