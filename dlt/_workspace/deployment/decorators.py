import inspect
import os
import sys
import warnings
from functools import update_wrapper, wraps
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
    overload,
)

from typing_extensions import TypeVar

from dlt.common import logger
from dlt.common.configuration import get_fun_spec, with_config
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.pipeline import SupportsPipeline, TRefreshMode
from dlt.common.reflection.inspect import iscoroutinefunction
from dlt.common.runtime.run_context import active
from dlt.common.typing import AnyFun, Generic, ParamSpec, Unpack
from dlt.common.utils import get_callable_name, get_module_name
from dlt.common.warnings import TNoExtraKwargs, apply_deprecations

from dlt._workspace import known_sections as ws_known_sections
from dlt._workspace.deployment import freshness as _freshness
from dlt._workspace.deployment import trigger as _triggers
from dlt._workspace.deployment._trigger_helpers import (
    normalize_timeout,
    normalize_triggers,
)
from dlt._workspace.deployment.freshness import normalize_freshness_constraints
from dlt.extract.reference import SourceFactory as AnySourceFactory
from dlt.extract.resource import DltResource
from dlt.extract.source import DltSource

from dlt._workspace.deployment._job_ref import job_category, make_job_ref
from dlt._workspace.deployment.exceptions import (
    InvalidJobName,
    InvalidJobSchema,
    InvalidJobSection,
)
from dlt._workspace.deployment.reflection import (
    entity_properties,
    injectable_fields,
    inputs_from_function,
    job_result_from_return,
)
from dlt._workspace.deployment.agent.configuration import (
    spec_from_agent_inputs,
    warn_unbound_inputs,
    warn_unreferenced_inputs,
)
from dlt._workspace.deployment.agent.manifest import (
    load_agent_spec,
    agent_manifest_path,
    resolve_agent_dir,
    to_agent_definition,
)
from dlt._workspace.deployment.agent.reflection import agent_source, agent_spec_from_function
from dlt._workspace.deployment.agent.typing import TAgentJobResult, TAgentLimits, TAgentSpec
from dlt._workspace.deployment.job_result import running_job
from dlt._workspace.deployment.launchers import (
    DEFAULT_AGENT_LOOP,
    LAUNCHER_AGENT,
    LAUNCHER_JOB,
    agent_loop_group,
)
from dlt._workspace.deployment.typing import (
    TWorkspaceAccess,
    TAgentDefinition,
    TDeliverSpec,
    TEntryPoint,
    TExecuteSpec,
    TExposeSpec,
    TFreshnessConstraint,
    TIncrementalSource,
    TInterfaceType,
    TIntervalSpec,
    TJobDefinition,
    TJobDefinitionDeprecated,
    TJobExposeSpec,
    TJobRef,
    TJobType,
    TRefreshPolicy,
    TRequireSpec,
    TTimeoutSpec,
    TTrigger,
    WORKSPACE_DEPRECATED_SINCE,
)

TJobFunParams = ParamSpec("TJobFunParams")
TJobResult = TypeVar("TJobResult", default=Any)


def _normalize_expose(
    expose: Optional[TJobExposeSpec],
) -> Optional[TJobExposeSpec]:
    """Normalize an expose spec for storage. Wraps single-string `tags` into a list.

    Returns a shallow copy when normalization changes anything; otherwise returns
    `expose` unchanged. Returns `None` if `expose` is `None`.
    """
    if expose is None:
        return None
    tags = expose.get("tags")
    if isinstance(tags, str):
        normalized: TJobExposeSpec = dict(expose)  # type: ignore[assignment]
        normalized["tags"] = [tags]
        return normalized
    return expose


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
        self.incremental_mode: Optional[TIncrementalSource] = None
        self.refresh_propagation: TRefreshPolicy = "auto"
        self.auto_refresh_pipeline_mode: Optional[TRefreshMode] = None
        self.launcher: str = LAUNCHER_JOB
        self.access: Optional[TWorkspaceAccess] = None
        """What the job may touch. Only an agent sets it today."""
        self.inputs: Optional[Dict[str, Any]] = None
        """JSON Schema of the arguments. Read from the function when the manifest is built."""
        self.output: Optional[Dict[str, Any]] = None
        """JSON Schema of the result, when the job returns a `TJobResult`."""

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

        # the stack is pushed inside the coroutine, not around it: `__call__` returns the
        # coroutine and the launcher awaits it later, so an outer scope would pop too early
        @wraps(conf_f)
        def _call(*args: Any, **kwargs: Any) -> Any:
            with running_job(self.job_ref):
                return conf_f(*args, **kwargs)

        @wraps(conf_f)
        async def _call_coro(*args: Any, **kwargs: Any) -> Any:
            with running_job(self.job_ref):
                return await conf_f(*args, **kwargs)

        self._deco_f = _call_coro if iscoroutinefunction(f) else _call

    def _update_wrapper(self) -> None:
        """Preserves signature and module from the original function."""
        if not callable(self._f):
            return
        update_wrapper(self, self._f)
        self.__signature__ = inspect.signature(self._f)

    def _entry_point(self) -> TEntryPoint:
        return {
            "module": self._f.__module__,
            "function": get_callable_name(self._f),
            "job_type": self.job_type,
            "launcher": self.launcher,
        }

    def _description(self) -> str:
        return (self._f.__doc__ or "").strip()

    def config_fields(self) -> Dict[str, Any]:
        """Job arguments configuration injects, name to hint. `config_keys` and `inputs` are these."""
        return injectable_fields(self._spec)

    @property
    def category(self) -> str:
        """Label the job is grouped under, and the middle segment of its result type."""
        deliver = self.deliver if isinstance(self.deliver, dict) else None
        return job_category(self.expose, deliver, self.job_type)

    def _reflect_schemas(self) -> None:
        """Inputs and output of the job, read from the function. Set already, they stand."""
        if self._f is None:
            return
        if self.inputs is None:
            try:
                self.inputs = inputs_from_function(self._f, self.job_ref, self.config_fields())
            except InvalidJobSchema as ex:
                # a job dlt cannot describe still deploys and still runs
                logger.warning(f"Job {self.job_ref} declares no inputs in the manifest: {ex}")
        if self.output is None:
            self.output = job_result_from_return(self._f, self.job_ref)

    def to_job_definition(self) -> TJobDefinition:
        """Builds a TJobDefinition manifest dict from this wrapper's metadata."""
        entry_point = self._entry_point()

        job_def: TJobDefinition = {
            "job_ref": self.job_ref,
            "entry_point": entry_point,
            "triggers": list(self.trigger),
            "execute": self.execute or TExecuteSpec(),
        }

        if self.expose:
            job_def["expose"] = self.expose  # type: ignore[typeddict-item]

        description = self._description()
        if description:
            job_def["description"] = description

        if self.access is not None:
            job_def["access"] = self.access

        config_keys = list(self.config_fields())
        if config_keys:
            job_def["config_keys"] = config_keys

        self._reflect_schemas()
        if (self.inputs or {}).get("properties"):
            job_def["inputs"] = self.inputs
        if self.output:
            job_def["output"] = self.output
        # an unknown entity_type fails here, at manifest time, for inputs and output alike
        entity_properties(self.output, self.job_ref)
        if entities := entity_properties(self.inputs, self.job_ref):
            expose = dict(job_def.get("expose") or {})
            expose["object_type"] = next(iter(entities.values()))
            job_def["expose"] = expose  # type: ignore[typeddict-item]

        if self.interval is not None:
            job_def["interval"] = self.interval
        if self.freshness:
            job_def["freshness"] = list(self.freshness)
        if self.incremental_mode is not None:
            job_def["incremental_mode"] = self.incremental_mode
        if self.refresh_propagation != "auto":
            job_def["refresh_propagation"] = self.refresh_propagation
        if self.auto_refresh_pipeline_mode:
            job_def["auto_refresh_pipeline_mode"] = self.auto_refresh_pipeline_mode

        if self.deliver is not None:
            if isinstance(self.deliver, dict):
                job_def["deliver"] = self.deliver  # type: ignore[typeddict-item]
            else:
                job_def["deliver"] = TDeliverSpec(source_ref=_source_ref_from_deliver(self.deliver))

        if self.require is not None:
            job_def["require"] = self.require

        return job_def


def _make_job_factory(
    *,
    factory_cls: Type[JobFactory[Any, Any]] = JobFactory,
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
    incremental_mode: Optional[TIncrementalSource] = None,
    refresh_propagation: Optional[TRefreshPolicy] = None,
    auto_refresh_pipeline_mode: Optional[TRefreshMode] = None,
    spec: Type[BaseConfiguration] = None,
    deco_name: str = "@job",
    **kwargs: Any,
) -> JobFactory[Any, Any]:
    """Builds an unbound job factory with all metadata normalized."""
    # accept deprecated arg names (including nested `require`), convert, warn
    if require is not None:
        kwargs["require"] = dict(require)
    apply_deprecations(
        TJobDefinitionDeprecated,
        kwargs,
        path=deco_name,
        since=WORKSPACE_DEPRECATED_SINCE,
        stacklevel=4,
    )
    require = kwargs.pop("require", None)
    if incremental_mode is None:
        incremental_mode = kwargs.pop("incremental_mode", None)
    else:
        kwargs.pop("incremental_mode", None)
    if refresh_propagation is None:
        refresh_propagation = kwargs.pop("refresh_propagation", None)
    else:
        kwargs.pop("refresh_propagation", None)
    if kwargs:
        raise TypeError(
            f"{deco_name.lstrip('@')}() got an unexpected keyword argument {next(iter(kwargs))!r}"
        )
    _validate_job_name(name)
    _validate_job_section(section)
    wrapper: JobFactory[Any, Any] = factory_cls()
    wrapper.name = name
    wrapper.section = section
    wrapper.job_type = job_type
    wrapper.trigger = normalize_triggers(trigger)
    # normalize execute and default concurrency to 1 (user can override by passing
    # any value, including None for no-limit)
    exec_spec: TExecuteSpec = dict(execute) if execute else {}  # type: ignore[assignment]
    if "timeout" in exec_spec:
        exec_spec["timeout"] = normalize_timeout(exec_spec["timeout"])
    exec_spec.setdefault("concurrency", 1)
    wrapper.execute = exec_spec
    wrapper.expose = _normalize_expose(expose)
    wrapper.require = require
    wrapper.deliver = deliver
    wrapper.interval = interval
    wrapper.freshness = normalize_freshness_constraints(freshness)
    wrapper.incremental_mode = incremental_mode
    wrapper.refresh_propagation = refresh_propagation or "auto"
    wrapper.auto_refresh_pipeline_mode = auto_refresh_pipeline_mode
    wrapper._user_spec = spec
    return wrapper


def _job(
    func: Optional[AnyFun] = None,
    /,
    **kwargs: Any,
) -> Any:
    """Common decorator implementation for all job types."""
    wrapper = _make_job_factory(**kwargs)
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
    incremental_mode: Optional[TIncrementalSource] = None,
    refresh_propagation: Optional[TRefreshPolicy] = None,
    auto_refresh_pipeline_mode: Optional[TRefreshMode] = None,
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
    incremental_mode: Optional[TIncrementalSource] = None,
    refresh_propagation: Optional[TRefreshPolicy] = None,
    auto_refresh_pipeline_mode: Optional[TRefreshMode] = None,
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
    incremental_mode: Optional[TIncrementalSource] = None,
    refresh_propagation: Optional[TRefreshPolicy] = None,
    auto_refresh_pipeline_mode: Optional[TRefreshMode] = None,
    spec: Type[BaseConfiguration] = None,
    **kwargs: Unpack[TNoExtraKwargs],
) -> Any:
    """Marks a function as a deployable batch job.

    Args:
        func: The function to decorate.

        name: Job name. Defaults to the function name.

        section: Config section. Defaults to the module name.

        trigger: One or more trigger strings or TTrigger values.

        execute: Execution constraints. Accepts `TExecuteSpec` with:
            `timeout` (seconds, human string like `"4h"`, or `TTimeoutSpec` dict),
            `concurrency` (max concurrent runs, defaults to `1`;
            pass `None` to remove the limit).

        expose: UI presentation. Accepts `TJobExposeSpec` with:
            `tags` (grouping labels), `starred` (top-level UI visibility),
            `manual` (`False` to disable manual triggering).

        require: Runtime resource requirements. Accepts `TRequireSpec` with:
            `dependency_groups`, `profile` (workspace profile), `instance`
            (runner instance requirements, e.g. `{"size": "medium"}`; consult
            the online documentation for all supported keys),
            `region` (runner placement), `static_egress_ips`
            (static outbound IPs for third-party allowlists).

        deliver: A `@dlt.source`, standalone `@dlt.resource`, or called source
            instance for delivery association.

        interval: Overall time range for interval-based scheduling.

        freshness: Upstream freshness constraints. Accepts a single constraint
            string, `TFreshnessConstraint`, or a list of them.

        incremental_mode: How incrementals obtain their range during a run.
            `interval` - incrementals assume the interval of the job, state is
            managed by the scheduler. `pipeline` - incrementals keep their own
            state in the pipeline. When not set, falls back to `jobs`
            configuration, then to `pipeline`.

        refresh_propagation: Refresh-signal propagation policy. `auto` (default) passes
            through if this run had `refresh=True`. `always` always clears
            direct downstream `prev_completed_run` on success. `block` never
            propagates. Ignored for interval-store jobs.

        auto_refresh_pipeline_mode: When a refresh run is requested, applies this
            refresh mode to every pipeline created in the job via `pipelines.refresh`
            configuration. Explicit `refresh` arguments on `dlt.pipeline()` still win.

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
        incremental_mode=incremental_mode,
        refresh_propagation=refresh_propagation,
        auto_refresh_pipeline_mode=auto_refresh_pipeline_mode,
        spec=spec,
        **kwargs,
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
    **kwargs: Unpack[TNoExtraKwargs],
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
        execute: Execution constraints. Accepts `TExecuteSpec` with:
            `timeout` and `concurrency`. Concurrency defaults to `1` for
            interactive jobs.
        expose: UI presentation. Accepts `TJobExposeSpec` with:
            `tags`, `starred`, `manual`. The `interface` argument is merged
            into expose automatically.
        require: Runtime resource requirements. Accepts `TRequireSpec` with:
            `dependency_groups`, `profile`, `instance`, `region`, `static_egress_ips`.
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
        exec_spec["timeout"] = normalize_timeout(idle_timeout)

    return _job(
        func,
        name=name,
        section=section,
        job_type="interactive",
        deco_name="@interactive",
        trigger=_triggers.http(),
        execute=exec_spec,
        expose=full_expose,
        require=require,
        spec=spec,
        **kwargs,
    )


def pipeline_run(
    pipeline: Union[str, SupportsPipeline],
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
    incremental_mode: Optional[TIncrementalSource] = None,
    refresh_propagation: Optional[TRefreshPolicy] = None,
    auto_refresh_pipeline_mode: Optional[TRefreshMode] = None,
    spec: Type[BaseConfiguration] = None,
    **kwargs: Unpack[TNoExtraKwargs],
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

        require: Resource requirements (`TRequireSpec`): `dependency_groups`,
            `profile`, `instance`, `region`, `static_egress_ips`.

        interval: Overall time range for interval-based scheduling.

        freshness: Upstream freshness constraints.

        incremental_mode: How incrementals obtain their range during a run.
            `interval` - incrementals assume the interval of the job, state is
            managed by the scheduler. `pipeline` - incrementals keep their own
            state in the pipeline. When not set, falls back to `jobs`
            configuration, then to `pipeline`.

        refresh_propagation: Refresh-signal propagation policy. `auto` (default) passes
            through if this run had `refresh=True`. `always` always clears
            direct downstream `prev_completed_run` on success. `block` never
            propagates. Ignored for interval-store jobs.

        auto_refresh_pipeline_mode: When a refresh run is requested, applies this
            refresh mode to every pipeline created in the job via `pipelines.refresh`
            configuration. Explicit `refresh` arguments on `dlt.pipeline()` still win.

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
            deco_name="@pipeline_run",
            trigger=trigger,
            execute=execute,
            expose=full_expose,
            require=require,
            deliver=deliver,
            interval=interval,
            freshness=freshness,
            incremental_mode=incremental_mode,
            refresh_propagation=refresh_propagation,
            auto_refresh_pipeline_mode=auto_refresh_pipeline_mode,
            spec=spec,
            **kwargs,
        )

    return decorator


def _job_name_from_agent(agent: Union[str, TAgentSpec]) -> str:
    """Job name derived from the agent name: the part after `:`, dashes turned into underscores."""
    name = agent if isinstance(agent, str) else agent["name"]
    return name.rpartition(":")[2].replace("-", "_")


def _workspace_relative(source: str, workspace_root: str) -> str:
    """`<file>:<name>` with the file made relative to the workspace when it sits inside it."""
    path, _, name = source.rpartition(":")
    try:
        return f"{os.path.relpath(path, workspace_root)}:{name}"
    except ValueError:
        return source


def _set_agent(wrapper: "AgentJobFactory[Any, Any]", agent: Union[None, str, TAgentSpec]) -> None:
    """Stores the agent on the factory, by reference or in full."""
    if agent is None:
        return
    if isinstance(agent, str):
        wrapper.agent_ref = agent
    else:
        wrapper.agent_spec = agent
        wrapper.agent_ref = agent["name"]


class AgentJobFactory(JobFactory[TJobFunParams, TJobResult]):
    """Job whose body is an agent loop.

    Declared either by decorating a function that drives the loop from
    `run_context["ai_loop"]`, or by naming an agent, in which case the launcher drives it.
    Either form names the agent with a `"<toolkit>:<agent>"` reference or a `TAgentSpec`.
    """

    def __init__(self) -> None:
        super().__init__()
        self.launcher = LAUNCHER_AGENT
        self.agent_ref: str = None
        self.agent_spec: Optional[TAgentSpec] = None
        """Agent declared inline, instead of referenced by name."""
        self.agent_file: Optional[str] = None
        """Folder the referenced agent was read from, relative to the workspace root."""
        self.loop: str = DEFAULT_AGENT_LOOP
        self.model: str = None
        self.instructions: str = None
        """The user turn opening every run of this job, until configuration says otherwise."""
        self.limits: Optional[TAgentLimits] = None
        self.loop_run_args: Optional[Dict[str, Any]] = None
        self.verbosity: Optional[int] = None
        self.inputs_validator: Optional[AnyFun] = None
        self.outputs_validator: Optional[AnyFun] = None
        self.agent_declaration: Dict[str, Any] = {}
        """`AGENT.md` fields the decorator carried, overriding the agent it referenced."""
        self.agent_definition: Optional[TAgentDefinition] = None
        """Manifest subset of the agent, resolved when the job definition is generated."""
        self._declared_module: str = None
        self._declared_attr: str = None

    @property
    def is_declared(self) -> bool:
        """True when the agent was named instead of decorating a function."""
        return self._f is None

    @property
    def has_agent(self) -> bool:
        """True when the job has an agent: named, given, or declared by the function itself."""
        return bool(self.agent_ref or self.agent_spec or not self.is_declared)

    def resolve_agent_spec(self, workspace_root: str) -> TAgentSpec:
        """The agent in full: declared by the decorated function, given inline, or named."""
        if self.agent_spec is None:
            base: Optional[TAgentSpec] = None
            if self.agent_ref:
                agent_dir = resolve_agent_dir(self.agent_ref, workspace_root)
                base = load_agent_spec(agent_dir)
                self.agent_file = os.path.relpath(agent_manifest_path(agent_dir), workspace_root)

            if self.is_declared:
                self.agent_spec = base
            else:
                source = agent_source(self._f, self.name)
                self.agent_spec = agent_spec_from_function(
                    self._f, source, self.agent_declaration, base
                )
                # a function-declared agent has no AGENT.md: it is the module it lives in
                self.agent_file = _workspace_relative(source, workspace_root)
                self.agent_ref = self.job_ref
        # the agent declares the result. A declared job's inputs are the agent's too; a
        # decorated one takes them from its signature, which is what configuration can inject
        self.output = self.agent_spec["output"]
        if self.is_declared:
            self.inputs = self.agent_spec["inputs"]
        return self.agent_spec

    def declare(self, module_name: str, attr_name: str) -> None:
        """Names the module attribute holding a declared agent, so the launcher can resolve it."""
        self._declared_module = self._declared_module or module_name
        self._declared_attr = self._declared_attr or attr_name
        self.section = self.section or get_module_name(sys.modules[module_name])

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if not self.is_declared:
            return super().__call__(*args, **kwargs)
        # the launcher imports this module, so it can only be reached from inside the call
        from dlt._workspace.deployment.launchers.agent import run_declared_agent

        return run_declared_agent(self, *args, **kwargs)

    def _entry_point(self) -> TEntryPoint:
        if not self.is_declared:
            return super()._entry_point()
        return {
            "module": self._declared_module,
            "function": self._declared_attr,
            "job_type": self.job_type,
            "launcher": self.launcher,
        }

    def _description(self) -> str:
        if not self.is_declared:
            return super()._description()
        # a declared job has no function to describe it, so the agent speaks for it
        return self.agent_spec.get("description", "") if self.agent_spec else ""

    @property
    def category(self) -> str:
        return "background_agent"

    def input_spec(self, agent_spec: TAgentSpec) -> Type[BaseConfiguration]:
        """Job configuration of a declared agent: its inputs, synthesized once."""
        if self._spec is None:
            # no function to inject config into, so the declared inputs are the job config
            self._spec = spec_from_agent_inputs(agent_spec)
        return self._spec

    def _resolve_agent(self) -> None:
        """Reads the agent and takes its job configuration from the inputs it declares."""
        spec = self.resolve_agent_spec(active().run_dir)
        self.agent_definition = to_agent_definition(
            spec, self.agent_file, self.instructions, self.model
        )
        self.access = spec.get("access") or {}
        warn_unreferenced_inputs(spec)
        if self.is_declared:
            self.input_spec(spec)
        else:
            warn_unbound_inputs(spec, self._f)

    def to_job_definition(self) -> TJobDefinition:
        if self.has_agent:
            self._resolve_agent()
        job_def = super().to_job_definition()
        expose: TExposeSpec = dict(job_def.get("expose") or {})  # type: ignore[assignment]
        expose["category"] = self.category  # type: ignore[typeddict-item]
        job_def["expose"] = expose
        if self.agent_definition is not None:
            job_def["agent"] = self.agent_definition
        require: TRequireSpec = dict(job_def.get("require") or {})  # type: ignore[assignment]
        groups = list(require.get("dependency_groups") or [])
        loop_group = agent_loop_group(self.loop)
        if loop_group not in groups:
            groups.append(loop_group)
        require["dependency_groups"] = groups
        job_def["require"] = require
        return job_def


@overload
def agent(
    func: Callable[TJobFunParams, TJobResult],
    /,
    *,
    agent: Union[str, TAgentSpec] = None,
    instructions: str = None,
    access: Optional[TWorkspaceAccess] = None,
    tools: Optional[List[str]] = None,
    skills: Optional[List[str]] = None,
    rules: Optional[List[str]] = None,
    name: str = None,
    section: str = None,
    loop: str = DEFAULT_AGENT_LOOP,
    model: str = None,
    identity: str = None,
    limits: Optional[TAgentLimits] = None,
    loop_run_args: Optional[Dict[str, Any]] = None,
    verbosity: Optional[int] = None,
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    spec: Type[BaseConfiguration] = None,
) -> AgentJobFactory[TJobFunParams, TJobResult]: ...


@overload
def agent(
    func: None = ...,
    /,
    *,
    agent: Union[str, TAgentSpec] = None,
    instructions: str = None,
    access: Optional[TWorkspaceAccess] = None,
    tools: Optional[List[str]] = None,
    skills: Optional[List[str]] = None,
    rules: Optional[List[str]] = None,
    name: str = None,
    section: str = None,
    loop: str = DEFAULT_AGENT_LOOP,
    model: str = None,
    identity: str = None,
    limits: Optional[TAgentLimits] = None,
    loop_run_args: Optional[Dict[str, Any]] = None,
    verbosity: Optional[int] = None,
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    spec: Type[BaseConfiguration] = None,
) -> Callable[
    [Callable[TJobFunParams, TJobResult]], AgentJobFactory[TJobFunParams, TJobResult]
]: ...


@overload
def agent(
    agent_ref: Union[str, TAgentSpec],
    /,
    *,
    name: str = None,
    section: str = None,
    loop: str = DEFAULT_AGENT_LOOP,
    model: str = None,
    instructions: str = None,
    identity: str = None,
    limits: Optional[TAgentLimits] = None,
    loop_run_args: Optional[Dict[str, Any]] = None,
    verbosity: Optional[int] = None,
    inputs_validator: Optional[AnyFun] = None,
    outputs_validator: Optional[AnyFun] = None,
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    spec: Type[BaseConfiguration] = None,
) -> AgentJobFactory[..., TAgentJobResult]: ...


def agent(
    func_or_ref: Union[Optional[AnyFun], str, TAgentSpec] = None,
    /,
    *,
    agent: Union[str, TAgentSpec] = None,
    instructions: str = None,
    access: Optional[TWorkspaceAccess] = None,
    tools: Optional[List[str]] = None,
    skills: Optional[List[str]] = None,
    rules: Optional[List[str]] = None,
    name: str = None,
    section: str = None,
    loop: str = DEFAULT_AGENT_LOOP,
    model: str = None,
    identity: str = None,
    limits: Optional[TAgentLimits] = None,
    loop_run_args: Optional[Dict[str, Any]] = None,
    verbosity: Optional[int] = None,
    inputs_validator: Optional[AnyFun] = None,
    outputs_validator: Optional[AnyFun] = None,
    trigger: Union[str, TTrigger, Sequence[Union[str, TTrigger]]] = None,
    execute: Optional[TExecuteSpec] = None,
    expose: Optional[TJobExposeSpec] = None,
    require: Optional[TRequireSpec] = None,
    spec: Type[BaseConfiguration] = None,
) -> Any:
    """Declares a background agent job, from a function or from an agent reference.

    Applied to a function, the function drives the loop itself and reads it from
    `run_context["ai_loop"]`. Called with an agent instead, there is no function and the
    agent launcher drives the loop and reports its output.

    An agent is named by a `"<toolkit>:<agent>"` reference or given as a `TAgentSpec`: the
    reference form takes it positionally, the function form as `agent`.

    Args:
        func_or_ref: Function to decorate, or the agent to run.
        agent (Union[str, TAgentSpec]): Agent the decorated function drives, overridden by the
            arguments below and by the function itself. Function form only.
        instructions (str): What to tell the agent to do: the user turn opening the run.
            Configuration replaces it, `agent.instructions` in the job's section.
        access (Optional[TWorkspaceAccess]): What the agent may touch: `toolkits`, `local`, `data`,
            `context`. A ceiling, not a floor.
        tools (Optional[List[str]]): MCP feature groups to request from the dlthub MCP server.
        skills (Optional[List[str]]): Skill references the agent loads or has inlined.
        rules (Optional[List[str]]): Rule references inlined into the system prompt.
        name (str): Job name. Defaults to the function name, or to the agent name.
        section (str): Config section. Defaults to the declaring module.
        loop (str): Loop implementation, e.g. `"pydantic-ai"` or `"claude-agent-sdk"`.
        model (str): Model id or alias, overriding the agent's declared default.
        identity (str): Accepted and ignored.
        limits (Optional[TAgentLimits]): `max_turns` and `max_tokens` for the loop.
        loop_run_args (Optional[Dict[str, Any]]): Arguments passed to the native loop.
        verbosity (Optional[int]): How much of the run to show: 0 quiet, 1 thoughts and tool
            detail, 2 everything.
        inputs_validator (Optional[AnyFun]): Callable that validates and extends the inputs.
            Agent reference only.
        outputs_validator (Optional[AnyFun]): Callable that validates the outputs.
            Agent reference only.
        trigger: One or more trigger strings or `TTrigger` values.
        execute (Optional[TExecuteSpec]): Execution constraints: `timeout`, `concurrency`.
        expose (Optional[TJobExposeSpec]): UI presentation: `tags`, `starred`, `manual`.
        require (Optional[TRequireSpec]): Runtime resource requirements.
        spec (Type[BaseConfiguration]): Optional configuration spec class.

    Returns:
        AgentJobFactory: Preserves the decorated function's signature and return type.

    Raises:
        TypeError: An argument was passed that the chosen form does not accept.
    """
    is_agent_ref = isinstance(func_or_ref, (str, Mapping))
    if not is_agent_ref:
        # the launcher applies these two only when it drives the loop itself
        for arg_name, value in (
            ("inputs_validator", inputs_validator),
            ("outputs_validator", outputs_validator),
        ):
            if value is not None:
                raise TypeError(
                    f"run.agent on a function does not accept {arg_name!r}. Pass it to"
                    " `loop.run()`, or name an agent: run.agent('<toolkit>:<agent>')."
                )

    if is_agent_ref and agent is not None:
        raise TypeError("run.agent takes the agent positionally here. Drop the 'agent' argument.")

    def _new_factory() -> AgentJobFactory[Any, Any]:
        wrapper: AgentJobFactory[Any, Any] = _make_job_factory(  # type: ignore[assignment]
            factory_cls=AgentJobFactory,
            name=name,
            section=section,
            job_type="batch",
            trigger=trigger,
            execute=execute,
            expose=expose,
            require=require,
            spec=spec,
            deco_name="@agent",
        )
        wrapper.loop = loop
        wrapper.model = model
        wrapper.instructions = instructions
        wrapper.limits = limits
        wrapper.loop_run_args = loop_run_args
        wrapper.verbosity = verbosity
        wrapper.inputs_validator = inputs_validator
        wrapper.outputs_validator = outputs_validator
        wrapper.agent_declaration = {
            "name": name,
            "access": access,
            "tools": tools,
            "skills": skills,
            "rules": rules,
            "model": model,
            "limits": limits,
            "loop_run_args": loop_run_args,
            "trigger": wrapper.trigger or None,
        }
        _set_agent(wrapper, agent)
        return wrapper

    if func_or_ref is None:
        # called with parens
        return lambda f: _new_factory().bind(f)
    if not is_agent_ref:
        # called as @run.agent, without parens
        return _new_factory().bind(cast(AnyFun, func_or_ref))

    # an agent was given: there is no function, the launcher drives the loop
    declared = cast(Union[str, TAgentSpec], func_or_ref)
    wrapper = _new_factory()
    _set_agent(wrapper, declared)
    wrapper.name = wrapper.name or _job_name_from_agent(declared)
    _validate_job_name(wrapper.name)
    return wrapper
