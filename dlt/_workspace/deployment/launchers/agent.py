"""Launcher for background agent jobs."""

import asyncio
import inspect
from functools import partial
from types import ModuleType
from typing import Any, Awaitable, Callable, Dict, List, Optional, cast

from dlt.common.configuration import resolve_configuration
from dlt.common.reflection.ref import object_from_ref
from dlt.common.runtime.run_context import active
from dlt.common.typing import TAny

from dlt._workspace.deployment.agent.loop import (
    AgentLoop,
    resolve_agent_loop,
    resolve_agent_settings,
    resolve_loop_type,
)
from dlt._workspace.deployment.agent.manifest import (
    VALIDATE_INPUT,
    VALIDATE_OUTPUT,
    load_agent_module,
    to_agent_definition,
)
from dlt._workspace.deployment.agent.typing import TAgentJobResult, TAgentSpec
from dlt._workspace.deployment.decorators import AgentJobFactory, JobFactory
from dlt._workspace.deployment.configuration import AgentConfiguration
from dlt._workspace.deployment.exceptions import JobAbortedException, JobResolutionError
from dlt._workspace.deployment.job_result import JobRun
from dlt._workspace.deployment.launchers._launcher import (
    apply_job_configuration,
    parse_launcher_args,
    prepare_run_env,
)
from dlt._workspace.deployment.launchers.job import (
    TJobInvoke,
    _wants_run_context,
    configured_inputs,
    deliver_job_result,
    job_sections,
    run as run_job,
    run_and_print_result,
)
from dlt._workspace.deployment.typing import (
    JOB_RESULT_ENGINE_VERSION,
    RUN_CONTEXT_INPUT,
    TJobResult,
    TJobRunContext,
    TRuntimeEntryPoint,
    TTrigger,
)


def _resolve_agent_job(entry_point: TRuntimeEntryPoint) -> AgentJobFactory[Any, Any]:
    """Imports the module and returns the `AgentJobFactory` the entry point names."""
    function = entry_point.get("function")
    if not function:
        raise JobResolutionError(entry_point["module"], "entry_point.function must be set")
    ref = f"{entry_point['module']}.{function}"

    def _typechecker(obj: Any) -> AgentJobFactory[Any, Any]:
        if isinstance(obj, AgentJobFactory):
            return obj
        raise JobResolutionError(ref, f"expected AgentJobFactory, got {type(obj).__name__}")

    result, trace = object_from_ref(ref, _typechecker, raise_exec_errors=True)
    if result is None:
        raise JobResolutionError(ref, f"{trace.reason}" + (f" ({trace.exc})" if trace.exc else ""))
    if not result.has_function:
        # bind it to the module attribute the manifest found it in, so job_ref and sections match
        result.bind_module_attr(entry_point["module"], function)
    return result  # type: ignore[no-any-return]


def build_agent_loop(job: AgentJobFactory[Any, Any], workspace_root: str) -> AgentLoop:
    """Resolves the agent definition and builds an initialized agent loop for it."""
    sections = job_sections(job)
    config = resolve_configuration(AgentConfiguration(), sections=sections)
    spec = job.resolve_agent_spec(workspace_root)

    loop_cls = resolve_agent_loop(resolve_loop_type(job.loop, config))
    decorator_args: Dict[str, Any] = {
        "model": job.model,
        "instructions": job.instructions,
        "limits": job.limits,
        "loop_run_args": job.loop_run_args,
        "verbosity": job.verbosity,
        "emojis": job.emojis,
    }
    settings = resolve_agent_settings(spec, config, decorator_args, loop_cls, workspace_root)
    loop = loop_cls(settings)
    loop.agent_ref = job.agent_ref
    loop.agent_file = job.agent_file or ""
    if job.agent_definition is None:
        job.agent_definition = to_agent_definition(
            spec, job.agent_file, job.instructions, job.model
        )
    loop.init(spec)
    return loop


def _collect_validators(
    agent_module: Optional[ModuleType], name: str, job_validator: Optional[Callable[..., Any]]
) -> List[Callable[..., Any]]:
    """Validators called `name`: the one from `agent.py` first, then the one the job passed."""
    return [v for v in (getattr(agent_module, name, None), job_validator) if v is not None]


def _collect_agent_inputs(
    job: AgentJobFactory[Any, Any],
    spec: TAgentSpec,
    run_context: TJobRunContext,
    agent_module: Optional[ModuleType] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Builds the inputs of an agent run and passes them through the input validators."""
    inputs: Dict[str, Any] = {RUN_CONTEXT_INPUT: run_context}
    # call arguments override run arguments, which override configuration
    given = {**(run_context.get("run_args") or {}), **kwargs}
    inputs.update(configured_inputs(job, job.input_spec(spec), given))
    inputs.update(given)
    run = JobRun.active()
    # a validator may abort the run: its result still needs the inputs the run received
    run.inputs = dict(inputs)
    for validate in _collect_validators(agent_module, VALIDATE_INPUT, job.inputs_validator):
        validated = validate(inputs)
        if validated is not None:
            inputs = validated
    run.inputs = dict(inputs)
    return inputs


async def _run_agent_definition(
    job: AgentJobFactory[Any, Any], loop: AgentLoop, run_context: TJobRunContext, **kwargs: Any
) -> Dict[str, Any]:
    """Runs the agent definition of an agent job without a function, with `agent.py` validators."""
    agent_module = load_agent_module(job.agent_dir) if job.agent_dir else None
    try:
        inputs = _collect_agent_inputs(job, loop.spec, loop.run_context, agent_module, **kwargs)
    except JobAbortedException as ex:
        # an input validator aborted the run: the loop does not start
        given: Dict[str, Any] = dict(ex.result) if ex.result else {}
        # raises inside the handler, so the reason the validator gave stays chained
        return _set_result_from_agent_output(
            job, {"summary": ex.summary, **given, "status": "aborted"}, loop
        )
    output = await loop.run(inputs=inputs)
    for validate in _collect_validators(agent_module, VALIDATE_OUTPUT, job.outputs_validator):
        validated = validate(output)
        if validated is not None:
            output = validated
    return output


def _function_args_from_run_args(
    job: AgentJobFactory[Any, Any], run_context: TJobRunContext
) -> Dict[str, Any]:
    """Run arguments that match the function parameters."""
    parameters = inspect.signature(job._f).parameters
    return {
        name: value
        for name, value in (run_context.get("run_args") or {}).items()
        if name in parameters
    }


def _set_result_from_agent_output(
    job: AgentJobFactory[Any, Any], output: TAny, loop: AgentLoop
) -> TAny:
    """Sets `output` as the job result when it is an agent output, and returns it."""
    if not (isinstance(output, dict) and "status" in output):
        return output
    run = JobRun.active()
    job_result: TAgentJobResult = {
        "type": job.result_name,
        "engine_version": JOB_RESULT_ENGINE_VERSION,
        "status": output["status"],
        "summary": output.get("summary", ""),
        "result": output,
        # a function may answer without calling the model: then the trace has no turns
        "trace": loop.trace if loop.completed else loop.base_trace(run.inputs or {}),
    }
    run.set_result(job_result)
    if output["status"] == "aborted":
        raise JobAbortedException(job_result["summary"] or "agent aborted", job_result)
    return output


async def _await_and_set_result(
    job: AgentJobFactory[Any, Any], output: Awaitable[Any], loop: AgentLoop
) -> Any:
    return _set_result_from_agent_output(job, await output, loop)


def _build_loop_and_run_agent(
    job: AgentJobFactory[Any, Any], run_context: TJobRunContext, **kwargs: Any
) -> Any:
    """Builds the agent loop into `run_context` and runs the agent job.

    Returns a coroutine when the job has no function or the function is `async def`.
    """
    loop = build_agent_loop(job, active().run_dir)
    # the loop, the inputs and the trace get the run context before the loop goes into it
    loop.run_context = cast(TJobRunContext, dict(run_context))
    run_context["ai_loop"] = loop
    # `kwargs` are function arguments here and agent run inputs otherwise
    if job.has_function:
        # run arguments fill what the caller left out
        kwargs = {**_function_args_from_run_args(job, run_context), **kwargs}
        JobRun.active().inputs = {RUN_CONTEXT_INPUT: loop.run_context, **kwargs}
        if _wants_run_context(job._f):
            kwargs[RUN_CONTEXT_INPUT] = run_context
        # calls the function itself: `job(...)` would call this function again
        output = JobFactory.__call__(job, **kwargs)
    else:
        output = _run_agent_definition(job, loop, run_context, **kwargs)
    if asyncio.iscoroutine(output):
        return _await_and_set_result(job, output, loop)
    return _set_result_from_agent_output(job, output, loop)


def _get_local_run_context(given: Optional[TJobRunContext] = None) -> TJobRunContext:
    """Fills in `run_id`, `trigger` and `refresh` missing from a run context, in place."""
    run_context: TJobRunContext = given if given is not None else cast(TJobRunContext, {})
    run_context.setdefault("run_id", getattr(active().runtime_config, "run_id", None) or "local")
    run_context.setdefault("trigger", TTrigger("manual:"))
    run_context.setdefault("refresh", False)
    return run_context


def _keep_job_result(job: AgentJobFactory[Any, Any], run: JobRun) -> None:
    """Finishes the job result of a direct call as the launcher does, and keeps it unsent."""
    job.last_job_result = cast(Optional[TAgentJobResult], deliver_job_result(job, run, send=False))


def _run_with_own_job_result(
    job: AgentJobFactory[Any, Any], run_context: TJobRunContext, **kwargs: Any
) -> Any:
    """Runs the agent job in a new job run context, so a calling job keeps its job result.

    The job result of the call is kept in `job.last_job_result`.
    """
    job.last_job_result = None
    run = JobRun()
    with run.activate():
        try:
            result = _build_loop_and_run_agent(job, run_context, **kwargs)
        except BaseException:
            _keep_job_result(job, run)
            raise
        if not asyncio.iscoroutine(result):
            _keep_job_result(job, run)
            return result

    async def await_with_own_job_result() -> Any:
        # entered again in the task that awaits, so concurrent calls do not share a run
        with run.activate():
            try:
                return await result
            finally:
                _keep_job_result(job, run)

    return await_with_own_job_result()


def call_agent_job(job: AgentJobFactory[Any, Any], *args: Any, **kwargs: Any) -> Any:
    """Runs an agent job called directly, returns the agent output or the function's return value.

    Returns a coroutine when the job has no function or the function is `async def`.
    """
    if not job.has_function:
        if args:
            raise TypeError(f"Agent job {job.name!r} takes its inputs as keyword arguments.")
    else:
        kwargs = dict(inspect.signature(job._f).bind_partial(*args, **kwargs).arguments)
    # a `run_context` the caller passed is used as is, missing keys filled in
    run_context = _get_local_run_context(kwargs.pop(RUN_CONTEXT_INPUT, None))
    return _run_with_own_job_result(job, run_context, **kwargs)


def run(entry_point: TRuntimeEntryPoint, run_id: str, trigger: str) -> Any:
    """Runs an agent job and delivers its job result.

    Args:
        entry_point (TRuntimeEntryPoint): What to run (module + factory attribute).
        run_id (str): Unique run identifier.
        trigger (str): Trigger string that fired this run.

    Returns:
        Any: The job result, or the function's return value when it is not an agent output.

    Raises:
        JobAbortedException: The agent output has `status: aborted`.
    """
    # the agent module is imported here, before run_job, so its config env must be set first
    apply_job_configuration(entry_point)
    prepare_run_env(entry_point)
    # the job launcher owns config env vars, profile, interval and signals; only the call differs
    return run_job(
        entry_point,
        run_id,
        trigger,
        job=_resolve_agent_job(entry_point),
        invoke=cast(TJobInvoke, _build_loop_and_run_agent),
    )


if __name__ == "__main__":
    args = parse_launcher_args()
    run_and_print_result(
        partial(run, entry_point=args.entry_point, run_id=args.run_id, trigger=args.trigger)
    )
