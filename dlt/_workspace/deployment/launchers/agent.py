"""Launcher for background agent jobs."""

import asyncio
import inspect
from typing import Any, Dict, Optional, cast

from dlt.common.configuration import resolve_configuration
from dlt.common.configuration.container import Container
from dlt.common.reflection.ref import object_from_ref
from dlt.common.runtime.run_context import active

from dlt._workspace.deployment.agent.loop import (
    AgentLoop,
    resolve_agent_loop,
    resolve_agent_settings,
    resolve_loop_type,
)
from dlt._workspace.deployment.agent.manifest import to_agent_definition
from dlt._workspace.deployment.agent.typing import TAgentJobResult, TAgentSpec
from dlt._workspace.deployment.decorators import AgentJobFactory
from dlt._workspace.deployment._run_views import print_job_result
from dlt._workspace.deployment.configuration import AgentConfiguration
from dlt._workspace.deployment.exceptions import JobAbortedException, JobResolutionError
from dlt._workspace.deployment.job_result import JobRunContext, set_job_inputs, set_job_result
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
)
from dlt._workspace.deployment.typing import (
    JOB_RESULT_ENGINE_VERSION,
    TJobResult,
    TJobRunContext,
    TRuntimeEntryPoint,
    TTrigger,
)


def _resolve_agent_job(entry_point: TRuntimeEntryPoint) -> AgentJobFactory[Any, Any]:
    """Import the module and resolve the AgentJobFactory named by the entry point."""
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
    if result.is_declared:
        # stamp the module the manifest found it in, so job_ref and config sections match
        result.declare(entry_point["module"], function)
    return result  # type: ignore[no-any-return]


def build_agent_loop(job: AgentJobFactory[Any, Any], workspace_root: str) -> AgentLoop:
    """Resolves the agent spec and builds an initialized loop for it."""
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


def _agent_inputs(
    job: AgentJobFactory[Any, Any],
    spec: TAgentSpec,
    run_context: TJobRunContext,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Declared inputs plus the implicit run context, extended by the inputs validator.

    A declared input is taken from configuration first, then from the trigger's run
    arguments, then from an explicit call argument.
    """
    # inputs go into the system prompt and the agent trace, where the loop handle is meaningless
    context = {k: v for k, v in run_context.items() if k != "ai_loop"}
    inputs: Dict[str, Any] = {"run_context": context}
    inputs.update(configured_inputs(job, job.input_spec(spec)))
    inputs.update(run_context.get("run_args") or {})
    inputs.update(kwargs)
    if job.inputs_validator is not None:
        extended = job.inputs_validator(inputs)
        if extended:
            inputs.update(extended)
    set_job_inputs(inputs)
    return inputs


def _finish(
    job: AgentJobFactory[Any, Any], output: Dict[str, Any], loop: AgentLoop
) -> TAgentJobResult:
    """Wraps a loop result in a job result and declares it; the job launcher delivers it."""
    if job.outputs_validator is not None:
        validated = job.outputs_validator(output)
        if validated:
            output = validated
    status = output.get("status", "succeeded")
    job_result: TAgentJobResult = {
        "type": job.agent_ref or job.name,
        "engine_version": JOB_RESULT_ENGINE_VERSION,
        "status": status,
        "summary": output.get("summary", ""),
        "result": output,
        "trace": loop.trace,
    }
    set_job_result(job_result)
    if status == "aborted":
        raise JobAbortedException(job_result["summary"] or "agent aborted", job_result)
    return job_result


def run_declared_agent(job: AgentJobFactory[Any, Any], **kwargs: Any) -> TAgentJobResult:
    """Runs an agent declared by reference, outside a launcher."""
    context = active()
    run_context: TJobRunContext = {
        "run_id": getattr(context.runtime_config, "run_id", None) or "local",
        "trigger": TTrigger("manual:"),
        "refresh": False,
    }
    loop = build_agent_loop(job, context.run_dir)
    run_context["ai_loop"] = loop
    with Container().injectable_context(JobRunContext()):
        inputs = _agent_inputs(job, loop.spec, run_context, **kwargs)
        output = asyncio.run(loop.run(inputs))
        try:
            _finish(job, output, loop)
        finally:
            # outside a launcher nothing is delivered, but the result is finished all the same
            finished = deliver_job_result(job, send=False)
    return cast(TAgentJobResult, finished)


def _function_kwargs(job: AgentJobFactory[Any, Any], run_context: TJobRunContext) -> Dict[str, Any]:
    """Run arguments for the inputs the function declares, plus the run context when it asks."""
    parameters = inspect.signature(job._f).parameters
    kwargs: Dict[str, Any] = {
        name: value
        for name, value in (run_context.get("run_args") or {}).items()
        if name in parameters
    }
    if _wants_run_context(job._f):
        kwargs["run_context"] = run_context
    return kwargs


def _invoke_agent(job: AgentJobFactory[Any, Any], run_context: TJobRunContext) -> Any:
    """Builds the loop and drives it. A decorated function drives it itself."""
    loop: Optional[AgentLoop] = None
    if job.has_agent:
        loop = build_agent_loop(job, active().run_dir)
        run_context["ai_loop"] = loop
    if job.is_declared:
        inputs = _agent_inputs(job, loop.spec, run_context)
        output = asyncio.run(loop.run(inputs))
        return _finish(job, output, loop)

    kwargs = _function_kwargs(job, run_context)
    set_job_inputs(kwargs)
    result = job(**kwargs)
    if asyncio.iscoroutine(result):
        result = asyncio.run(result)
    if isinstance(result, dict) and loop is not None and "status" in result:
        return _finish(job, result, loop)
    return result


def run(entry_point: TRuntimeEntryPoint, run_id: str, trigger: str) -> Any:
    """Runs an agent job: builds its loop, drives it, and delivers the structured output.

    Args:
        entry_point (TRuntimeEntryPoint): What to run (module + factory attribute).
        run_id (str): Unique run identifier.
        trigger (str): Trigger string that fired this run.

    Returns:
        Any: The agent job output, or the decorated function's return value.

    Raises:
        JobAbortedException: The agent reported `status: aborted`.
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
        invoke=cast(TJobInvoke, _invoke_agent),
    )


if __name__ == "__main__":
    args = parse_launcher_args()
    result = run(entry_point=args.entry_point, run_id=args.run_id, trigger=args.trigger)
    if isinstance(result, dict) and "type" in result:
        print_job_result(cast(TJobResult, result))
    elif result is not None:
        print(result)  # noqa: T201
