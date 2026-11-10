"""Jobs that need every data access, deployed with the profiles of an environment. Only manifests
are generated from them: the agent definitions are inline and never run."""

from typing import Any, Dict

from dlt.hub.run import TAgentOutput, TAgentSpec, TJobRunContext, agent, interactive, job

TRIAGE: TAgentSpec = {"name": "triage", "system_prompt": "Triage the failed runs."}  # type: ignore[typeddict-item]
WRITER: TAgentSpec = {"name": "writer", "system_prompt": "Fix the data."}  # type: ignore[typeddict-item]


@job(trigger="schedule:0 2 * * *")
def nightly() -> Dict[str, Any]:
    """A batch job: needs a profile with write data access."""
    return {"rows": 10}


@interactive(interface="rest_api")
def api() -> None:
    """An interactive job: needs a profile with read data access."""


@job(access={"data": ["read"]})
def freshness() -> str:
    """A batch job that declares read data access: gets the profile interactive jobs get."""
    return "fresh"


@interactive(interface="rest_api", access={"data": ["read", "write"]})
def editor() -> None:
    """An interactive job that declares write data access: gets the default write profile."""


@agent(agent=TRIAGE)
def triage(run_context: TJobRunContext = None) -> TAgentOutput:
    """An agent job without data access."""
    return {"status": "succeeded", "summary": "nothing to triage"}


@agent(agent=WRITER, access={"data": ["write"]})
def writer(run_context: TJobRunContext = None) -> TAgentOutput:
    """An agent job that declares the data access it needs."""
    return {"status": "succeeded", "summary": "wrote"}


@job(require={"profile": "prod"})
def declared() -> str:
    """Declares its profile: the default write profile does not change it."""
    return "declared"


@job(require={"profile": "analytics"})
def reporting() -> str:
    """Declares a profile with write data access of `prod`, other than its default one."""
    return "report"


@job
def ci_only() -> str:
    """Deployed to the `ci` environment only."""
    return "ci"
