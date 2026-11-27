"""An agent definition run once per error, and a job that reports on all of them."""

from collections import Counter
from typing import Any, Dict, List, cast

from dlt.common.typing import TypedDict
from dlt.hub.run import JobAbortedException, TAgentSpec, TJobRunContext, agent


class TriageReport(TypedDict):
    by_category: Dict[str, int]
    owners: List[str]
    skipped: int


triage = agent("dlthub-platform:error-triage", loop="pydantic-ai")


@agent(
    agent=cast(TAgentSpec, {"name": "triage-report", "output": TriageReport}), loop="pydantic-ai"
)
async def triage_report(
    errors: List[str] = None, run_context: TJobRunContext = None
) -> Dict[str, Any]:
    """Triages each error and reports how many fall into each category."""
    outputs: List[Dict[str, Any]] = []
    skipped = 0
    for error in errors:
        try:
            # the output keys come from AGENT.md, which the type checker does not read
            outputs.append(cast(Dict[str, Any], await triage(error_message=error)))
        except JobAbortedException:
            skipped += 1
    return {
        "status": "succeeded",
        "summary": f"triaged {len(outputs)} errors, skipped {skipped}",
        "by_category": dict(Counter(output["category"] for output in outputs)),
        "owners": sorted({output["owner"] for output in outputs}),
        "skipped": skipped,
    }
