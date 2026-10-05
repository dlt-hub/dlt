"""A sibling module `agent.py` imports relatively."""

from typing import Any, Dict

STAMP = "checked by agent.py"


def prepare(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """What the checks found before the loop, kept for the checks after it."""
    return {"run_id": inputs.get("failed_run_id") or "r-prepared"}
