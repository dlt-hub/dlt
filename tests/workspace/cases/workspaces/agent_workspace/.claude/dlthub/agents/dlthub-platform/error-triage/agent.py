"""Checks error-triage runs around its loop."""

from typing import Any, Dict

from dlt.hub.run import JobAbortedException

OWNERS = {"config": "data-eng", "data": "analytics", "infra": "platform"}


def validate_input(inputs: Dict[str, Any]) -> Dict[str, Any]:
    message = (inputs.get("error_message") or "").strip()
    if not message:
        raise JobAbortedException("nothing to triage", {"summary": "empty error message"})
    return {**inputs, "error_message": message}


def validate_output(output: Dict[str, Any]) -> Dict[str, Any]:
    return {**output, "owner": OWNERS[output["category"]]}
