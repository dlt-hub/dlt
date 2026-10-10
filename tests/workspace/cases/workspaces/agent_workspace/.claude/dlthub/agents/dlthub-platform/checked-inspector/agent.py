"""Checks the checked-inspector runs around its loop. The failed run id steers each case."""

from typing import Any, Dict, Optional

from dlt.hub.run import JobAbortedException

from .helpers import STAMP, prepare

_prep: Optional[Dict[str, Any]] = None


def validate_input(inputs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    global _prep
    run_id = inputs.get("failed_run_id")
    if run_id == "boom":
        raise ValueError("the checks could not read the run")
    if run_id == "nothing":
        raise JobAbortedException("nothing to inspect", {"summary": "no failed run found"})
    _prep = prepare(inputs)
    if run_id == "keep":
        return None
    return {**inputs, "failed_run_id": _prep["run_id"]}


def validate_output(output: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if _prep is None or _prep["run_id"] == "keep":
        return None
    return {**output, "checked": STAMP, "prepared_for": _prep["run_id"]}
