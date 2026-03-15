"""Tests for job reference construction, parsing, and resolution."""

from typing import Tuple

import pytest

from dlt._workspace.deployment._job_ref import (
    make_job_ref,
    parse_job_ref,
    resolve_job_ref,
    short_name,
)
from dlt._workspace.deployment.typing import (
    TEntryPoint,
    TExecutionSpec,
    TJobDefinition,
    TJobRef,
    TTrigger,
)


def _job(ref: str) -> TJobDefinition:
    return {
        "job_ref": TJobRef(ref),
        "entry_point": TEntryPoint(module="m", function="f", job_type="batch"),
        "triggers": [TTrigger(f"manual:{ref}")],
        "execution": TExecutionSpec(),
        "starred": False,
    }


JOBS = [
    _job("jobs.batch.backfill"),
    _job("jobs.batch.transform"),
    _job("jobs.stream.ingest"),
    _job("jobs.marimo_notebook"),
]


# ---- make_job_ref ----


@pytest.mark.parametrize(
    "section,name,expected",
    [
        ("batch", "backfill", "jobs.batch.backfill"),
        ("", "marimo_notebook", "jobs.marimo_notebook"),
    ],
    ids=["with-section", "module-level"],
)
def test_make_job_ref(section: str, name: str, expected: str) -> None:
    assert make_job_ref(section, name) == expected


# ---- parse_job_ref ----


@pytest.mark.parametrize(
    "ref,expected",
    [
        ("jobs.batch.backfill", ("batch", "backfill")),
        ("jobs.marimo_notebook", ("", "marimo_notebook")),
    ],
    ids=["three-part", "two-part"],
)
def test_parse_job_ref(ref: str, expected: Tuple[str, str]) -> None:
    assert parse_job_ref(TJobRef(ref)) == expected


@pytest.mark.parametrize(
    "ref,error_frag",
    [
        ("batch.backfill", "must start with"),
        ("jobs.a.b.c", "must be"),
    ],
    ids=["missing-prefix", "too-many-parts"],
)
def test_parse_job_ref_invalid(ref: str, error_frag: str) -> None:
    with pytest.raises(ValueError, match=error_frag):
        parse_job_ref(TJobRef(ref))


# ---- short_name ----


@pytest.mark.parametrize(
    "ref,expected",
    [
        ("jobs.batch.backfill", "backfill"),
        ("jobs.marimo_notebook", "marimo_notebook"),
    ],
    ids=["three-part", "two-part"],
)
def test_short_name(ref: str, expected: str) -> None:
    assert short_name(ref) == expected


# ---- resolve_job_ref ----


@pytest.mark.parametrize(
    "ref,jobs,expected",
    [
        # full ref passthrough
        ("jobs.batch.backfill", None, "jobs.batch.backfill"),
        # two-part passthrough
        ("jobs.marimo_notebook", None, "jobs.marimo_notebook"),
        # section.name -> prepend jobs.
        ("batch.backfill", None, "jobs.batch.backfill"),
        # bare name with jobs list
        ("backfill", JOBS, "jobs.batch.backfill"),
        # module-level bare name
        ("marimo_notebook", JOBS, "jobs.marimo_notebook"),
        # whitespace stripped
        ("  jobs.batch.backfill  ", None, "jobs.batch.backfill"),
    ],
    ids=[
        "full-ref",
        "two-part-ref",
        "section-name",
        "bare-name",
        "module-level-bare",
        "whitespace",
    ],
)
def test_resolve_job_ref(ref, jobs, expected) -> None:
    assert resolve_job_ref(ref, jobs) == expected


@pytest.mark.parametrize(
    "ref,jobs,error_frag",
    [
        # bare name without jobs list
        ("backfill", None, "requires a jobs list"),
        # ambiguous bare name
        ("ingest", [_job("jobs.a.ingest"), _job("jobs.b.ingest")], "ambiguous"),
        # not found bare name
        ("nonexistent", JOBS, "no job matching"),
        # full ref not in jobs list
        ("jobs.batch.missing", JOBS, "not found"),
        # section.name not in jobs list
        ("batch.missing", JOBS, "not found"),
        # empty ref
        ("", None, "must not be empty"),
    ],
    ids=[
        "bare-no-jobs",
        "ambiguous",
        "not-found",
        "full-ref-missing",
        "section-name-missing",
        "empty",
    ],
)
def test_resolve_job_ref_invalid(ref, jobs, error_frag) -> None:
    with pytest.raises(ValueError, match=error_frag):
        resolve_job_ref(ref, jobs)
