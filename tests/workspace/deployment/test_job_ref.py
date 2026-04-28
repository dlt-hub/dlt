"""Tests for job reference construction, parsing, resolution, and display."""

from typing import List, Optional, Tuple

import pytest

from dlt._workspace.deployment._job_ref import (
    format_job_label,
    make_job_ref,
    parse_job_ref,
    resolve_job_ref,
    short_name,
)
from dlt._workspace.deployment.exceptions import AmbiguousJobRef, InvalidJobRef, JobRefNotFound
from dlt._workspace.deployment.typing import TDeliverSpec, TExposeSpec, TJobRef


JOB_REFS: List[TJobRef] = [
    TJobRef("jobs.batch.backfill"),
    TJobRef("jobs.batch.transform"),
    TJobRef("jobs.stream.ingest"),
    TJobRef("jobs.marimo_notebook"),
]


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
    with pytest.raises(InvalidJobRef, match=error_frag):
        parse_job_ref(TJobRef(ref))


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


@pytest.mark.parametrize(
    "ref,job_refs,expected",
    [
        # full ref passthrough
        ("jobs.batch.backfill", None, "jobs.batch.backfill"),
        # two-part passthrough
        ("jobs.marimo_notebook", None, "jobs.marimo_notebook"),
        # section.name -> prepend jobs.
        ("batch.backfill", None, "jobs.batch.backfill"),
        # bare name with job_refs list
        ("backfill", JOB_REFS, "jobs.batch.backfill"),
        # module-level bare name
        ("marimo_notebook", JOB_REFS, "jobs.marimo_notebook"),
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
def test_resolve_job_ref(ref: str, job_refs: Optional[List[TJobRef]], expected: str) -> None:
    assert resolve_job_ref(ref, job_refs) == expected


@pytest.mark.parametrize(
    "ref,job_refs,error_frag",
    [
        # bare name without job_refs list
        ("backfill", None, "requires a job_refs list"),
        # ambiguous bare name
        (
            "ingest",
            [TJobRef("jobs.a.ingest"), TJobRef("jobs.b.ingest")],
            "ambiguous",
        ),
        # not found bare name
        ("nonexistent", JOB_REFS, "not found"),
        # full ref not in job_refs list
        ("jobs.batch.missing", JOB_REFS, "not found"),
        # section.name not in job_refs list
        ("batch.missing", JOB_REFS, "not found"),
        # empty ref
        ("", None, "must not be empty"),
    ],
    ids=[
        "bare-no-refs",
        "ambiguous",
        "not-found",
        "full-ref-missing",
        "section-name-missing",
        "empty",
    ],
)
def test_resolve_job_ref_invalid(
    ref: str, job_refs: Optional[List[TJobRef]], error_frag: str
) -> None:
    with pytest.raises((InvalidJobRef, JobRefNotFound, AmbiguousJobRef), match=error_frag):
        resolve_job_ref(ref, job_refs)


@pytest.mark.parametrize(
    ("job_ref", "display_name", "pipeline_name", "expected"),
    [
        # sectioned, no pipeline
        ("jobs.batch_jobs.backfill", None, None, "backfill (batch_jobs)"),
        ("jobs.batch_jobs.backfill", "Daily ETL", None, "Daily ETL (batch_jobs)"),
        # module-level, no pipeline
        ("jobs.notebook", None, None, "notebook"),
        ("jobs.notebook", "Customer Dashboard", None, "Customer Dashboard"),
        # pipeline overrides section
        ("jobs.batch_jobs.transform", None, "analytics", "transform (analytics)"),
        ("jobs.batch_jobs.transform", "Transform step", "analytics", "Transform step (analytics)"),
        # section equals name part -> deduplicated
        ("jobs.dashboard.dashboard", None, None, "dashboard"),
        ("jobs.dashboard.dashboard", "Workspace dashboard", None, "Workspace dashboard"),
    ],
    ids=[
        "sectioned",
        "sectioned+display",
        "module",
        "module+display",
        "pipeline",
        "pipeline+display",
        "section_eq_name",
        "section_eq_name+display",
    ],
)
def test_format_job_label(
    job_ref: str,
    display_name: Optional[str],
    pipeline_name: Optional[str],
    expected: str,
) -> None:
    expose: Optional[TExposeSpec] = {"display_name": display_name} if display_name else None
    deliver: Optional[TDeliverSpec] = {"pipeline_name": pipeline_name} if pipeline_name else None
    assert format_job_label(job_ref, expose, deliver) == expected
