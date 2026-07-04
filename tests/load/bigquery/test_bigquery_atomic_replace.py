"""Unit tests for BigQuery atomic replace (enable_atomic_replace / WRITE_TRUNCATE_DATA).

These exercise the routing and followup-creation logic without a live BigQuery connection.
End-to-end replacement and metadata survival are covered in tests/load/pipeline and the live
BigQuery tests.
"""
import os
import warnings
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from dlt.common import pendulum
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.destination.client import (
    DestinationClientStagingConfiguration,
    HasFollowupJobs,
    PreparedTableSchema,
)
from dlt.common.schema import Schema
from dlt.common.storages.load_package import LoadJobInfo
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.utils import uniq_id

from dlt.destinations import bigquery
from dlt.destinations.impl.bigquery.bigquery import (
    ATOMIC_REPLACE_FILE_ID_PREFIX,
    BigQueryAtomicReplaceLoadJob,
    BigQueryClient,
    BigQueryLoadJob,
)
from dlt.destinations.impl.bigquery.configuration import BigQueryClientConfiguration
from dlt.destinations.job_impl import (
    FinalizedLoadJobWithFollowupJobs,
    ReferenceFollowupJobRequest,
)

pytestmark = pytest.mark.essential


def _client(
    *, enable_atomic_replace: bool = True, staging_bucket: str = "gs://bucket/path"
) -> BigQueryClient:
    creds = GcpServiceAccountCredentials()
    creds.project_id = "test_project_id"
    config = BigQueryClientConfiguration(credentials=creds)._bind_dataset_name(
        dataset_name=f"test_{uniq_id()}"
    )
    client = bigquery().client(Schema("test"), config)
    client.config.enable_atomic_replace = enable_atomic_replace
    if staging_bucket is not None:
        client.config.staging_config = DestinationClientStagingConfiguration(
            bucket_url=staging_bucket
        )
    return client


def _config(
    *, enable_atomic_replace: bool = True, replace_strategy: str = None, staging_bucket: str = None
) -> BigQueryClientConfiguration:
    creds = GcpServiceAccountCredentials()
    creds.project_id = "test_project_id"
    config = BigQueryClientConfiguration(
        credentials=creds, enable_atomic_replace=enable_atomic_replace
    )
    config.replace_strategy = replace_strategy  # type: ignore[assignment]
    if staging_bucket is not None:
        config.staging_config = DestinationClientStagingConfiguration(bucket_url=staging_bucket)
    return config


def _replace_table(name: str = "items") -> PreparedTableSchema:
    return cast(
        PreparedTableSchema,
        {"name": name, "write_disposition": "replace", "x-replace-strategy": "truncate-and-insert"},
    )


def _job_info(table_name: str, file_id: str, file_path: str) -> LoadJobInfo:
    return LoadJobInfo(
        "completed_jobs",
        file_path,
        0,
        pendulum.now(),
        0.0,
        ParsedLoadJobFileName(table_name, file_id, 0, "reference"),
        None,
    )


def test_load_job_class_followup_traits() -> None:
    # per-file job drives the chain hook, the aggregated job must not re-fire it
    assert issubclass(BigQueryLoadJob, HasFollowupJobs)
    assert not issubclass(BigQueryAtomicReplaceLoadJob, HasFollowupJobs)


@pytest.mark.parametrize(
    "enable,disposition,strategy,expected",
    [
        (True, "replace", "truncate-and-insert", True),
        (False, "replace", "truncate-and-insert", False),
        (True, "append", "truncate-and-insert", False),
        (True, "replace", "insert-from-staging", False),
    ],
    ids=["enabled", "flag-off", "append", "wrong-strategy"],
)
def test_use_atomic_replace_per_table_gating(
    enable: bool, disposition: str, strategy: str, expected: bool
) -> None:
    client = _client(enable_atomic_replace=enable)
    table = cast(
        PreparedTableSchema,
        {"name": "items", "write_disposition": disposition, "x-replace-strategy": strategy},
    )
    assert client._use_atomic_replace(table) is expected


@pytest.mark.parametrize(
    "staging_bucket,expected",
    [
        ("gs://b/p", True),
        ("gcs://b/p", True),
        (None, False),
        ("s3://b/p", False),
        ("/local/path", False),
    ],
    ids=["gs", "gcs", "no-staging", "s3", "local"],
)
def test_use_atomic_replace_requires_gcs_staging(staging_bucket: str, expected: bool) -> None:
    client = _client(enable_atomic_replace=True, staging_bucket=staging_bucket)
    assert client._use_atomic_replace(_replace_table()) is expected


@pytest.mark.parametrize(
    "replace_strategy,stays_enabled",
    [
        (None, True),
        ("truncate-and-insert", True),
        ("staging-optimized", False),
        ("insert-from-staging", False),
    ],
    ids=["none", "truncate-and-insert", "staging-optimized", "insert-from-staging"],
)
def test_on_resolved_gates_replace_strategy(replace_strategy: str, stays_enabled: bool) -> None:
    config = _config(enable_atomic_replace=True, replace_strategy=replace_strategy)
    if stays_enabled:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            config.on_resolved()
    else:
        with pytest.warns(UserWarning):
            config.on_resolved()
    assert config.enable_atomic_replace is stays_enabled


def test_on_resolved_noop_when_flag_off() -> None:
    # flag off, or a conflicting strategy without the flag: never warns, never mutates
    config = _config(enable_atomic_replace=False, replace_strategy="staging-optimized")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        config.on_resolved()
    assert config.enable_atomic_replace is False


def test_should_truncate_skips_upfront_for_atomic(mocker: MockerFixture) -> None:
    client = _client(enable_atomic_replace=True)
    mocker.patch.object(client, "prepare_load_table", return_value=_replace_table())
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert client.should_truncate_table_before_load("items") is False


def test_should_truncate_truncates_when_flag_off(mocker: MockerFixture) -> None:
    client = _client(enable_atomic_replace=False)
    mocker.patch.object(client, "prepare_load_table", return_value=_replace_table())
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert client.should_truncate_table_before_load("items") is True


def test_should_truncate_warns_when_flag_on_without_gcs_staging(mocker: MockerFixture) -> None:
    client = _client(enable_atomic_replace=True, staging_bucket=None)
    mocker.patch.object(client, "prepare_load_table", return_value=_replace_table())
    with pytest.warns(UserWarning):
        assert client.should_truncate_table_before_load("items") is True


@pytest.mark.parametrize(
    "enable,file_id,expected_cls",
    [
        (True, f"{ATOMIC_REPLACE_FILE_ID_PREFIX}{uniq_id(5)}", BigQueryAtomicReplaceLoadJob),
        (True, uniq_id(5), FinalizedLoadJobWithFollowupJobs),
        (False, uniq_id(5), BigQueryLoadJob),
    ],
    ids=["aggregated-atomic", "per-file-noop", "flag-off-regular"],
)
def test_create_load_job_routing(enable: bool, file_id: str, expected_cls: type) -> None:
    client = _client(enable_atomic_replace=enable)
    file_path = os.path.join("/tmp", f"items.{file_id}.0.reference")
    job = client.create_load_job(_replace_table(), file_path, "load_1")
    assert isinstance(job, expected_cls)


def test_create_atomic_replace_followup_jobs() -> None:
    """One aggregated reference job per chain table storing its per-file reference paths; the
    nested table that received no data gets a job with an empty reference list (it truncates)."""
    client = _client()
    table_chain = [_replace_table("items"), _replace_table("items__children")]
    completed_jobs = [
        _job_info("items", "aaaaa", "/pkg/completed_jobs/items.aaaaa.0.reference"),
        _job_info("items", "bbbbb", "/pkg/completed_jobs/items.bbbbb.0.reference"),
    ]

    jobs = client._create_atomic_replace_followup_jobs(table_chain, completed_jobs)
    by_table = {ParsedLoadJobFileName.parse(j.new_file_path()).table_name: j for j in jobs}

    assert set(by_table) == {"items", "items__children"}
    assert all(
        ParsedLoadJobFileName.parse(j.new_file_path()).file_id.startswith(
            ATOMIC_REPLACE_FILE_ID_PREFIX
        )
        for j in jobs
    )
    assert ReferenceFollowupJobRequest.resolve_references(by_table["items"].new_file_path()) == [
        "/pkg/completed_jobs/items.aaaaa.0.reference",
        "/pkg/completed_jobs/items.bbbbb.0.reference",
    ]
    # the jobless nested table references nothing -> resolves to no urls -> truncates
    assert ReferenceFollowupJobRequest.resolve_references(
        by_table["items__children"].new_file_path()
    ) == [""]


def test_create_load_job_resolves_and_uses_write_truncate_data(tmp_path: Any) -> None:
    client = _client()
    conn = MagicMock()
    client.sql_client._client = conn
    # per-file reference jobs each hold one gs url
    ref1 = tmp_path / "items.aaaaa.0.reference"
    ref1.write_text("gs://bucket/items/f1.jsonl")
    ref2 = tmp_path / "items.bbbbb.0.reference"
    ref2.write_text("gs://bucket/items/f2.jsonl")
    # the aggregated job references those per-file reference jobs
    agg = tmp_path / f"items.{ATOMIC_REPLACE_FILE_ID_PREFIX}{uniq_id(5)}.0.reference"
    agg.write_text(f"{ref1}\n{ref2}")

    client._create_load_job(_replace_table(), str(agg))

    call = conn.load_table_from_uri.call_args
    assert call.args[0] == ["gs://bucket/items/f1.jsonl", "gs://bucket/items/f2.jsonl"]
    job_config = call.kwargs["job_config"]
    assert job_config.write_disposition == "WRITE_TRUNCATE_DATA"
    assert job_config.create_disposition == "CREATE_NEVER"
    assert job_config.autodetect is False


def test_restored_atomic_job_resumes_by_stable_id(tmp_path: Any) -> None:
    """When the load step restarts, resume_started_jobs re-creates the started aggregated job with
    restore=True. It must come back as the atomic job, and creating vs. retrieving its BigQuery job
    must use the same deterministic id, so a job already submitted before the crash is resumed via
    the 409 path rather than loaded twice."""
    client = _client()
    conn = MagicMock()
    client.sql_client._client = conn
    ref = tmp_path / "items.aaaaa.0.reference"
    ref.write_text("gs://bucket/items/f1.jsonl")
    agg = tmp_path / f"items.{ATOMIC_REPLACE_FILE_ID_PREFIX}xxxxx.0.reference"
    agg.write_text(str(ref))

    job = client.create_load_job(_replace_table(), str(agg), "load_1", restore=True)
    assert isinstance(job, BigQueryAtomicReplaceLoadJob)

    client._create_load_job(_replace_table(), str(agg))
    created_id = conn.load_table_from_uri.call_args.kwargs["job_id"]
    client._retrieve_load_job(str(agg))
    assert conn.get_job.call_args.args[0] == created_id
