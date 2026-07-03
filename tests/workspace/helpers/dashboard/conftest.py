import os
from pathlib import Path

import pytest
import tempfile

from dlt._workspace.helpers.dashboard import strings

from tests.workspace.helpers.dashboard.example_pipelines import (
    create_success_pipeline_duckdb,
    create_success_pipeline_filesystem,
    create_extract_exception_pipeline,
    create_normalize_exception_pipeline,
    create_never_ran_pipeline,
    create_load_exception_pipeline,
    create_no_destination_pipeline,
    create_sync_exception_pipeline,
    create_custom_destination_pipeline,
    create_custom_dest_callable_pipeline,
    create_custom_dest_string_ref_pipeline,
)


def _visible_prefix(markdown_text: str) -> str:
    """First plain-text run of a markdown landing string, before any markup.

    marimo renders the landing strings as markdown (backticks become `<code>`,
    links become `<a>`), so only this prefix appears verbatim in the rendered
    output while staying tied to the source-of-truth copy in `strings.py`.
    """
    prefix = markdown_text.strip().split("`")[0].split(".")[0].strip()
    assert prefix, f"no plain-text prefix in landing string: {markdown_text!r}"
    return prefix


NO_PIPELINES_TEXT = _visible_prefix(strings.home_no_pipelines)
NO_TRACE_TEXT = _visible_prefix(strings.app_pipeline_no_trace)


# resolver to resolve strings to pipelines
@pytest.fixture
def pipeline(request):
    # request.param is one of the strings from parametrize
    return request.getfixturevalue(request.param)


@pytest.fixture
def no_destination_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_no_destination_pipeline(temp_dir)


@pytest.fixture
def success_pipeline_duckdb():
    with tempfile.TemporaryDirectory() as temp_dir:
        import duckdb

        db_conn = duckdb.connect()
        try:
            yield create_success_pipeline_duckdb(temp_dir, db_conn=db_conn)
        finally:
            db_conn.close()


@pytest.fixture
def success_pipeline_filesystem():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_success_pipeline_filesystem(temp_dir)


@pytest.fixture
def extract_exception_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_extract_exception_pipeline(temp_dir)


@pytest.fixture
def normalize_exception_pipeline():
    """Fixture that creates a normalize exception pipeline"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_normalize_exception_pipeline(temp_dir)


@pytest.fixture
def never_ran_pipline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_never_ran_pipeline(temp_dir)


@pytest.fixture
def load_exception_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_load_exception_pipeline(temp_dir)


@pytest.fixture
def temp_pipelines_dir():
    """Create a temporary directory structure for testing pipelines"""
    with tempfile.TemporaryDirectory() as temp_dir:
        from dlt.pipeline.trace import TRACE_FILE_NAME

        pipelines_dir = Path(temp_dir) / "pipelines"
        pipelines_dir.mkdir()

        # Create some test pipeline directories
        (pipelines_dir / "success_pipeline_1").mkdir()
        (pipelines_dir / "success_pipeline_2").mkdir()
        (pipelines_dir / "_dlt_internal").mkdir()

        # Create trace files with different timestamps
        trace_file_1 = pipelines_dir / "success_pipeline_1" / TRACE_FILE_NAME
        trace_file_1.touch()
        # Set modification time to 2 days ago
        os.utime(trace_file_1, (1000000, 1000000))

        trace_file_2 = pipelines_dir / "success_pipeline_2" / TRACE_FILE_NAME
        trace_file_2.touch()
        # Set modification time to 1 day ago (more recent)
        os.utime(trace_file_2, (2000000, 2000000))

        yield str(pipelines_dir)


@pytest.fixture
def sync_exception_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_sync_exception_pipeline(temp_dir)


@pytest.fixture
def custom_destination_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_custom_destination_pipeline(temp_dir)


@pytest.fixture
def custom_dest_callable_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_custom_dest_callable_pipeline(temp_dir)


@pytest.fixture
def custom_dest_string_ref_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_custom_dest_string_ref_pipeline(temp_dir)
