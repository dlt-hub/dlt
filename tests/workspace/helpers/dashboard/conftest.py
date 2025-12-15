import pytest
import tempfile

from tests.workspace.helpers.dashboard.example_pipelines import (
    create_success_pipeline_duckdb,
    create_success_pipeline_filesystem,
    create_extract_exception_pipeline,
    create_normalize_exception_pipeline,
    create_never_ran_pipeline,
    create_load_exception_pipeline,
    create_no_destination_pipeline,
)


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
        with tempfile.TemporaryDirectory() as storage:
            yield create_success_pipeline_filesystem(temp_dir, storage)


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
        from pathlib import Path
        import os
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
