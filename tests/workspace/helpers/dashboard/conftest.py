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


@pytest.fixture(scope="session")
def no_destination_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_no_destination_pipeline(temp_dir)


@pytest.fixture(scope="session")
def success_pipeline_duckdb():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_success_pipeline_duckdb(temp_dir, ":memory:")


@pytest.fixture(scope="session")
def success_pipeline_filesystem():
    with tempfile.TemporaryDirectory() as temp_dir:
        with tempfile.TemporaryDirectory() as storage:
            yield create_success_pipeline_filesystem(temp_dir, storage)


@pytest.fixture(scope="session")
def extract_exception_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_extract_exception_pipeline(temp_dir)


@pytest.fixture(scope="session")
def normalize_exception_pipeline():
    """Fixture that creates a normalize exception pipeline"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_normalize_exception_pipeline(temp_dir)


@pytest.fixture(scope="session")
def never_ran_pipline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_never_ran_pipeline(temp_dir)


@pytest.fixture(scope="session")
def load_exception_pipeline():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield create_load_exception_pipeline(temp_dir)
