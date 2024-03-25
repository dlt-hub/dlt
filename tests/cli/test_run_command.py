import io
import os
import contextlib

from unittest import mock

import pytest

from dlt.cli import run_command

from tests.utils import TESTS_ROOT

RUNNER_PIPELINES = TESTS_ROOT / "cli/cases/cli_runner"
TEST_PIPELINE = RUNNER_PIPELINES / "pipeline.py"
TEST_PIPELINE_WITH_IMMEDIATE_RUN = RUNNER_PIPELINES / "pipeline_with_immediate_run.py"
TEST_PIPELINE_CONTENTS = open(RUNNER_PIPELINES / "pipeline.py").read().strip()


@pytest.fixture(scope="module")
def ch_pipeline_dir():
    cwd = os.getcwd()
    os.chdir(RUNNER_PIPELINES)
    yield
    os.chdir(cwd)


def test_run_command_requires_working_directory_same_as_pipeline_working_directory():
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            "squares_pipeline",
            "squares_resource_instance",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert (
            "Current working directory is different from the pipeline script" in output
        )
        assert "If needed please change your current directory to" in output


def test_run_command_fails_with_relevant_error_if_pipeline_resource_or_source_not_found():
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            "pipeline_404",
            "squares_resource_instance",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert "Pipeline pipeline_404 has not been found in pipeline script" in output
        assert "You can choose one of: quads_pipeline, squares_pipeline" in output

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            "squares_pipeline",
            "resource_404",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert (
            "Source or resouce with name: resource_404 has not been found in pipeline script."
            in output
        )
        assert (
            "You can choose one of: quads_resource_instance, squares_resource_instance"
            in output
        )


def test_run_command_allows_selection_of_pipeline_source_or_resource():
    with mock.patch(
        "dlt.common.cli.runner.inquirer.Inquirer.ask", return_value=0
    ) as mocked_ask:
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            None,
            None,
            ["write_disposition=append", "loader_file_format=parquet"],
        )
        assert mocked_ask.call_count == 2
