import io
import os
import contextlib
import pytest
import logging
from subprocess import CalledProcessError

import dlt
from dlt.common.runners.venv import Venv
from dlt.common.storages.file_storage import FileStorage

from dlt.cli import echo, run_command

from tests.utils import TESTS_ROOT

RUNNER_PIPELINES = TESTS_ROOT / "cli/cases/cli_runner"
TEST_PIPELINE = RUNNER_PIPELINES / "pipeline.py"


def test_run_command_requires_working_directory_same_as_pipeline_working_directory(tmp_path):
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            "squares_pipeline",
            "squares_resource_instance",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert "Current working directory is different from the pipeline script" in output
        assert "Please change your current directory to" in output


def test_run_command_fails_with_relevant_error_if_pipeline_resource_or_source_not_found(tmp_path):
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            "pipeline_404",
            "squares_resource_instance",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert "Pipeline pipeline_404 has not been found in pipeline script" in output
        assert "You can choose one of: squares_pipeline" in output

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
        assert "You can choose one of: squares_resource_instance" in output
