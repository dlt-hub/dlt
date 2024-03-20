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
DUMMY_PIPELINE = RUNNER_PIPELINES / "pipeline.py"


def test_run_command_requires_working_directory_same_as_pipeline_working_directory(tmp_path):
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(DUMMY_PIPELINE),
            "dummy_pipeline",
            "squares_resource",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert "WARNING: Current working directory is different from the pipeline script" in output
