import io
import contextlib

import shutil
from typing import List
from unittest import mock

import dlt
import pytest

from dlt.cli import run_command

from dlt.common.utils import set_working_dir
from tests.utils import TEST_STORAGE_ROOT, TESTS_ROOT


CLI_RUNNER_PIPELINES = TESTS_ROOT / "cli/cases/cli_runner"
TEST_PIPELINE = CLI_RUNNER_PIPELINES / "pipeline.py"
TEST_PIPELINE_WITH_IMMEDIATE_RUN = CLI_RUNNER_PIPELINES / "pipeline_with_immediate_run.py"


def test_run_command_happy_path_works_as_expected():
    # pipeline variable name
    pipeline_name = "numbers_pipeline"
    real_pipeline_name = "my_numbers_pipeline"
    p = dlt.pipeline(pipeline_name=pipeline_name)
    p._wipe_working_folder()
    shutil.copytree(CLI_RUNNER_PIPELINES, TEST_STORAGE_ROOT, dirs_exist_ok=True)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf), set_working_dir(TEST_STORAGE_ROOT):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            pipeline_name,
            "numbers_resource_instance",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert "Current working directory is different from the pipeline script" in output
        assert "Pipeline: numbers_pipeline (my_numbers_pipeline)" in output
        assert "Resource: numbers_resource_instance (numbers_resource)" in output
        assert "Pipeline my_numbers_pipeline load step completed" in output
        assert "contains no failed jobs" in output

        # Check if we can successfully attach to pipeline
        pipeline = dlt.attach(real_pipeline_name)
        assert pipeline.schema_names == ["my_numbers"]

        trace = pipeline.last_trace
        assert trace is not None
        assert len(trace.steps) == 4

        step = trace.steps[0]
        assert step.step == "extract"

        with pipeline.sql_client() as c:
            with c.execute_query("select count(id) from numbers_resource") as cur:
                row = list(cur.fetchall())[0]
                assert row[0] == 10


def test_run_command_fails_with_relevant_error_if_pipeline_resource_or_source_not_found():
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            "pipeline_404",
            "numbers_resource_instance",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert "Pipeline pipeline_404 has not been found in pipeline script" in output
        assert "You can choose one of: quads_pipeline, numbers_pipeline" in output

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            "numbers_pipeline",
            "resource_404",
            ["write_disposition=merge", "loader_file_format=jsonl"],
        )

        output = buf.getvalue()
        assert (
            "Source or resouce with name: resource_404 has not been found in pipeline script."
            in output
        )
        assert "You can choose one of: quads_resource_instance, numbers_resource_instance" in output


def test_run_command_allows_selection_of_pipeline_source_or_resource():
    with mock.patch(
        "dlt.common.cli.runner.inquirer.Inquirer.ask", return_value=0
    ) as mocked_ask, io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            None,
            None,
            ["write_disposition=append", "loader_file_format=parquet"],
        )

        # expect 2 calls to Inquirer.ask
        # first for pipeline selection
        # second for reource or source
        assert mocked_ask.call_count == 2


def test_run_command_exits_if_exit_choice_selected():
    with mock.patch(
        "dlt.common.cli.runner.inquirer.fmt.prompt", return_value="n"
    ), io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            None,
            None,
            ["write_disposition=append", "loader_file_format=parquet"],
        )

        output = buf.getvalue()
        assert "Stopping..." in output


def test_run_command_exits_if_pipeline_run_calls_exist_at_the_top_level():
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE_WITH_IMMEDIATE_RUN),
            None,
            None,
            ["write_disposition=append", "loader_file_format=parquet"],
        )
        output = buf.getvalue()
        assert "Please move all pipeline.run calls inside __main__ or remove them" in output


@pytest.mark.parametrize(
    "arguments,expected_error_message",
    [
        [
            ["write_disposition=merge", "loader_file_format=markdown"],
            (
                "Invalid loader file format, select one of"
                " jsonl|puae-jsonl|insert_values|sql|parquet|reference|arrow"
            ),
        ],
        [
            ["write_disposition=guess", "loader_file_format=jsonl"],
            "Invalid write disposition, select one of skip|append|replace|merge",
        ],
        [
            ["schema_contract=guess", "loader_file_format=jsonl"],
            "Invalid schema_contract mode, select one of evolve|discard_value|freeze|discard_row",
        ],
        [
            ["yolo=yes", "moon=yes"],
            "Invalid argument yolo\nInvalid argument moon",
        ],
        # Good case
        [["write_disposition=append", "loader_file_format=parquet"], ""],
    ],
)
def test_run_command_with_invalid_pipeline_run_arguments(
    arguments: List[str], expected_error_message: str
):
    pipeline_name = "numbers_pipeline"
    p = dlt.pipeline(pipeline_name=pipeline_name)
    p._wipe_working_folder()
    shutil.copytree(CLI_RUNNER_PIPELINES, TEST_STORAGE_ROOT, dirs_exist_ok=True)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf), set_working_dir(TEST_STORAGE_ROOT):
        run_command.run_pipeline_command(
            str(TEST_PIPELINE),
            pipeline_name,
            "numbers_resource_instance",
            arguments,
        )

        output = buf.getvalue()
        if expected_error_message:
            assert expected_error_message in output
        else:
            # Check if we can attach to pipeline
            dlt.attach("my_numbers_pipeline")
