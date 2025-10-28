import os
import sys
import pytest
import shutil
from pathlib import Path
from pytest_console_scripts import ScriptRunner
from unittest.mock import patch

from dlt.common.runners.venv import Venv
from dlt.common.utils import custom_environ

from dlt._workspace.cli import echo as fmt
from tests.workspace.cli.utils import WORKSPACE_CLI_CASES_DIR

BASE_COMMANDS = ["init", "deploy", "pipeline", "telemetry", "schema"]


@pytest.fixture(autouse=True)
def disable_debug() -> None:
    # reset debug flag so other tests may pass
    from dlt._workspace.cli import _debug

    _debug.disable_debug()


def test_invoke_basic(script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlt", "--version"])
    assert result.returncode == 0
    assert result.stdout.startswith("dlt ")
    assert result.stderr == ""

    result = script_runner.run(["dlt", "--version"], shell=True)
    assert result.returncode == 0
    assert result.stdout.startswith("dlt ")
    assert result.stderr == ""

    for command in BASE_COMMANDS:
        result = script_runner.run(["dlt", command, "--help"])
        assert result.returncode == 0
        assert result.stdout.startswith(f"Usage: dlt {command}")

    result = script_runner.run(["dlt", "N/A", "--help"])
    assert result.returncode != 0


def test_invoke_list_pipelines(script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlt", "pipeline", "--list-pipelines"])
    # directory does not exist (we point to TEST_STORAGE)
    assert result.returncode == 0
    assert "No pipelines found in" in result.stdout
    # this is current workspace data dir
    expected_path = os.path.join("_storage", "empty", ".dlt", ".var", "dev")
    assert expected_path in result.stdout

    result = script_runner.run(["dlt", "pipeline", "--list-pipelines"])
    assert result.returncode == 0
    assert "No pipelines found in" in result.stdout


def test_invoke_pipeline(script_runner: ScriptRunner) -> None:
    # info on non existing pipeline
    result = script_runner.run(["dlt", "pipeline", "debug_pipeline", "info"])
    assert result.returncode == -2
    assert "the pipeline was not found in" in result.stderr

    shutil.copytree(
        os.path.join(WORKSPACE_CLI_CASES_DIR, "deploy_pipeline"), ".", dirs_exist_ok=True
    )

    with custom_environ({"COMPLETED_PROB": "1.0"}):
        venv = Venv.restore_current()
        print(venv.run_script("dummy_pipeline.py"))

    # we check output test_pipeline_command else
    result = script_runner.run(["dlt", "pipeline", "dummy_pipeline", "info"])
    assert result.returncode == 0
    result = script_runner.run(["dlt", "pipeline", "dummy_pipeline", "trace"])
    assert result.returncode == 0
    result = script_runner.run(["dlt", "pipeline", "dummy_pipeline", "failed-jobs"])
    assert result.returncode == 0
    result = script_runner.run(["dlt", "pipeline", "dummy_pipeline", "load-package"])
    assert result.returncode == 0
    result = script_runner.run(
        ["dlt", "pipeline", "dummy_pipeline", "load-package", "NON EXISTENT"]
    )
    assert result.returncode == -1
    # use debug flag to raise an exception
    result = script_runner.run(
        ["dlt", "--debug", "pipeline", "dummy_pipeline", "load-package", "NON EXISTENT"]
    )
    # exception terminates command
    assert result.returncode == 1
    assert "LoadPackageNotFound" in result.stderr


def test_invoke_init_chess_and_template(script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlt", "init", "chess", "dummy"])
    assert "Verified source chess was added to your project!" in result.stdout
    assert result.returncode == 0
    result = script_runner.run(["dlt", "init", "debug_pipeline", "dummy"])
    assert "Your new pipeline debug_pipeline is ready to be customized!" in result.stdout
    assert result.returncode == 0


def test_invoke_list_sources(script_runner: ScriptRunner) -> None:
    known_sources = ["chess", "sql_database", "google_sheets", "pipedrive"]
    result = script_runner.run(["dlt", "init", "--list-sources"])
    assert result.returncode == 0
    for known_source in known_sources:
        assert known_source in result.stdout


def test_invoke_deploy_project(script_runner: ScriptRunner) -> None:
    result = script_runner.run(
        ["dlt", "deploy", "debug_pipeline.py", "github-action", "--schedule", "@daily"]
    )
    assert result.returncode == -5
    assert "The pipeline script does not exist" in result.stderr
    result = script_runner.run(["dlt", "deploy", "debug_pipeline.py", "airflow-composer"])
    assert result.returncode == -5
    assert "The pipeline script does not exist" in result.stderr
    # now init
    result = script_runner.run(["dlt", "init", "chess", "dummy"])
    assert result.returncode == 0
    result = script_runner.run(
        ["dlt", "deploy", "chess_pipeline.py", "github-action", "--schedule", "@daily"]
    )
    assert "NOTE: You must run the pipeline locally" in result.stdout
    result = script_runner.run(["dlt", "deploy", "chess_pipeline.py", "airflow-composer"])
    assert "NOTE: You must run the pipeline locally" in result.stdout


def test_invoke_deploy_mock(script_runner: ScriptRunner) -> None:
    # NOTE: you can mock only once per test with ScriptRunner !!
    with patch("dlt._workspace.cli._deploy_command.deploy_command") as _deploy_command:
        script_runner.run(
            [
                "dlt",
                "--debug",
                "deploy",
                "debug_pipeline.py",
                "github-action",
                "--schedule",
                "@daily",
            ]
        )
        assert _deploy_command.called
        assert _deploy_command.call_args[1] == {
            "pipeline_script_path": "debug_pipeline.py",
            "deployment_method": "github-action",
            "no_pwd": False,
            "repo_location": "https://github.com/dlt-hub/dlt-deploy-template.git",
            "branch": None,
            "command": "deploy",
            "schedule": "@daily",
            "run_manually": True,
            "run_on_push": False,
        }

        _deploy_command.reset_mock()
        script_runner.run(
            [
                "dlt",
                "deploy",
                "debug_pipeline.py",
                "github-action",
                "--schedule",
                "@daily",
                "--location",
                "folder",
                "--branch",
                "branch",
                "--run-on-push",
            ]
        )
        assert _deploy_command.called
        assert _deploy_command.call_args[1] == {
            "pipeline_script_path": "debug_pipeline.py",
            "deployment_method": "github-action",
            "no_pwd": False,
            "repo_location": "folder",
            "branch": "branch",
            "command": "deploy",
            "schedule": "@daily",
            "run_manually": True,
            "run_on_push": True,
        }
        # no schedule fails
        _deploy_command.reset_mock()
        result = script_runner.run(["dlt", "deploy", "debug_pipeline.py", "github-action"])
        assert not _deploy_command.called
        assert result.returncode != 0
        assert "the following arguments are required: --schedule" in result.stderr
        # airflow without schedule works
        _deploy_command.reset_mock()
        result = script_runner.run(["dlt", "deploy", "debug_pipeline.py", "airflow-composer"])
        assert _deploy_command.called
        assert result.returncode == 0
        assert _deploy_command.call_args[1] == {
            "pipeline_script_path": "debug_pipeline.py",
            "deployment_method": "airflow-composer",
            "no_pwd": False,
            "repo_location": "https://github.com/dlt-hub/dlt-deploy-template.git",
            "branch": None,
            "command": "deploy",
            "secrets_format": "toml",
        }
        # env secrets format
        _deploy_command.reset_mock()
        result = script_runner.run(
            ["dlt", "deploy", "debug_pipeline.py", "airflow-composer", "--secrets-format", "env"]
        )
        assert _deploy_command.called
        assert result.returncode == 0
        assert _deploy_command.call_args[1] == {
            "pipeline_script_path": "debug_pipeline.py",
            "deployment_method": "airflow-composer",
            "no_pwd": False,
            "repo_location": "https://github.com/dlt-hub/dlt-deploy-template.git",
            "branch": None,
            "command": "deploy",
            "secrets_format": "env",
        }


@pytest.mark.skipif(sys.stdin.isatty(), reason="stdin connected, test skipped")
def test_no_tty() -> None:
    with fmt.maybe_no_stdin():
        assert fmt.confirm("test", default=True) is True
        assert fmt.prompt("test prompt", ("y", "n"), default="y") == "y"
