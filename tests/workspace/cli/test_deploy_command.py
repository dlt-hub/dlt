import os
import io
import contextlib
import shutil
from subprocess import CalledProcessError
from unittest.mock import patch
from git import InvalidGitRepositoryError, NoSuchPathError
import pytest
from pytest_console_scripts import ScriptRunner

import dlt
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.profile import save_profile_pin
from dlt.common.runners import Venv
from dlt.common.typing import StrAny
from dlt.pipeline.exceptions import CannotRestorePipelineException

from dlt._workspace.cli import _deploy_command, echo
from dlt._workspace.cli.exceptions import CliCommandInnerException, PipelineWasNotRun
from dlt._workspace.cli._deploy_command_helpers import get_schedule_description
from dlt._workspace.cli.exceptions import CliCommandException

# from tests.utils import reset_providers
from tests.workspace.cli.utils import WORKSPACE_CLI_CASES_DIR


DEPLOY_PARAMS = [
    ("github-action", {"schedule": "*/30 * * * *", "run_on_push": True, "run_manually": True}),
    ("airflow-composer", {"secrets_format": "toml"}),
    ("airflow-composer", {"secrets_format": "env"}),
]


@pytest.mark.parametrize("deployment_method,deployment_args", DEPLOY_PARAMS)
def test_deploy_command_no_repo(deployment_method: str, deployment_args: StrAny) -> None:
    # don't look up
    os.environ["GIT_CEILING_DIRECTORIES"] = os.path.abspath(".")

    shutil.copytree(
        os.path.join(WORKSPACE_CLI_CASES_DIR, "deploy_pipeline"), ".", dirs_exist_ok=True
    )
    # we do not have repo
    with pytest.raises(InvalidGitRepositoryError):
        _deploy_command.deploy_command(
            "debug_pipeline.py",
            deployment_method,
            _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
            **deployment_args,
        )

    # test wrapper
    with pytest.raises(CliCommandException) as ex:
        _deploy_command.deploy_command_wrapper(
            "debug_pipeline.py",
            deployment_method,
            _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
            **deployment_args,
        )
    assert ex._excinfo[1].error_code == -4


@pytest.mark.parametrize("deployment_method,deployment_args", DEPLOY_PARAMS)
def test_deploy_command(deployment_method: str, deployment_args: StrAny) -> None:
    shutil.copytree(
        os.path.join(WORKSPACE_CLI_CASES_DIR, "deploy_pipeline"), ".", dirs_exist_ok=True
    )

    from git import Repo, Remote

    # we have a repo without git origin
    with Repo.init(".") as repo:
        # test no origin
        with pytest.raises(CliCommandInnerException) as py_ex:
            _deploy_command.deploy_command(
                "debug_pipeline.py",
                deployment_method,
                _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args,
            )
        assert "Your current repository has no origin set" in py_ex.value.args[0]
        with pytest.raises(CliCommandInnerException):
            _deploy_command.deploy_command_wrapper(
                "debug_pipeline.py",
                deployment_method,
                _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args,
            )

        # we have a repo that was never run
        Remote.create(repo, "origin", "git@github.com:rudolfix/dlt-cmd-test-2.git")
        with pytest.raises(CannotRestorePipelineException):
            _deploy_command.deploy_command(
                "debug_pipeline.py",
                deployment_method,
                _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args,
            )
        with pytest.raises(CliCommandException) as ex:
            _deploy_command.deploy_command_wrapper(
                "debug_pipeline.py",
                deployment_method,
                _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args,
            )
        assert ex._excinfo[1].error_code == -3

        # run the script with wrong credentials (it is postgres there)
        venv = Venv.restore_current()
        # mod environ so wrong password is passed to override secrets.toml and we have exception
        os.environ["DESTINATION__DUCKDB__CREDENTIALS__DATABASE"] = ":memory:"
        os.environ["API_KEY"] = ""
        with pytest.raises(CalledProcessError):
            venv.run_script("debug_pipeline.py")
        # print(py_ex.value.output)
        with pytest.raises(PipelineWasNotRun) as py_ex2:
            _deploy_command.deploy_command(
                "debug_pipeline.py",
                deployment_method,
                _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args,
            )
        assert "The last pipeline run ended with error" in py_ex2.value.args[0]

        with pytest.raises(CliCommandException) as ex:
            _deploy_command.deploy_command_wrapper(
                "debug_pipeline.py",
                deployment_method,
                _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args,
            )
        assert ex._excinfo[1].error_code == -3

        del os.environ["DESTINATION__DUCKDB__CREDENTIALS__DATABASE"]

        # ci profile has api key value set, prod profile has not so we expect placeholder
        for profile, api_key_value, emit_env in [
            ("ci", "ci_api_key_9x3ehash", False),
            ("prod", "please set me up!", True),
        ]:
            # pin ci profile
            run_context = dlt.current.workspace()
            save_profile_pin(run_context, profile)
            # switch profile
            run_context.switch_profile(profile)
            p = dlt.pipeline(pipeline_name="debug_pipeline")
            assert isinstance(p.run_context, WorkspaceRunContext)
            assert p.run_context.profile == profile

            # emit env if required
            if emit_env:
                os.environ["API_KEY"] = "env_api_key_9x3ehash"
            else:
                del os.environ["API_KEY"]

            # this time script will run
            venv.run_script("debug_pipeline.py")

            # drop env value so command below does not see it and will trigger display callback
            os.environ.pop("API_KEY", None)
            with echo.always_choose(False, always_choose_value=True):
                with io.StringIO() as buf, contextlib.redirect_stdout(buf):
                    _deploy_command.deploy_command(
                        "debug_pipeline.py",
                        deployment_method,
                        _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                        **deployment_args,
                    )
                    _out = buf.getvalue()
                print(_out)
                # make sure our secret and config values are all present
                assert api_key_value in _out

                if "schedule" in deployment_args:
                    assert get_schedule_description(deployment_args["schedule"])
                secrets_format = deployment_args.get("secrets_format", "env")
                if secrets_format == "env":
                    assert "API_KEY" in _out
                else:
                    assert "api_key = " in _out

        # non existing script name
        with pytest.raises(NoSuchPathError):
            _deploy_command.deploy_command(
                "no_pipeline.py",
                deployment_method,
                _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args,
            )
        with echo.always_choose(False, always_choose_value=True):
            with pytest.raises(CliCommandException) as ex:
                _deploy_command.deploy_command_wrapper(
                    "no_pipeline.py",
                    deployment_method,
                    _deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                    **deployment_args,
                )
            assert ex._excinfo[1].error_code == -5


def test_invoke_deploy_project(legacy_workspace_context, script_runner: ScriptRunner) -> None:
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


def test_invoke_deploy_mock(legacy_workspace_context, script_runner: ScriptRunner) -> None:
    # NOTE: you can mock only once per test with ScriptRunner !!
    with patch("dlt._workspace.cli._deploy_command.deploy_command") as _deploy_command_mock:
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
        assert _deploy_command_mock.called
        assert _deploy_command_mock.call_args[1] == {
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

        _deploy_command_mock.reset_mock()
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
        assert _deploy_command_mock.called
        assert _deploy_command_mock.call_args[1] == {
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
        _deploy_command_mock.reset_mock()
        result = script_runner.run(["dlt", "deploy", "debug_pipeline.py", "github-action"])
        assert not _deploy_command_mock.called
        assert result.returncode != 0
        assert "the following arguments are required: --schedule" in result.stderr
        # airflow without schedule works
        _deploy_command_mock.reset_mock()
        result = script_runner.run(["dlt", "deploy", "debug_pipeline.py", "airflow-composer"])
        assert _deploy_command_mock.called
        assert result.returncode == 0
        assert _deploy_command_mock.call_args[1] == {
            "pipeline_script_path": "debug_pipeline.py",
            "deployment_method": "airflow-composer",
            "no_pwd": False,
            "repo_location": "https://github.com/dlt-hub/dlt-deploy-template.git",
            "branch": None,
            "command": "deploy",
            "secrets_format": "toml",
        }
        # env secrets format
        _deploy_command_mock.reset_mock()
        result = script_runner.run(
            ["dlt", "deploy", "debug_pipeline.py", "airflow-composer", "--secrets-format", "env"]
        )
        assert _deploy_command_mock.called
        assert result.returncode == 0
        assert _deploy_command_mock.call_args[1] == {
            "pipeline_script_path": "debug_pipeline.py",
            "deployment_method": "airflow-composer",
            "no_pwd": False,
            "repo_location": "https://github.com/dlt-hub/dlt-deploy-template.git",
            "branch": None,
            "command": "deploy",
            "secrets_format": "env",
        }
