import os
import io
import contextlib
import shutil
import tempfile
from subprocess import CalledProcessError
from git import InvalidGitRepositoryError, NoSuchPathError
import pytest

import dlt

from dlt.common.runners import Venv
from dlt.common.storages.file_storage import FileStorage
from dlt.common.typing import StrAny
from dlt.common.utils import set_working_dir

from dlt.cli import deploy_command, _dlt, echo
from dlt.cli.exceptions import CliCommandInnerException
from dlt.pipeline.exceptions import CannotRestorePipelineException
from dlt.cli.deploy_command_helpers import get_schedule_description
from dlt.cli.exceptions import CliCommandException

from tests.utils import TEST_STORAGE_ROOT, reset_providers, test_storage


DEPLOY_PARAMS = [
    ("github-action", {"schedule": "*/30 * * * *", "run_on_push": True, "run_manually": True}),
    ("airflow-composer", {"secrets_format": "toml"}),
    ("airflow-composer", {"secrets_format": "env"}),
]


@pytest.mark.parametrize("deployment_method,deployment_args", DEPLOY_PARAMS)
def test_deploy_command_no_repo(
    test_storage: FileStorage, deployment_method: str, deployment_args: StrAny
) -> None:
    pipeline_wf = tempfile.mkdtemp()
    shutil.copytree("tests/cli/cases/deploy_pipeline", pipeline_wf, dirs_exist_ok=True)

    with set_working_dir(pipeline_wf):
        # we do not have repo
        with pytest.raises(InvalidGitRepositoryError):
            deploy_command.deploy_command(
                "debug_pipeline.py",
                deployment_method,
                deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args
            )

        # test wrapper
        with pytest.raises(CliCommandException) as ex:
            _dlt.deploy_command_wrapper(
                "debug_pipeline.py",
                deployment_method,
                deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                **deployment_args
            )
        assert ex._excinfo[1].error_code == -4


@pytest.mark.parametrize("deployment_method,deployment_args", DEPLOY_PARAMS)
def test_deploy_command(
    test_storage: FileStorage, deployment_method: str, deployment_args: StrAny
) -> None:
    # drop pipeline
    p = dlt.pipeline(pipeline_name="debug_pipeline")
    p._wipe_working_folder()

    shutil.copytree("tests/cli/cases/deploy_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    with set_working_dir(TEST_STORAGE_ROOT):
        from git import Repo, Remote

        # we have a repo without git origin
        with Repo.init(".") as repo:
            # test no origin
            with pytest.raises(CliCommandInnerException) as py_ex:
                deploy_command.deploy_command(
                    "debug_pipeline.py",
                    deployment_method,
                    deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                    **deployment_args
                )
            assert "Your current repository has no origin set" in py_ex.value.args[0]
            with pytest.raises(CliCommandInnerException):
                _dlt.deploy_command_wrapper(
                    "debug_pipeline.py",
                    deployment_method,
                    deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                    **deployment_args
                )

            # we have a repo that was never run
            Remote.create(repo, "origin", "git@github.com:rudolfix/dlt-cmd-test-2.git")
            with pytest.raises(CannotRestorePipelineException):
                deploy_command.deploy_command(
                    "debug_pipeline.py",
                    deployment_method,
                    deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                    **deployment_args
                )
            with pytest.raises(CliCommandException) as ex:
                _dlt.deploy_command_wrapper(
                    "debug_pipeline.py",
                    deployment_method,
                    deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                    **deployment_args
                )
            assert ex._excinfo[1].error_code == -3

            # run the script with wrong credentials (it is postgres there)
            venv = Venv.restore_current()
            # mod environ so wrong password is passed to override secrets.toml
            pg_credentials = os.environ.pop("DESTINATION__POSTGRES__CREDENTIALS", "")
            # os.environ["DESTINATION__POSTGRES__CREDENTIALS__PASSWORD"] = "password"
            with pytest.raises(CalledProcessError):
                venv.run_script("debug_pipeline.py")
            # print(py_ex.value.output)
            with pytest.raises(deploy_command.PipelineWasNotRun) as py_ex2:
                deploy_command.deploy_command(
                    "debug_pipeline.py",
                    deployment_method,
                    deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                    **deployment_args
                )
            assert "The last pipeline run ended with error" in py_ex2.value.args[0]
            with pytest.raises(CliCommandException) as ex:
                _dlt.deploy_command_wrapper(
                    "debug_pipeline.py",
                    deployment_method,
                    deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                    **deployment_args
                )
            assert ex._excinfo[1].error_code == -3

            os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = pg_credentials
            # also delete secrets so credentials are not mixed up on CI
            test_storage.delete(".dlt/secrets.toml")
            test_storage.atomic_rename(".dlt/secrets.toml.ci", ".dlt/secrets.toml")

            # reset toml providers to (1) where secrets exist (2) non existing dir so API_KEY is not found
            for settings_dir, api_key in [
                (os.path.join(test_storage.storage_path, ".dlt"), "api_key_9x3ehash"),
                (".", "please set me up!"),
            ]:
                with reset_providers(settings_dir=settings_dir):
                    # this time script will run
                    venv.run_script("debug_pipeline.py")
                    with echo.always_choose(False, always_choose_value=True):
                        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
                            deploy_command.deploy_command(
                                "debug_pipeline.py",
                                deployment_method,
                                deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                                **deployment_args
                            )
                            _out = buf.getvalue()
                        print(_out)
                        # make sure our secret and config values are all present
                        assert api_key in _out
                        assert "dlt_data" in _out
                        if "schedule" in deployment_args:
                            assert get_schedule_description(deployment_args["schedule"])
                        secrets_format = deployment_args.get("secrets_format", "env")
                        if secrets_format == "env":
                            assert "API_KEY" in _out
                        else:
                            assert "api_key = " in _out

            # non existing script name
            with pytest.raises(NoSuchPathError):
                deploy_command.deploy_command(
                    "no_pipeline.py",
                    deployment_method,
                    deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                    **deployment_args
                )
            with echo.always_choose(False, always_choose_value=True):
                with pytest.raises(CliCommandException) as ex:
                    _dlt.deploy_command_wrapper(
                        "no_pipeline.py",
                        deployment_method,
                        deploy_command.COMMAND_DEPLOY_REPO_LOCATION,
                        **deployment_args
                    )
                assert ex._excinfo[1].error_code == -5
