import os
import shutil
from subprocess import CalledProcessError
from unittest.mock import patch
import pytest

import dlt

from dlt.cli.exceptions import CliCommandException
from dlt.common.configuration.container import Container
from dlt.common.pipeline import PipelineContext
from dlt.common.runners.venv import Venv
from dlt.common.storages.file_storage import FileStorage

from dlt.common.utils import set_working_dir
from dlt.cli import init_command, deploy_command
from dlt.cli.init_command import utils as cli_utils
from dlt.pipeline.exceptions import CannotRestorePipelineException

from tests.utils import preserve_environ, autouse_test_storage, TEST_STORAGE_ROOT, test_storage
from dlt.extract.decorators import _SOURCES


@pytest.fixture(autouse=True)
def deactivate_pipeline() -> FileStorage:
    yield
    Container()[PipelineContext].deactivate()


def test_init_command_template() -> None:
    _SOURCES.clear()

    with set_working_dir(TEST_STORAGE_ROOT):
        init_command.init_command("debug_pipeline", "bigquery", False)
        assert_init_files("debug_pipeline", "bigquery")


def test_init_command_chess_verified_pipeline() -> None:
    _SOURCES.clear()

    with set_working_dir(TEST_STORAGE_ROOT):
        init_command.init_command("chess", "postgres", False)
        assert_init_files("chess_pipeline", "postgres")


def test_init_command_generic() -> None:
    _SOURCES.clear()

    with set_working_dir(TEST_STORAGE_ROOT):
        init_command.init_command("generic_pipeline", "redshift", True)
        assert_init_files("chess_pipeline", "postgres")


def test_init_repo_was_really_cloned() -> None:
    _SOURCES.clear()

    with patch.object(cli_utils, "clone_command_repo", return_value=None):
        with set_working_dir(TEST_STORAGE_ROOT):
            # will raise attribute error
            with pytest.raises(AttributeError):
                init_command.init_command("generic_pipeline", "redshift", True, branch="twitter_variant")


def test_deploy_command(test_storage: FileStorage) -> None:
    # drop pipeline
    p = dlt.pipeline(pipeline_name="debug_pipeline")
    p._wipe_working_folder()

    shutil.copytree("tests/cli/cases/deploy_pipeline", TEST_STORAGE_ROOT, dirs_exist_ok=True)

    with set_working_dir(TEST_STORAGE_ROOT):
        from git import Repo, Remote

        # we have a repo without git origin
        with Repo.init(".") as repo:
            # test_storage.atomic_rename("empty_git", ".git")
            with pytest.raises(CliCommandException) as py_ex:
                deploy_command.deploy_command("debug_pipeline.py", "github-action", "*/30 * * * *", True, True)
            assert "Your current repository has no origin set" in py_ex.value.args[0]
            # we have a repo that was never run
            Remote.create(repo, "origin", "git@github.com:rudolfix/dlt-cmd-test-2.git")
            with pytest.raises(CannotRestorePipelineException):
                deploy_command.deploy_command("debug_pipeline.py", "github-action", "*/30 * * * *", True, True)
            # run the script with wrong credentials (it is postgres there)
            venv = Venv.restore_current()
            # mod environ so wrong password is passed to override secrets.toml
            pg_credentials = os.environ.pop("DESTINATION__POSTGRES__CREDENTIALS")
            # os.environ["DESTINATION__POSTGRES__CREDENTIALS__PASSWORD"] = "password"
            with pytest.raises(CalledProcessError) as py_ex:
                venv.run_script("debug_pipeline.py")
            print(py_ex.value.output)
            with pytest.raises(deploy_command.PipelineWasNotRun) as py_ex:
                deploy_command.deploy_command("debug_pipeline.py", "github-action", "*/30 * * * *", True, True)
            assert "The last pipeline run ended with error" in py_ex.value.args[0]
            os.environ["DESTINATION__POSTGRES__CREDENTIALS"] = pg_credentials
            # also delete secrets so credentials are not mixed up on CI
            test_storage.delete(".dlt/secrets.toml")
            test_storage.atomic_rename(".dlt/secrets.toml.ci", ".dlt/secrets.toml")
            # this time script will run
            venv.run_script("debug_pipeline.py")
            deploy_command.deploy_command("debug_pipeline.py", "github-action", "*/30 * * * *", True, True)


def assert_init_files(pipeline_name: str, destination_name: str) -> None:
    pass
