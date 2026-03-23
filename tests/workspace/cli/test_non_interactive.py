import io
import os
import contextlib
from subprocess import CalledProcessError

import dlt
from dlt.common.runners.venv import Venv

from dlt._workspace.cli import echo, _init_command, _pipeline_command

from tests.workspace.cli.utils import (
    repo_dir,
    cloned_init_repo,
    _cached_init_repo,
)


def test_non_interactive_drop_command(repo_dir: str) -> None:
    """Test that --non-interactive flag makes drop command proceed without prompts."""
    # init_command will use auto_echo_default_choice from the fixture
    _init_command.init_command("chess", "duckdb", repo_dir)

    # clean up any existing pipeline
    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        pipeline.drop()
    except Exception:
        pass

    # run the pipeline
    os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
    venv = Venv.restore_current()
    try:
        venv.run_script("chess_pipeline.py")
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise

    # verify the resource exists before drop
    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    assert "players_games" in pipeline.default_schema.tables

    # test drop command with non-interactive mode (simulates --non-interactive flag)
    # We use echo.always_choose to set ALWAYS_CHOOSE_VALUE = True
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "drop", "chess_pipeline", None, 0, resources=["players_games"]
            )
        _out = buf.getvalue()

        # verify the command output shows it will drop the resource
        assert "Selected resource(s): ['players_games']" in _out

    # verify the command actually executed (resource was dropped)
    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    assert "players_games" not in pipeline.default_schema.tables


def test_non_interactive_sync_command(repo_dir: str) -> None:
    """Test that --non-interactive flag makes sync command proceed without prompts."""
    _init_command.init_command("chess", "duckdb", repo_dir)

    # clean up any existing pipeline
    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        pipeline.drop()
    except Exception:
        pass

    # run the pipeline
    os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
    venv = Venv.restore_current()
    try:
        venv.run_script("chess_pipeline.py")
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise

    # test sync command with non-interactive mode
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command("sync", "chess_pipeline", None, 0)
        _out = buf.getvalue()

        # verify sync was executed
        assert "Dropping local state" in _out
        assert "Restoring from destination" in _out

    # after sync there's no trace
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("info", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        assert "Pipeline does not have last run trace." in _out


def test_non_interactive_drop_pending_packages(repo_dir: str) -> None:
    """Test that --non-interactive flag makes drop-pending-packages command proceed."""
    _init_command.init_command("chess", "dummy", repo_dir)
    os.environ["EXCEPTION_PROB"] = "1.0"

    # clean up any existing pipeline
    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        pipeline.drop()
    except Exception:
        pass

    # run pipeline with exception to create pending packages
    venv = Venv.restore_current()
    try:
        venv.run_script("chess_pipeline.py")
    except CalledProcessError:
        pass  # expected to fail

    # verify there are pending packages
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("info", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        assert (
            "extracted packages ready to be normalized" in _out
            or "normalized packages ready to be loaded" in _out
        )

    # test drop-pending-packages with non-interactive mode
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command("drop-pending-packages", "chess_pipeline", None, 1)
        _out = buf.getvalue()

        assert "Pending packages deleted" in _out

    # verify packages were deleted
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("drop-pending-packages", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        assert "No pending packages found" in _out
