import io
import os
import contextlib
from subprocess import CalledProcessError

import dlt
from dlt.common.runners.venv import Venv
from dlt.common.storages.file_storage import FileStorage

from dlt.cli import echo, init_command, pipeline_command

from tests.cli.utils import echo_default_choice, repo_dir, project_files, cloned_init_repo, get_repo_dir, get_project_files


def test_pipeline_command_operations(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("chess", "duckdb", False, repo_dir)

    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        print(pipeline.working_dir)
        pipeline.drop()
    except Exception as e:
        print(e)

    # now run the pipeline
    os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)  # settings from local project (secrets.toml etc.)
    venv = Venv.restore_current()
    try:
        print(venv.run_script("chess_pipeline.py"))
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise

    # we are in the project working dir (thanks to project_files fixture)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("list", "-", None, 0)
        _out = buf.getvalue()
        # do we have chess pipeline in the list
        assert "chess_pipeline" in _out.splitlines()

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("info", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # do we have duckdb destination
        assert "dlt.destinations.duckdb" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("info", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        # were the sources state displayed
        assert '"chess": {' in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("trace", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # basic trace
        assert "Pipeline chess_pipeline completed in" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("trace", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        # extended trace
        assert "span id:" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("trace", "chess_pipeline", None, 2)
        _out = buf.getvalue()
        # trace with job info
        assert "Jobs details:" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("trace", "chess_pipeline", None, 2)
        _out = buf.getvalue()
        # trace with job info
        assert "Jobs details:" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("load-package", "chess_pipeline", None, 2)
        _out = buf.getvalue()
        # has package info
        assert "The package with load" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("failed-jobs", "chess_pipeline", None, 2)
        _out = buf.getvalue()
        # no failed jobs
        assert "No failed jobs found" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        # execute sync
        with echo.always_choose(False, True):
            pipeline_command.pipeline_command("sync", "chess_pipeline", None, 0)
        _out = buf.getvalue()

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        # after sync there's no trace
        pipeline_command.pipeline_command("info", "chess_pipeline", None, 0)
        print(_out)
        _out = buf.getvalue()
        # sync was executed
        assert "Pipeline does not have last run trace." in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            pipeline_command.pipeline_command("drop", "chess_pipeline", None, 0, resources=["players_games"])

        _out = buf.getvalue()
        assert "Selected resource(s): ['players_games']" in _out

        # Command was executed
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        assert "players_games" not in pipeline.default_schema.tables

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        # Test sync destination and drop when local state is missing
        pipeline._pipeline_storage.delete_folder('', recursively=True)
        with echo.always_choose(False, True):
            pipeline_command.pipeline_command("drop", "chess_pipeline", None, 0, destination=pipeline.destination, dataset_name=pipeline.dataset_name, resources=["players_profiles"])
        _out = buf.getvalue()

        assert "could not be restored: the pipeline was not found in " in _out
        assert "Selected resource(s): ['players_profiles']" in _out

        # Command was executed
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        assert "players_profiles" not in pipeline.default_schema.tables


def test_pipeline_command_failed_jobs(repo_dir: str, project_files: FileStorage) -> None:
    init_command.init_command("chess", "dummy", False, repo_dir)

    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        print(pipeline.working_dir)
        pipeline.drop()
    except Exception as e:
        print(e)

    # now run the pipeline
    os.environ["FAIL_PROB"] = "1.0"
    venv = Venv.restore_current()
    try:
        print(venv.run_script("chess_pipeline.py"))
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("trace", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # trace has LoadInfo with failed job
        assert "1 FAILED job(s)" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        pipeline_command.pipeline_command("failed-jobs", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # actual failed job data
        assert "JOB file type: jsonl" in _out
