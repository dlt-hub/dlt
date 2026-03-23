import io
import os
import contextlib
import pytest
import logging
from subprocess import CalledProcessError

import dlt
from dlt.common.runners.venv import Venv
from dlt.common.schema import Schema
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.extract import DltSource
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt._workspace.cli import echo, _init_command, _pipeline_command

from tests.workspace.cli.utils import (
    auto_echo_default_choice,
    repo_dir,
    cloned_init_repo,
    _cached_init_repo,
)


def test_pipeline_command_operations(repo_dir: str) -> None:
    _init_command.init_command("chess", "duckdb", repo_dir)

    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        print(pipeline.working_dir)
        pipeline.drop()
    except Exception as e:
        print(e)

    # now run the pipeline
    os.environ.pop(
        "DESTINATION__DUCKDB__CREDENTIALS", None
    )  # settings from local project (secrets.toml etc.)
    venv = Venv.restore_current()
    try:
        print(venv.run_script("chess_pipeline.py"))
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise

    # we are in the project working dir (thanks to project_files fixture)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("list", "-", None, 0)
        _out = buf.getvalue()
        # do we have chess pipeline in the list
        assert _out.splitlines()[1].startswith("chess_pipeline")
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("info", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # do we have duckdb destination
        assert "destination_name: None" in _out
        assert "destination_type: dlt.destinations.duckdb" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("info", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        # were the sources state displayed
        assert '"chess": {' in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("trace", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # basic trace
        assert "Pipeline chess_pipeline load step completed in" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("trace", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        # extended trace
        assert "span id:" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("trace", "chess_pipeline", None, 2)
        _out = buf.getvalue()
        # trace with job info
        assert "Jobs details:" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("trace", "chess_pipeline", None, 2)
        _out = buf.getvalue()
        # trace with job info
        assert "Jobs details:" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("load-package", "chess_pipeline", None, 2)
        _out = buf.getvalue()
        # has package info
        assert "The package with load" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("failed-jobs", "chess_pipeline", None, 2)
        _out = buf.getvalue()
        # no failed jobs
        assert "No failed jobs found" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        # execute sync
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command("sync", "chess_pipeline", None, 0)
        _out = buf.getvalue()
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        # after sync there's no trace
        _pipeline_command.pipeline_command("info", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # sync was executed
        assert "Pipeline does not have last run trace." in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "drop", "chess_pipeline", None, 0, resources=["players_games"]
            )

        _out = buf.getvalue()
        assert "Selected resource(s): ['players_games']" in _out
        assert (
            "WARNING: Unless hardcoded, credentials are loaded from environment variables and/or"
            " configuration files."
            not in _out
        )

        # Command was executed
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        assert "players_games" not in pipeline.default_schema.tables

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        # Test sync destination and drop when local state is missing
        pipeline._pipeline_storage.delete_folder("", recursively=True)
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "drop",
                "chess_pipeline",
                None,
                0,
                destination=pipeline.destination,
                dataset_name=pipeline.dataset_name,
                resources=["players_profiles"],
            )
        _out = buf.getvalue()

        assert "could not be restored: the pipeline was not found in " in _out
        assert "Selected resource(s): ['players_profiles']" in _out

        # Command was executed
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        assert "players_profiles" not in pipeline.default_schema.tables


def test_pipeline_command_failed_jobs(repo_dir: str) -> None:
    _init_command.init_command("chess", "dummy", repo_dir)

    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        pipeline.drop()
    except Exception as e:
        print(e)

    # now run the pipeline
    os.environ["FAIL_PROB"] = "1.0"
    # let it fail without an exception
    os.environ["RAISE_ON_FAILED_JOBS"] = "false"
    venv = Venv.restore_current()
    try:
        print(venv.run_script("chess_pipeline.py"))
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise

    # disable logging output for discovery cache for this test
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("trace", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # trace has LoadInfo with failed job
        assert "1 FAILED job(s)" in _out

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("failed-jobs", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        # actual failed job data
        assert "JOB file type: jsonl" in _out


def test_pipeline_command_drop_partial_loads(repo_dir: str) -> None:
    _init_command.init_command("chess", "dummy", repo_dir)
    os.environ["EXCEPTION_PROB"] = "1.0"

    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        pipeline.drop()
    except Exception as e:
        print(e)

    venv = Venv.restore_current()
    with pytest.raises(CalledProcessError) as cpe:
        print(venv.run_script("chess_pipeline.py"))
    assert "PipelineStepFailed" in cpe.value.stdout

    # complete job manually to make a partial load
    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    load_storage = pipeline._get_load_storage()
    load_id = load_storage.normalized_packages.list_packages()[0]
    job = load_storage.normalized_packages.list_new_jobs(load_id)[0]
    load_storage.normalized_packages.start_job(
        load_id, FileStorage.get_file_name_from_file_path(job)
    )
    load_storage.normalized_packages.complete_job(
        load_id, FileStorage.get_file_name_from_file_path(job)
    )

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("info", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        # one package is partially loaded
        assert "This package is partially loaded" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command("drop-pending-packages", "chess_pipeline", None, 1)
            _out = buf.getvalue()
            assert "Pending packages deleted" in _out
    print(_out)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("drop-pending-packages", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        assert "No pending packages found" in _out
    print(_out)


def test_drop_from_wrong_dir(repo_dir: str) -> None:
    # import contextlib

    # with contextlib.contextmanager(create_test_run_context)():

    _init_command.init_command("chess", "duckdb", repo_dir)

    os.environ.pop(
        "DESTINATION__DUCKDB__CREDENTIALS", None
    )  # settings from local project (secrets.toml etc.)
    venv = Venv.restore_current()
    try:
        print(venv.run_script("chess_pipeline.py"))
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise

    # Running from the correct location should not raise warning
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command(
            "drop", "chess_pipeline", None, 0, resources=["players_games"]
        )
        _out = buf.getvalue()
        assert (
            "WARNING: You should run this from the same directory as the pipeline script"
            not in _out
        )

    # load pipeline and last run dir to trigger the warning
    pipeline = dlt.attach("chess_pipeline")
    last_run_context = pipeline.get_local_state_val("last_run_context")
    last_run_context["run_dir"] = "wrong_dir"
    pipeline.set_local_state_val("last_run_context", last_run_context)

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command(
            "drop", "chess_pipeline", None, 0, resources=["players_games"]
        )
        _out = buf.getvalue()
        assert "WARNING: You should run this from the same directory as the pipeline script" in _out


def test_pipeline_command_drop_with_global_args(repo_dir: str) -> None:
    """Test that global CLI arguments don't cause errors in pipeline drop command."""
    _init_command.init_command("chess", "duckdb", repo_dir)

    os.environ.pop("DESTINATION__DUCKDB__CREDENTIALS", None)
    venv = Venv.restore_current()
    try:
        print(venv.run_script("chess_pipeline.py"))
    except CalledProcessError as cpe:
        print(cpe.stdout)
        print(cpe.stderr)
        raise

    # Test drop command with global arguments that should be ignored
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "drop",
                "chess_pipeline",
                None,
                0,
                resources=["players_games"],
                no_pwd=False,  # Global arg that should be ignored
                debug=False,  # Another global arg
            )
        _out = buf.getvalue()
        assert "Selected resource(s): ['players_games']" in _out

    # Verify the command actually executed
    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    assert "players_games" not in pipeline.default_schema.tables


def test_pipeline_command_abort_packages(repo_dir: str) -> None:
    _init_command.init_command("chess", "dummy", repo_dir)
    os.environ["EXCEPTION_PROB"] = "1.0"

    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        pipeline.drop()
    except Exception as e:
        print(e)

    venv = Venv.restore_current()
    with pytest.raises(CalledProcessError) as cpe:
        print(venv.run_script("chess_pipeline.py"))
    assert "PipelineStepFailed" in cpe.value.stdout

    # verify pending packages exist before abort
    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    assert len(pipeline.list_normalized_load_packages()) > 0

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command("abort-packages", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        assert "The following packages will be aborted" in _out
        assert "Packages aborted" in _out
        assert "resynced from destination" in _out
    print(_out)

    # after abort, running again should show no pending packages
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("abort-packages", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        assert "No pending packages found" in _out
    print(_out)


def test_pipeline_command_drop_pending_packages_deprecation(repo_dir: str) -> None:
    _init_command.init_command("chess", "dummy", repo_dir)
    os.environ["EXCEPTION_PROB"] = "1.0"

    try:
        pipeline = dlt.attach(pipeline_name="chess_pipeline")
        pipeline.drop()
    except Exception as e:
        print(e)

    venv = Venv.restore_current()
    with pytest.raises(CalledProcessError) as cpe:
        print(venv.run_script("chess_pipeline.py"))
    assert "PipelineStepFailed" in cpe.value.stdout

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command("drop-pending-packages", "chess_pipeline", None, 1)
        _out = buf.getvalue()
        assert "drop-pending-packages is deprecated" in _out
        assert "abort-packages" in _out
        assert "Pending packages deleted" in _out
    print(_out)


def test_pipeline_command_fail_job() -> None:
    os.environ["LOAD__AUTO_ABORT_ON_TERMINAL_ERROR"] = "false"
    os.environ["LOAD__RAISE_ON_FAILED_JOBS"] = "true"
    os.environ["DESTINATION__DUMMY__FAIL_TABLE_NAMES"] = '["numbers"]'
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    s = DltSource(
        Schema("source"),
        "module",
        [
            dlt.resource([1, 2, 3], table_name="numbers", name="numbers"),
            dlt.resource(["a", "b", "c"], table_name="letters", name="letters"),
        ],
    )

    with pytest.raises(PipelineStepFailed):
        p.run(s)

    load_id = p.list_normalized_load_packages()[0]
    pending = p.list_pending_retry_jobs_in_package(load_id, exception_type="terminal")
    assert len(pending) > 0
    job_file_name = os.path.basename(pending[0])
    job_id = ParsedLoadJobFileName.parse(job_file_name).job_id()

    # test with job_id (as users would copy from load-package output)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "fail-job", pipeline_name, None, 0, load_id=load_id, job=job_id
            )
        _out = buf.getvalue()
        assert "Job:" in _out
        assert "Retry count:" in _out
        assert "Exception type: terminal" in _out
        assert "moved to failed_jobs" in _out
    print(_out)

    # job should now be in failed_jobs
    failed = p.list_failed_jobs_in_package(load_id)
    failed_ids = [j.job_file_info.job_id() for j in failed]
    assert job_id in failed_ids


def test_pipeline_command_fail_job_with_file_name() -> None:
    os.environ["LOAD__AUTO_ABORT_ON_TERMINAL_ERROR"] = "false"
    os.environ["LOAD__RAISE_ON_FAILED_JOBS"] = "true"
    os.environ["DESTINATION__DUMMY__FAIL_TABLE_NAMES"] = '["numbers"]'
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    s = DltSource(
        Schema("source"),
        "module",
        [
            dlt.resource([1, 2, 3], table_name="numbers", name="numbers"),
        ],
    )

    with pytest.raises(PipelineStepFailed):
        p.run(s)

    load_id = p.list_normalized_load_packages()[0]
    pending = p.list_pending_retry_jobs_in_package(load_id)
    assert len(pending) > 0
    job_file_name = os.path.basename(pending[0])

    # test with full file_name (including retry count)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "fail-job", pipeline_name, None, 0, load_id=load_id, job=job_file_name
            )
        _out = buf.getvalue()
        assert "moved to failed_jobs" in _out
    print(_out)


def test_pipeline_command_fail_job_not_found() -> None:
    os.environ["LOAD__AUTO_ABORT_ON_TERMINAL_ERROR"] = "false"
    os.environ["LOAD__RAISE_ON_FAILED_JOBS"] = "true"
    os.environ["DESTINATION__DUMMY__FAIL_TABLE_NAMES"] = '["numbers"]'
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="dummy")
    s = DltSource(
        Schema("source"),
        "module",
        [
            dlt.resource([1, 2, 3], table_name="numbers", name="numbers"),
        ],
    )

    with pytest.raises(PipelineStepFailed):
        p.run(s)

    load_id = p.list_normalized_load_packages()[0]

    from dlt._workspace.cli.exceptions import CliCommandInnerException

    with pytest.raises(CliCommandInnerException, match="not found in pending retry jobs"):
        _pipeline_command.pipeline_command(
            "fail-job", pipeline_name, None, 0, load_id=load_id, job="nonexistent.abc.jsonl"
        )
