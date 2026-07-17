import io
import os
import contextlib
import shutil
import pytest
import logging
from subprocess import CalledProcessError
from typing import Any
from pytest_console_scripts import ScriptRunner

import dlt
from dlt.common.runners.venv import Venv
from dlt.common.schema import Schema
from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import custom_environ, uniq_id
from dlt.extract import DltSource
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt._workspace.cli import echo, _init_command, _pipeline_command

from tests.workspace.cli.utils import (
    WORKSPACE_CLI_CASES_DIR,
    auto_echo_default_choice,
    repo_dir,
    cloned_init_repo,
    _cached_init_repo,
)

CHOOSE_PRIORITY_CASES = [
    # always_confirm=True overrides everything
    (dict(always_choose_default=True, always_choose_value=False, always_confirm=True), True),
    # always_choose_value overrides always_choose_default
    (dict(always_choose_default=True, always_choose_value=True, always_confirm=False), True),
    # always_choose_default kicks in only when value/confirm are off
    (dict(always_choose_default=True, always_choose_value=None, always_confirm=False), False),
]
CHOOSE_PRIORITY_IDS = [
    "always-confirm-overrides-everything",
    "always-choose-value-overrides-always-choose-default",
    "always-choose-default-kicks-in-if-allowed",
]


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
        assert "Pipeline chess_pipeline load step finished in" in _out
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

        assert "No local pipeline state found" in _out
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


@pytest.mark.parametrize("choose_kwargs,confirms", CHOOSE_PRIORITY_CASES, ids=CHOOSE_PRIORITY_IDS)
def test_pipeline_command_drop_partial_loads(
    repo_dir: str, choose_kwargs: dict[str, Any], confirms: bool
) -> None:
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
        with echo.always_choose(**choose_kwargs):
            _pipeline_command.pipeline_command("drop-pending-packages", "chess_pipeline", None, 1)
            _out = buf.getvalue()
    print(_out)

    if confirms:
        assert "Packages aborted" in _out
        # verify packages are gone
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            _pipeline_command.pipeline_command("drop-pending-packages", "chess_pipeline", None, 1)
            _out = buf.getvalue()
            assert "No pending packages found" in _out
        print(_out)
    else:
        assert "Packages aborted" not in _out


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


@pytest.mark.parametrize("choose_kwargs,confirms", CHOOSE_PRIORITY_CASES, ids=CHOOSE_PRIORITY_IDS)
def test_pipeline_command_drop_with_global_args(
    repo_dir: str, choose_kwargs: dict[str, Any], confirms: bool
) -> None:
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

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(**choose_kwargs):
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

    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    if confirms:
        assert "Selected resource(s): ['players_games']" in _out
        assert "players_games" not in pipeline.default_schema.tables
    else:
        assert "players_games" in pipeline.default_schema.tables


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
        assert "will be aborted" in _out
        assert "Packages aborted" in _out
        assert "local state and schemas restored" in _out
    print(_out)

    # aborted packages stay in local storage with their failed jobs
    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    assert len(pipeline.list_completed_load_packages()) > 0

    # after abort, running again should show no pending packages
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command("abort-packages", "chess_pipeline", None, 0)
        _out = buf.getvalue()
        assert "No pending packages found" in _out
    print(_out)


def test_pipeline_command_abort_packages_partial_warning() -> None:
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
    # letters completes, numbers fails and is retried -> the package is partially loaded
    with pytest.raises(PipelineStepFailed):
        p.run(s)
    load_id = p.list_normalized_load_packages()[0]

    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        # decline the confirmation, we only inspect the warning
        with echo.always_choose(False, False):
            _pipeline_command.pipeline_command("abort-packages", pipeline_name, None, 0)
        _out = buf.getvalue()
    assert "partially loaded" in _out
    assert "will NOT revert" in _out
    assert "load-package %s row-counts" % load_id in _out
    print(_out)


def test_pipeline_command_load_package_abort() -> None:
    """`load-package <id> abort` aborts the target package and newer ones; older stay intact."""
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")

    @dlt.resource(incremental=dlt.sources.incremental("id"))
    def numbers(data):
        yield data

    p.run(numbers([{"id": 1}, {"id": 2}, {"id": 3}]))
    for batch in ([{"id": 4}], [{"id": 5}], [{"id": 6}]):
        p.extract(numbers(batch))
        p.normalize()
    pending = p.list_normalized_load_packages()
    assert len(pending) == 3

    # abort at the middle package: it and the newest are deleted, the oldest stays loadable
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "load-package", pipeline_name, None, 0, load_id=pending[1], action="abort"
            )
        _out = buf.getvalue()
    # a non-oldest package is not being loaded, so it is deleted without an aborted record
    assert "Normalized packages to delete" in _out
    assert pending[1] in _out and pending[2] in _out

    p = dlt.attach(pipeline_name=pipeline_name)
    assert p.list_normalized_load_packages() == [pending[0]]
    with p.sql_client() as client:
        loaded = [row[0] for row in client.execute_sql("SELECT id FROM numbers ORDER BY id")]
    # only the first run's rows are loaded; the oldest pending package is still runnable
    assert loaded == [1, 2, 3]
    p.load()
    with p.sql_client() as client:
        loaded = [row[0] for row in client.execute_sql("SELECT id FROM numbers ORDER BY id")]
    assert loaded == [1, 2, 3, 4]


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
        # deprecated alias runs the abort flow
        assert "drop-pending-packages is deprecated" in _out
        assert "abort-packages" in _out
        assert "Packages aborted" in _out
    print(_out)
    # pending packages are gone after the abort
    pipeline = dlt.attach(pipeline_name="chess_pipeline")
    assert pipeline.has_pending_data is False


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
    pending = p.list_pending_retry_jobs_in_package(load_id)
    assert len(pending) > 0
    job_file_name = os.path.basename(pending[0][0])
    job_id = ParsedLoadJobFileName.parse(job_file_name).job_id()

    # test with job_id (as users would copy from load-package output)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "load-package",
                pipeline_name,
                None,
                0,
                load_id=load_id,
                action="fail-job",
                job=job_id,
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
    job_file_name = os.path.basename(pending[0][0])

    # test with full file_name (including retry count)
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        with echo.always_choose(False, True):
            _pipeline_command.pipeline_command(
                "load-package",
                pipeline_name,
                None,
                0,
                load_id=load_id,
                action="fail-job",
                job=job_file_name,
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
            "load-package",
            pipeline_name,
            None,
            0,
            load_id=load_id,
            action="fail-job",
            job="nonexistent.abc.jsonl",
        )


def test_pipeline_command_load_package_job_filter() -> None:
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

    # first run retries the numbers job terminally, second run retries it again -> two attempts
    with pytest.raises(PipelineStepFailed):
        p.run(s)
    with pytest.raises(PipelineStepFailed):
        p.run()

    load_id = p.list_normalized_load_packages()[0]

    # filter shows the matching job with its full retry exception history
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command(
            "load-package", pipeline_name, None, 0, load_id=load_id, action="job", job="numbers"
        )
        _out = buf.getvalue()
        assert "Job numbers." in _out
        assert "state: new_jobs" in _out
        assert _out.count("terminal") >= 2
        assert "configured to fail" in _out
        # the non-matching job is not shown
        assert "letters" not in _out
    print(_out)

    # no match reports cleanly
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command(
            "load-package", pipeline_name, None, 0, load_id=load_id, action="job", job="nonexistent"
        )
        assert "No jobs matching" in buf.getvalue()


def test_pipeline_command_load_package_row_counts() -> None:
    os.environ["COMPLETED_PROB"] = "1.0"
    pipeline_name = "pipe_" + uniq_id()
    p = dlt.pipeline(pipeline_name=pipeline_name, destination="duckdb")
    load_info = p.run([{"id": 1}, {"id": 2}, {"id": 3}], table_name="numbers")
    load_id = load_info.loads_ids[0]

    # row counts for a completed load, works with an explicit load id
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command(
            "load-package", pipeline_name, None, 0, load_id=load_id, action="row-counts"
        )
        _out = buf.getvalue()
    assert "Row counts for load %s" % load_id in _out
    assert "Package is COMPLETED" in _out
    assert "numbers: 3" in _out
    # each table shows its write disposition
    assert "write disposition: append" in _out
    # dlt tables are shown too, so an updated state row is visible
    assert "_dlt_pipeline_state: 1" in _out
    assert "total: 4 rows across 2 tables" in _out
    print(_out)

    # works even after the local package is gone from the working dir
    p._get_load_storage().loaded_packages.delete_package(load_id)
    assert load_id not in p.list_completed_load_packages()
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command(
            "load-package", pipeline_name, None, 0, load_id=load_id, action="row-counts"
        )
        _out = buf.getvalue()
    assert "Package is COMPLETED" in _out
    assert "numbers: 3" in _out

    # an unknown load id: not completed, no rows, and a link to the partial-package docs
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command(
            "load-package", pipeline_name, None, 0, load_id="9999999999.0", action="row-counts"
        )
        _out = buf.getvalue()
    assert "NOT completed" in _out
    assert "partially-loaded-packages" in _out

    # a root table with nested tables reports how many could also be modified
    nested_info = p.run([{"id": 1, "children": [{"x": 1}, {"x": 2}]}], table_name="parent")
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        _pipeline_command.pipeline_command(
            "load-package",
            pipeline_name,
            None,
            0,
            load_id=nested_info.loads_ids[0],
            action="row-counts",
        )
        _out = buf.getvalue()
    assert "nested tables could also be modified" in _out


def test_invoke_list_pipelines(legacy_workspace_context, script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlt", "pipeline", "--list-pipelines"])
    # directory does not exist (we point to TEST_STORAGE)
    assert result.returncode == 0
    assert "No pipelines found in" in result.stdout

    result = script_runner.run(["dlt", "pipeline", "--list-pipelines"])
    assert result.returncode == 0
    assert "No pipelines found in" in result.stdout


def test_invoke_pipeline(legacy_workspace_context, script_runner: ScriptRunner) -> None:
    # info on non existing pipeline
    result = script_runner.run(["dlt", "pipeline", "debug_pipeline", "info"])
    assert result.returncode == -2
    assert "No local pipeline state found" in result.stderr

    shutil.copytree(
        os.path.join(WORKSPACE_CLI_CASES_DIR, "deploy_pipeline"), ".", dirs_exist_ok=True
    )

    # dummy_pipeline.py needs `api_key` via `dlt.secrets.value`; the case provides it only via
    # profile-aware `dev.secrets.toml`, which the bare legacy `RunContext` does not load
    with custom_environ({"COMPLETED_PROB": "1.0", "SOURCES__API_KEY": "legacy_api_key"}):
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
