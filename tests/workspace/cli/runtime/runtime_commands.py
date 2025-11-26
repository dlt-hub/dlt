import uuid
from unittest.mock import patch

import pytest
from pytest_console_scripts import ScriptRunner


def _stub_auth():
    class _Auth:
        workspace_id = str(uuid.uuid4())

    return _Auth()


@pytest.fixture(autouse=True)
def stub_login_and_client():
    with (
        patch("dlt._workspace.cli._runtime_command.login", return_value=_stub_auth()),
        patch("dlt._workspace.cli._runtime_command.get_api_client", return_value=object()),
    ):
        yield


def test_runtime_logs_latest_routes_to_helper(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.runtime_logs_by_ref") as helper:
        result = script_runner.run(["dlt", "runtime", "logs", "foo.py"])
    assert result.returncode == 0
    helper.assert_called_once_with("foo.py", None)


def test_runtime_cancel_with_number_routes(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.runtime_cancel_by_ref") as helper:
        result = script_runner.run(["dlt", "runtime", "cancel", "foo.py", "3"])
    assert result.returncode == 0
    helper.assert_called_once()
    # args: ("foo.py", 3)
    assert helper.call_args[0][0] == "foo.py"
    assert helper.call_args[0][1] == 3


def test_runtime_job_list_routes(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.jobs_list") as helper:
        result = script_runner.run(["dlt", "runtime", "job", "list"])
    assert result.returncode == 0
    assert helper.called


def test_runtime_job_info_routes(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.job_info") as helper:
        result = script_runner.run(["dlt", "runtime", "job", "info", "foo.py"])
    assert result.returncode == 0
    helper.assert_called_once()
    assert helper.call_args[0][0] == "foo.py"


def test_runtime_job_create_routes(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.job_create") as helper:
        result = script_runner.run(["dlt", "runtime", "job", "create", "foo.py", "--name", "myjob"])
    assert result.returncode == 0
    helper.assert_called_once()
    assert helper.call_args[0][0] == "foo.py"
    assert helper.call_args[0][1] == "myjob"


def test_runtime_job_runs_list_routes(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.get_runs") as helper:
        result = script_runner.run(["dlt", "runtime", "job-run", "list"])
    assert result.returncode == 0
    # when no script/job passed, should be called without script_name
    helper.assert_called_once()
    kwargs = helper.call_args.kwargs
    assert "script_name" not in kwargs or kwargs.get("script_name") in (None, "")


def test_runtime_launch_flag_detach_routes(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.runtime_launch") as helper:
        result = script_runner.run(["dlt", "runtime", "launch", "foo.py", "--detach"])
    assert result.returncode == 0
    helper.assert_called_once()
    assert helper.call_args[0][0] == "foo.py"
    assert helper.call_args.kwargs.get("detach") is True


def test_runtime_serve_routes(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.runtime_serve") as helper:
        result = script_runner.run(["dlt", "runtime", "serve", "foo.py"])
    assert result.returncode == 0
    helper.assert_called_once_with("foo.py")


def test_runtime_schedule_and_cancel_routes(script_runner: ScriptRunner) -> None:
    with patch("dlt._workspace.cli._runtime_command.runtime_schedule") as schedule_helper:
        r1 = script_runner.run(["dlt", "runtime", "schedule", "foo.py", "*/5 * * * *"])
    assert r1.returncode == 0
    schedule_helper.assert_called_once_with("foo.py", "*/5 * * * *")

    with patch("dlt._workspace.cli._runtime_command.runtime_schedule_cancel") as cancel_helper:
        r2 = script_runner.run(["dlt", "runtime", "schedule", "foo.py", "cancel", "--current"])
    assert r2.returncode == 0
    cancel_helper.assert_called_once()
    assert cancel_helper.call_args.kwargs.get("cancel_current") is True
