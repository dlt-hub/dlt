import datetime
import os
import uuid
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

import pytest
from pytest_console_scripts import ScriptRunner

from dlt._workspace._workspace_context import active
from dlt._workspace.cli import _runtime_command as commands
from dlt._workspace.cli.exceptions import CliCommandInnerException
from dlt._workspace.runtime_clients.api.models.configuration_response import ConfigurationResponse
from dlt._workspace.runtime_clients.api.models.deployment_response import DeploymentResponse
from dlt._workspace.runtime_clients.api.models.detailed_run_response import DetailedRunResponse
from dlt._workspace.runtime_clients.api.models.detailed_script_response import (
    DetailedScriptResponse,
)
from dlt._workspace.runtime_clients.api.models.run_response import RunResponse
from dlt._workspace.runtime_clients.api.models.run_status import RunStatus
from dlt._workspace.runtime_clients.api.models.run_trigger_type import RunTriggerType
from dlt._workspace.runtime_clients.api.models.script_response import ScriptResponse
from dlt._workspace.runtime_clients.api.models.script_type import ScriptType


@pytest.fixture(autouse=True)
def stub_login_and_client():
    class _Auth:
        workspace_id = "11111111-2222-3333-4444-555555555555"

    with (
        patch.object(commands, "login", return_value=_Auth()),
        patch.object(commands, "get_api_client", return_value=object()),
    ):
        yield


def _sr(
    *,
    name: str,
    entry_point: str,
    script_type: ScriptType = ScriptType.BATCH,
    schedule: Optional[str] = None,
    version: int = 1,
) -> ScriptResponse:
    now = datetime.datetime(2024, 6, 1, 12, 0, 0)
    return ScriptResponse(
        active=True,
        created_by=uuid.uuid4(),
        date_added=now,
        date_updated=now,
        description=f"{name} description",
        entry_point=entry_point,
        id=uuid.uuid4(),
        name=name,
        script_type=script_type,
        script_url="https://example.com",
        version=version,
        workspace_id=uuid.uuid4(),
        next_scheduled_run=None,
        profile=None,
        schedule=schedule,
    )


def _dsr(
    *,
    name: str,
    entry_point: str,
    script_type: ScriptType = ScriptType.BATCH,
    schedule: Optional[str] = None,
    version: int = 1,
) -> DetailedScriptResponse:
    # Create via ScriptResponse to_dict to keep field naming consistent
    return DetailedScriptResponse.from_dict(
        _sr(
            name=name,
            entry_point=entry_point,
            script_type=script_type,
            schedule=schedule,
            version=version,
        ).to_dict()
    )


def _run(
    *,
    number: int,
    status: RunStatus,
    script: ScriptResponse,
    time_started: Optional[datetime.datetime],
    time_ended: Optional[datetime.datetime],
) -> DetailedRunResponse:
    now = datetime.datetime(2024, 6, 1, 12, 0, 0)
    return DetailedRunResponse(
        configuration_id=uuid.uuid4(),
        date_added=now,
        date_updated=now,
        deployment_id=uuid.uuid4(),
        id=uuid.uuid4(),
        number=number,
        script=script,
        script_version_id=uuid.uuid4(),
        status=status,
        trigger=RunTriggerType.MANUAL,
        workspace_id=uuid.uuid4(),
        duration=None,
        logs=None,
        profile="default",
        time_started=time_started,
        time_ended=time_ended,
        triggered_by=None,
    )


def test_launch_happy_path_calls_sync_and_follows(script_runner: ScriptRunner) -> None:
    with (
        patch.object(commands, "_ensure_profile_warning"),
        patch.object(commands, "sync_deployment") as dep_mock,
        patch.object(commands, "sync_configuration") as conf_mock,
        patch.object(commands, "run_script", return_value=uuid.uuid4()) as run_mock,
        patch.object(commands, "follow_run_status") as status_mock,
        patch.object(commands, "follow_run_logs") as logs_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "launch", "pipeline.py"])
    assert result.returncode == 0
    assert dep_mock.called and conf_mock.called and run_mock.called
    status_mock.assert_called_once()
    logs_mock.assert_called_once()


def test_launch_with_detach_does_not_follow(script_runner: ScriptRunner) -> None:
    with (
        patch.object(commands, "_ensure_profile_warning"),
        patch.object(commands, "sync_deployment"),
        patch.object(commands, "sync_configuration"),
        patch.object(commands, "run_script", return_value=uuid.uuid4()),
        patch.object(commands, "follow_run_status") as status_mock,
        patch.object(commands, "follow_run_logs") as logs_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "launch", "pipeline.py", "--detach"])
    assert result.returncode == 0
    status_mock.assert_not_called()
    logs_mock.assert_not_called()


def test_serve_warns_non_marimo_opens_url(script_runner: ScriptRunner) -> None:
    # create a script under run_dir without marimo import
    script_name = "app.py"
    path = os.path.join(active().run_dir, script_name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("print('hello')\n")
    try:
        dsr = _dsr(name=script_name, entry_point=script_name, script_type=ScriptType.INTERACTIVE)
        with (
            patch.object(commands.fmt, "warning") as warn_mock,
            patch.object(commands, "sync_deployment"),
            patch.object(commands, "sync_configuration"),
            patch.object(commands, "run_script", return_value=uuid.uuid4()),
            patch.object(commands, "follow_run_status"),
            patch.object(
                commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=dsr)
            ),
            patch.object(commands.webbrowser, "open") as web_open_mock,
        ):
            result = script_runner.run(["dlt", "runtime", "serve", script_name])
        assert result.returncode == 0
        assert warn_mock.called
        web_open_mock.assert_called()
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def test_serve_missing_file_raises(script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlt", "runtime", "serve", "no_such_app.py"])
    assert result.returncode != 0


def test_launch_and_serve_messages(script_runner: ScriptRunner) -> None:
    # prepare file
    script_name = "do_run.py"
    path = os.path.join(active().run_dir, script_name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("print('x')\n")
    try:
        # batch
        run_resp = RunResponse(
            configuration_id=uuid.uuid4(),
            date_added=datetime.datetime(2024, 6, 1, 0, 0, 0),
            date_updated=datetime.datetime(2024, 6, 1, 0, 0, 0),
            deployment_id=uuid.uuid4(),
            id=uuid.uuid4(),
            number=1,
            script_version_id=uuid.uuid4(),
            status=RunStatus.PENDING,
            trigger=RunTriggerType.MANUAL,
            workspace_id=uuid.uuid4(),
            duration=None,
            logs=None,
            profile=None,
            time_ended=None,
            time_started=datetime.datetime(2024, 6, 1, 0, 0, 0),
            triggered_by=None,
        )
        with (
            patch.object(commands, "sync_deployment"),
            patch.object(commands, "sync_configuration"),
            patch.object(
                commands.create_or_update_script,
                "sync_detailed",
                return_value=SimpleNamespace(parsed=_sr(name=script_name, entry_point=script_name)),
            ) as upsert_mock,
            patch.object(
                commands.create_run, "sync_detailed", return_value=SimpleNamespace(parsed=run_resp)
            ),
            patch.object(commands, "follow_run_status"),
            patch.object(commands, "follow_run_logs"),
        ):
            result = script_runner.run(["dlt", "runtime", "launch", script_name])
        assert result.returncode == 0
        out = result.stdout
        assert "created or updated successfully" in out
        assert "Job do_run.py run successfully" in out
        # interactive
        with (
            patch.object(commands, "sync_deployment"),
            patch.object(commands, "sync_configuration"),
            patch.object(
                commands.create_or_update_script,
                "sync_detailed",
                return_value=SimpleNamespace(
                    parsed=_sr(
                        name=script_name,
                        entry_point=script_name,
                        script_type=ScriptType.INTERACTIVE,
                    )
                ),
            ),
            patch.object(
                commands.create_run, "sync_detailed", return_value=SimpleNamespace(parsed=run_resp)
            ),
            patch.object(commands, "follow_run_status"),
        ):
            result2 = script_runner.run(["dlt", "runtime", "serve", script_name])
        assert result2.returncode == 0
        out2 = result2.stdout
        assert "Job is accessible on" in out2
        # ensure upsert body captured correct type for first call
        body = upsert_mock.call_args.kwargs["body"]
        assert body.script_type == ScriptType.BATCH
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    # follow behaviors covered via launch/serve and job-runs logs tests


def test_schedule_happy_path(script_runner: ScriptRunner) -> None:
    # create script to schedule
    script_name = "sched.py"
    path = os.path.join(active().run_dir, script_name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("print('sched')\n")
    try:
        with (
            patch.object(commands, "sync_deployment"),
            patch.object(commands, "sync_configuration"),
            patch.object(
                commands.create_or_update_script,
                "sync_detailed",
                return_value=SimpleNamespace(
                    parsed=_sr(
                        name=script_name, entry_point=script_name, script_type=ScriptType.BATCH
                    )
                ),
            ) as upsert_mock,
        ):
            result = script_runner.run(["dlt", "runtime", "schedule", script_name, "0 0 * * *"])
        assert result.returncode == 0
        out = result.stdout
        assert "Scheduled" in out and "0 0 * * *" in out
        body = upsert_mock.call_args.kwargs["body"]
        assert body.name == script_name and body.schedule == "0 0 * * *"
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def test_schedule_invalid_cron_raises(script_runner: ScriptRunner) -> None:
    result = script_runner.run(["dlt", "runtime", "schedule", "any.py", "bad_cron"])
    assert result.returncode != 0


def test_schedule_cancel_happy_and_current(script_runner: ScriptRunner) -> None:
    scr = _dsr(
        name="jobx", entry_point="jobx.py", script_type=ScriptType.BATCH, schedule="0 * * * *"
    )
    with (
        patch.object(
            commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=scr)
        ),
        patch.object(
            commands.create_or_update_script,
            "sync_detailed",
            return_value=SimpleNamespace(parsed=_sr(name="jobx", entry_point="jobx.py")),
        ) as upsert_mock,
        patch.object(commands, "request_run_cancel") as cancel_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "schedule", "jobx.py", "cancel", "--current"])
    assert result.returncode == 0
    # schedule should be unset
    body = upsert_mock.call_args.kwargs["body"]
    assert body.schedule is None
    cancel_mock.assert_called_once()


def test_schedule_cancel_not_scheduled_prints_error(script_runner: ScriptRunner) -> None:
    scr = _dsr(name="joby", entry_point="joby.py", script_type=ScriptType.BATCH, schedule=None)
    with (
        patch.object(
            commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=scr)
        ),
        patch.object(commands.fmt, "error") as err_mock,
        patch.object(commands.create_or_update_script, "sync_detailed") as upsert_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "schedule", "joby.py", "cancel"])
    assert result.returncode == 0
    err_mock.assert_called_once()
    upsert_mock.assert_not_called()


def test_open_dashboard_opens_or_errors(script_runner: ScriptRunner) -> None:
    dsr = _dsr(name="dashboard", entry_point="dashboard", script_type=ScriptType.INTERACTIVE)
    dsr.script_url = "https://dashboard.example"
    with (
        patch.object(
            commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=dsr)
        ),
        patch.object(commands.webbrowser, "open") as web_open_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "dashboard"])
    assert result.returncode == 0
    web_open_mock.assert_called()

    # No URL -> prints error
    dsr2 = _dsr(name="dashboard", entry_point="dashboard", script_type=ScriptType.INTERACTIVE)
    dsr2.script_url = ""
    with (
        patch.object(
            commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=dsr2)
        ),
        patch.object(commands.fmt, "error") as err_mock,
    ):
        result2 = script_runner.run(["dlt", "runtime", "dashboard"])
    assert result2.returncode == 0
    err_mock.assert_called_once()


def test_runtime_info_prints_sections(script_runner: ScriptRunner) -> None:
    # jobs count
    jobs = [
        _dsr(name="a", entry_point="a.py"),
        _dsr(name="b", entry_point="b.py"),
        _dsr(name="c", entry_point="c.py"),
    ]
    # latest run
    latest_run = _run(
        number=10,
        status=RunStatus.COMPLETED,
        script=_sr(name="a", entry_point="a.py"),
        time_started=datetime.datetime(2024, 6, 1, 10, 0, 0),
        time_ended=datetime.datetime(2024, 6, 1, 10, 5, 0),
    )
    # deployment and configuration
    dep = DeploymentResponse(
        content_hash="abc",
        created_by=uuid.uuid4(),
        date_added=datetime.datetime(2024, 6, 1, 9, 0, 0),
        date_updated=datetime.datetime(2024, 6, 1, 9, 5, 0),
        file_count=2,
        file_names="x,y",
        id=uuid.uuid4(),
        size=1024,
        version=7,
        workspace_id=uuid.uuid4(),
    )
    conf = ConfigurationResponse(
        content_hash="def",
        created_by=uuid.uuid4(),
        date_added=datetime.datetime(2024, 6, 1, 8, 0, 0),
        date_updated=datetime.datetime(2024, 6, 1, 8, 5, 0),
        file_count=1,
        file_names="z",
        id=uuid.uuid4(),
        profiles="default",
        size=512,
        version=3,
        workspace_id=uuid.uuid4(),
    )
    with (
        patch.object(
            commands.list_scripts,
            "sync_detailed",
            return_value=SimpleNamespace(
                parsed=commands.list_scripts.ListScriptsResponse200(items=jobs)
            ),
        ),
        patch.object(commands, "_get_latest_run", return_value=latest_run),
        patch.object(
            commands.get_latest_deployment,
            "sync_detailed",
            return_value=SimpleNamespace(parsed=dep),
        ),
        patch.object(
            commands.get_latest_configuration,
            "sync_detailed",
            return_value=SimpleNamespace(parsed=conf),
        ),
    ):
        result = script_runner.run(["dlt", "runtime", "info"])
    assert result.returncode == 0
    out = result.stdout
    assert "# registered jobs: 3" in out
    assert "Latest job run: a (" in out and "started at" in out and "ended at" in out
    assert "Current deployment version: 7" in out
    assert "Current configuration version: 3" in out
