import datetime
import uuid
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

import pytest
from pytest_console_scripts import ScriptRunner

from dlt._workspace.cli import _runtime_command as commands
from dlt._workspace.runtime_clients.api.models.detailed_run_response import DetailedRunResponse
from dlt._workspace.runtime_clients.api.models.list_runs_response_200 import ListRunsResponse200
from dlt._workspace.runtime_clients.api.models.logs_response import LogsResponse
from dlt._workspace.runtime_clients.api.models.run_response import RunResponse
from dlt._workspace.runtime_clients.api.models.run_status import RunStatus
from dlt._workspace.runtime_clients.api.models.run_trigger_type import RunTriggerType
from dlt._workspace.runtime_clients.api.models.script_response import ScriptResponse
from dlt._workspace.runtime_clients.api.models.script_type import ScriptType


def assert_job_run_headers(out: str) -> None:
    for h in ["Job name", "Run #", "Status", "Profile", "Started at", "Ended at"]:
        assert h in out


def assert_run_headers_no_job(out: str) -> None:
    for h in ["Run #", "Status", "Profile", "Started at", "Ended at"]:
        assert h in out


def assert_out_order(out: str, first: str, second: str) -> None:
    assert out.find(first) < out.find(second)


def _script(
    *, name: str, entry_point: str, version: int = 1, stype: ScriptType = ScriptType.BATCH
) -> ScriptResponse:
    now = datetime.datetime(2024, 6, 1, 12, 0, 0)
    return ScriptResponse(
        active=True,
        created_by=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        date_added=now,
        date_updated=now,
        description=f"{name} description",
        entry_point=entry_point,
        id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        name=name,
        script_type=stype,
        script_url="https://example.com",
        version=version,
        workspace_id=uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        next_scheduled_run=None,
        profile=None,
        schedule=None,
    )


def _detailed_script(
    *, name: str, entry_point: str, version: int = 1, stype: ScriptType = ScriptType.BATCH
):
    # Build DetailedScriptResponse via ScriptResponse -> to_dict -> DetailedScriptResponse.from_dict
    s = _script(name=name, entry_point=entry_point, version=version, stype=stype)
    from dlt._workspace.runtime_clients.api.models.detailed_script_response import (
        DetailedScriptResponse,
    )

    return DetailedScriptResponse.from_dict(s.to_dict())


def _run(
    *,
    run_id: Optional[uuid.UUID] = None,
    number: int,
    status: RunStatus,
    script: ScriptResponse,
    time_started: Optional[datetime.datetime],
    time_ended: Optional[datetime.datetime],
) -> DetailedRunResponse:
    now = datetime.datetime(2024, 6, 1, 12, 0, 0)
    return DetailedRunResponse(
        configuration_id=uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd"),
        date_added=now,
        date_updated=now,
        deployment_id=uuid.UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"),
        id=run_id or uuid.uuid4(),
        number=number,
        script=script,
        script_version_id=uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        status=status,
        trigger=RunTriggerType.MANUAL,
        workspace_id=uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc"),
        duration=None,
        logs=None,
        profile="default",
        time_started=time_started,
        time_ended=time_ended,
        triggered_by=None,
    )


@pytest.fixture(autouse=True)
def stub_login_and_client():
    class _Auth:
        workspace_id = "11111111-2222-3333-4444-555555555555"

    with (
        patch.object(commands, "login", return_value=_Auth()),
        patch.object(commands, "get_api_client", return_value=object()),
    ):
        yield


def test_runtime_job_runs_list_all(script_runner: ScriptRunner) -> None:
    scr = _script(name="job_a", entry_point="a.py")
    r1 = _run(
        number=1,
        status=RunStatus.COMPLETED,
        script=scr,
        time_started=datetime.datetime(2024, 6, 1, 12, 0, 0),
        time_ended=datetime.datetime(2024, 6, 1, 12, 5, 0),
    )
    r2 = _run(
        number=2,
        status=RunStatus.FAILED,
        script=scr,
        time_started=datetime.datetime(2024, 6, 1, 13, 0, 0),
        time_ended=datetime.datetime(2024, 6, 1, 13, 1, 0),
    )
    response = ListRunsResponse200(items=[r1, r2])
    with patch.object(
        commands.list_runs, "sync_detailed", return_value=SimpleNamespace(parsed=response)
    ):
        result = script_runner.run(["dlt", "runtime", "job-runs", "list"])

    assert result.returncode == 0
    out = result.stdout
    assert_job_run_headers(out)
    # Latest first (reversed)
    assert "job_a" in out
    assert "2" in out and "failed" in out
    assert "1" in out and "completed" in out
    assert_out_order(out, "2", "1")


def test_runtime_job_runs_list_for_job_filters(script_runner: ScriptRunner) -> None:
    scr = _detailed_script(name="job_b", entry_point="b.py")
    r = _run(
        number=7,
        status=RunStatus.RUNNING,
        script=scr,
        time_started=datetime.datetime(2024, 6, 1, 14, 0, 0),
        time_ended=None,
    )
    with (
        patch.object(
            commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=scr)
        ) as get_script_mock,
        patch.object(
            commands.list_runs,
            "sync_detailed",
            return_value=SimpleNamespace(parsed=ListRunsResponse200(items=[r])),
        ) as list_runs_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "job-runs", "job_b", "list"])

    assert result.returncode == 0
    out = result.stdout
    assert_job_run_headers(out)
    assert "job_b" in out and "running" in out and "7" in out

    # Ensure script lookup happened and list_runs used script id
    kwargs = list_runs_mock.call_args.kwargs
    assert kwargs["script_id"] == scr.id
    assert "client" in kwargs
    assert "workspace_id" in kwargs
    get_kwargs = get_script_mock.call_args.kwargs
    assert get_kwargs["script_id_or_name"] == "job_b"


def test_runtime_job_run_info_latest(script_runner: ScriptRunner) -> None:
    scr = _script(name="info_job", entry_point="info.py")
    latest = _run(
        number=3,
        status=RunStatus.COMPLETED,
        script=scr,
        time_started=datetime.datetime(2024, 6, 1, 15, 0, 0),
        time_ended=datetime.datetime(2024, 6, 1, 15, 10, 0),
    )
    with (
        patch.object(commands, "_get_latest_run", return_value=latest),
        patch.object(
            commands.get_run, "sync_detailed", return_value=SimpleNamespace(parsed=latest)
        ),
    ):
        result = script_runner.run(["dlt", "runtime", "job-runs", "info_job", "info"])

    assert result.returncode == 0
    out = result.stdout
    assert_run_headers_no_job(out)
    assert "3" in out and "completed" in out


def test_runtime_job_run_info_by_number(script_runner: ScriptRunner) -> None:
    scr = _script(name="count_job", entry_point="count.py")
    run_id = uuid.uuid4()
    detailed = _run(
        run_id=run_id,
        number=11,
        status=RunStatus.CANCELLED,
        script=scr,
        time_started=datetime.datetime(2024, 6, 1, 16, 0, 0),
        time_ended=datetime.datetime(2024, 6, 1, 16, 2, 0),
    )
    with (
        patch.object(commands, "_resolve_run_id_by_number", return_value=run_id),
        patch.object(
            commands.get_run, "sync_detailed", return_value=SimpleNamespace(parsed=detailed)
        ) as get_run_mock,
    ):
        result = script_runner.run(["dlt", "runtime", "job-runs", "count_job", "11", "info"])
    assert result.returncode == 0
    out = result.stdout
    assert_run_headers_no_job(out)
    assert "11" in out and "cancelled" in out
    kwargs = get_run_mock.call_args.kwargs
    assert kwargs["run_id"] == run_id


def test_runtime_job_run_logs_latest_non_follow(script_runner: ScriptRunner) -> None:
    scr = _script(name="logs_job", entry_point="logs.py")
    latest = _run(
        number=5,
        status=RunStatus.RUNNING,
        script=scr,
        time_started=datetime.datetime(2024, 6, 1, 17, 0, 0),
        time_ended=None,
    )
    resp = LogsResponse(run=latest, logs="hello\nworld")
    with (
        patch.object(commands, "_get_latest_run", return_value=latest),
        patch.object(
            commands.get_run_logs, "sync_detailed", return_value=SimpleNamespace(parsed=resp)
        ),
    ):
        result = script_runner.run(["dlt", "runtime", "job-runs", "logs_job", "logs"])
    assert result.returncode == 0
    out = result.stdout
    assert "Run logs for Run # 5 of job logs_job" in out
    assert "hello" in out and "world" in out
    assert "End of run logs" in out


def test_runtime_job_run_logs_follow_delegates(script_runner: ScriptRunner) -> None:
    with (
        patch.object(commands, "follow_job_run") as follow_mock,
        patch.object(commands, "_get_latest_run") as latest_mock,
    ):
        latest_mock.return_value = _run(
            number=1,
            status=RunStatus.STARTING,
            script=_script(name="follow_job", entry_point="f.py"),
            time_started=None,
            time_ended=None,
        )
        result = script_runner.run(["dlt", "runtime", "job-runs", "follow_job", "logs", "--follow"])
    assert result.returncode == 0
    # Should pass final states for logs following
    args, kwargs = follow_mock.call_args
    # positional args: (run_id, final_states, start_status, follow_logs)
    assert args[3] is True
    assert "api_client" in kwargs and "auth_service" in kwargs


def test_runtime_job_run_cancel_latest(script_runner: ScriptRunner) -> None:
    scr = _script(name="cancel_job", entry_point="x.py")
    latest = _run(
        number=9,
        status=RunStatus.RUNNING,
        script=scr,
        time_started=datetime.datetime(2024, 6, 1, 18, 0, 0),
        time_ended=None,
    )
    # cancel returns a DetailedRunResponse per implementation
    with (
        patch.object(commands, "_get_latest_run", return_value=latest),
        patch.object(
            commands.cancel_run,
            "sync_detailed",
            return_value=SimpleNamespace(parsed=latest),
        ),
    ):
        result = script_runner.run(["dlt", "runtime", "job-runs", "cancel_job", "cancel"])
    assert result.returncode == 0
    out = result.stdout
    assert "Successfully requested cancellation of run # 9" in out


def test_runtime_job_run_cancel_by_number(script_runner: ScriptRunner) -> None:
    # Ensure cancel works when run number is provided
    run_id = uuid.uuid4()
    parsed = _run(
        run_id=run_id,
        number=4,
        status=RunStatus.CANCELLING,
        script=_script(name="cancel_by_no", entry_point="cbn.py"),
        time_started=datetime.datetime(2024, 6, 1, 19, 0, 0),
        time_ended=None,
    )
    with (
        patch.object(commands, "_resolve_run_id_by_number", return_value=run_id),
        patch.object(
            commands.cancel_run, "sync_detailed", return_value=SimpleNamespace(parsed=parsed)
        ),
    ):
        result = script_runner.run(["dlt", "runtime", "job-runs", "cancel_by_no", "4", "cancel"])
    # Should not crash on undefined variable; message may not include number in current impl
    assert result.returncode == 0
    assert "Failed" not in result.stdout


def test_follow_job_run_streams_until_final(auth_service_stub, api_client_stub, capsys) -> None:
    scr = _script(name="stream_job", entry_point="s.py")
    base = _run(
        number=1,
        status=RunStatus.STARTING,
        script=scr,
        time_started=datetime.datetime(2024, 6, 1, 20, 0, 0),
        time_ended=None,
    )
    r_starting = base
    r_running = DetailedRunResponse(
        configuration_id=base.configuration_id,
        date_added=base.date_added,
        date_updated=base.date_updated,
        deployment_id=base.deployment_id,
        id=base.id,
        number=base.number,
        script=base.script,
        script_version_id=base.script_version_id,
        status=RunStatus.RUNNING,
        trigger=base.trigger,
        workspace_id=base.workspace_id,
        duration=base.duration,
        logs=base.logs,
        profile=base.profile,
        time_ended=base.time_ended,
        time_started=base.time_started,
        triggered_by=base.triggered_by,
    )
    r_completed = DetailedRunResponse(
        configuration_id=base.configuration_id,
        date_added=base.date_added,
        date_updated=base.date_updated,
        deployment_id=base.deployment_id,
        id=base.id,
        number=base.number,
        script=base.script,
        script_version_id=base.script_version_id,
        status=RunStatus.COMPLETED,
        trigger=base.trigger,
        workspace_id=base.workspace_id,
        duration=base.duration,
        logs=base.logs,
        profile=base.profile,
        time_ended=datetime.datetime(2024, 6, 1, 20, 5, 0),
        time_started=base.time_started,
        triggered_by=base.triggered_by,
    )

    _run_counter = {"i": 0}

    def _get_run_side_effect(**kwargs):
        # On each call, advance status: STARTING -> RUNNING -> COMPLETED
        _run_counter["i"] += 1
        if _run_counter["i"] == 1:
            return SimpleNamespace(parsed=r_starting)
        if _run_counter["i"] == 2:
            return SimpleNamespace(parsed=r_running)
        return SimpleNamespace(parsed=r_completed)

    _logs_counter = {"i": 0}

    def _get_logs_side_effect(**kwargs):
        _logs_counter["i"] += 1
        logs = "\n".join(["line1", "line2"][: _logs_counter["i"]])
        return SimpleNamespace(parsed=LogsResponse(run=r_running, logs=logs))

    with (
        patch.object(commands.get_run, "sync_detailed", side_effect=_get_run_side_effect),
        patch.object(commands.get_run_logs, "sync_detailed", side_effect=_get_logs_side_effect),
        patch.object(commands.time, "sleep", return_value=None),
    ):
        commands.follow_job_run(
            r_starting.id,
            final_states={RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.COMPLETED},
            start_status=RunStatus.STARTING,
            follow_logs=True,
            auth_service=auth_service_stub,
            api_client=api_client_stub,
        )

    out = capsys.readouterr().out
    # Should show logs header and stream lines progressively and finish
    assert "========== Run logs ==========" in out
    assert "line1" in out and "line2" in out
    assert "End of run logs" in out


def test_create_job_run_happy_path(script_runner: ScriptRunner) -> None:
    now = datetime.datetime(2024, 6, 1, 21, 0, 0)
    run_resp = RunResponse(
        configuration_id=uuid.uuid4(),
        date_added=now,
        date_updated=now,
        deployment_id=uuid.uuid4(),
        id=uuid.uuid4(),
        number=42,
        script_version_id=uuid.uuid4(),
        status=RunStatus.PENDING,
        trigger=RunTriggerType.MANUAL,
        workspace_id=uuid.uuid4(),
        duration=None,
        logs=None,
        profile="default",
        time_ended=None,
        time_started=now,
        triggered_by=None,
    )
    with patch.object(
        commands.create_run, "sync_detailed", return_value=SimpleNamespace(parsed=run_resp)
    ):
        result = script_runner.run(["dlt", "runtime", "job-runs", "some_job", "create"])
    assert result.returncode == 0
    out = result.stdout
    assert_run_headers_no_job(out)
    assert "42" in out and "pending" in out
