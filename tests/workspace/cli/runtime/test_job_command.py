import argparse
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
from dlt._workspace.runtime_clients.api.models.detailed_script_response import (
    DetailedScriptResponse,
)
from dlt._workspace.runtime_clients.api.models.list_scripts_response_200 import (
    ListScriptsResponse200,
)
from dlt._workspace.runtime_clients.api.models.script_response import ScriptResponse
from dlt._workspace.runtime_clients.api.models.script_type import ScriptType


def assert_job_headers(out: str) -> None:
    for h in ["Job name", "Version #", "Script path", "Created at", "Script type", "Schedule"]:
        assert h in out


def assert_job_values(out: str, job: "DetailedScriptResponse | ScriptResponse") -> None:
    assert job.name in out
    assert job.entry_point in out
    stype = (
        job.script_type.value if isinstance(job.script_type, ScriptType) else str(job.script_type)
    )
    assert stype.lower() in out
    # schedule may be UNSET/None
    if getattr(job, "schedule", None):
        assert str(job.schedule) in out


def assert_out_order(out: str, first: str, second: str) -> None:
    assert out.find(first) < out.find(second)


_WORKSPACE_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


@pytest.fixture(autouse=True)
def stub_login_and_client():
    class _Auth:
        workspace_id = str(_WORKSPACE_ID)

    with (
        patch.object(commands, "login", return_value=_Auth()),
        patch.object(commands, "get_api_client", return_value=object()),
    ):
        yield


def _dsr(
    *,
    name: str,
    entry_point: str,
    script_type: ScriptType = ScriptType.BATCH,
    schedule: Optional[str] = None,
    version: int = 1,
) -> DetailedScriptResponse:
    now = datetime.datetime(2024, 6, 1, 12, 0, 0)
    return DetailedScriptResponse(
        active=True,
        created_by=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        date_added=now,
        date_updated=now,
        description=f"{name} description",
        entry_point=entry_point,
        id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        name=name,
        script_type=script_type,
        script_url="https://example.com",
        version=version,
        workspace_id=_WORKSPACE_ID,
        last_run=None,
        next_scheduled_run=None,
        profile=None,
        schedule=schedule,
    )


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
        created_by=uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        date_added=now,
        date_updated=now,
        description=f"{name} description",
        entry_point=entry_point,
        id=uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
        name=name,
        script_type=script_type,
        script_url="https://example.com",
        version=version,
        workspace_id=_WORKSPACE_ID,
        next_scheduled_run=None,
        profile=None,
        schedule=schedule,
    )


def test_runtime_job_list_outputs_all(script_runner: ScriptRunner) -> None:
    job1 = _dsr(name="job_one", entry_point="scripts/one.py", version=1, schedule="0 0 * * *")
    job2 = _dsr(name="job_two", entry_point="scripts/two.py", version=2, schedule=None)
    response = ListScriptsResponse200(items=[job1, job2])

    with patch.object(
        commands.list_scripts, "sync_detailed", return_value=SimpleNamespace(parsed=response)
    ) as sync_detailed_mock:
        result = script_runner.run(["dlt", "runtime", "job", "list"])

    assert result.returncode == 0
    out = result.stdout
    assert_job_headers(out)
    # Values (latest first due to reverse)
    assert_job_values(out, job2)
    assert_job_values(out, job1)
    # Order: job2 (version 2) before job1 (version 1)
    assert_out_order(out, "job_two", "job_one")

    kwargs = sync_detailed_mock.call_args.kwargs
    assert str(kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert "client" in kwargs


def test_runtime_job_info_by_name(script_runner: ScriptRunner) -> None:
    job = _dsr(name="job_info", entry_point="scripts/info.py", version=5, schedule=None)
    with patch.object(
        commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=job)
    ) as sync_detailed_mock:
        result = script_runner.run(["dlt", "runtime", "job", "job_info", "info"])

    assert result.returncode == 0
    out = result.stdout
    assert_job_headers(out)
    assert_job_values(out, job)

    kwargs = sync_detailed_mock.call_args.kwargs
    assert str(kwargs["workspace_id"]) == str(_WORKSPACE_ID)
    assert kwargs["script_id_or_name"] == "job_info"
    assert "client" in kwargs


def test_runtime_job_create_happy_path(script_runner: ScriptRunner, tmp_path) -> None:
    # Create a real script file relative to CWD used by the subprocess
    script_name = "create_job.py"
    with open(script_name, "w", encoding="utf-8") as f:
        f.write("print('hello')\n")
    try:
        # Simulate: job does not exist yet (no warnings), then create/upsert
        with (
            patch.object(
                commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=None)
            ) as get_mock,
            # capture created response to assert
            patch.object(
                commands.create_or_update_script,
                "sync_detailed",
                return_value=SimpleNamespace(
                    parsed=_sr(name=script_name, entry_point=script_name, version=1, schedule=None)
                ),
            ) as upsert_mock,
        ):
            result = script_runner.run(
                ["dlt", "runtime", "job", script_name, "create", "--description", "Nice job"]
            )

        assert result.returncode == 0
        out = result.stdout
        # Output shows created job in a table
        assert_job_headers(out)
        assert_job_values(out, upsert_mock.call_args.kwargs["body"])  # assert basic fields shown

        # Check API call args: get_script for existence
        get_kwargs = get_mock.call_args.kwargs
        assert str(get_kwargs["workspace_id"]) == str(_WORKSPACE_ID)
        assert "client" in get_kwargs

        # Check API call args: upsert body
        up_kwargs = upsert_mock.call_args.kwargs
        assert str(up_kwargs["workspace_id"]) == str(_WORKSPACE_ID)
        assert "client" in up_kwargs
        body = up_kwargs["body"]
        # Name defaults to script path when --name not provided
        assert body.name == script_name
        assert body.entry_point == script_name
        assert body.script_type == ScriptType.BATCH
        assert body.description == f"The {script_name} job" or body.description == "Nice job"
    finally:
        try:
            os.remove(script_name)
        except FileNotFoundError:
            pass


def test_job_info_requires_name_raises(auth_service_stub, api_client_stub) -> None:
    with pytest.raises(CliCommandInnerException):
        commands.job_info(None, auth_service=auth_service_stub, api_client=api_client_stub)


def test_job_create_requires_script_path_raises(auth_service_stub, api_client_stub) -> None:
    args = argparse.Namespace(name=None, schedule=None, interactive=False, description=None)
    with pytest.raises(CliCommandInnerException):
        commands.job_create(None, args, auth_service=auth_service_stub, api_client=api_client_stub)


def test_job_create_nonexistent_path_raises(auth_service_stub, api_client_stub) -> None:
    args = argparse.Namespace(name=None, schedule=None, interactive=False, description=None)
    with pytest.raises(CliCommandInnerException):
        commands.job_create(
            "no_such_file.py", args, auth_service=auth_service_stub, api_client=api_client_stub
        )


def test_job_create_invalid_schedule_raises(auth_service_stub, api_client_stub) -> None:
    # create a real file under active run_dir
    run_dir = active().run_dir
    script_name = "cron_bad.py"
    path = os.path.join(run_dir, script_name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("print('x')\n")
    try:
        args = argparse.Namespace(
            name=None, schedule="bad cron", interactive=False, description=None
        )
        with pytest.raises(CliCommandInnerException):
            commands.job_create(
                script_name, args, auth_service=auth_service_stub, api_client=api_client_stub
            )
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def test_job_create_interactive_with_name_and_schedule(auth_service_stub, api_client_stub) -> None:
    # prepare a file under run_dir
    run_dir = active().run_dir
    script_name = "interactive_app.py"
    path = os.path.join(run_dir, script_name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("print('app')\n")
    try:
        args = argparse.Namespace(
            name="custom_job",
            schedule="0 12 * * *",
            interactive=True,
            description="Custom desc",
        )
        with (
            patch.object(
                commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=None)
            ),
            patch.object(
                commands.create_or_update_script,
                "sync_detailed",
                return_value=SimpleNamespace(
                    parsed=_sr(
                        name="custom_job",
                        entry_point=script_name,
                        script_type=ScriptType.INTERACTIVE,
                        schedule="0 12 * * *",
                        version=3,
                    )
                ),
            ) as upsert_mock,
        ):
            commands.job_create(
                script_name,
                args,
                auth_service=auth_service_stub,
                api_client=api_client_stub,
            )
        body = upsert_mock.call_args.kwargs["body"]
        assert body.name == "custom_job"
        assert body.entry_point == script_name
        assert body.script_type == ScriptType.INTERACTIVE
        assert body.schedule == "0 12 * * *"
        assert body.description == "Custom desc"
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def test_job_create_warns_on_existing_path_mismatch(auth_service_stub, api_client_stub) -> None:
    # prepare file
    run_dir = active().run_dir
    script_name = "a.py"
    path = os.path.join(run_dir, script_name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("print('a')\n")
    try:
        args = argparse.Namespace(
            name="different_name", schedule=None, interactive=False, description=None
        )
        existing = _dsr(name="different_name", entry_point="different.py")
        with (
            patch.object(commands.fmt, "warning") as warn_mock,
            patch.object(
                commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=existing)
            ),
            patch.object(
                commands.create_or_update_script,
                "sync_detailed",
                return_value=SimpleNamespace(
                    parsed=_sr(name="different_name", entry_point=script_name)
                ),
            ),
        ):
            commands.job_create(
                script_name,
                args,
                auth_service=auth_service_stub,
                api_client=api_client_stub,
            )
        assert warn_mock.called
        assert "already exists for different script path" in warn_mock.call_args.args[0]
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def test_job_create_warns_on_existing_schedule_mismatch(auth_service_stub, api_client_stub) -> None:
    # prepare file
    run_dir = active().run_dir
    script_name = "b.py"
    path = os.path.join(run_dir, script_name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("print('b')\n")
    try:
        args = argparse.Namespace(
            name=None, schedule="0 0 * * *", interactive=False, description=None
        )
        existing = _dsr(name=script_name, entry_point=script_name, schedule="0 1 * * *")
        with (
            patch.object(commands.fmt, "warning") as warn_mock,
            patch.object(
                commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=existing)
            ),
            patch.object(
                commands.create_or_update_script,
                "sync_detailed",
                return_value=SimpleNamespace(parsed=_sr(name=script_name, entry_point=script_name)),
            ),
        ):
            commands.job_create(
                script_name,
                args,
                auth_service=auth_service_stub,
                api_client=api_client_stub,
            )
        assert warn_mock.called
        assert "already exists with different schedule" in warn_mock.call_args.args[0]
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


def test_job_create_warns_on_existing_script_type_mismatch(
    auth_service_stub, api_client_stub
) -> None:
    # prepare file
    run_dir = active().run_dir
    script_name = "c.py"
    path = os.path.join(run_dir, script_name)
    with open(path, "w", encoding="utf-8") as f:
        f.write("print('c')\n")
    try:
        args = argparse.Namespace(name=None, schedule=None, interactive=True, description=None)
        existing = _dsr(
            name=script_name, entry_point=script_name, script_type=ScriptType.BATCH, schedule=None
        )
        with (
            patch.object(commands.fmt, "warning") as warn_mock,
            patch.object(
                commands.get_script, "sync_detailed", return_value=SimpleNamespace(parsed=existing)
            ),
            patch.object(
                commands.create_or_update_script,
                "sync_detailed",
                return_value=SimpleNamespace(
                    parsed=_sr(
                        name=script_name,
                        entry_point=script_name,
                        script_type=ScriptType.INTERACTIVE,
                        schedule=None,
                    )
                ),
            ),
        ):
            commands.job_create(
                script_name,
                args,
                auth_service=auth_service_stub,
                api_client=api_client_stub,
            )
        assert warn_mock.called
        assert "already exists with different interactive mode" in warn_mock.call_args.args[0]
    finally:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
