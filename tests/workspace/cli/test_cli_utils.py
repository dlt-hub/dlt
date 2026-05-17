"""Tests for `dlt._workspace.cli.utils` — host-shared utilities (telemetry / track_command)."""

import contextlib
import io
from argparse import Namespace
from typing import Any, Dict
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture

from dlt._workspace.cli import DEFAULT_VERIFIED_SOURCES_REPO, echo
from dlt._workspace.cli._deploy_command import (
    COMMAND_DEPLOY_REPO_LOCATION,
    DeploymentMethods,
    deploy_command_wrapper,
)
from dlt._workspace.cli._init_command import (
    init_command_wrapper,
    list_sources_command_wrapper,
)
from dlt._workspace.cli.commands import InitCommand, PipelineCommand, TelemetryCommand
from dlt._workspace.cli.utils import display_run_context_info, track_command
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration
from dlt.common.runtime.anon_tracker import disable_anon_tracker

from tests.utils import start_test_telemetry
from tests.workspace.utils import isolated_workspace


# telemetry helpers local to this module (avoid depending on other test modules)
SENT_ITEMS: list[dict[str, Any]] = []


def _mock_before_send(event: dict[str, Any], _unused_hint: Any = None) -> dict[str, Any]:
    # capture event for assertions
    SENT_ITEMS.append(event)
    return event


@contextlib.contextmanager
def _captured_telemetry() -> Any:
    """Start the test telemetry sink and clear captured events on entry."""
    config = WorkspaceRuntimeConfiguration(dlthub_telemetry=True)
    SENT_ITEMS.clear()
    saved_host = echo.get_cli_host_name()
    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)
        try:
            yield
        finally:
            disable_anon_tracker()
            echo.set_cli_host_name(saved_host)


def test_track_command_track_after_passes_params() -> None:
    """verify track_command wraps with telemetry and forwards arg names and extra kwargs."""
    with _captured_telemetry():

        @track_command("my_cmd", False, "x", "y", extra_const="value")
        def _fn(x: Any, y: Any, z: Any = None) -> Any:
            return "ok"

        _fn("X", 7, z="ignored")

    assert len(SENT_ITEMS) == 1
    event = SENT_ITEMS[0]

    # event basics
    assert event["event"] == "command_my_cmd"
    props = event["properties"]
    assert props["event_category"] == "command"
    assert props["event_name"] == "my_cmd"

    # captured args and extra kwargs
    assert props["x"] == "X"
    assert props["y"] == 7
    assert props["extra_const"] == "value"

    # automatic props
    assert isinstance(props["elapsed"], (int, float)) and props["elapsed"] >= 0
    assert props["success"] is True
    # auto-injected host (resolved at call time, not at decoration time)
    assert props["host"] in ("dlt", "dlthub")


def test_track_command_track_before_passes_params() -> None:
    """when tracking before, event is emitted once with success True and includes provided params."""
    with _captured_telemetry():

        @track_command("before_cmd", True, "p", ignored="const")
        def _fn(p: Any) -> Any:
            # raising should not affect success flag in before mode
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            _fn(123)

    assert len(SENT_ITEMS) == 1
    event = SENT_ITEMS[0]
    assert event["event"] == "command_before_cmd"
    props = event["properties"]
    assert props["event_category"] == "command"
    assert props["event_name"] == "before_cmd"
    assert props["p"] == 123
    assert props["ignored"] == "const"
    assert isinstance(props["elapsed"], (int, float)) and props["elapsed"] >= 0
    assert props["success"] is True


def test_command_instrumentation() -> None:
    @track_command("instrument_ok", False, "in_ok_param", "in_ok_param_2")
    def instrument_ok(in_ok_param: str, in_ok_param_2: int) -> int:
        return 0

    @track_command("instrument_err_status", False, "in_err_status", "no_se")
    def instrument_err_status(in_err_status: int) -> int:
        return 1

    @track_command("instrument_raises", False, "in_raises")
    def instrument_raises(in_raises: bool) -> int:
        raise Exception("failed")

    @track_command("instrument_raises", True, "in_raises_2")
    def instrument_raises_2(in_raises_2: bool) -> int:
        raise Exception("failed")

    with _captured_telemetry():
        instrument_ok("ok_param", 7)
        msg = SENT_ITEMS[0]
        assert msg["event"] == "command_instrument_ok"
        assert msg["properties"]["in_ok_param"] == "ok_param"
        assert msg["properties"]["in_ok_param_2"] == 7
        assert msg["properties"]["success"] is True
        assert isinstance(msg["properties"]["elapsed"], float)

        SENT_ITEMS.clear()
        instrument_err_status(88)
        msg = SENT_ITEMS[0]
        assert msg["event"] == "command_instrument_err_status"
        assert msg["properties"]["in_err_status"] == 88
        assert msg["properties"]["success"] is False

        SENT_ITEMS.clear()
        with pytest.raises(Exception):
            instrument_raises(True)
        msg = SENT_ITEMS[0]
        assert msg["properties"]["success"] is False

        SENT_ITEMS.clear()
        with pytest.raises(Exception):
            instrument_raises_2(True)
        msg = SENT_ITEMS[0]
        # this one is tracked BEFORE command is executed so success
        assert msg["properties"]["success"] is True


def test_instrumentation_wrappers() -> None:
    with _captured_telemetry():
        # init_command_wrapper is no longer decorated at definition; production wraps it
        # at the call site in InitCommand.execute. Mirror that here.
        tracked_init = track_command("init", False, "source_name", "destination_type")(
            init_command_wrapper
        )
        with io.StringIO() as buf, contextlib.redirect_stderr(buf):
            try:
                tracked_init("instrumented_source", "<UNK>", None, None)
            except Exception:
                pass
        msg = SENT_ITEMS[0]
        assert msg["event"] == "command_init"
        assert msg["properties"]["source_name"] == "instrumented_source"
        assert msg["properties"]["destination_type"] == "<UNK>"
        assert msg["properties"]["success"] is False

        SENT_ITEMS.clear()
        list_sources_command_wrapper(DEFAULT_VERIFIED_SOURCES_REPO, None)
        msg = SENT_ITEMS[0]
        assert msg["event"] == "command_list_sources"

        SENT_ITEMS.clear()
        try:
            deploy_command_wrapper(
                "list.py",
                DeploymentMethods.github_actions.value,
                COMMAND_DEPLOY_REPO_LOCATION,
                schedule="* * * * *",
            )
        except Exception:
            pass
        msg = SENT_ITEMS[0]
        assert msg["event"] == "command_deploy"
        assert msg["properties"]["deployment_method"] == DeploymentMethods.github_actions.value
        assert msg["properties"]["success"] is False


_INIT_ARGS = Namespace(
    list_sources=False,
    list_destinations=False,
    source="my_source",
    destination="duckdb",
    location=DEFAULT_VERIFIED_SOURCES_REPO,
    branch=None,
    eject=False,
)
_PIPELINE_ARGS = Namespace(
    list_pipelines=False,
    operation="list",
    pipeline_name=None,
    pipelines_dir=None,
    command="pipeline",
)


# (host, command_cls, args, inner_mock_path, expected_event, expected_extra_props)
# proves the same controller fires a host-specific event name and that `host` is
# resolved at call time via the lazy callable in `with_telemetry`.
_DUAL_CASES = [
    pytest.param(
        "dlt",
        InitCommand,
        _INIT_ARGS,
        "dlt._workspace.cli._init_command.init_command",
        "command_init",
        {"source_name": "my_source", "destination_type": "duckdb"},
        id="init-dlt",
    ),
    pytest.param(
        "dlthub",
        InitCommand,
        _INIT_ARGS,
        "dlt._workspace.cli._init_command.init_command",
        "command_pipeline.init",
        {"source_name": "my_source", "destination_type": "duckdb"},
        id="init-dlthub",
    ),
    pytest.param(
        "dlt",
        TelemetryCommand,
        Namespace(),
        "dlt._workspace.cli._telemetry_command.telemetry_status_command",
        "command_telemetry",
        {},
        id="telemetry-dlt",
    ),
    pytest.param(
        "dlthub",
        TelemetryCommand,
        Namespace(),
        "dlt._workspace.cli._telemetry_command.telemetry_status_command",
        "command_local.telemetry",
        {},
        id="telemetry-dlthub",
    ),
    pytest.param(
        "dlt",
        PipelineCommand,
        _PIPELINE_ARGS,
        "dlt._workspace.cli._pipeline_command.pipeline_command",
        "command_pipeline",
        {"operation": "list"},
        id="pipeline-dlt",
    ),
    pytest.param(
        "dlthub",
        PipelineCommand,
        _PIPELINE_ARGS,
        "dlt._workspace.cli._pipeline_command.pipeline_command",
        "command_local.pipeline",
        {"operation": "list"},
        id="pipeline-dlthub",
    ),
]


@pytest.mark.parametrize(
    "host,command_cls,args,mock_path,expected_event,expected_props", _DUAL_CASES
)
def test_cross_host_dual_tracking(
    mocker: MockerFixture,
    host: str,
    command_cls: Any,
    args: Namespace,
    mock_path: str,
    expected_event: str,
    expected_props: Dict[str, Any],
) -> None:
    """Cross-host controllers emit host-specific event names and the right `host` prop."""
    mocker.patch(mock_path, return_value=None)
    with _captured_telemetry():
        echo.set_cli_host_name(host)
        command_cls().execute(args)

    assert len(SENT_ITEMS) == 1
    msg = SENT_ITEMS[0]
    assert msg["event"] == expected_event
    props = msg["properties"]
    assert props["host"] == host
    for k, v in expected_props.items():
        assert props[k] == v


@pytest.mark.parametrize("profile", ["dev", "tests"])
def test_display_run_context_info_silent_for_local_profile(
    profile: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """No warning when active profile is local-only."""
    with isolated_workspace("configured_workspace", profile=profile):
        display_run_context_info()
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""


@pytest.mark.parametrize("profile", ["prod", "access", "analytics"])
def test_display_run_context_info_warns_for_non_local_profile(
    profile: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Yellow advisory printed when active profile is synced (prod/access) or custom."""
    with isolated_workspace("configured_workspace", profile=profile):
        display_run_context_info()
    err = capsys.readouterr().err
    assert profile in err
    assert "local-only" in err
