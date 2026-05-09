"""Tests for `dlt._workspace.cli.utils` — host-shared utilities (telemetry / track_command)."""

import contextlib
import io
import os
from typing import Any
from unittest.mock import patch, Mock

import pytest
from pytest_mock import MockerFixture

from dlt._workspace.cli import DEFAULT_VERIFIED_SOURCES_REPO
from dlt._workspace.cli._deploy_command import (
    COMMAND_DEPLOY_REPO_LOCATION,
    DeploymentMethods,
    deploy_command_wrapper,
)
from dlt._workspace.cli._init_command import (
    init_command_wrapper,
    list_sources_command_wrapper,
)
from dlt._workspace.cli.utils import track_command
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration
from dlt.common.runtime.anon_tracker import disable_anon_tracker

from tests.common.runtime.utils import mock_github_env, mock_pod_env
from tests.utils import disable_temporary_telemetry, start_test_telemetry


def test_track_command_track_after_passes_params(
    mocker: MockerFixture, disable_temporary_telemetry
) -> None:
    """verify track_command wraps with telemetry and forwards arg names and extra kwargs."""
    # init test telemetry and capture outgoing events
    mock_github_env(os.environ)
    mock_pod_env(os.environ)
    SENT_ITEMS.clear()
    config = WorkspaceRuntimeConfiguration(dlthub_telemetry=True)

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)
        mocker.patch(
            "dlt.common.runtime.anon_tracker.requests.post",
            return_value=Mock(status_code=204),
        )

        @track_command("my_cmd", False, "x", "y", extra_const="value")
        def _fn(x: Any, y: Any, z: Any = None) -> Any:
            return "ok"

        _fn("X", 7, z="ignored")
        disable_anon_tracker()

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


def test_track_command_track_before_passes_params(
    mocker: MockerFixture, disable_temporary_telemetry
) -> None:
    """when tracking before, event is emitted once with success True and includes provided params."""
    mock_github_env(os.environ)
    mock_pod_env(os.environ)
    SENT_ITEMS.clear()
    config = WorkspaceRuntimeConfiguration(dlthub_telemetry=True)

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)
        mocker.patch(
            "dlt.common.runtime.anon_tracker.requests.post",
            return_value=Mock(status_code=204),
        )

        @track_command("before_cmd", True, "p", ignored="const")
        def _fn(p: Any) -> Any:
            # raising should not affect success flag in before mode
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            _fn(123)

        disable_anon_tracker()

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

    config = WorkspaceRuntimeConfiguration(dlthub_telemetry=True)

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)

        SENT_ITEMS.clear()
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
    config = WorkspaceRuntimeConfiguration(dlthub_telemetry=True)

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)

        SENT_ITEMS.clear()
        with io.StringIO() as buf, contextlib.redirect_stderr(buf):
            try:
                init_command_wrapper("instrumented_source", "<UNK>", None, None)
            except Exception:
                pass
            # output = buf.getvalue()
            # assert "is not one of the standard dlt destinations" in output
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


# telemetry helpers local to this module (avoid depending on other test modules)
SENT_ITEMS: list[dict[str, Any]] = []


def _mock_before_send(event: dict[str, Any], _unused_hint: Any = None) -> dict[str, Any]:
    # capture event for assertions
    SENT_ITEMS.append(event)
    return event
