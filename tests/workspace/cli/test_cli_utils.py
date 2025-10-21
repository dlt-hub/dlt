import contextlib
import io
import os
from typing import Any

import pytest
from _pytest.capture import CaptureFixture
from _pytest.monkeypatch import MonkeyPatch
from pytest_mock import MockerFixture
from unittest.mock import patch, Mock

from dlt._workspace.cli.utils import delete_local_data, check_delete_local_data, track_command
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.configuration import WorkspaceRuntimeConfiguration
from dlt.common.runtime.run_context import RunContext
from dlt.common.runtime.anon_tracker import disable_anon_tracker
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase

from tests.workspace.utils import fruitshop_pipeline_context as fruitshop_pipeline_context
from dlt._workspace.cli import echo
from tests.common.runtime.utils import mock_github_env, mock_pod_env
from tests.utils import SentryLoggerConfiguration, start_test_telemetry, disable_temporary_telemetry


@pytest.mark.parametrize(
    "skip_data_dir,recreate_dirs",
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False),
    ],
    ids=[
        "skip-data|recreate",
        "skip-data|no-recreate",
        "with-data|recreate",
        "with-data|no-recreate",
    ],
)
def test_delete_local_data_recreate_behavior(
    fruitshop_pipeline_context: RunContextBase,
    capsys: CaptureFixture[str],
    skip_data_dir: bool,
    recreate_dirs: bool,
) -> None:
    """verify delete_local_data echoes actions and recreates dirs conditionally.

    the test removes local_dir and data_dir before calling delete_local_data to
    clearly observe the recreate behavior without relying on any previous state.
    """
    ctx = fruitshop_pipeline_context

    # list dirs to delete and auto-confirm
    with echo.always_choose(always_choose_default=False, always_choose_value=True):
        attrs = check_delete_local_data(ctx, skip_data_dir=skip_data_dir)
    # perform deletion (which will only recreate when requested)
    delete_local_data(ctx, attrs, recreate_dirs=recreate_dirs)

    # local_dir is always processed
    assert os.path.isdir(ctx.local_dir) is recreate_dirs

    # data_dir depends on skip_data_dir flag
    expected_data_exists = skip_data_dir or recreate_dirs
    assert os.path.isdir(ctx.data_dir) is expected_data_exists

    # capture and check user-facing messages from check_delete_local_data
    out = capsys.readouterr().out
    assert "The following dirs will be deleted:" in out
    assert "(locally loaded data)" in out
    if skip_data_dir:
        assert "(pipeline working folders)" not in out
    else:
        assert "(pipeline working folders)" in out


def test_delete_local_data_with_plain_run_context_raises(capsys: CaptureFixture[str]) -> None:
    """ensure CliCommandException is raised when context lacks profiles."""
    plain_ctx = RunContext(run_dir=".")
    with pytest.raises(CliCommandException):
        # should fail before any confirmation prompt
        check_delete_local_data(plain_ctx, skip_data_dir=False)

    out = capsys.readouterr().out
    assert "ERROR: Cannot delete local data for a context without profiles" in out


def _assert_protected_deletion(
    ctx: RunContextBase,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
    dir_attr: str,
    equals_attr: str,
    *,
    skip_data: bool,
) -> None:
    """helper to assert that attempting to delete a protected dir raises and logs an error."""
    # compute target path to match the protected attribute
    target_path = getattr(ctx, equals_attr)

    # patch the property on the class so getattr(ctx, dir_attr) returns the protected path
    monkeypatch.setattr(type(ctx), dir_attr, property(lambda self: target_path), raising=True)

    # exercise and assert
    with pytest.raises(CliCommandException):
        check_delete_local_data(ctx, skip_data_dir=skip_data)

    out = capsys.readouterr().out
    label = "run dir (workspace root)" if equals_attr == "run_dir" else "settings dir"
    assert f"ERROR: {dir_attr} `{target_path}` is the same as {label} and cannot be deleted" in out


@pytest.mark.parametrize(
    "dir_attr,equals_attr,skip_data",
    [
        ("local_dir", "run_dir", True),
        ("local_dir", "settings_dir", True),
        ("data_dir", "run_dir", False),
        ("data_dir", "settings_dir", False),
    ],
    ids=[
        "local_dir==run_dir",
        "local_dir==settings_dir",
        "data_dir==run_dir",
        "data_dir==settings_dir",
    ],
)
def test_delete_local_data_protects_run_and_settings_dirs(
    fruitshop_pipeline_context: RunContextBase,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
    dir_attr: str,
    equals_attr: str,
    skip_data: bool,
) -> None:
    """verify that delete_local_data refuses to delete run_dir or settings_dir via local/data dir.

    we patch the context so the target dir equals a protected dir and expect a CliCommandException.
    """
    _assert_protected_deletion(
        fruitshop_pipeline_context, capsys, monkeypatch, dir_attr, equals_attr, skip_data=skip_data
    )


def test_delete_local_data_rejects_dirs_outside_run_dir(
    fruitshop_pipeline_context: RunContextBase,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
    tmp_path: Any,
) -> None:
    """ensure we refuse to operate on dirs that are not under the workspace run_dir."""
    ctx = fruitshop_pipeline_context

    # point local_dir to a path outside of run_dir
    outside_dir = tmp_path / "outside_local"
    outside_dir.mkdir(parents=True, exist_ok=True)

    # patch attribute to simulate unsafe location

    monkeypatch.setattr(
        type(ctx), "local_dir", property(lambda self: str(outside_dir)), raising=True
    )

    with pytest.raises(CliCommandException):
        check_delete_local_data(ctx, skip_data_dir=True)

    out = capsys.readouterr().out
    assert (
        f"ERROR: local_dir `{ctx.local_dir}` is not within run dir (workspace root) and cannot be"
        " deleted"
        in out
    )


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
    from dlt._workspace.cli import (
        DEFAULT_VERIFIED_SOURCES_REPO,
    )
    from dlt._workspace.cli._deploy_command import (
        DeploymentMethods,
        COMMAND_DEPLOY_REPO_LOCATION,
    )
    from dlt._workspace.cli._init_command import (
        init_command_wrapper,
        list_sources_command_wrapper,
    )
    from dlt._workspace.cli._deploy_command import (
        deploy_command_wrapper,
    )

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
