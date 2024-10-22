import pytest
import io
import os
import contextlib
from typing import Any
from unittest.mock import patch

from dlt.common.configuration.container import Container
from dlt.common.runtime.run_context import DOT_DLT
from dlt.common.configuration.providers import ConfigTomlProvider, CONFIG_TOML
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.storages import FileStorage
from dlt.common.typing import DictStrAny
from dlt.common.utils import set_working_dir

from dlt.cli.utils import track_command
from dlt.cli.telemetry_command import telemetry_status_command, change_telemetry_status_command

from tests.utils import patch_random_home_dir, start_test_telemetry, test_storage


def test_main_telemetry_command(test_storage: FileStorage) -> None:
    # home dir is patched to TEST_STORAGE, create project dir
    test_storage.create_folder("project")

    container = Container()
    run_context = container[PluggableRunContext].context
    os.makedirs(run_context.global_dir, exist_ok=True)

    # inject provider context so the original providers are restored at the end
    def _initial_providers(self):
        return [ConfigTomlProvider(run_context.settings_dir, global_dir=run_context.global_dir)]

    with set_working_dir(test_storage.make_full_path("project")), patch(
        "dlt.common.runtime.run_context.RunContext.initial_providers",
        _initial_providers,
    ):
        # no config files: status is ON
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            telemetry_status_command()
            assert "ENABLED" in buf.getvalue()
        # disable telemetry
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            change_telemetry_status_command(False)
            telemetry_status_command()
            output = buf.getvalue()
            assert "OFF" in output
            assert "DISABLED" in output
        # make sure no config.toml exists in project (it is not created if it was not already there)
        project_dot = os.path.join("project", DOT_DLT)
        assert not test_storage.has_folder(project_dot)
        # enable telemetry
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            change_telemetry_status_command(True)
            telemetry_status_command()
            output = buf.getvalue()
            assert "ON" in output
            assert "ENABLED" in output
        # create config toml in project dir
        test_storage.create_folder(project_dot)
        test_storage.save(os.path.join("project", DOT_DLT, CONFIG_TOML), "# empty")
        # disable telemetry
        with io.StringIO() as buf, contextlib.redirect_stdout(buf):
            # this command reloads providers
            change_telemetry_status_command(False)
            telemetry_status_command()
            output = buf.getvalue()
            assert "OFF" in output
            assert "DISABLED" in output
            # load local config provider
            project_toml = ConfigTomlProvider(run_context.settings_dir)
            # local project toml was modified
            assert project_toml._config_doc["runtime"]["dlthub_telemetry"] is False


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

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry()

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
    from dlt.cli.deploy_command import (
        DeploymentMethods,
        COMMAND_DEPLOY_REPO_LOCATION,
    )
    from dlt.cli.init_command import (
        DEFAULT_VERIFIED_SOURCES_REPO,
    )
    from dlt.cli.command_wrappers import (
        init_command_wrapper,
        deploy_command_wrapper,
        list_sources_command_wrapper,
    )

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry()

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

        # SENT_ITEMS.clear()
        # pipeline_command_wrapper("list", "-", None, 1)
        # msg = SENT_ITEMS[0]
        # assert msg["event"] == "command_pipeline"
        # assert msg["properties"]["operation"] == "list"

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


SENT_ITEMS = []


def _mock_before_send(event: DictStrAny, _unused_hint: Any = None) -> DictStrAny:
    SENT_ITEMS.append(event)
    # do not send this
    return None
