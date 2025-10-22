import io
import os
import contextlib

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import ConfigTomlProvider, CONFIG_TOML
from dlt.common.configuration.specs import PluggableRunContext

from dlt._workspace.cli._telemetry_command import (
    telemetry_status_command,
    change_telemetry_status_command,
)


def test_main_telemetry_command() -> None:
    # run dir is patched to TEST_STORAGE/default (workspace root)

    container = Container()
    run_context = container[PluggableRunContext].context

    # we just make sure that workspace auto fixture generates right global dir
    assert run_context.global_dir.endswith(".global_dir")
    os.makedirs(run_context.global_dir, exist_ok=True)

    # no config files: status is ON (empty workspace has no toml files)
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
    # make sure no local config.toml exists in project (it is not created if it was not already there)
    assert not os.path.isfile(run_context.get_setting(CONFIG_TOML))
    # load global config
    global_toml = ConfigTomlProvider(run_context.global_dir)
    assert global_toml._config_doc["runtime"]["dlthub_telemetry"] is False

    # enable telemetry
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        change_telemetry_status_command(True)
        telemetry_status_command()
        output = buf.getvalue()
        assert "ON" in output
        assert "ENABLED" in output
    # load global config
    global_toml = ConfigTomlProvider(run_context.global_dir)
    assert global_toml._config_doc["runtime"]["dlthub_telemetry"] is True

    # create config toml in project dir
    with open(run_context.get_setting(CONFIG_TOML), "wt", encoding="utf-8") as f:
        f.write("# empty")

    # disable telemetry
    with io.StringIO() as buf, contextlib.redirect_stdout(buf):
        # this command reloads providers
        change_telemetry_status_command(False)
        telemetry_status_command()
        output = buf.getvalue()
        assert "OFF" in output
        assert "DISABLED" in output

    # load global config provider
    global_toml = ConfigTomlProvider(run_context.global_dir)
    assert global_toml._config_doc["runtime"]["dlthub_telemetry"] is False
    # load local config provider
    project_toml = ConfigTomlProvider(run_context.settings_dir)
    # local project toml was modified
    assert project_toml._config_doc["runtime"]["dlthub_telemetry"] is False
