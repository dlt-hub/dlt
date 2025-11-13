import os

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers.toml import ConfigTomlProvider
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.runtime.anon_tracker import get_anonymous_id

from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli.utils import get_telemetry_status
from dlt._workspace.cli.config_toml_writer import WritableConfigValue, write_values

DLT_TELEMETRY_DOCS_URL = "https://dlthub.com/docs/reference/telemetry"


def telemetry_status_command() -> None:
    if get_telemetry_status():
        fmt.echo("Telemetry is %s" % fmt.bold("ENABLED"))
        fmt.echo("Anonymous id %s" % fmt.bold(get_anonymous_id()))
    else:
        fmt.echo("Telemetry is %s" % fmt.bold("DISABLED"))


def change_telemetry_status_command(enabled: bool) -> None:
    from dlt.common.runtime import run_context

    # value to write
    telemetry_value = [
        WritableConfigValue("dlthub_telemetry", bool, enabled, (RuntimeConfiguration.__section__,))
    ]
    # write local config
    # TODO: use designated (main) config provider (for non secret values) ie. taken from run context
    run_ctx = run_context.active()
    config = ConfigTomlProvider(run_ctx.settings_dir)
    if not config.is_empty:
        write_values(config._config_toml, telemetry_value, overwrite_existing=True)
        config.write_toml()

    # write global config
    global_path = run_ctx.global_dir
    os.makedirs(global_path, exist_ok=True)
    config = ConfigTomlProvider(settings_dir=global_path)
    write_values(config._config_toml, telemetry_value, overwrite_existing=True)
    config.write_toml()

    if enabled:
        fmt.echo("Telemetry switched %s" % fmt.bold("ON"))
    else:
        fmt.echo("Telemetry switched %s" % fmt.bold("OFF"))
    # reload config providers
    Container()[PluggableRunContext].reload_providers()


@utils.track_command("telemetry", False)
def telemetry_status_command_wrapper() -> None:
    telemetry_status_command()


@utils.track_command("telemetry_switch", False, "enabled")
def telemetry_change_status_command_wrapper(enabled: bool) -> None:
    try:
        change_telemetry_status_command(enabled)
    except Exception as ex:
        raise CliCommandException(docs_url=DLT_TELEMETRY_DOCS_URL, raiseable_exception=ex)
