from dlt.common import logger

from .toml import SettingsTomlProvider
from .yaml import SettingsYamlProvider


def warn_on_toml_yaml_collision(
    toml_provider: SettingsTomlProvider, yaml_provider: SettingsYamlProvider
) -> None:
    """Logs a warning if both `toml_provider` and `yaml_provider` found files on disk.

    Config/secret resolution order gives precedence to `toml` files over `yaml` files (see
    `RunContext.initial_providers`), so values present in both formats may silently shadow
    the `yaml` ones. This warns the user so name collisions do not go unnoticed.
    """
    if toml_provider.present_locations and yaml_provider.present_locations:
        logger.warning(
            f"Both {toml_provider.present_locations} and {yaml_provider.present_locations} were"
            " found. Values from toml files take precedence over yaml files with the same name,"
            " which may silently shadow yaml configuration."
        )
