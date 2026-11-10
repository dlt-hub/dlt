"""Files that dlt keeps in the settings dir: the yaml files `.workspace`, `.sources` and
`.toolkits`, the settings files of profiles and `profile-name`, the record of the active profile."""

import os
import re
from typing import Any, List, Mapping, Optional, Sequence, Tuple

import yaml

from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.typing import DictStrAny

PIN_FILE_NAME = "profile-name"

PROFILE_FILE_RE = re.compile(r"^(?P<profile>[A-Za-z0-9_-]+)\.(?:config|secrets)\.toml$")
"""A profile's settings file in `.dlt/`: `<profile>.config.toml` or `<profile>.secrets.toml`."""


def dump_yaml(doc: Mapping[str, Any]) -> str:
    return yaml.dump(doc, allow_unicode=True, default_flow_style=False, sort_keys=False)


def load_yaml_file(path: str) -> DictStrAny:
    """Loads a yaml mapping from `path`. A missing or empty file is an empty mapping.

    Raises:
        ValueError: The content is not valid yaml or not a mapping.
    """
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        try:
            doc = yaml.safe_load(f)
        except yaml.YAMLError as ex:
            raise ValueError(f"`{path}` is not valid yaml: {ex}") from ex
    if doc is None:
        return {}
    if not isinstance(doc, dict):
        raise ValueError(f"`{path}` must contain a yaml mapping, not {type(doc).__name__}")
    return doc


def save_yaml_file(path: str, doc: Mapping[str, Any]) -> None:
    if dir_name := os.path.dirname(path):
        os.makedirs(dir_name, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(dump_yaml(doc))


def profiles_with_settings(settings_dir: str) -> List[str]:
    """Profiles with a `<profile>.config.toml` or `<profile>.secrets.toml` in `settings_dir`."""
    try:
        names = os.listdir(settings_dir)
    except OSError:
        return []
    found = {m.group("profile") for n in names if (m := PROFILE_FILE_RE.match(n))}
    return sorted(found)


def profile_settings_files(context: RunContextBase, name: str) -> Tuple[str, str]:
    """Paths of the `<name>.config.toml` and `<name>.secrets.toml` of profile `name`."""
    return context.get_setting(f"{name}.config.toml"), context.get_setting(f"{name}.secrets.toml")


def create_profile_settings(context: RunContextBase, names: Sequence[str]) -> List[str]:
    """Creates the settings files of profiles `names` that do not exist yet.

    Returns:
        List[str]: The paths created.
    """
    created: List[str] = []
    for name in names:
        config, secrets = profile_settings_files(context, name)
        headers = (
            (config, f"# settings of profile {name}, they override config.toml\n"),
            (secrets, f"# credentials of profile {name}, they override secrets.toml\n"),
        )
        for path, header in headers:
            if os.path.exists(path):
                continue
            with open(path, "x", encoding="utf-8") as f:
                f.write(header)
            created.append(path)
    return created


def get_profile_pin_file(context: RunContextBase) -> str:
    return context.get_setting(PIN_FILE_NAME)


def read_profile_pin(context: RunContextBase) -> Optional[str]:
    profile_pin = get_profile_pin_file(context)
    if os.path.isfile(profile_pin):
        with open(profile_pin, "tr", encoding="utf-8") as pin:
            return pin.readline().strip()
    return None


def save_profile_pin(context: RunContextBase, profile: str) -> None:
    profile_pin = get_profile_pin_file(context)
    with open(profile_pin, "wt", encoding="utf-8") as pin:
        pin.write(profile)
