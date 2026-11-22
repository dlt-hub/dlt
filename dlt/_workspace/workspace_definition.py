"""The environments and local profiles a workspace declares: the built-in definition, lookups and
the facts about each entry. No files and no run context."""

import re
from typing import Collection, Dict, Optional, Tuple

from dlt._workspace.access import DATA_ACCESS_LEVELS
from dlt._workspace.typing import (
    TEnvironmentDefinition,
    TProfileDataAccess,
    TProfileDefinition,
    TProfileStoredDefinition,
    TWorkspaceDefinition,
)

WORKSPACE_ENGINE_VERSION = 1

PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")
"""A name of a profile or an environment."""

DEFAULT_PROFILE = "dev"


def create_built_in_workspace_definition() -> TWorkspaceDefinition:
    """The definition of a new workspace."""
    return {
        "engine_version": WORKSPACE_ENGINE_VERSION,
        "environments": {
            "prod": {
                "name": "prod",
                "default": True,
                "strict_access": False,
                "profiles": {
                    "prod": {
                        "location": "remote",
                        "data_access": "write",
                        "env": "prod",
                        "default": True,
                    },
                    "access": {
                        "location": "remote",
                        "data_access": "read",
                        "env": "prod",
                        "default": True,
                    },
                    "no-access": {
                        "location": "remote",
                        "data_access": "none",
                        "env": "prod",
                        "default": True,
                    },
                },
            }
        },
        "local_profiles": {
            "dev": {"location": "local", "data_access": "write", "default": True},
            "tests": {"location": "local", "data_access": "write"},
        },
    }


def get_declared_profiles(doc: TWorkspaceDefinition) -> Dict[str, TProfileDefinition]:
    """All declared profiles: the local ones, then those of each environment."""
    profiles = dict(doc["local_profiles"])
    for environment in doc["environments"].values():
        profiles.update(environment["profiles"])
    return profiles


def is_profile_declared(doc: TWorkspaceDefinition, name: str) -> bool:
    return name in doc["local_profiles"] or any(
        name in e["profiles"] for e in doc["environments"].values()
    )


def get_profile_definition(doc: TWorkspaceDefinition, name: str) -> TProfileDefinition:
    """Declared profile `name`, local or in its environment.

    Raises:
        KeyError: `name` is not declared.
    """
    if (profile := doc["local_profiles"].get(name)) is not None:
        return profile
    for environment in doc["environments"].values():
        if (profile := environment["profiles"].get(name)) is not None:
            return profile
    raise KeyError(name)


def get_environment_for_profile(doc: TWorkspaceDefinition, name: str) -> Optional[str]:
    """Environment of the declared profile `name`, `None` for a local profile."""
    return get_profile_definition(doc, name).get("env")


def get_environment_definition(doc: TWorkspaceDefinition, env: str) -> TEnvironmentDefinition:
    """Declared environment `env`, with its profiles.

    Raises:
        KeyError: `env` is not declared.
    """
    return doc["environments"][env]


def get_profile_data_access(profile: TProfileStoredDefinition) -> TProfileDataAccess:
    """Data access level of `profile`: `none`, `read` or `write`."""
    return "write" if profile["data_access"] == "all" else profile["data_access"]


def needs_settings_files(profile: TProfileStoredDefinition) -> bool:
    """False for a profile without data access: it holds no credentials."""
    return get_profile_data_access(profile) != "none"


def set_configured_profiles(
    profiles: Dict[str, TProfileDefinition], with_settings: Collection[str]
) -> None:
    """Sets `configured` on `profiles`: whether a profile has settings files."""
    for name, profile in profiles.items():
        profile["configured"] = name in with_settings


def is_profile_usable(profile: TProfileDefinition) -> bool:
    """True when `profile` is configured or holds no credentials and needs no settings files."""
    return profile.get("configured", True) or not needs_settings_files(profile)


def get_default_profile_for_env(
    environment: TEnvironmentDefinition, data_access: str
) -> Optional[str]:
    """The default profile of `environment` for `data_access`, `None` when it has no profile
    with that data access."""
    for name, profile in environment["profiles"].items():
        if profile.get("default") and get_profile_data_access(profile) == data_access:
            return name
    return None


def find_default_profile_for_access(
    environment: TEnvironmentDefinition, data_access: TProfileDataAccess
) -> Optional[Tuple[str, TProfileDataAccess]]:
    """The default profile of `environment` that serves `data_access`, with the data access it
    gives. A profile without settings files does not count. In an environment that is not strict,
    the closest default profile with more data access takes over. `None` when no profile fits."""
    strict = bool(environment.get("strict_access"))
    levels = DATA_ACCESS_LEVELS[DATA_ACCESS_LEVELS.index(data_access) :]
    for level in levels[:1] if strict else levels:
        name = get_default_profile_for_env(environment, level)
        if name is None:
            continue
        # nothing is above the profile with write data access, so it is taken as it is
        if is_profile_usable(environment["profiles"][name]) or (level == "write" and not strict):
            return name, level
    return None
