"""The environments and local profiles a workspace declares, kept in the settings dir."""

import re
from typing import Collection, Dict, List, Optional, Sequence, Tuple

from dlt.common import validation
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.exceptions import DictValidationException
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.utils import clone_dict_nested

from dlt._workspace.access import ACCESS_AXIS_VERBS
from dlt._workspace.exceptions import InvalidWorkspaceFile, WorkspaceFileEngineNoUpgradePath
from dlt._workspace.run_context import display_path
from dlt._workspace.settings_files import (
    load_yaml_file,
    profiles_with_settings,
    read_profile_pin,
    save_yaml_file,
)
from dlt._workspace.typing import (
    TEnvironmentDefinition,
    TProfileDefinition,
    TWorkspaceDefinition,
)

WORKSPACE_FILE_NAME = ".workspace"
WORKSPACE_ENGINE_VERSION = 1

LOCAL_PROFILES = ["dev", "tests"]
"""Profiles that are local when not declared, that is outside a workspace."""

DATA_ACCESS_LEVELS: Tuple[str, ...] = ("none",) + ACCESS_AXIS_VERBS["data"]
"""Data access levels, the lowest first: `none`, then the data verbs of job access."""

PROFILE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def format_data_access(data_access: str) -> str:
    return "no data access" if data_access == "none" else f"{data_access} data access"


def profile_data_access(profile: TProfileDefinition) -> str:
    """Data access level of `profile`: `none`, `read` or `write`."""
    return "write" if profile["data_access"] == "all" else profile["data_access"]


def needs_settings_files(profile: TProfileDefinition) -> bool:
    """False for a profile without data access: it holds no credentials."""
    return profile_data_access(profile) != "none"


def set_configured_profiles(
    profiles: Dict[str, TProfileDefinition], with_settings: Collection[str]
) -> None:
    """Sets `configured` on `profiles`: a profile has settings files or needs none."""
    for name, profile in profiles.items():
        profile["configured"] = not needs_settings_files(profile) or name in with_settings


def built_in_workspace_definition() -> TWorkspaceDefinition:
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


def _own_environment(name: str) -> TEnvironmentDefinition:
    """The environment that profile `name` gets on migration, with that profile alone."""
    return {
        "name": name,
        "profiles": {
            name: {"location": "remote", "data_access": "write", "env": name, "default": True}
        },
    }


def validate_workspace_definition(doc: StrAny, path: str) -> None:
    """Validates a workspace definition against `TWorkspaceDefinition` and the rules of profiles
    and environments.

    Raises:
        InvalidWorkspaceFile: With every problem found, `path` tells where the definition lives.
    """
    try:
        validation.validate_dict(TWorkspaceDefinition, doc, ".")
    except DictValidationException as ex:
        raise InvalidWorkspaceFile(path, [str(ex)]) from ex

    errors: List[str] = []
    environments: Dict[str, TEnvironmentDefinition] = doc["environments"]
    profiles: Dict[str, TProfileDefinition] = doc["local_profiles"]
    declared_in: Dict[str, str] = {}

    def _declare(name: str, where: str) -> None:
        if not PROFILE_NAME_RE.match(name):
            errors.append(f"profile `{name}`: a name has only letters, digits, `_` and `-`")
        if name in declared_in:
            errors.append(
                f"profile `{name}`: declared {where} and {declared_in[name]}. Declare it once."
            )
        else:
            declared_in[name] = where

    for name, profile in profiles.items():
        _declare(name, "as a local profile")
        if profile["location"] != "local":
            errors.append(
                f"profile `{name}`: a profile declared under `local_profiles` is local. Set"
                " `location: local`, or declare the remote profile in its environment."
            )
        if profile_data_access(profile) != "write":
            errors.append(
                f"profile `{name}`: a local profile has write data access, set `data_access: write`"
            )
        if profile.get("env") is not None:
            errors.append(
                f"profile `{name}`: a local profile belongs to no environment, remove `env`"
            )
    for env, environment in environments.items():
        if not PROFILE_NAME_RE.match(env):
            errors.append(f"environment `{env}`: a name has only letters, digits, `_` and `-`")
        if environment["name"] != env:
            errors.append(
                f"environment `{env}`: declared with `name: {environment['name']}`, the name"
                " must match the key. Remove `name`."
            )
        for name, profile in environment["profiles"].items():
            _declare(name, f"in environment `{env}`")
            if profile["location"] != "remote":
                errors.append(
                    f"profile `{name}`: a profile of environment `{env}` is remote, set"
                    " `location: remote`"
                )
            if profile.get("env") != env:
                errors.append(
                    f"profile `{name}`: declared in environment `{env}`, set `env: {env}`"
                )
        by_level: Dict[str, List[str]] = {}
        for name, profile in environment["profiles"].items():
            by_level.setdefault(profile_data_access(profile), []).append(name)
        if "write" not in by_level:
            errors.append(
                f"environment `{env}`: no profile with write data access. Declare one, a"
                " deployment is made with it."
            )
        for level, names in by_level.items():
            defaults = [n for n in names if environment["profiles"][n].get("default")]
            if len(defaults) == 1:
                continue
            problem, fix = (
                (f"profiles {', '.join(defaults)} are all the default", "Keep")
                if defaults
                else (f"none of {', '.join(names)} is the default", "Set")
            )
            errors.append(
                f"environment `{env}`: {problem} with {format_data_access(level)}. {fix}"
                " `default: true` on one."
            )
    defaults = [n for n, p in profiles.items() if p.get("default")]
    if len(defaults) > 1:
        errors.append(
            f"profiles {', '.join(defaults)} are all the default local profile. Keep"
            " `default: true` on one."
        )
    defaults = [n for n, e in environments.items() if e.get("default")]
    if len(defaults) > 1:
        errors.append(
            f"environments {', '.join(defaults)} are all the default. Keep `default: true` on one."
        )
    if errors:
        raise InvalidWorkspaceFile(path, errors)


def migrate_workspace_definition(
    doc: DictStrAny,
    from_engine: int,
    to_engine: int,
    path: str = WORKSPACE_FILE_NAME,
    implicit_profiles: Sequence[str] = (),
) -> TWorkspaceDefinition:
    """Migrates a workspace definition between engine versions, in place."""
    if from_engine == to_engine:
        return doc  # type: ignore[return-value]
    if from_engine == 0 and to_engine > 0:
        # an empty file: the built-ins, and each of `implicit_profiles` as its own environment
        definition = built_in_workspace_definition()
        for name in implicit_profiles:
            if PROFILE_NAME_RE.match(name) and not is_profile_declared(definition, name):
                definition["environments"][name] = _own_environment(name)
        # the version goes first in the written file
        doc.clear()
        doc.update(definition)
        from_engine = 1
    if from_engine != to_engine:
        raise WorkspaceFileEngineNoUpgradePath(path, from_engine, to_engine)
    doc["engine_version"] = to_engine
    return doc  # type: ignore[return-value]


def get_workspace_file(context: RunContextBase) -> str:
    return context.get_setting(WORKSPACE_FILE_NAME)


def shown_workspace_file(context: RunContextBase) -> str:
    """The workspace file as messages show it, see `display_path`."""
    return display_path(get_workspace_file(context), context.run_dir)


def _profiles_in_use(context: RunContextBase) -> List[str]:
    """Profiles that a workspace definition without profiles keeps on migration: those with
    settings files and the one recorded in `.dlt/profile-name`."""
    names = profiles_with_settings(context.settings_dir)
    if pinned := read_profile_pin(context):
        names.append(pinned)
    return names


def load_workspace_definition(context: RunContextBase) -> TWorkspaceDefinition:
    """Loads the workspace definition from the settings dir, migrated to the current engine
    version and validated.

    Raises:
        InvalidWorkspaceFile: The definition is not a mapping or declares invalid profiles.
        WorkspaceFileEngineNoUpgradePath: The definition was written by a newer dlt.
    """
    path = get_workspace_file(context)
    shown = shown_workspace_file(context)
    try:
        doc = load_yaml_file(path)
    except ValueError as ex:
        raise InvalidWorkspaceFile(shown, [str(ex)]) from ex
    # only an empty file is migrated: the migration replaces the content
    if doc and "engine_version" not in doc:
        raise InvalidWorkspaceFile(shown, ["`engine_version` is missing, set `engine_version: 1`"])
    from_engine = doc.get("engine_version", 0)
    if not isinstance(from_engine, int):
        raise InvalidWorkspaceFile(
            shown, [f"`engine_version` must be an integer, not {from_engine!r}"]
        )
    migrated = from_engine != WORKSPACE_ENGINE_VERSION
    if migrated:
        migrate_workspace_definition(
            doc, from_engine, WORKSPACE_ENGINE_VERSION, shown, _profiles_in_use(context)
        )
    restore_stored_workspace_definition(doc)
    validate_workspace_definition(doc, shown)
    if migrated:
        # a read-only settings dir keeps the empty file, the next load migrates again
        try:
            save_workspace_file(context, doc)  # type: ignore[arg-type]
        except OSError:
            pass
    # set after the write back and over a value read from the file: the flag is never stored
    with_settings = profiles_with_settings(context.settings_dir)
    set_configured_profiles(declared_profiles(doc), with_settings)  # type: ignore[arg-type]
    return doc  # type: ignore[return-value]


def stored_workspace_definition(doc: TWorkspaceDefinition) -> DictStrAny:
    """The definition as stored, without what follows from where an entry is declared."""
    stored: DictStrAny = clone_dict_nested(doc)  # type: ignore[arg-type]
    for environment in stored["environments"].values():
        environment.pop("name", None)
        for profile in environment["profiles"].values():
            profile.pop("env", None)
            profile.pop("location", None)
            profile.pop("configured", None)
    for profile in stored["local_profiles"].values():
        profile.pop("location", None)
        profile.pop("configured", None)
    return stored


def restore_stored_workspace_definition(doc: DictStrAny) -> None:
    """Fills in what `stored_workspace_definition` left out, in place."""

    def _restore(profile: DictStrAny, location: str) -> None:
        # a value in `doc` is kept so validation reports a mismatch
        profile.setdefault("location", location)
        # `all` is a data verb of job access, on a profile it is write
        if profile.get("data_access") == "all":
            profile["data_access"] = "write"

    for name, environment in (doc.get("environments") or {}).items():
        if not isinstance(environment, dict):
            continue
        environment.setdefault("name", name)
        for profile in (environment.get("profiles") or {}).values():
            if isinstance(profile, dict):
                profile.setdefault("env", name)
                _restore(profile, "remote")
    local_profiles = doc.get("local_profiles")
    if isinstance(local_profiles, dict):
        for profile in local_profiles.values():
            if isinstance(profile, dict):
                _restore(profile, "local")


def save_workspace_file(context: RunContextBase, doc: TWorkspaceDefinition) -> None:
    save_yaml_file(get_workspace_file(context), stored_workspace_definition(doc))


def declared_profiles(doc: TWorkspaceDefinition) -> Dict[str, TProfileDefinition]:
    """All declared profiles: the local ones, then those of each environment."""
    profiles = dict(doc["local_profiles"])
    for environment in doc["environments"].values():
        profiles.update(environment["profiles"])
    return profiles


def is_profile_declared(doc: TWorkspaceDefinition, name: str) -> bool:
    return name in doc["local_profiles"] or any(
        name in e["profiles"] for e in doc["environments"].values()
    )


def profile_definition(doc: TWorkspaceDefinition, name: str) -> TProfileDefinition:
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


def environment_definition(doc: TWorkspaceDefinition, env: str) -> TEnvironmentDefinition:
    """Declared environment `env`, with its profiles.

    Raises:
        KeyError: `env` is not declared.
    """
    return doc["environments"][env]


def default_profile(environment: TEnvironmentDefinition, data_access: str) -> Optional[str]:
    """The default profile of `environment` for `data_access`, `None` when it has no profile
    with that data access."""
    for name, profile in environment["profiles"].items():
        if profile.get("default") and profile_data_access(profile) == data_access:
            return name
    return None


def environment_of_profile(doc: TWorkspaceDefinition, name: str) -> Optional[str]:
    """Environment of the declared profile `name`, `None` for a local profile."""
    return profile_definition(doc, name).get("env")


def environment_profiles(environment: TEnvironmentDefinition) -> List[str]:
    """The profiles of `environment`, deployed together: the default profile with write data
    access first, then the others in declaration order."""
    first = default_profile(environment, "write")
    return ([first] if first else []) + [n for n in environment["profiles"] if n != first]
