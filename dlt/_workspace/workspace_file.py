"""The `.dlt/.workspace` file: reads, migrates, validates and writes the stored definition of the
environments and local profiles."""

from typing import Any, Dict, List, Optional, Sequence

from dlt.common import validation
from dlt.common.configuration.specs.pluggable_run_context import RunContextBase
from dlt.common.exceptions import DictValidationException
from dlt.common.typing import DictStrAny, StrAny

from dlt._workspace.access import format_data_access
from dlt._workspace.exceptions import InvalidWorkspaceFile, WorkspaceFileEngineNoUpgradePath
from dlt._workspace.run_context import format_display_path
from dlt._workspace.settings_files import (
    load_yaml_file,
    list_profiles_with_settings,
    read_profile_pin,
    save_yaml_file,
)
from dlt._workspace.typing import (
    TEnvironmentDefinition,
    TEnvironmentStoredDefinition,
    TProfileDefinition,
    TProfileLocation,
    TProfileStoredDefinition,
    TWorkspaceDefinition,
    TWorkspaceStoredDefinition,
)
from dlt._workspace.workspace_definition import (
    PROFILE_NAME_RE,
    WORKSPACE_ENGINE_VERSION,
    create_built_in_workspace_definition,
    get_declared_profiles,
    is_profile_declared,
    get_profile_data_access,
    set_configured_profiles,
)

WORKSPACE_FILE_NAME = ".workspace"


def get_workspace_file(context: RunContextBase) -> str:
    return context.get_setting(WORKSPACE_FILE_NAME)


def format_workspace_file_path(context: RunContextBase) -> str:
    """The workspace file as messages show it, see `format_display_path`."""
    return format_display_path(get_workspace_file(context), context.run_dir)


def load_workspace_definition(context: RunContextBase) -> TWorkspaceDefinition:
    """Loads the workspace definition from the settings dir, migrated to the current engine
    version and validated.

    Raises:
        InvalidWorkspaceFile: The definition is not a mapping or declares invalid profiles.
        WorkspaceFileEngineNoUpgradePath: The definition was written by a newer dlt.
    """
    path = get_workspace_file(context)
    shown = format_workspace_file_path(context)
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
            doc, from_engine, WORKSPACE_ENGINE_VERSION, shown, _list_profiles_in_use(context)
        )
    validate_workspace_definition(doc, shown)
    if migrated:
        # a read-only settings dir keeps the empty file, the next load migrates again
        try:
            save_yaml_file(path, doc)
        except OSError:
            pass
    definition = expand_workspace_definition(doc)  # type: ignore[arg-type]
    with_settings = list_profiles_with_settings(context.settings_dir)
    set_configured_profiles(get_declared_profiles(definition), with_settings)
    return definition


def save_workspace_file(context: RunContextBase, doc: TWorkspaceDefinition) -> None:
    """Saves `doc` to `.dlt/.workspace` with only the stored fields, validated first."""
    stored = build_stored_workspace_definition(doc, format_workspace_file_path(context))
    save_yaml_file(get_workspace_file(context), stored)


def build_stored_workspace_definition(
    doc: TWorkspaceDefinition, path: str
) -> TWorkspaceStoredDefinition:
    """The definition as `.dlt/.workspace` stores it: only the fields of the stored types.

    Raises:
        InvalidWorkspaceFile: The stored definition is not valid, `path` tells where it goes.
    """

    def _store_profiles(profiles: Dict[str, TProfileDefinition]) -> DictStrAny:
        return {
            name: _pick_stored_fields(profile, TProfileStoredDefinition)
            for name, profile in profiles.items()
        }

    stored: DictStrAny = {
        "engine_version": doc["engine_version"],
        "environments": {
            name: {
                **_pick_stored_fields(environment, TEnvironmentStoredDefinition),
                "profiles": _store_profiles(environment["profiles"]),
            }
            for name, environment in doc["environments"].items()
        },
        "local_profiles": _store_profiles(doc["local_profiles"]),
    }
    validate_workspace_definition(stored, path)
    return stored  # type: ignore[return-value]


def expand_workspace_definition(stored: TWorkspaceStoredDefinition) -> TWorkspaceDefinition:
    """The definition with what follows from where an entry is declared: the environment
    `name`, the profile `location` and the `env` of a remote profile."""

    def _expand_profile(
        profile: TProfileStoredDefinition, location: TProfileLocation, env: Optional[str] = None
    ) -> TProfileDefinition:
        expanded = TProfileDefinition(**profile, location=location)
        if env is not None:
            expanded["env"] = env
        # `all` is a data verb of job access, on a profile it is write
        if expanded["data_access"] == "all":
            expanded["data_access"] = "write"
        return expanded

    environments: Dict[str, TEnvironmentDefinition] = {}
    for name, environment in stored["environments"].items():
        environments[name] = {
            **environment,
            "name": name,
            "profiles": {
                p: _expand_profile(profile, "remote", name)
                for p, profile in environment["profiles"].items()
            },
        }
    return {
        "engine_version": stored["engine_version"],
        "environments": environments,
        "local_profiles": {
            name: _expand_profile(profile, "local")
            for name, profile in stored["local_profiles"].items()
        },
    }


def validate_workspace_definition(doc: StrAny, path: str) -> None:
    """Validates a stored workspace definition against `TWorkspaceStoredDefinition` and the rules
    of profiles and environments.

    Raises:
        InvalidWorkspaceFile: With every problem found, `path` tells where the definition lives.
    """
    try:
        validation.validate_dict(TWorkspaceStoredDefinition, doc, ".")
    except DictValidationException as ex:
        raise InvalidWorkspaceFile(path, [str(ex)]) from ex

    errors: List[str] = []
    environments: Dict[str, TEnvironmentStoredDefinition] = doc["environments"]
    profiles: Dict[str, TProfileStoredDefinition] = doc["local_profiles"]
    declared_in: Dict[str, str] = {}

    def _check_profile_name(name: str, where: str) -> None:
        if not PROFILE_NAME_RE.match(name):
            errors.append(f"profile `{name}`: a name has only letters, digits, `_` and `-`")
        if name in declared_in:
            errors.append(
                f"profile `{name}`: declared {where} and {declared_in[name]}. Declare it once."
            )
        else:
            declared_in[name] = where

    for name, profile in profiles.items():
        _check_profile_name(name, "as a local profile")
        if get_profile_data_access(profile) != "write":
            errors.append(
                f"profile `{name}`: a local profile has write data access, set `data_access: write`"
            )
    for env, environment in environments.items():
        if not PROFILE_NAME_RE.match(env):
            errors.append(f"environment `{env}`: a name has only letters, digits, `_` and `-`")
        for name in environment["profiles"]:
            _check_profile_name(name, f"in environment `{env}`")
        by_level: Dict[str, List[str]] = {}
        for name, profile in environment["profiles"].items():
            by_level.setdefault(get_profile_data_access(profile), []).append(name)
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
) -> TWorkspaceStoredDefinition:
    """Migrates a stored workspace definition between engine versions, in place."""
    if from_engine == to_engine:
        return doc  # type: ignore[return-value]
    if from_engine == 0 and to_engine > 0:
        # an empty file: the built-ins, and each of `implicit_profiles` as its own environment
        definition = create_built_in_workspace_definition()
        for name in implicit_profiles:
            if PROFILE_NAME_RE.match(name) and not is_profile_declared(definition, name):
                definition["environments"][name] = _create_own_environment(name)
        # the version goes first in the written file
        doc.clear()
        doc.update(build_stored_workspace_definition(definition, path))
        from_engine = 1
    if from_engine != to_engine:
        raise WorkspaceFileEngineNoUpgradePath(path, from_engine, to_engine)
    doc["engine_version"] = to_engine
    return doc  # type: ignore[return-value]


def _create_own_environment(name: str) -> TEnvironmentDefinition:
    """The environment that profile `name` gets on migration, with that profile alone."""
    return {
        "name": name,
        "profiles": {
            name: {"location": "remote", "data_access": "write", "env": name, "default": True}
        },
    }


def _list_profiles_in_use(context: RunContextBase) -> List[str]:
    """Profiles that a workspace definition without profiles keeps on migration: those with
    settings files and the one recorded in `.dlt/profile-name`."""
    names = list_profiles_with_settings(context.settings_dir)
    if pinned := read_profile_pin(context):
        names.append(pinned)
    return names


def _pick_stored_fields(entry: StrAny, stored_type: Any) -> DictStrAny:
    """The items of `entry` that the TypedDict `stored_type` declares."""
    fields = stored_type.__required_keys__ | stored_type.__optional_keys__
    return {key: value for key, value in entry.items() if key in fields}
