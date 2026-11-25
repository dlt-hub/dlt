"""Declares, checks and activates the profiles of the active workspace."""

import os
from contextlib import contextmanager
from typing import Iterator, Mapping, Optional, Sequence

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    ProfilesRunContext,
    RunContextBase,
)
from dlt.common.runtime.run_context import active as active_run_context
from dlt.common.utils import custom_environ

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.exceptions import (
    EnvironmentNotDeclared,
    InvalidProfileName,
    ProfileAlreadyExists,
    ProfileNotDeclared,
)
from dlt._workspace.settings_files import (  # noqa: F401, re-exported, the dlthub package imports them from here
    get_profile_pin_file,
    read_profile_pin,
    save_profile_pin,
)
from dlt._workspace.typing import (
    TProfileDefinition,
    TProfileStoredDefinition,
    TWorkspaceDefinition,
)
from dlt._workspace.workspace_definition import (  # noqa: F401, DEFAULT_PROFILE re-exported for dlthub
    DEFAULT_PROFILE,
    PROFILE_NAME_RE,
    get_declared_profiles,
    get_default_profile_for_env,
    get_profile_data_access,
    is_profile_declared,
)
from dlt._workspace.workspace_file import (
    format_workspace_file_path,
    load_workspace_definition,
    save_workspace_file,
)

LOCAL_PROFILES = ["dev", "tests"]
"""Profiles that are local when not declared, that is outside a workspace."""

PROFILE_ARGUMENT_HELP = "Profile to run with."
"""Help of the `--profile` option of the `dlthub` CLI and of the `profile` argument of MCP tools."""


def check_new_profiles(doc: TWorkspaceDefinition, path: str, names: Sequence[str]) -> None:
    """Refuses `names` that are not profile names or are declared already in `doc`.

    Raises:
        InvalidProfileName: A name is not a profile name.
        ProfileAlreadyExists: A profile is declared already.
    """
    for name in names:
        if not PROFILE_NAME_RE.match(name):
            raise InvalidProfileName(name)
        if is_profile_declared(doc, name):
            raise ProfileAlreadyExists(name, path)


def declare_profiles(
    doc: TWorkspaceDefinition,
    profiles: Mapping[str, TProfileStoredDefinition],
    env: Optional[str],
    path: str,
) -> None:
    """Declares `profiles` in `doc`: as local profiles, or as profiles of the environment `env`.
    A new profile becomes a default when `doc` has no default local profile, or no default
    profile of its data access in `env`.

    Raises:
        InvalidProfileName: A name is not a profile name.
        ProfileAlreadyExists: A profile is declared already.
        EnvironmentNotDeclared: `env` is not declared.
    """
    check_new_profiles(doc, path, list(profiles))
    if env is None:
        declared = doc["local_profiles"]
        # a declared `dev` without the flag is the default already
        has_default = DEFAULT_PROFILE in declared or any(
            p.get("default") for p in declared.values()
        )
        for name, profile in profiles.items():
            local: TProfileDefinition = {**profile, "location": "local"}
            # a local profile has no environment, even when an untyped caller passes `env`
            local.pop("env", None)
            if not has_default:
                local["default"] = has_default = True
            declared[name] = local
        return
    if env not in doc["environments"]:
        raise EnvironmentNotDeclared(env, path, list(doc["environments"]))
    environment = doc["environments"][env]
    for name, profile in profiles.items():
        remote: TProfileDefinition = {**profile, "location": "remote", "env": env}
        if get_default_profile_for_env(environment, get_profile_data_access(remote)) is None:
            remote["default"] = True
        environment["profiles"][name] = remote


def add_profiles(
    context: WorkspaceRunContext,
    profiles: Mapping[str, TProfileStoredDefinition],
    env: Optional[str] = None,
) -> None:
    """Declares `profiles` in the workspace file (see `declare_profiles`) and reloads the
    definition in `context`.

    Raises:
        InvalidWorkspaceFile: The declarations are invalid together with the existing ones.
    """
    # read from disk, so the declarations never leak into the cached content of the context
    doc = load_workspace_definition(context)
    declare_profiles(doc, profiles, env, format_workspace_file_path(context))
    save_workspace_file(context, doc)
    context.reload_workspace_definition()


def is_local_profile(name: str) -> bool:
    """True for local profiles: they never leave the machine and never deploy.

    Asks the active workspace. Outside a workspace only `dev` and `tests` are local.
    """
    ctx = active_run_context()
    if isinstance(ctx, WorkspaceRunContext):
        return ctx.is_local_profile(name)
    return name in LOCAL_PROFILES


def check_profile_declared(profile: str) -> None:
    """Refuses `profile` when the active workspace does not declare it.

    Raises:
        InvalidProfileName: `profile` is not a profile name, so it cannot be declared either.
        ProfileNotDeclared: The profile is not declared.
    """
    if not PROFILE_NAME_RE.match(profile):
        raise InvalidProfileName(profile)
    ctx = active()
    doc = ctx.workspace_definition
    if not is_profile_declared(doc, profile):
        raise ProfileNotDeclared(
            profile,
            format_workspace_file_path(ctx),
            list(doc["environments"]),
            profiles=list(get_declared_profiles(doc)),
        )


def activate_profile(profile: str) -> None:
    """Activates `profile` in this process and, via `WORKSPACE__PROFILE`, in the processes it
    starts.

    Raises:
        ProfileNotDeclared: The active workspace does not declare `profile`.
    """
    ctx = active_run_context()
    if isinstance(ctx, WorkspaceRunContext):
        # refused before the variable is set, so a typo never reaches the processes started here
        check_profile_declared(profile)
    os.environ[WORKSPACE__PROFILE] = profile
    # a run context created before the variable was set keeps its profile until switched
    if isinstance(ctx, ProfilesRunContext) and ctx.profile != profile:
        ctx.switch_profile(profile)


@contextmanager
def use_profile(profile: str) -> Iterator[RunContextBase]:
    """Runs the block with `profile` active in this process and, via `WORKSPACE__PROFILE`, in the
    processes it starts. Restores the run context and the variable afterwards.

    Raises:
        InvalidProfileName: `profile` is not a profile name.
        ProfileNotDeclared: The active workspace does not declare `profile`.
    """
    plug = Container()[PluggableRunContext]
    if isinstance(plug.context, WorkspaceRunContext):
        check_profile_declared(profile)
    cookie = plug.push_context()
    try:
        with custom_environ({WORKSPACE__PROFILE: profile}):
            if not isinstance(plug.context, ProfilesRunContext) or plug.context.profile != profile:
                plug.reload(plug.context.run_dir, runtime_kwargs={"profile": profile})
            yield plug.context
    finally:
        plug.pop_context(cookie)
