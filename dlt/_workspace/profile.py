import os
from typing import Mapping, Optional, Sequence, TYPE_CHECKING

from dlt.common.configuration.specs.pluggable_run_context import (
    ProfilesRunContext,
    RunContextBase,
)
from dlt.common.runtime.run_context import active as active_run_context

from dlt._workspace._known_env import WORKSPACE__PROFILE
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
from dlt._workspace.typing import TProfileDefinition, TWorkspaceDefinition
from dlt._workspace.workspace_definition import (
    LOCAL_PROFILES,
    PROFILE_NAME_RE,
    declared_profiles,
    default_profile,
    is_profile_declared,
    load_workspace_definition,
    profile_data_access,
    save_workspace_file,
    shown_workspace_file,
    validate_workspace_definition,
)

if TYPE_CHECKING:
    from dlt._workspace._workspace_context import WorkspaceRunContext

DEFAULT_PROFILE = "dev"

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
    context: RunContextBase,
    profiles: Mapping[str, TProfileDefinition],
    env: Optional[str] = None,
) -> TWorkspaceDefinition:
    """The workspace definition with `profiles` declared, local ones or profiles of environment
    `env`, validated and not saved.

    Raises:
        InvalidProfileName: A name is not a profile name.
        ProfileAlreadyExists: A profile is declared already.
        EnvironmentNotDeclared: `env` is not declared.
        InvalidWorkspaceFile: The declarations are invalid together with the existing ones.
    """
    path = shown_workspace_file(context)
    # read from disk, so the declarations never leak into the cached content of the context
    doc = load_workspace_definition(context)
    check_new_profiles(doc, path, list(profiles))
    if env is None:
        declared = doc["local_profiles"]
        has_default = any(p.get("default") for p in declared.values())
        for name, profile in profiles.items():
            profile = dict(profile)  # type: ignore[assignment]
            profile.setdefault("location", "local")
            if not has_default:
                profile["default"] = has_default = True
            declared[name] = profile
    else:
        if env not in doc["environments"]:
            raise EnvironmentNotDeclared(env, path, list(doc["environments"]))
        environment = doc["environments"][env]
        for name, profile in profiles.items():
            profile = dict(profile)  # type: ignore[assignment]
            profile.setdefault("env", env)
            profile.setdefault("location", "remote")
            # the first profile of a data access is its default
            if default_profile(environment, profile_data_access(profile)) is None:
                profile["default"] = True
            environment["profiles"][name] = profile
    validate_workspace_definition(doc, path)
    return doc


def add_profiles(
    context: "WorkspaceRunContext",
    profiles: Mapping[str, TProfileDefinition],
    env: Optional[str] = None,
) -> None:
    """Declares `profiles` in the workspace (see `declare_profiles`), saves the definition and
    reloads it in `context`."""
    save_workspace_file(context, declare_profiles(context, profiles, env))
    context.reload_workspace_definition()


def _active_workspace() -> "WorkspaceRunContext":
    from dlt._workspace._workspace_context import active

    return active()


def is_local_profile(name: str) -> bool:
    """True for local profiles: they never leave the machine and never deploy.

    Asks the active workspace. Outside a workspace only `dev` and `tests` are local.
    """
    from dlt._workspace._workspace_context import WorkspaceRunContext

    ctx = active_run_context()
    if isinstance(ctx, WorkspaceRunContext):
        return ctx.is_local_profile(name)
    return name in LOCAL_PROFILES


def check_profile_declared(profile: str) -> None:
    """Refuses `profile` when the active workspace does not declare it.

    Raises:
        ProfileNotDeclared: The profile is not declared.
    """
    ctx = _active_workspace()
    doc = ctx.workspace_definition
    if not is_profile_declared(doc, profile):
        raise ProfileNotDeclared(
            profile,
            shown_workspace_file(ctx),
            list(doc["environments"]),
            profiles=list(declared_profiles(doc)),
        )


def activate_profile(profile: str) -> None:
    """Activates `profile` in this process and, via `WORKSPACE__PROFILE`, in the processes it
    starts.

    Raises:
        ProfileNotDeclared: The active workspace does not declare `profile`.
    """
    from dlt._workspace._workspace_context import WorkspaceRunContext

    ctx = active_run_context()
    if isinstance(ctx, WorkspaceRunContext):
        # refused before the variable is set, so a typo never reaches the processes started here
        check_profile_declared(profile)
    os.environ[WORKSPACE__PROFILE] = profile
    # a run context created before the env var was set keeps its profile until switched
    if isinstance(ctx, ProfilesRunContext) and ctx.profile != profile:
        ctx.switch_profile(profile)
