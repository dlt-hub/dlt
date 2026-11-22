import os
from typing import Optional, Sequence

from dlt.common.configuration.providers.toml import GLOBAL_ORIGIN_PREFIX
from dlt.common.exceptions import DltException
from dlt.common.typing import Literal
from dlt.common.runtime.exceptions import RunContextNotAvailable
from dlt.common.runtime.run_context import DOT_DLT


class WorkspaceException(DltException):
    pass


class WorkspaceRunContextNotAvailable(RunContextNotAvailable, WorkspaceException):
    def __init__(self, run_dir: str):
        msg = (
            f"Workspace could not be found in `{run_dir}` (`{os.path.abspath(run_dir)}`). To enable"
            " workspace, create an empty file with the name `.workspace` in `.dlt` settings"
            " folder."
        )
        super().__init__(run_dir, msg)


def _format_environments(environments: Sequence[str]) -> str:
    return ", ".join(environments) if environments else "none declared"


class InvalidWorkspaceFile(WorkspaceException, ValueError):
    def __init__(self, path: str, errors: Sequence[str]) -> None:
        self.path = path
        self.errors = list(errors)
        listed = "\n".join(f"  - {e}" for e in errors)
        super().__init__(
            f"`{path}` declares the profiles of this workspace and is invalid:\n{listed}\nFix the"
            " entries above, or empty the file: dlt then declares the built-in profiles again."
        )


class WorkspaceFileEngineNoUpgradePath(WorkspaceException):
    def __init__(self, path: str, from_engine: int, to_engine: int) -> None:
        self.path = path
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(
            f"`{path}` declares the profiles of this workspace with engine version {from_engine}."
            f" This dlt supports version {to_engine}. A newer dlt wrote the file: upgrade dlt."
        )


class ToolkitsIndexEngineNoUpgradePath(WorkspaceException):
    def __init__(self, path: str, from_engine: int, to_engine: int) -> None:
        self.path = path
        self.from_engine = from_engine
        self.to_engine = to_engine
        super().__init__(
            f"`{path}` lists the installed toolkits with engine version {from_engine}. This dlt"
            f" supports version {to_engine}. A newer dlt wrote the file: upgrade dlt."
        )


class ProfileException(WorkspaceException):
    pass


class InvalidProfileName(ProfileException, ValueError):
    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(
            f"`{name}` is not a valid profile or environment name: use letters, digits, `_` and"
            " `-`. A profile name prefixes its settings files, `<name>.config.toml` and"
            " `<name>.secrets.toml` in `.dlt`."
        )


class ProfileAlreadyExists(ProfileException):
    def __init__(self, name: str, path: str, conflicting: Sequence[str] = ()) -> None:
        self.name = name
        self.path = path
        self.conflicting = list(conflicting)
        conflict = f" Its declaration differs from {', '.join(conflicting)}." if conflicting else ""
        super().__init__(
            f"Profile `{name}` is already declared in `{path}`.{conflict} `dlthub profile list`"
            f" shows the declared profiles. To change `{name}`, edit `{path}`."
        )


class EnvironmentAlreadyExists(ProfileException):
    def __init__(self, name: str, path: str) -> None:
        self.name = name
        self.path = path
        super().__init__(
            f"Environment `{name}` is already declared in `{path}`. `dlthub profile list` shows"
            f" its profiles. To change it, edit `{path}`."
        )


TProfileSource = Literal["argument", "environ", "recorded", "default"]
"""Where a profile name comes from: an argument such as `--profile`, the `WORKSPACE__PROFILE`
environment variable, `.dlt/profile-name`, or the default when nothing names a profile."""


class ProfileNotDeclared(ProfileException):
    """A profile that the workspace does not declare was named."""

    def __init__(
        self,
        name: str,
        path: str,
        environments: Sequence[str],
        source: TProfileSource = "argument",
        profiles: Sequence[str] = (),
    ) -> None:
        self.name = name
        self.path = path
        self.environments = list(environments)
        self.source = source
        declared = f" Declared profiles: {', '.join(profiles)}." if profiles else ""
        is_environment = name in environments
        hint = f" `{name}` is an environment. Name one of its profiles." if is_environment else ""
        if source == "recorded":
            # no command runs while the file records the profile, so it goes first
            add = (
                ""
                if is_environment
                else (
                    f" Then, to use `{name}`, run `dlthub profile add {name}` and `dlthub local"
                    f" profile use {name}`."
                )
            )
            remedy = (
                f"{hint} `.dlt/profile-name` records it as the active profile. Remove"
                f" `.dlt/profile-name` to run with the default profile.{declared}{add}"
            )
        elif source == "environ":
            remedy = (
                f"{hint} `WORKSPACE__PROFILE` names it. Set `WORKSPACE__PROFILE` to a declared"
                f" profile, or unset it.{declared}"
            )
        elif source == "default":
            remedy = (
                " It is the default profile, and nothing names another one. Name a declared"
                f" profile with `WORKSPACE__PROFILE`.{declared} Then declare a local profile with"
                f" `dlthub profile add {name} --local`."
            )
        elif is_environment:
            remedy = f"{hint}{declared}"
        else:
            remedy = (
                f" To use `{name}` on this machine only, run `dlthub profile add {name} --local`."
                f" To add it to an environment, run `dlthub profile add {name} --environment"
                f" <environment>`. Environments: {_format_environments(environments)}. `dlthub"
                " profile list` shows the declared profiles."
            )
        super().__init__(f"`{name}` is not a profile declared in `{path}`.{remedy}")


class EnvironmentNotDeclared(ProfileException):
    """`env` names no declared environment."""

    def __init__(
        self,
        env: str,
        path: str,
        environments: Sequence[str],
        is_profile: bool = False,
        profile_env: Optional[str] = None,
    ) -> None:
        self.env = env
        self.path = path
        self.environments = list(environments)
        if profile_env:
            remedy = (
                f" `{env}` is a profile of environment `{profile_env}`: name `{profile_env}`, or"
                " the profile with --profile."
            )
        elif is_profile:
            remedy = f" `{env}` is a local profile, not an environment."
        else:
            remedy = f" Declare it with `dlthub profile add-environment {env}`."
        super().__init__(
            f"`{env}` is not an environment declared in `{path}` (declared:"
            f" {_format_environments(environments)}).{remedy}"
        )


class NoEnvironment(ProfileException):
    """No environment to deploy to: nothing names one and none is the default."""

    def __init__(self, active_profile: str, path: str, environments: Sequence[str]) -> None:
        self.active_profile = active_profile
        self.path = path
        self.environments = list(environments)
        remedy = (
            f" Name one with `--environment`: {', '.join(environments)}. Or set `default: true`"
            f" on one under `environments` in `{path}`."
            if environments
            else (
                f" No environment is declared in `{path}`. Declare one with `dlthub profile"
                " add-environment <name>`."
            )
        )
        super().__init__(
            f"The active profile `{active_profile}` is a local profile, so it selects no"
            " environment. No environment is the default. A command that deploys or works with a"
            f" deployment needs one.{remedy}"
        )


class ProfileNotInEnvironment(ProfileException):
    """A command that takes an environment and a profile got a local profile, or a profile of
    another environment."""

    def __init__(
        self,
        profile: str,
        profile_env: Optional[str],
        env: Optional[str] = None,
        profiles: Sequence[str] = (),
    ) -> None:
        self.profile = profile
        self.profile_env = profile_env
        self.env = env
        self.profiles = list(profiles)
        if profile_env is None:
            problem = f"`{profile}` is a local profile and reaches no remote workspace."
        else:
            problem = f"`{profile}` is a profile of environment `{profile_env}`, not of `{env}`."
        if env:
            remedy = f" Name a profile of `{env}`: {', '.join(profiles)}."
        else:
            remedy = " Name a profile of an environment. `dlthub profile list` shows them."
        super().__init__(problem + remedy)


class WorkspaceIdConflict(WorkspaceException):
    """`workspace_id` under `[runtime]` in a settings file, and another one in the section of the
    default environment."""

    def __init__(self, environment: str, location: str) -> None:
        self.environment = environment
        self.location = location
        if location.startswith(GLOBAL_ORIGIN_PREFIX):
            where = f"the global `{location.removeprefix(GLOBAL_ORIGIN_PREFIX)}`"
        elif location.endswith(".toml"):
            where = f"`{os.path.join(DOT_DLT, location)}`"
        else:
            # a vault or a plugin provider, by its name
            where = f"`{location}`"
        super().__init__(
            f"`workspace_id` under `[runtime]` in {where}, set before environments existed, and"
            f" `workspace_id` under `[workspace.environment.{environment}]` both name the remote"
            f" workspace of the default environment `{environment}`. Remove `workspace_id` under"
            f" `[runtime]` in {where}."
        )


class EnvironmentNotConnected(WorkspaceException):
    """The local workspace is not connected to a remote workspace in `environment`."""

    def __init__(self, environment: str, path: str, remedy: str) -> None:
        self.environment = environment
        self.path = path
        super().__init__(
            f"The local workspace is not connected to a remote workspace in `{environment}`:"
            f" `{path}` has no `workspace_id` under `[workspace.environment.{environment}]`."
            f" {remedy}".rstrip()
        )


class OrganizationMismatch(WorkspaceException):
    """A workspace is connected to one organization, all its environments share it."""

    def __init__(self, environment: str, connected: str, requested: str, path: str) -> None:
        self.environment = environment
        self.connected = connected
        self.requested = requested
        self.path = path
        super().__init__(
            f"dlt cannot connect the local workspace in `{environment}` to a remote workspace of"
            f" organization `{requested}`. This workspace is connected to organization"
            f" `{connected}`, and every environment shares it. To change the organization, remove"
            f" `organization_id` from `[runtime]` in the settings files in `{path}`."
        )
