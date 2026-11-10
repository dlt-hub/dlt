"""Environments: what a deployment is made to, with the profiles deployed to it and the remote
workspace it is connected to."""

import os
from typing import Any, Mapping, Optional

import tomlkit

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    CONFIG_TOML,
    ConfigTomlProvider,
    SecretsTomlProvider,
)
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
    RunContextBase,
)

from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.configuration import ENVIRONMENT_SECTION, WorkspaceRuntimeConfiguration
from dlt._workspace.exceptions import (
    EnvironmentAlreadyExists,
    EnvironmentNotDeclared,
    InvalidProfileName,
    NoEnvironment,
    OrganizationMismatch,
)
from dlt._workspace.profile import activate_profile, check_new_profiles
from dlt._workspace.typing import TEnvironmentDefinition, TWorkspaceDefinition
from dlt._workspace.workspace_definition import (
    PROFILE_NAME_RE,
    default_profile,
    environment_profiles,
    get_workspace_file,
    load_workspace_definition,
    save_workspace_file,
    validate_workspace_definition,
)

ENVIRONMENT_ARGUMENT_HELP = (
    "Environment to deploy to or work with. Defaults to the environment of the active profile,"
    " else to the default environment."
)
"""Help of the `--environment` option of commands that deploy or work with a deployment and of the
`environment` argument of MCP tools."""


def new_environment(name: str) -> TEnvironmentDefinition:
    """A new environment `name`: strict about access, with `<name>-write`, its default write
    profile, as its only profile."""
    return {
        "name": name,
        "strict_access": True,
        "profiles": {
            f"{name}-write": {
                "location": "remote",
                "data_access": "write",
                "env": name,
                "default": True,
            },
        },
    }


def declare_environments(
    context: RunContextBase, environments: Mapping[str, TEnvironmentDefinition]
) -> TWorkspaceDefinition:
    """The workspace definition with `environments` and their profiles declared, validated and
    not saved. The first environment becomes the default.

    Raises:
        InvalidProfileName: A name is not a profile name.
        EnvironmentAlreadyExists: An environment is declared already.
        ProfileAlreadyExists: A profile of an environment is declared already.
        InvalidWorkspaceFile: The declarations are invalid together with the existing ones.
    """
    path = get_workspace_file(context)
    # read from disk, so the declarations never leak into the cached content of the context
    doc = load_workspace_definition(context)
    declared = doc["environments"]
    for name, environment in environments.items():
        if not PROFILE_NAME_RE.match(name):
            raise InvalidProfileName(name)
        if name in declared:
            raise EnvironmentAlreadyExists(name, path)
        check_new_profiles(doc, path, list(environment["profiles"]))
    has_default = any(e.get("default") for e in declared.values())
    for name, environment in environments.items():
        environment = dict(environment)  # type: ignore[assignment]
        if not has_default:
            environment["default"] = has_default = True
        declared[name] = environment
    validate_workspace_definition(doc, path)
    return doc


def add_environments(
    context: WorkspaceRunContext, environments: Mapping[str, TEnvironmentDefinition]
) -> None:
    """Declares `environments` in the workspace (see `declare_environments`), saves the
    definition and reloads it in `context`."""
    save_workspace_file(context, declare_environments(context, environments))
    context.reload_workspace_definition()


def select_environment(name: Optional[str] = None) -> TEnvironmentDefinition:
    """The environment that a command that deploys or works with a deployment uses.

    `name` is the environment the user named with `--environment`. Without it, dlt takes the
    environment of the active profile, else the default environment.

    Returns:
        TEnvironmentDefinition: The environment, with its profiles.

    Raises:
        EnvironmentNotDeclared: `name` is not declared.
        NoEnvironment: Nothing names an environment and none is the default.
    """
    ctx = active()
    if name:
        return _declared_environment(ctx, name)
    name = ctx.environment or ctx.default_environment
    if not name:
        raise NoEnvironment(ctx.profile, get_workspace_file(ctx), list(ctx.environment_definitions))
    return ctx.get_environment_definition(name)


def activate_environment(name: Optional[str] = None) -> TEnvironmentDefinition:
    """Activates the default write profile of the environment that `select_environment` selects.

    Returns:
        TEnvironmentDefinition: The environment, with its profiles, to generate the manifest from.
    """
    environment = select_environment(name)
    activate_profile(default_profile(environment, "write"))
    return environment


def _declared_environment(ctx: WorkspaceRunContext, environment: str) -> TEnvironmentDefinition:
    definitions = ctx.environment_definitions
    if environment not in definitions:
        raise EnvironmentNotDeclared(environment, get_workspace_file(ctx), list(definitions))
    return definitions[environment]


def connect_remote_workspace(
    ctx: WorkspaceRunContext,
    environment: str,
    *,
    workspace_id: Optional[str] = None,
    workspace_name: Optional[str] = None,
    organization_id: Optional[str] = None,
) -> None:
    """Connects the local workspace to a remote workspace in `environment`.

    Writes `workspace_id` and `workspace_name` under `[workspace.environment.<environment>]` in
    `config.toml` and `organization_id`, the one organization of the workspace, under
    `[runtime]`. A value that is `None`, or that `environment` resolves already, is not written.

    Raises:
        EnvironmentNotDeclared: `environment` is not declared.
        OrganizationMismatch: The workspace is connected to another organization.
    """
    _declared_environment(ctx, environment)
    resolved = ctx.environment_config(environment)
    sections = (known_sections.WORKSPACE, ENVIRONMENT_SECTION, environment)
    runtime_section = WorkspaceRuntimeConfiguration.__section__
    config = ConfigTomlProvider(ctx.settings_dir)
    changed = False
    if organization_id:
        connected = resolved.runtime.organization_id
        if connected and connected != organization_id:
            raise OrganizationMismatch(environment, connected, organization_id, ctx.settings_dir)
        if not connected:
            config.set_value("organization_id", organization_id, None, runtime_section)
            changed = True
    if workspace_id and workspace_id != resolved.runtime.workspace_id:
        config.set_value("workspace_id", workspace_id, None, *sections)
        changed = True
    if workspace_name and workspace_name != resolved.settings.name:
        config.set_value("workspace_name", workspace_name, None, *sections)
        changed = True
    if changed:
        config.write_toml()
        _reload(ctx)


def disconnect_remote_workspace(ctx: WorkspaceRunContext, environment: str) -> None:
    """Disconnects the local workspace from the remote workspace of `environment`.

    Removes the section of `environment` and the `[runtime] workspace_id` set before
    environments existed: from the settings files of the profiles of `environment`, and from
    `config.toml` when `environment` is the default one. The organization and the workspace
    name stay. Environment variables and the global settings dir are not touched.

    Raises:
        EnvironmentNotDeclared: `environment` is not declared.
    """
    definition = _declared_environment(ctx, environment)
    is_default = environment == ctx.default_environment
    files = [(CONFIG_TOML, is_default)]
    files += [(f"{profile}.{CONFIG_TOML}", True) for profile in environment_profiles(definition)]
    changed = False
    for file_name, drop_runtime_id in files:
        path = os.path.join(ctx.settings_dir, file_name)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())
        workspace = doc.get(known_sections.WORKSPACE, {})
        dropped = workspace.get(ENVIRONMENT_SECTION, {}).pop(environment, None) is not None
        if drop_runtime_id:
            # `[workspace.runtime]` is the other place the runtime section resolves from
            for runtime in (
                doc.get(WorkspaceRuntimeConfiguration.__section__),
                workspace.get(WorkspaceRuntimeConfiguration.__section__),
            ):
                if runtime is not None and runtime.pop("workspace_id", None) is not None:
                    dropped = True
        if dropped:
            with open(path, "w", encoding="utf-8") as f:
                tomlkit.dump(doc, f)
            changed = True
    if changed:
        _reload(ctx)


def set_global_secret(ctx: WorkspaceRunContext, key: str, value: Any, *sections: str) -> None:
    """Writes `value` under `key` in `sections` of the global `secrets.toml` in `ctx.global_dir`."""
    secrets = SecretsTomlProvider(ctx.global_dir)
    secrets.set_value(key, value, None, *sections)
    secrets.write_toml()
    _reload(ctx)


def _reload(ctx: WorkspaceRunContext) -> None:
    plug = Container()[PluggableRunContext]
    if plug.context is ctx:
        plug.reload_providers()
    else:
        ctx.reset_config()
