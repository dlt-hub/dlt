"""Environments of the active workspace: what a deployment is made to. Declare, select and activate
them, and connect each to its remote workspace."""

import os
from typing import List, Mapping, Optional

import tomlkit

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import CONFIG_TOML, ConfigTomlProvider
from dlt.common.configuration.specs import known_sections
from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext

from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.configuration import ENVIRONMENT_SECTION, WorkspaceRuntimeConfiguration
from dlt._workspace.exceptions import (
    EnvironmentAlreadyExists,
    EnvironmentNotDeclared,
    InvalidProfileName,
    NoEnvironment,
    OrganizationMismatch,
    WorkspaceRunContextNotAvailable,
)
from dlt._workspace.profile import activate_profile, check_new_profiles
from dlt._workspace.run_context import format_display_path
from dlt._workspace.typing import (
    DEFAULT_DEPLOYMENT_MODULE,
    TEnvironmentDefinition,
    TWorkspaceDefinition,
)
from dlt._workspace.workspace_definition import PROFILE_NAME_RE, get_default_profile_for_env
from dlt._workspace.workspace_file import (
    load_workspace_definition,
    save_workspace_file,
    format_workspace_file_path,
)

ENVIRONMENT_ARGUMENT_HELP = (
    "Environment to deploy to or work with. Defaults to the environment of the active profile,"
    " else to the default environment."
)
"""Help of the `--environment` option of commands that deploy or work with a deployment and of the
`environment` argument of MCP tools."""


def create_environment_definition(name: str) -> TEnvironmentDefinition:
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


def get_profiles_for_env(environment: TEnvironmentDefinition) -> List[str]:
    """The profiles of `environment`, deployed together: the default profile with write data
    access first, then the others in declaration order."""
    first = get_default_profile_for_env(environment, "write")
    return ([first] if first else []) + [n for n in environment["profiles"] if n != first]


def declare_environments(
    doc: TWorkspaceDefinition, environments: Mapping[str, TEnvironmentDefinition], path: str
) -> None:
    """Declares `environments` with their profiles in `doc`. Without a default environment, the
    first one becomes the default.

    Raises:
        InvalidProfileName: A name is not a profile name.
        EnvironmentAlreadyExists: An environment is declared already.
        ProfileAlreadyExists: A profile of an environment is declared already.
    """
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


def add_environments(
    context: WorkspaceRunContext, environments: Mapping[str, TEnvironmentDefinition]
) -> None:
    """Declares `environments` in the workspace file (see `declare_environments`) and reloads
    the definition in `context`.

    Raises:
        InvalidWorkspaceFile: The declarations are invalid together with the existing ones.
    """
    doc = load_workspace_definition(context)
    declare_environments(doc, environments, format_workspace_file_path(context))
    save_workspace_file(context, doc)
    context.reload_workspace_definition()


def select_environment(name: Optional[str] = None) -> TEnvironmentDefinition:
    """The environment that a command that deploys or works with a deployment uses.

    `name` is the environment the user named with `--environment`. Without it, dlt takes the
    environment of the active profile, else the default environment.

    Returns:
        TEnvironmentDefinition: The environment, with its profiles and its deployment module.

    Raises:
        EnvironmentNotDeclared: `name` is not declared.
        NoEnvironment: Nothing names an environment and none is the default.
    """
    ctx = active()
    if name:
        definition = _get_declared_environment(ctx, name)
    else:
        name = ctx.environment or ctx.default_environment
        if not name:
            raise NoEnvironment(
                ctx.profile, format_workspace_file_path(ctx), list(ctx.environment_definitions)
            )
        definition = ctx.get_environment_definition(name)
    return {**definition, "deployment_module": _get_configured_deployment_module(ctx, name)}


def activate_environment(name: Optional[str] = None) -> TEnvironmentDefinition:
    """Activates the default write profile of the environment that `select_environment` selects.

    Returns:
        TEnvironmentDefinition: The environment, with its profiles, to generate the manifest from.
    """
    environment = select_environment(name)
    activate_profile(get_default_profile_for_env(environment, "write"))
    return environment


def get_deployment_environment(
    environment: Optional[TEnvironmentDefinition] = None,
) -> Optional[TEnvironmentDefinition]:
    """The environment whose deployment module dlt imports: `environment`, else the one that
    `select_environment` selects. `None` when there is no environment or no workspace."""
    if environment is not None:
        return environment
    try:
        return select_environment()
    except (NoEnvironment, WorkspaceRunContextNotAvailable):
        return None


def get_deployment_module_for_env(environment: Optional[TEnvironmentDefinition] = None) -> str:
    """The deployment module of `get_deployment_environment(environment)`, `__deployment__` when
    there is none or the workspace does not declare it."""
    environment = get_deployment_environment(environment)
    if environment is None:
        return DEFAULT_DEPLOYMENT_MODULE
    if "deployment_module" in environment:
        return environment["deployment_module"]
    try:
        ctx = active()
    except WorkspaceRunContextNotAvailable:
        return DEFAULT_DEPLOYMENT_MODULE
    if environment["name"] not in ctx.environment_definitions:
        return DEFAULT_DEPLOYMENT_MODULE
    return _get_configured_deployment_module(ctx, environment["name"])


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
    _get_declared_environment(ctx, environment)
    resolved = ctx.get_environment_config(environment)
    sections = (known_sections.WORKSPACE, ENVIRONMENT_SECTION, environment)
    runtime_section = WorkspaceRuntimeConfiguration.__section__
    config = ConfigTomlProvider(ctx.settings_dir)
    changed = False
    if organization_id:
        connected = resolved.runtime.organization_id
        if connected and connected != organization_id:
            raise OrganizationMismatch(
                environment,
                connected,
                organization_id,
                format_display_path(ctx.settings_dir, ctx.run_dir),
            )
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
        _reload_context(ctx)


def disconnect_remote_workspace(ctx: WorkspaceRunContext, environment: str) -> None:
    """Disconnects the local workspace from the remote workspace of `environment`.

    Removes the section of `environment` and the `[runtime] workspace_id` set before
    environments existed: from the settings files of the profiles of `environment`, and from
    `config.toml` when `environment` is the default one. The organization and the workspace
    name stay. Environment variables and the global settings dir are not touched.

    Raises:
        EnvironmentNotDeclared: `environment` is not declared.
    """
    definition = _get_declared_environment(ctx, environment)
    is_default = environment == ctx.default_environment
    files = [(CONFIG_TOML, is_default)]
    files += [(f"{profile}.{CONFIG_TOML}", True) for profile in get_profiles_for_env(definition)]
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
        _reload_context(ctx)


def _get_declared_environment(ctx: WorkspaceRunContext, environment: str) -> TEnvironmentDefinition:
    definitions = ctx.environment_definitions
    if environment not in definitions:
        raise EnvironmentNotDeclared(
            environment, format_workspace_file_path(ctx), list(definitions)
        )
    return definitions[environment]


def _get_configured_deployment_module(ctx: WorkspaceRunContext, environment: str) -> str:
    return ctx.get_environment_config(environment).deployment_module or DEFAULT_DEPLOYMENT_MODULE


def _reload_context(ctx: WorkspaceRunContext) -> None:
    plug = Container()[PluggableRunContext]
    if plug.context is ctx:
        plug.reload_providers()
    else:
        ctx.reset_config()
