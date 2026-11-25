"""Environments of the active workspace: what a deployment is made to. Declares and selects them,
activates their default write profiles, and connects the local workspace to remote workspaces."""

import os
from contextlib import contextmanager, nullcontext
from typing import Any, ContextManager, Iterator, List, Mapping, Optional, Tuple

import tomlkit

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    CONFIG_TOML,
    SECRETS_TOML,
    ConfigTomlProvider,
    SecretsTomlProvider,
)
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
    ProfileNotInEnvironment,
    WorkspaceRunContextNotAvailable,
)
from dlt._workspace.profile import (
    activate_profile,
    check_new_profiles,
    check_profile_declared,
    use_profile,
)
from dlt._workspace.run_context import format_display_path
from dlt._workspace.settings_files import save_file_atomic
from dlt._workspace.typing import (
    DEFAULT_DEPLOYMENT_MODULE,
    TEnvironmentDefinition,
    TProfileOptionsTakes,
    TWorkspaceDefinition,
)
from dlt._workspace.workspace_definition import (
    PROFILE_NAME_RE,
    get_default_profile_for_env,
    get_environment_for_profile,
    is_profile_declared,
)
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

ENVIRONMENT_PROFILE_ARGUMENT_HELP = (
    "Profile of the environment to run with. Selects the environment when no environment is given."
)
"""Help of the `--profile` option of commands that also take `--environment` and of the `profile`
argument of MCP tools that also take `environment`."""

WORKSPACE_ARGUMENT_HELP = (
    "Selects the environment of the active profile, else the default environment, as"
    " --environment without a name does. --profile is not allowed with it."
)
"""Help of the `--workspace` option."""


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
    """Selects the environment for a command that deploys or works with a deployment.

    `name` is the environment the user named with `--environment`. Without it, dlt takes the
    environment of the active profile, else the default environment.

    Returns:
        TEnvironmentDefinition: The environment, with its profiles, its deployment module and
            whether it deploys the workspace dashboard.

    Raises:
        InvalidProfileName: `name` is not an environment name.
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
    config = ctx.get_environment_config(name)
    selected: TEnvironmentDefinition = {
        **definition,
        "deployment_module": config.deployment_module or DEFAULT_DEPLOYMENT_MODULE,
    }
    # unset, `should_deploy_dashboard` decides from the strictness of the environment
    if config.deploy_dashboard is not None:
        selected["deploy_dashboard"] = config.deploy_dashboard
    return selected


def activate_environment(name: Optional[str] = None) -> TEnvironmentDefinition:
    """Activates the default write profile of the environment that `select_environment` selects.

    Returns:
        TEnvironmentDefinition: The environment, with its profiles, to generate the manifest from.
    """
    environment = select_environment(name)
    activate_profile(get_default_profile_for_env(environment, "write"))
    return environment


def select_environment_profile(
    environment: Optional[str] = None, profile: Optional[str] = None
) -> Tuple[TEnvironmentDefinition, str]:
    """The environment and the profile of a command that takes both.

    `profile` must belong to `environment`. Without `environment`, dlt uses the environment of
    `profile`. Without `profile`, dlt uses the active profile when it belongs to the environment
    that `select_environment` selects, else the default write profile of that environment.

    Returns:
        Tuple[TEnvironmentDefinition, str]: The environment and the profile to run with.

    Raises:
        InvalidProfileName: `profile` or `environment` is not a profile or environment name.
        ProfileNotDeclared: `profile` is not declared.
        ProfileNotInEnvironment: `profile` is a local profile or a profile of another environment.
        EnvironmentNotDeclared: `environment` is not declared.
        NoEnvironment: Nothing names an environment and none is the default.
    """
    ctx = active()
    if not profile:
        definition = select_environment(environment)
        if ctx.environment == definition["name"]:
            return definition, ctx.profile
        return definition, get_default_profile_for_env(definition, "write")
    check_profile_declared(profile)
    profile_env = get_environment_for_profile(ctx.workspace_definition, profile)
    definition = select_environment(environment) if environment else None
    if profile_env is None or (definition is not None and definition["name"] != profile_env):
        raise ProfileNotInEnvironment(
            profile,
            profile_env,
            environment,
            get_profiles_for_env(definition) if definition else (),
        )
    return definition or select_environment(profile_env), profile


def activate_environment_profile(
    environment: Optional[str] = None, profile: Optional[str] = None
) -> TEnvironmentDefinition:
    """Activates the profile that `select_environment_profile` selects.

    Returns:
        TEnvironmentDefinition: The environment of the activated profile.
    """
    definition, selected = select_environment_profile(environment, profile)
    activate_profile(selected)
    return definition


@contextmanager
def use_environment(name: Optional[str] = None) -> Iterator[TEnvironmentDefinition]:
    """Runs the block with the default write profile of the environment that `select_environment`
    selects, see `use_profile`. Restores the run context afterwards.

    Yields:
        TEnvironmentDefinition: The environment, with its profiles, to generate the manifest from.
    """
    environment = select_environment(name)
    with use_profile(get_default_profile_for_env(environment, "write")):
        yield environment


@contextmanager
def use_environment_profile(
    environment: Optional[str] = None, profile: Optional[str] = None
) -> Iterator[TEnvironmentDefinition]:
    """Runs the block with the profile that `select_environment_profile` selects, see
    `use_profile`. Restores the run context afterwards.

    Yields:
        TEnvironmentDefinition: The environment of the profile.
    """
    definition, selected = select_environment_profile(environment, profile)
    with use_profile(selected):
        yield definition


def use_profile_options(
    takes: TProfileOptionsTakes, environment: Optional[str] = None, profile: Optional[str] = None
) -> ContextManager[Any]:
    """Runs the block with the profile that a command or an MCP tool that takes `takes` selects
    with `environment` and `profile`, see `use_environment`, `use_environment_profile` and
    `use_profile`. Without a selection, the block runs with the active profile."""
    if takes == "both":
        return use_environment_profile(environment, profile)
    if takes == "environment":
        return use_environment(environment)
    if takes == "profile" and profile:
        return use_profile(profile)
    return nullcontext()


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

    Writes `workspace_id` and `workspace_name` to the section of `environment` in `config.toml`,
    and `organization_id` to `[runtime]`. Skips a value that is `None` or already resolved.

    Raises:
        EnvironmentNotDeclared: `environment` is not declared.
        OrganizationMismatch: The workspace is connected to another organization.
    """
    definition = _get_declared_environment(ctx, environment)
    # a `[runtime] workspace_id` from before environments moves to the section first
    _migrate_runtime_workspace_id(ctx, definition, environment == ctx.default_environment)
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
        config.set_value("name", workspace_name, None, *sections)
        changed = True
    if changed:
        config.write_toml()
        _reload_context(ctx)


def disconnect_remote_workspace(ctx: WorkspaceRunContext, environment: str) -> None:
    """Disconnects the local workspace from the remote workspace of `environment`.

    Removes `workspace_id` from the section of `environment` in the shared settings files and in
    the settings files of its profiles, and the `[runtime] workspace_id` from before environments.
    The organization and the other settings of the environment stay. Environment variables and
    the global settings files stay unchanged.

    Raises:
        EnvironmentNotDeclared: `environment` is not declared.
    """
    definition = _get_declared_environment(ctx, environment)
    is_default = environment == ctx.default_environment
    changed = False
    shared_files = (CONFIG_TOML, SECRETS_TOML)
    for file_name in [*shared_files, *_get_profile_settings_files(definition)]:
        path = os.path.join(ctx.settings_dir, file_name)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())
        dropped = _drop_environment_workspace_id(doc, environment)
        # the one in the shared files belongs to the default environment
        if is_default or file_name not in shared_files:
            dropped = _drop_runtime_workspace_id(doc) is not None or dropped
        if dropped:
            save_file_atomic(path, tomlkit.dumps(doc))
            changed = True
    if changed:
        _reload_context(ctx)


def _migrate_runtime_workspace_id(
    ctx: WorkspaceRunContext, definition: TEnvironmentDefinition, is_default: bool
) -> None:
    """Moves the `[runtime] workspace_id` of the environment `definition` to its section."""
    file_names = _get_profile_settings_files(definition)
    # the shared files hold the id of the default environment
    if is_default:
        file_names += [CONFIG_TOML, SECRETS_TOML]
    runtime_id: Optional[str] = None
    # the first id found wins: the files of the default write profile come first
    for file_name in file_names:
        path = os.path.join(ctx.settings_dir, file_name)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            doc = tomlkit.parse(f.read())
        dropped = _drop_runtime_workspace_id(doc)
        if dropped is None:
            continue
        runtime_id = runtime_id or dropped
        save_file_atomic(path, tomlkit.dumps(doc))
    if runtime_id is None:
        return
    sections = (known_sections.WORKSPACE, ENVIRONMENT_SECTION, definition["name"])
    config = ConfigTomlProvider(ctx.settings_dir)
    # a section that has an id keeps it
    if config.get_value("workspace_id", str, None, *sections)[0] is None:
        config.set_value("workspace_id", runtime_id, None, *sections)
        config.write_toml()
    _reload_context(ctx)


def _get_profile_settings_files(definition: TEnvironmentDefinition) -> List[str]:
    """The settings files of the profiles of the environment `definition`, the ones of its
    default write profile first."""
    return [
        f"{profile}.{file_name}"
        for profile in get_profiles_for_env(definition)
        for file_name in (CONFIG_TOML, SECRETS_TOML)
    ]


def _drop_environment_workspace_id(doc: tomlkit.TOMLDocument, environment: str) -> bool:
    """Removes `workspace_id` from the section of `environment` in the settings file `doc`, and
    the section when nothing else is left in it. Returns whether `doc` had it."""
    environments = doc.get(known_sections.WORKSPACE, {}).get(ENVIRONMENT_SECTION, {})
    section = environments.get(environment)
    if section is None or section.pop("workspace_id", None) is None:
        return False
    if not section:
        del environments[environment]
    return True


def _drop_runtime_workspace_id(doc: tomlkit.TOMLDocument) -> Optional[str]:
    """Removes `workspace_id` from the runtime section of the settings file `doc`. Returns it,
    `None` when `doc` has none."""
    dropped: Optional[str] = None
    workspace = doc.get(known_sections.WORKSPACE, {})
    # `[workspace.runtime]` is the other place the runtime section resolves from
    for runtime in (
        doc.get(WorkspaceRuntimeConfiguration.__section__),
        workspace.get(WorkspaceRuntimeConfiguration.__section__),
    ):
        if runtime is not None and (value := runtime.pop("workspace_id", None)) is not None:
            dropped = dropped or str(value)
    return dropped


def set_global_secret(
    ctx: WorkspaceRunContext, key: str, value: Any, *sections: str, reload_context: bool = True
) -> None:
    """Writes `value` under `key` in `sections` of the global `secrets.toml` in `ctx.global_dir`:
    a secret of the user, not of the workspace, like an auth token. A dict `value` is merged into
    the existing one.

    With `reload_context`, `ctx` drops its resolved config and its providers, so the value
    takes effect right away. Without it, `ctx` sees the value only after its next reload.
    """
    os.makedirs(ctx.global_dir, exist_ok=True)
    secrets = SecretsTomlProvider(ctx.global_dir)
    secrets.set_value(key, value, None, *sections)
    secrets.write_toml()
    if reload_context:
        _reload_context(ctx)


def _get_declared_environment(ctx: WorkspaceRunContext, environment: str) -> TEnvironmentDefinition:
    if not PROFILE_NAME_RE.match(environment):
        raise InvalidProfileName(environment)
    definitions = ctx.environment_definitions
    if environment not in definitions:
        doc = ctx.workspace_definition
        is_profile = is_profile_declared(doc, environment)
        raise EnvironmentNotDeclared(
            environment,
            format_workspace_file_path(ctx),
            list(definitions),
            is_profile=is_profile,
            profile_env=get_environment_for_profile(doc, environment) if is_profile else None,
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
