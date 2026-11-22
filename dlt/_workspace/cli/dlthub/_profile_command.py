import os
from typing import List, Mapping, Optional, Sequence

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.access import format_data_access
from dlt._workspace.exceptions import (
    EnvironmentAlreadyExists,
    EnvironmentNotDeclared,
    ProfileAlreadyExists,
    ProfileException,
)
from dlt._workspace.environment import (
    create_environment_definition,
    declare_environments,
    get_profiles_for_env,
)
from dlt._workspace.profile import check_profile_declared, declare_profiles
from dlt._workspace.run_context import format_display_path
from dlt._workspace.settings_files import (
    create_profile_settings,
    get_profile_pin_file,
    get_profile_settings_files,
    read_profile_pin,
    save_profile_pin,
)
from dlt._workspace.workspace_definition import (
    get_default_profile_for_env,
    get_environment_definition,
    get_environment_for_profile,
    get_profile_data_access,
    get_profile_definition,
    needs_settings_files,
)
from dlt._workspace.workspace_file import (
    build_stored_workspace_definition,
    format_workspace_file_path,
    load_workspace_definition,
    save_workspace_file,
)
from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.dlthub._local_workspace_command import (
    print_profile_section,
    print_providers,
    format_remote_workspace,
)
from dlt._workspace.cli.dlthub.typing import (
    TCurrentProfileFullInfo,
    TEnvironmentInfo,
    TProfileInfo,
)
from dlt._workspace.cli.dlthub.utils import fetch_environments_list, fetch_profiles_list
from dlt._workspace.typing import (
    TEnvironmentDefinition,
    TProfileDefinition,
    TProfileDataAccess,
    TProfileLocation,
    TProfileStoredDefinition,
    TWorkspaceDefinition,
)


@utils.track_command("profile", track_before=False, operation="info")
def print_profile_info(info: TCurrentProfileFullInfo, verbosity: int = 0) -> None:
    """Renders the active profile: name, paths, the recorded and default markers, providers
    (verbose)."""
    print_profile_section(info, info["configured_profiles"])
    if verbosity > 0 and info["providers"]:
        fmt.echo()
        print_providers(info["providers"], verbosity)


def _format_profile_line(p: TProfileInfo) -> str:
    """One line of the profile listing: name, status markers and data access."""

    def _format_marker(name: str) -> str:
        return " " + fmt.bold("(%s)" % name)

    markers = [_format_marker("active")] if p["is_current"] else []
    # shown when `WORKSPACE__PROFILE` overrides the recorded profile
    if p["is_pinned"] and not p["is_current"]:
        markers.append(_format_marker("recorded"))
    # a remote profile is the default of its data access in its environment
    remote_default = bool(p.get("default")) and p["location"] == "remote"
    if remote_default:
        level = get_profile_data_access(p)
        markers.append(
            " (%s for %s data access)"
            % (fmt.bold("default"), fmt.bold("no" if level == "none" else level))
        )
    elif p.get("default"):
        markers.append(_format_marker("default"))
    if p.get("configured"):
        markers.append(_format_marker("has settings files"))
    line = fmt.bold(p["name"]) + "".join(markers)
    return line if remote_default else "%s - %s" % (line, format_data_access(p["data_access"]))


def _print_environment(env: TEnvironmentInfo) -> None:
    """The environment line of the listing, with its profiles below it."""
    markers = ["default"] if env.get("default") else []
    if env.get("strict_access"):
        markers.append("strict about access")
    suffix = " (%s)" % ", ".join(markers) if markers else ""
    fmt.echo("  environment %s%s:" % (fmt.bold(env["name"]), suffix))
    fmt.echo(
        "    remote workspace %s"
        % format_remote_workspace(env["workspace_name"], env.get("workspace_id"))
    )
    if deployment_module := env.get("deployment_module"):
        fmt.echo("    deployment module %s" % fmt.bold(deployment_module))
    for p in env["profile_infos"]:
        fmt.echo("    * %s" % _format_profile_line(p))


def _print_profile(p: TProfileInfo, settings: Sequence[str]) -> None:
    """The profile line of the listing, with its environment and settings files below it."""
    fmt.echo("  * %s" % _format_profile_line(p))
    if env := p.get("env"):
        also = (
            ", other profiles of the environment: %s" % ", ".join(p["also_in_env"])
            if p["also_in_env"]
            else ""
        )
        fmt.echo("    environment %s%s" % (fmt.bold(env), also))
    if settings:
        listed = ", ".join(fmt.bold(f) for f in settings)
        fmt.echo("    settings files: %s" % listed)
    else:
        fmt.echo(
            "    settings files: none. A profile without data access reads the shared config.toml"
            " and secrets.toml."
        )


@utils.track_command("profile", track_before=False, operation="list")
def list_profiles(workspace_run_context: WorkspaceRunContext) -> None:
    """Lists the local profiles, then each environment with its profiles."""
    fmt.echo(
        "Profiles declared in %s" % fmt.bold(format_workspace_file_path(workspace_run_context))
    )
    local = [p for p in fetch_profiles_list() if p["location"] == "local"]
    fmt.echo()
    fmt.echo("Local profiles:")
    for p in local:
        fmt.echo("  * %s" % _format_profile_line(p))
    if not local:
        fmt.echo("  (none)")
    fmt.echo()
    fmt.echo("Remote profiles:")
    environments = fetch_environments_list()
    for env in environments:
        _print_environment(env)
    if not environments:
        fmt.echo("  (none)")


def _describe_profile(name: str, profile: TProfileDefinition) -> str:
    parts = [
        "environment %s" % profile["env"] if profile.get("env") else "local",
        format_data_access(profile["data_access"]),
    ]
    if profile.get("default"):
        parts.append("default")
    return "profile %s: %s" % (fmt.bold(name), ", ".join(parts))


def _describe_environment(environment: TEnvironmentDefinition) -> str:
    parts = [
        "deployments to it use %s" % fmt.bold(get_default_profile_for_env(environment, "write"))
    ]
    if environment.get("strict_access"):
        parts.append("strict about access")
    if environment.get("default"):
        parts.append("default environment")
    return "environment %s: %s" % (fmt.bold(environment["name"]), ", ".join(parts))


def _confirm() -> bool:
    if fmt.confirm("Proceed?", default=True):
        return True
    fmt.echo("dlt changed nothing.")
    return False


def _declare(
    workspace_run_context: WorkspaceRunContext,
    doc: TWorkspaceDefinition,
    profiles: Mapping[str, TProfileStoredDefinition],
    environments: Mapping[str, TEnvironmentDefinition],
) -> bool:
    """Validates `doc`, then shows what it adds to the workspace and the settings files to create.
    Asks for confirmation, then saves `doc` and creates the files.

    Raises:
        InvalidWorkspaceFile: `doc` is not valid. dlt shows and saves nothing.
    """
    path = format_workspace_file_path(workspace_run_context)
    # an invalid `doc` raises here, before dlt asks for confirmation
    build_stored_workspace_definition(doc, path)
    run_dir = workspace_run_context.run_dir
    with_settings = [n for n, p in profiles.items() if needs_settings_files(p)]
    files = [f for n in with_settings for f in get_profile_settings_files(workspace_run_context, n)]
    fmt.echo("dlt will add to %s:" % fmt.bold(path))
    for name in environments:
        fmt.echo("  * %s" % _describe_environment(doc["environments"][name]))
    for name in profiles:
        # as declared in `doc`, with the default that the declaration gave it
        fmt.echo("  * %s" % _describe_profile(name, get_profile_definition(doc, name)))
    to_create = [f for f in files if not os.path.exists(f)]
    if to_create:
        fmt.echo("dlt will create:")
        for f in to_create:
            fmt.echo("  * %s" % fmt.bold(format_display_path(f, run_dir)))
    for f in files:
        if f not in to_create:
            fmt.echo("dlt will keep the existing %s" % fmt.bold(format_display_path(f, run_dir)))
    if not _confirm():
        return False
    save_workspace_file(workspace_run_context, doc)
    create_profile_settings(workspace_run_context, with_settings)
    workspace_run_context.reload_workspace_definition()
    if to_create:
        _echo_settings_hint()
    return True


def _echo_settings_hint() -> None:
    fmt.echo(
        "Put the settings of each profile in its %s and the credentials in its %s."
        % (fmt.bold("<profile>.config.toml"), fmt.bold("<profile>.secrets.toml"))
    )


def _list_settings_files(
    workspace_run_context: WorkspaceRunContext, names: Sequence[str]
) -> List[str]:
    """Settings files of the declared profiles `names` that need them."""
    return [
        f
        for n in names
        if needs_settings_files(workspace_run_context.get_profile_definition(n))
        for f in get_profile_settings_files(workspace_run_context, n)
    ]


def _create_missing_settings(
    workspace_run_context: WorkspaceRunContext, names: Sequence[str], missing: Sequence[str]
) -> None:
    """Creates the `missing` settings files of the declared profiles `names` after
    confirmation."""
    fmt.echo("dlt will create the missing settings files:")
    for f in missing:
        fmt.echo("  * %s" % fmt.bold(format_display_path(f, workspace_run_context.run_dir)))
    if not _confirm():
        return
    create_profile_settings(
        workspace_run_context,
        [n for n in names if needs_settings_files(workspace_run_context.get_profile_definition(n))],
    )
    # the `configured` flag of a profile depends on its settings files
    workspace_run_context.reload_workspace_definition()
    _echo_settings_hint()


def _find_conflicting_options(
    profile: TProfileDefinition,
    location: Optional[TProfileLocation],
    data_access: Optional[TProfileDataAccess],
    env: Optional[str],
) -> List[str]:
    """The options given to `profile add` that contradict the declared `profile`."""
    conflicting: List[str] = []
    if location and location != profile["location"]:
        conflicting.append(f"--{location}")
    if data_access and data_access != profile["data_access"]:
        conflicting.append(f"--access {data_access}")
    if env and env != profile.get("env"):
        conflicting.append(f"--environment {env}")
    return conflicting


@utils.track_command("profile", track_before=False, operation="add")
def add_profile(
    workspace_run_context: WorkspaceRunContext,
    profile_name: str,
    location: Optional[TProfileLocation] = None,
    data_access: Optional[TProfileDataAccess] = None,
    env: Optional[str] = None,
) -> None:
    """Declares one profile and creates its settings files. A profile with `location` local is a
    local profile. Any other profile belongs to `env` and gets `data_access`, which default to the
    default environment and to write. For a declared profile, creates only the missing settings
    files.

    Raises:
        ProfileAlreadyExists: The profile is declared and has its settings files, or is declared
            differently than the options say.
    """
    path = format_workspace_file_path(workspace_run_context)
    if (declared := workspace_run_context.profile_definitions.get(profile_name)) is not None:
        if conflicting := _find_conflicting_options(declared, location, data_access, env):
            raise ProfileAlreadyExists(profile_name, path, conflicting)
        settings = _list_settings_files(workspace_run_context, [profile_name])
        if missing := [f for f in settings if not os.path.exists(f)]:
            _create_missing_settings(workspace_run_context, [profile_name], missing)
            return
        info = next(p for p in fetch_profiles_list() if p["name"] == profile_name)
        _print_profile(
            info, [format_display_path(f, workspace_run_context.run_dir) for f in settings]
        )
        raise ProfileAlreadyExists(profile_name, path)
    profile: TProfileStoredDefinition
    if location == "local":
        if env or data_access not in (None, "write"):
            raise ProfileException(
                f"dlt cannot add local profile `{profile_name}` with --environment or with"
                " --access"
                " other than write. A local profile has write data access and belongs to no"
                f" environment. Run `dlthub profile add {profile_name} --local` without other"
                " options."
            )
        env = None
        profile = {"data_access": "write"}
    else:
        environments = workspace_run_context.environment_definitions
        env = env or workspace_run_context.default_environment
        if env is None:
            raise ProfileException(
                f"dlt cannot add `{profile_name}`: no environment is the default in `{path}`."
                " Name the environment with `--environment`:"
                f" {', '.join(environments) or 'none declared'}."
            )
        if env not in environments:
            raise EnvironmentNotDeclared(env, path, list(environments))
        profile = {"data_access": data_access or "write"}
    doc = load_workspace_definition(workspace_run_context)
    declare_profiles(doc, {profile_name: profile}, env, path)
    _declare(workspace_run_context, doc, {profile_name: profile}, {})


@utils.track_command("profile", track_before=False, operation="add-environment")
def add_environment(workspace_run_context: WorkspaceRunContext, name: str) -> None:
    """Declares environment `name` with `<name>-write` as its default write profile and creates
    the settings files of that profile. Then names the command that adds the profile with read
    data access that interactive jobs need. For a declared environment, creates only the settings
    files that its profiles miss.

    Raises:
        EnvironmentAlreadyExists: The environment is declared and its profiles have their
            settings files.
    """
    if (declared := workspace_run_context.environment_definitions.get(name)) is not None:
        profiles = get_profiles_for_env(declared)
        settings = _list_settings_files(workspace_run_context, profiles)
        if missing := [f for f in settings if not os.path.exists(f)]:
            _create_missing_settings(workspace_run_context, profiles, missing)
            return
        _print_environment(next(e for e in fetch_environments_list() if e["name"] == name))
        raise EnvironmentAlreadyExists(name, format_workspace_file_path(workspace_run_context))
    environment = create_environment_definition(name)
    doc = load_workspace_definition(workspace_run_context)
    declare_environments(
        doc, {name: environment}, format_workspace_file_path(workspace_run_context)
    )
    if _declare(workspace_run_context, doc, environment["profiles"], {name: environment}):
        fmt.echo(
            "Environment %s is strict about access, and deployments to it use %s. Interactive"
            " jobs (notebooks, dashboards) need a profile with read data access. Add it with %s."
            % (
                fmt.bold(name),
                fmt.bold(get_default_profile_for_env(environment, "write")),
                fmt.bold(f"dlthub profile add {name}-read --access read --environment {name}"),
            )
        )


@utils.track_command("profile", track_before=False, operation="pin")
def pin_profile(workspace_run_context: WorkspaceRunContext, profile_name: str) -> None:
    """Shows the active profile, or makes `profile_name` the active profile after confirmation."""
    pin_file = format_display_path(
        get_profile_pin_file(workspace_run_context), workspace_run_context.run_dir
    )
    if not profile_name:
        pinned_profile = read_profile_pin(workspace_run_context)
        if pinned_profile:
            fmt.echo(
                "%s is the active profile recorded in %s. To return to the default profile,"
                " remove this file." % (fmt.bold(pinned_profile), fmt.bold(pin_file))
            )
        else:
            fmt.echo(
                "No active profile is recorded in %s. Local commands run with the default"
                " profile, %s. To make a profile the active profile, run"
                " `dlthub local profile use <name>`."
                % (fmt.bold(pin_file), fmt.bold(workspace_run_context.default_profile))
            )
        if overriding := os.environ.get(WORKSPACE__PROFILE):
            fmt.warning(
                "WORKSPACE__PROFILE is set in this shell: local commands run with %s. To run them"
                " with the profile above, unset it."
                % fmt.bold(overriding)
            )
        return
    check_profile_declared(profile_name)
    doc = workspace_run_context.workspace_definition
    env = get_environment_for_profile(doc, profile_name)
    in_env = " (environment %s)" % fmt.bold(env) if env else ""
    fmt.echo(
        "dlt will make %s%s the active profile and record it in %s."
        % (fmt.bold(profile_name), in_env, fmt.bold(pin_file))
    )
    if not _confirm():
        return
    save_profile_pin(workspace_run_context, profile_name)
    fmt.echo(
        "%s is the active profile: local commands run with it until you make another profile"
        " active."
        % fmt.bold(profile_name)
    )
    if env:
        fmt.echo(
            "Commands that deploy or work with a deployment now use environment %s and its"
            " default write profile %s."
            % (
                fmt.bold(env),
                fmt.bold(
                    get_default_profile_for_env(get_environment_definition(doc, env), "write")
                ),
            )
        )
    elif default_env := workspace_run_context.default_environment:
        fmt.echo(
            "%s belongs to no environment: commands that deploy or work with a deployment use the"
            " default environment %s." % (fmt.bold(profile_name), fmt.bold(default_env))
        )
    if overriding := os.environ.get(WORKSPACE__PROFILE):
        fmt.warning(
            "WORKSPACE__PROFILE=%s is set in this shell and overrides the recorded profile. To run"
            " local commands with %s, unset it." % (overriding, profile_name)
        )
