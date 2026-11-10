import os
from typing import List, Mapping, Optional, Sequence

from dlt._workspace._known_env import WORKSPACE__PROFILE
from dlt._workspace._workspace_context import WorkspaceRunContext
from dlt._workspace.exceptions import (
    EnvironmentAlreadyExists,
    EnvironmentNotDeclared,
    ProfileAlreadyExists,
    ProfileException,
)
from dlt._workspace.environment import declare_environments, new_environment
from dlt._workspace.profile import check_profile_declared, declare_profiles
from dlt._workspace.settings_files import (
    create_profile_settings,
    get_profile_pin_file,
    profile_settings_files,
    read_profile_pin,
    save_profile_pin,
)
from dlt._workspace.workspace_definition import (
    default_profile,
    environment_definition,
    environment_of_profile,
    environment_profiles,
    format_data_access,
    get_workspace_file,
    needs_settings_files,
    profile_definition,
    save_workspace_file,
)
from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.dlthub._local_workspace_command import (
    print_profile_section,
    print_providers,
    remote_workspace,
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
    TWorkspaceDefinition,
)


@utils.track_command("profile", track_before=False, operation="info")
def print_profile_info(info: TCurrentProfileFullInfo, verbosity: int = 0) -> None:
    """Renders the active profile: name, paths, active and default markers, providers
    (verbose)."""
    print_profile_section(info, info["configured_profiles"])
    if verbosity > 0 and info["providers"]:
        fmt.echo()
        print_providers(info["providers"], verbosity)


def _profile_line(p: TProfileInfo) -> str:
    """One line of the profile listing: name, status markers and data access."""
    flags = (
        ("active", p["is_current"]),
        # shown when `WORKSPACE__PROFILE` overrides the recorded profile
        ("recorded", p["is_pinned"] and not p["is_current"]),
        ("default", bool(p.get("default"))),
        ("has settings files", p["is_configured"]),
    )
    markers = "".join(" " + fmt.bold("(%s)" % name) for name, on in flags if on)
    return "%s%s - %s" % (fmt.bold(p["name"]), markers, format_data_access(p["data_access"]))


def _print_environment(env: TEnvironmentInfo) -> None:
    """The environment line of the listing, with its profiles below it."""
    markers = ["default"] if env.get("default") else []
    if env.get("strict_access"):
        markers.append("strict about access")
    suffix = " (%s)" % ", ".join(markers) if markers else ""
    fmt.echo("  environment %s%s:" % (fmt.bold(env["name"]), suffix))
    fmt.echo(
        "    remote workspace %s" % remote_workspace(env["workspace_name"], env.get("workspace_id"))
    )
    for p in env["profile_infos"]:
        fmt.echo("    * %s" % _profile_line(p))


def _print_profile(p: TProfileInfo, settings: Sequence[str]) -> None:
    """The profile line of the listing, with its environment and settings files below it."""
    fmt.echo("  * %s" % _profile_line(p))
    if env := p.get("env"):
        also = (
            ", other profiles of the environment: %s" % ", ".join(p["also_in_env"])
            if p["also_in_env"]
            else ""
        )
        fmt.echo("    environment %s%s" % (fmt.bold(env), also))
    if settings:
        listed = ", ".join(fmt.bold(os.path.relpath(f)) for f in settings)
        fmt.echo("    settings files: %s" % listed)
    else:
        fmt.echo(
            "    settings files: none. A profile without data access reads the shared config.toml"
            " and secrets.toml."
        )


@utils.track_command("profile", track_before=False, operation="list")
def list_profiles(workspace_run_context: WorkspaceRunContext) -> None:
    """Lists the local profiles, then each environment with its profiles."""
    fmt.echo("Profiles declared in %s" % fmt.bold(get_workspace_file(workspace_run_context)))
    local = [p for p in fetch_profiles_list() if p["location"] == "local"]
    fmt.echo()
    fmt.echo("Local profiles:")
    for p in local:
        fmt.echo("  * %s" % _profile_line(p))
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
    parts = ["deployed with %s" % fmt.bold(default_profile(environment, "write"))]
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
    profiles: Mapping[str, TProfileDefinition],
    environments: Mapping[str, TEnvironmentDefinition],
) -> bool:
    """Shows what `doc`, the validated definition, adds to the workspace and the settings files
    to create. Asks for confirmation, then saves `doc` and creates the files."""
    path = os.path.relpath(get_workspace_file(workspace_run_context))
    with_settings = [n for n, p in profiles.items() if needs_settings_files(p)]
    files = [f for n in with_settings for f in profile_settings_files(workspace_run_context, n)]
    fmt.echo("dlt will add to %s:" % fmt.bold(path))
    for name in environments:
        fmt.echo("  * %s" % _describe_environment(doc["environments"][name]))
    for name in profiles:
        # as declared in `doc`, with the default that the declaration gave it
        fmt.echo("  * %s" % _describe_profile(name, profile_definition(doc, name)))
    to_create = [f for f in files if not os.path.exists(f)]
    if to_create:
        fmt.echo("dlt will create:")
        for f in to_create:
            fmt.echo("  * %s" % fmt.bold(os.path.relpath(f)))
    for f in files:
        if f not in to_create:
            fmt.echo("dlt will keep the existing %s" % fmt.bold(os.path.relpath(f)))
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


def _settings_of(workspace_run_context: WorkspaceRunContext, names: Sequence[str]) -> List[str]:
    """Settings files of the declared profiles `names` that need them."""
    return [
        f
        for n in names
        if needs_settings_files(workspace_run_context.get_profile_definition(n))
        for f in profile_settings_files(workspace_run_context, n)
    ]


def _create_missing_settings(
    workspace_run_context: WorkspaceRunContext, names: Sequence[str], missing: Sequence[str]
) -> None:
    """Creates the `missing` settings files of the declared profiles `names` after
    confirmation."""
    fmt.echo("dlt will create the missing settings files:")
    for f in missing:
        fmt.echo("  * %s" % fmt.bold(os.path.relpath(f)))
    if not _confirm():
        return
    create_profile_settings(
        workspace_run_context,
        [n for n in names if needs_settings_files(workspace_run_context.get_profile_definition(n))],
    )
    # `configured` of a profile follows its settings files
    workspace_run_context.reload_workspace_definition()
    _echo_settings_hint()


def _conflicting_options(
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
        conflicting.append(f"--env {env}")
    return conflicting


@utils.track_command("profile", track_before=False, operation="add")
def add_profile(
    workspace_run_context: WorkspaceRunContext,
    profile_name: str,
    location: Optional[TProfileLocation] = None,
    data_access: Optional[TProfileDataAccess] = None,
    env: Optional[str] = None,
) -> None:
    """Declares one profile and creates its settings files. With `location` local, the profile is
    a local one. Otherwise it belongs to `env`, the default environment when none is named, with
    `data_access`, write when none is given. For a declared profile, creates only the settings
    files that are missing.

    Raises:
        ProfileAlreadyExists: The profile is declared and has its settings files, or is declared
            differently than the options say.
    """
    path = get_workspace_file(workspace_run_context)
    if (declared := workspace_run_context.profile_definitions.get(profile_name)) is not None:
        if conflicting := _conflicting_options(declared, location, data_access, env):
            raise ProfileAlreadyExists(profile_name, path, conflicting)
        settings = _settings_of(workspace_run_context, [profile_name])
        if missing := [f for f in settings if not os.path.exists(f)]:
            _create_missing_settings(workspace_run_context, [profile_name], missing)
            return
        info = next(p for p in fetch_profiles_list() if p["name"] == profile_name)
        _print_profile(info, settings)
        raise ProfileAlreadyExists(profile_name, path)
    profile: TProfileDefinition
    if location == "local":
        if env or data_access not in (None, "write"):
            raise ProfileException(
                f"dlt cannot declare local profile `{profile_name}` with --env or with --access"
                " other than write. A local profile has write data access and belongs to no"
                f" environment. Run `dlthub profile add {profile_name} --local` alone."
            )
        env = None
        profile = {"location": "local", "data_access": "write"}
    else:
        environments = workspace_run_context.environment_definitions
        env = env or workspace_run_context.default_environment
        if env is None:
            raise ProfileException(
                f"dlt cannot declare `{profile_name}`: no environment is the default in `{path}`."
                f" Name the environment with `--env`: {', '.join(environments) or 'none declared'}."
            )
        if env not in environments:
            raise EnvironmentNotDeclared(env, path, list(environments))
        profile = {"location": "remote", "data_access": data_access or "write", "env": env}
    # declare_profiles validates before dlt asks, so a refusal never comes after the confirmation
    doc = declare_profiles(workspace_run_context, {profile_name: profile}, env)
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
        profiles = environment_profiles(declared)
        settings = _settings_of(workspace_run_context, profiles)
        if missing := [f for f in settings if not os.path.exists(f)]:
            _create_missing_settings(workspace_run_context, profiles, missing)
            return
        _print_environment(next(e for e in fetch_environments_list() if e["name"] == name))
        raise EnvironmentAlreadyExists(name, get_workspace_file(workspace_run_context))
    environment = new_environment(name)
    doc = declare_environments(workspace_run_context, {name: environment})
    if _declare(workspace_run_context, doc, environment["profiles"], {name: environment}):
        fmt.echo(
            "Environment %s is strict about access and deploys with %s. Interactive jobs"
            " (notebooks, dashboards) need a profile with read data access. Declare it with %s."
            % (
                fmt.bold(name),
                fmt.bold(default_profile(environment, "write")),
                fmt.bold(f"dlthub profile add {name}-read --access read --env {name}"),
            )
        )


@utils.track_command("profile", track_before=False, operation="pin")
def pin_profile(workspace_run_context: WorkspaceRunContext, profile_name: str) -> None:
    """Shows the active profile, or makes `profile_name` the active profile after confirmation."""
    pin_file = os.path.relpath(get_profile_pin_file(workspace_run_context))
    if not profile_name:
        pinned_profile = read_profile_pin(workspace_run_context)
        if pinned_profile:
            fmt.echo(
                "The active profile is %s, recorded in %s. To return to the default profile,"
                " remove this file." % (fmt.bold(pinned_profile), fmt.bold(pin_file))
            )
        else:
            fmt.echo(
                "No active profile is recorded in %s. Local commands run with the default"
                " profile, %s. Make a profile active with `dlthub local profile use <name>`."
                % (fmt.bold(pin_file), fmt.bold(workspace_run_context.default_profile))
            )
        return
    check_profile_declared(profile_name)
    doc = workspace_run_context.workspace_definition
    env = environment_of_profile(doc, profile_name)
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
            % (fmt.bold(env), fmt.bold(default_profile(environment_definition(doc, env), "write")))
        )
    elif default_env := workspace_run_context.default_environment:
        fmt.echo(
            "%s belongs to no environment: commands that deploy or work with a deployment use the"
            " default environment %s." % (fmt.bold(profile_name), fmt.bold(default_env))
        )
    if overriding := os.environ.get(WORKSPACE__PROFILE):
        fmt.warning(
            "WORKSPACE__PROFILE=%s is set in this shell and overrides the active profile. To run"
            " local commands with %s, unset it." % (overriding, profile_name)
        )
