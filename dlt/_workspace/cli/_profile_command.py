import os
from typing import Optional

from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.profile import (
    get_profile_pin_file,
    read_profile_pin,
    save_profile_pin,
)
from dlt._workspace.typing import TCurrentProfileFullInfo
from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.exceptions import CliCommandException
from dlt._workspace.cli._workspace_command import print_profile_section, print_providers
from dlt._workspace.cli.utils import (
    check_delete_local_data,
    delete_local_data,
    fetch_profiles_list,
)


@utils.track_command("profile", track_before=False, operation="info")
def print_profile_info(info: TCurrentProfileFullInfo, verbosity: int = 0) -> None:
    """Renders the active profile: name, paths, pinned status, providers (verbose)."""
    print_profile_section(info, info["configured_profiles"])
    if verbosity > 0 and info["providers"]:
        fmt.echo()
        print_providers(info["providers"], verbosity)


@utils.track_command("profile", track_before=False, operation="list")
def list_profiles(workspace_run_context: WorkspaceRunContext) -> None:
    fmt.echo("Available profiles:")
    for p in fetch_profiles_list():
        markers = []
        if p["is_current"]:
            markers.append(fmt.bold("(current)"))
        if p["is_configured"]:
            markers.append(fmt.bold("(configured)"))
        marker_str = " ".join(markers)
        if marker_str:
            fmt.echo("* %s %s - %s" % (fmt.bold(p["name"]), marker_str, p["description"]))
        else:
            fmt.echo("* %s - %s" % (fmt.bold(p["name"]), p["description"]))


@utils.track_command("profile", track_before=False, operation="pin")
def pin_profile(workspace_run_context: WorkspaceRunContext, profile_name: str) -> None:
    if not profile_name:
        pinned_profile = read_profile_pin(workspace_run_context)
        if pinned_profile:
            pin_file = get_profile_pin_file(workspace_run_context)
            fmt.echo(
                "Currently pinned profile is: %s. To unpin remove %s file."
                % (fmt.bold(pinned_profile), fmt.bold(os.path.relpath(pin_file)))
            )
        else:
            fmt.echo("No pinned profile.")
    else:
        fmt.echo("Will pin the profile %s to current Workspace." % fmt.bold(profile_name))
        save_profile_pin(workspace_run_context, profile_name)


@utils.track_command("profile", track_before=False, operation="clean")
def clean_profile(
    workspace_run_context: WorkspaceRunContext,
    profile_name: Optional[str],
    skip_data_dir: bool,
) -> None:
    """Cleans data/local dirs for the named profile (current if `profile_name` is None)."""
    target = profile_name or workspace_run_context.profile
    if target == workspace_run_context.profile:
        ctx = workspace_run_context
    else:
        # validate the profile exists (configured + built-in/available) before switching
        valid = set(workspace_run_context.configured_profiles()) | set(
            workspace_run_context.available_profiles()
        )
        if target not in valid:
            fmt.error(
                "Profile %r not found in this workspace. Available profiles: %s"
                % (target, ", ".join(sorted(valid)) or "(none)")
            )
            raise CliCommandException()
        ctx = workspace_run_context.switch_profile(target)

    fmt.echo(
        "Local data for profile %s will be removed. Remote destinations are not affected."
        % fmt.bold(target)
    )
    deleted_dirs = check_delete_local_data(ctx, skip_data_dir)
    if deleted_dirs:
        delete_local_data(ctx, deleted_dirs)
