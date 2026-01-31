import os

from dlt._workspace._workspace_context import WorkspaceRunContext, active
from dlt._workspace.profile import (
    BUILT_IN_PROFILES,
    get_profile_pin_file,
    read_profile_pin,
    save_profile_pin,
)
from dlt._workspace.cli import SupportsCliCommand, echo as fmt, utils


@utils.track_command("profile", track_before=False, operation="info")
def print_profile_info(workspace_run_context: WorkspaceRunContext) -> None:
    fmt.echo("Current profile: %s" % fmt.bold(workspace_run_context.profile))
    if pinned_profile := read_profile_pin(workspace_run_context):
        fmt.echo("Pinned profile: %s" % fmt.bold(pinned_profile))


@utils.track_command("profile", track_before=False, operation="list")
def list_profiles(workspace_run_context: WorkspaceRunContext) -> None:
    fmt.echo("Available profiles:")
    current_profile = workspace_run_context.profile
    configured_profiles = workspace_run_context.configured_profiles()
    for profile in workspace_run_context.available_profiles():
        desc = BUILT_IN_PROFILES.get(profile, "Pinned custom profile")
        markers = []
        if profile == current_profile:
            markers.append(fmt.bold("(current)"))
        if profile in configured_profiles:
            markers.append(fmt.bold("(configured)"))
        marker_str = " ".join(markers)
        if marker_str:
            fmt.echo("* %s %s - %s" % (fmt.bold(profile), marker_str, desc))
        else:
            fmt.echo("* %s - %s" % (fmt.bold(profile), desc))


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
