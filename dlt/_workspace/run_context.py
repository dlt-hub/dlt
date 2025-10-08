import os
import tempfile

from dlt.common.configuration.specs.pluggable_run_context import (
    ProfilesRunContext,
)
from dlt.common.runtime.run_context import is_folder_writable, switch_context

DEFAULT_WORKSPACE_WORKING_FOLDER = "_data"
DEFAULT_LOCAL_FOLDER = "_local"


def default_working_dir(run_dir: str, name: str, profile: str, default_folder: str) -> str:
    """Computes default data dir which is relative to `run_dir` and separated by `profile_name`
    If `project_dir` is not writable, we fall back to temp dir.
    """
    data_dir = os.path.join(run_dir, default_folder)
    if not is_folder_writable(run_dir):
        # fallback to temp dir which should be writable, project name is used to separate projects
        data_dir = os.path.join(tempfile.gettempdir(), "dlt", default_folder, name)
    return os.path.join(data_dir, profile)


def default_name(run_dir: str) -> str:
    run_dir = os.path.abspath(run_dir)
    name = os.path.basename(run_dir.rstrip(os.path.sep)) or "_dlt"
    return name


def switch_profile(profile: str) -> ProfilesRunContext:
    """Switch the active profile and reload the workspace run context.

    This is a convenience wrapper around `switch_context` that uses the
    `run_dir` of the current `active()` context.

    Args:
        profile: Name of the profile to activate.

    Returns:
        SupportsProfilesRunContext: The refreshed run context bound to the given profile.

    """
    from dlt.common.runtime.run_context import active

    return switch_context(active().run_dir, profile=profile, required=True)  # type: ignore[return-value]
