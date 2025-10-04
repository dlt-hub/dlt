import os
import tempfile
from typing import Optional

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.pluggable_run_context import (
    ProfilesRunContext,
    PluggableRunContext,
)
from dlt.common.runtime.run_context import (
    is_folder_writable,
)

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

    return switch_context(active().run_dir, profile=profile, required=True)


def switch_context(
    run_dir: Optional[str], profile: str = None, required: bool = True, validate: bool = False
) -> ProfilesRunContext:
    """Switch the run context to a project at `run_dir` with an optional profile.

    Calls `reload` on `PluggableRunContext` to re-trigger the plugin hook
    (`plug_run_context` spec), which will query all active context plugins.

    The `required` argument is passed to each context plugin via the
    `_required` key of `runtime_kwargs` and should cause an exception if a
    given plugin cannot instantiate its context at `run_dir`.

    The `validate` argument is passed to each context plugin via the
    `_validate` key of `runtime_kwargs` and should cause a strict validation
    of any config files and manifests associated with the run context.

    Args:
        run_dir: Filesystem path of the project directory to activate. If None,
            plugins may resolve the directory themselves.
        profile: Profile name to activate for the run context.
        required: If True, plugins should raise if a context cannot be created
            for the provided `run_dir`.
        validate: If True, plugins should perform strict validation of config
            files and manifests associated with the run context.

    Returns:
        SupportsProfilesRunContext: The new run context.
    """
    container = Container()
    # reload run context via plugins
    container[PluggableRunContext].reload(
        run_dir, dict(profile=profile, _required=required, _validate=validate)
    )
    # return new run context
    return container[PluggableRunContext].context  # type: ignore[return-value]
