from contextlib import contextmanager
import os
import tempfile
import warnings
from types import ModuleType
from typing import Any, ClassVar, Dict, Iterator, List, Optional
from urllib.parse import urlencode

from dlt.common import known_env
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs.pluggable_run_context import (
    RunContextBase,
    PluggableRunContext,
)

# dlt settings folder
DOT_DLT = os.environ.get(known_env.DLT_CONFIG_FOLDER, ".dlt")


class RunContext(RunContextBase):
    """A default run context used by dlt"""

    def __init__(self, run_dir: Optional[str]):
        self._init_run_dir = run_dir or "."

    @property
    def global_dir(self) -> str:
        return global_dir()

    @property
    def uri(self) -> str:
        return context_uri(self.name, self.run_dir, self.runtime_kwargs)

    @property
    def run_dir(self) -> str:
        """The default run dir is the current working directory but may be overridden by DLT_PROJECT_DIR env variable."""
        return os.environ.get(known_env.DLT_PROJECT_DIR, self._init_run_dir)

    @property
    def local_dir(self) -> str:
        return os.environ.get(known_env.DLT_LOCAL_DIR, self._init_run_dir)

    @property
    def settings_dir(self) -> str:
        """Returns a path to dlt settings directory. If not overridden it resides in current working directory

        The name of the setting folder is '.dlt'. The path is current working directory '.' but may be overridden by DLT_PROJECT_DIR env variable.
        """
        return os.path.join(self.run_dir, DOT_DLT)

    @property
    def data_dir(self) -> str:
        return os.environ.get(known_env.DLT_DATA_DIR, global_dir())

    def initial_providers(self) -> List[ConfigProvider]:
        providers = [
            EnvironProvider(),
            SecretsTomlProvider(self.settings_dir, self.global_dir),
            ConfigTomlProvider(self.settings_dir, self.global_dir),
        ]
        return providers

    @property
    def module(self) -> Optional[ModuleType]:
        try:
            return self.import_run_dir_module(self.run_dir)
        except (ImportError, TypeError):
            return None

    @property
    def runtime_kwargs(self) -> Dict[str, Any]:
        return None

    def get_data_entity(self, entity: str) -> str:
        return os.path.join(self.data_dir, entity)

    def get_run_entity(self, entity: str) -> str:
        """Default run context assumes that entities are defined in root dir"""
        return self.run_dir

    def get_setting(self, setting_path: str) -> str:
        return os.path.join(self.settings_dir, setting_path)

    def plug(self) -> None:
        pass

    def unplug(self) -> None:
        pass

    @property
    def name(self) -> str:
        return "dlt"


def switch_context(
    run_dir: Optional[str], profile: str = None, required: bool = True, validate: bool = False
) -> RunContextBase:
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
    return container[PluggableRunContext].context


@contextmanager
def switched_run_context(new_context: RunContext) -> Iterator[RunContext]:
    """Context manager that switches run context to `new_context` into pluggable run context."""
    container = Container()
    cookie = container[PluggableRunContext].push_context()
    try:
        container[PluggableRunContext].reload(new_context)
        yield new_context
    finally:
        container[PluggableRunContext].pop_context(cookie)


def global_dir() -> str:
    """Gets default directory where pipelines' data (working directories) will be stored
    1. if XDG_DATA_HOME is set in env then it is used
    2. in user home directory: ~/.dlt/
    3. if current user is root: in /var/dlt/
    4. if current user does not have a home directory: in /tmp/dlt/
    """
    # geteuid not available on Windows
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        # we are root so use standard /var
        return os.path.join("/var", "dlt")

    home = os.path.expanduser("~")
    if home is None or not is_folder_writable(home):
        # no home dir - use temp
        return os.path.join(tempfile.gettempdir(), "dlt")
    else:
        # use XDG_DATA_HOME only if ~/.dlt doesn't exist
        if "XDG_DATA_HOME" in os.environ:
            if not os.path.isdir(os.path.join(home, DOT_DLT)):
                return os.path.join(os.environ["XDG_DATA_HOME"], "dlt")
            else:
                warnings.warn(
                    f"XDG_DATA_HOME is set to {os.environ['XDG_DATA_HOME']} but ~/.dlt already"
                    " exists. Using ~/.dlt"
                )

        # else default to ~/.dlt
        return os.path.join(home, DOT_DLT)


def is_folder_writable(path: str) -> bool:
    import tempfile

    try:
        # Ensure the path exists
        if not os.path.exists(path):
            return False
        # Attempt to create a temporary file
        with tempfile.TemporaryFile(dir=path):
            pass
        return True
    except OSError:
        return False


def get_plugin_modules() -> List[str]:
    """Return top level module names of all discovered plugins, including `dlt`.

    If current run context is a top level module it is also included, otherwise empty string.
    """
    from dlt.common.configuration.plugins import PluginContext

    # get current run module
    ctx_module = active().module
    run_module_name = ctx_module.__name__ if ctx_module else ""

    return [run_module_name] + [p for p in Container()[PluginContext].plugin_modules]


def context_uri(name: str, run_dir: str, runtime_kwargs: Optional[Dict[str, Any]]) -> str:
    from dlt.common.storages.configuration import FilesystemConfiguration

    uri_no_qs = FilesystemConfiguration.make_file_url(run_dir)
    # add query string from self.runtime_kwargs
    if runtime_kwargs:
        # skip kwargs starting with _
        query_string = urlencode({k: v for k, v in runtime_kwargs.items() if not k.startswith("_")})
        if query_string:
            return f"{uri_no_qs}?{query_string}"
    return uri_no_qs


def active() -> RunContextBase:
    """Returns currently active run context"""
    return Container()[PluggableRunContext].context
