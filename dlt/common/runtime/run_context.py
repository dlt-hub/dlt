from contextlib import contextmanager, suppress
import os
import tempfile
import warnings
from types import ModuleType
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlencode

from packaging.specifiers import SpecifierSet

from dlt.common import known_env
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.configuration.specs.pluggable_run_context import (
    RunContextBase,
    PluggableRunContext,
)
from dlt.common.configuration.specs.runtime_configuration import RuntimeConfiguration
from dlt.common.runtime.init import initialize_runtime

# dlt settings folder
DOT_DLT = os.environ.get(known_env.DLT_CONFIG_FOLDER, ".dlt")


class RunContext(RunContextBase):
    """A default run context used by dlt"""

    def __init__(self, run_dir: Optional[str]):
        self._init_run_dir = run_dir or "."
        self._runtime_config: RuntimeConfiguration = None

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

    def initialize_runtime(self, runtime_config: RuntimeConfiguration = None) -> None:
        if runtime_config is None:
            self._runtime_config = resolve_configuration(RuntimeConfiguration())
        else:
            self._runtime_config = runtime_config

        initialize_runtime(self.name, self._runtime_config)

    @property
    def runtime_config(self) -> RuntimeConfiguration:
        if self._runtime_config is None:
            self.initialize_runtime()
        return self._runtime_config

    @property
    def config(self) -> BaseConfiguration:
        return None

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

    def reset_config(self) -> None:
        self._runtime_config = None

    @property
    def name(self) -> str:
        return "dlt"


def switch_context(
    run_dir: Optional[str], profile: str = None, required: str = None, validate: bool = False
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
        run_dir (str): Filesystem path of the project directory to activate. If None,
            plugins may resolve the directory themselves.
        profile (str): Profile name to activate for the run context.
        required (str, optional): A class name of the context be instantiated at `run_dir` ie. setting
            it to `WorkspaceRunContext` will cause the workspace context plugin to raise if workspace is
            not found at `run_dir`.
        validate (str, optional): If True, plugins should perform strict validation of config
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

    plugin_modules = Container()[PluginContext].plugin_modules.copy()
    with suppress(ValueError):
        plugin_modules.remove(run_module_name)
    plugin_modules.insert(0, run_module_name)
    return plugin_modules


def ensure_plugin_version_match(
    pkg_name: str,
    dlt_version: str,
    plugin_version: str,
    plugin_module_name: str,
    dlt_extra: str,
    dlt_version_specifier: Optional[SpecifierSet] = None,
) -> None:
    """Ensures that installed plugin version matches dlt requirements. Plugins are tightly bound
    to `dlt` and released together.

    If `dlt_version_specifier` is provided, it is used to check if the plugin version satisfies
    the specifier. Otherwise, the specifier is read from dlt's package metadata (Requires-Dist).
    If specifier cannot be determined, the function returns without checking.

    Args:
        pkg_name: Name of the plugin package (e.g., "dlthub")
        dlt_version: The installed dlt version string
        plugin_version: The installed plugin version string
        plugin_module_name: The module name for MissingDependencyException (e.g., "dlthub")
        dlt_extra: The dlt extra to install the plugin (e.g., "hub")
        dlt_version_specifier: Optional version specifier for the plugin. If not provided,
            reads from dlt's package metadata.

    Raises:
        MissingDependencyException: If version mismatch is detected
    """
    # Get specifier from dlt's package metadata if not provided
    if dlt_version_specifier is None:
        from dlt.version import get_dependency_requirement

        req = get_dependency_requirement(pkg_name)
        if req is not None:
            dlt_version_specifier = req.specifier

    # If specifier still not available, exit without checking
    if dlt_version_specifier is None or len(dlt_version_specifier) == 0:
        return

    # Use specifier.contains() for proper version check (allowing prereleases)
    if not dlt_version_specifier.contains(plugin_version, prereleases=True):
        from dlt.common.exceptions import MissingDependencyException

        custom_msg = (
            f"`{pkg_name}` is a `dlt` plugin and must satisfy version requirement "
            f"`{dlt_version_specifier}` but you have {plugin_version}. "
            f"Please install the right version of {pkg_name} with:\n\n"
            f'pip install "dlt[{dlt_extra}]=={dlt_version}"\n\n'
            "or if you are upgrading the plugin:\n\n"
            f'pip install "dlt[{dlt_extra}]=={dlt_version}" -U {pkg_name}'
        )
        missing_dep_ex = MissingDependencyException(plugin_module_name, [])
        missing_dep_ex.args = (custom_msg,)
        missing_dep_ex.msg = custom_msg
        raise missing_dep_ex


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
