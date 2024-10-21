import os
import tempfile
from typing import Any, ClassVar, Dict, List, Optional

from dlt.common import known_env
from dlt.common.configuration import plugins
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.specs.pluggable_run_context import (
    SupportsRunContext,
    PluggableRunContext,
)

# dlt settings folder
DOT_DLT = os.environ.get(known_env.DLT_CONFIG_FOLDER, ".dlt")


class RunContext(SupportsRunContext):
    """A default run context used by dlt"""

    CONTEXT_NAME: ClassVar[str] = "dlt"

    def __init__(self, run_dir: Optional[str]):
        self._init_run_dir = run_dir or "."

    @property
    def global_dir(self) -> str:
        return self.data_dir

    @property
    def run_dir(self) -> str:
        """The default run dir is the current working directory but may be overridden by DLT_PROJECT_DIR env variable."""
        return os.environ.get(known_env.DLT_PROJECT_DIR, self._init_run_dir)

    @property
    def settings_dir(self) -> str:
        """Returns a path to dlt settings directory. If not overridden it resides in current working directory

        The name of the setting folder is '.dlt'. The path is current working directory '.' but may be overridden by DLT_PROJECT_DIR env variable.
        """
        return os.path.join(self.run_dir, DOT_DLT)

    @property
    def data_dir(self) -> str:
        """Gets default directory where pipelines' data (working directories) will be stored
        1. if DLT_DATA_DIR is set in env then it is used
        2. in user home directory: ~/.dlt/
        3. if current user is root: in /var/dlt/
        4. if current user does not have a home directory: in /tmp/dlt/
        """
        if known_env.DLT_DATA_DIR in os.environ:
            return os.environ[known_env.DLT_DATA_DIR]

        # geteuid not available on Windows
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            # we are root so use standard /var
            return os.path.join("/var", "dlt")

        home = os.path.expanduser("~")
        if home is None:
            # no home dir - use temp
            return os.path.join(tempfile.gettempdir(), "dlt")
        else:
            # if home directory is available use ~/.dlt/pipelines
            return os.path.join(home, DOT_DLT)

    def initial_providers(self) -> List[ConfigProvider]:
        providers = [
            EnvironProvider(),
            SecretsTomlProvider(self.settings_dir, self.global_dir),
            ConfigTomlProvider(self.settings_dir, self.global_dir),
        ]
        return providers

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

    @property
    def name(self) -> str:
        return self.__class__.CONTEXT_NAME


@plugins.hookspec(firstresult=True)
def plug_run_context(
    run_dir: Optional[str], runtime_kwargs: Optional[Dict[str, Any]]
) -> SupportsRunContext:
    """Spec for plugin hook that returns current run context.

    Args:
        run_dir (str): An initial run directory of the context
        runtime_kwargs: Any additional arguments passed to the context via PluggableRunContext.reload

    Returns:
        SupportsRunContext: A run context implementing SupportsRunContext protocol
    """


@plugins.hookimpl(specname="plug_run_context")
def plug_run_context_impl(
    run_dir: Optional[str], runtime_kwargs: Optional[Dict[str, Any]]
) -> SupportsRunContext:
    return RunContext(run_dir)


def current() -> SupportsRunContext:
    """Returns currently active run context"""
    return Container()[PluggableRunContext].context
