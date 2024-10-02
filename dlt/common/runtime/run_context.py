import os
import tempfile
from typing import ClassVar

from dlt.common import known_env
from dlt.common.configuration import plugins
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.run_context import SupportsRunContext, PluggableRunContext

# dlt settings folder
DOT_DLT = os.environ.get(known_env.DLT_CONFIG_FOLDER, ".dlt")


class RunContext(SupportsRunContext):
    """A default run context used by dlt"""

    CONTEXT_NAME: ClassVar[str] = "dlt"

    @property
    def global_dir(self) -> str:
        return self.data_dir

    @property
    def run_dir(self) -> str:
        """The default run dir is the current working directory but may be overridden by DLT_PROJECT_DIR env variable."""
        return os.environ.get(known_env.DLT_PROJECT_DIR, ".")

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

    @property
    def name(self) -> str:
        return self.__class__.CONTEXT_NAME


@plugins.hookspec(firstresult=True)
def plug_run_context() -> SupportsRunContext:
    """Spec for plugin hook that returns current run context."""


@plugins.hookimpl(specname="plug_run_context")
def plug_run_context_impl() -> SupportsRunContext:
    return RunContext()


def current() -> SupportsRunContext:
    """Returns currently active run context"""
    return Container()[PluggableRunContext].context
