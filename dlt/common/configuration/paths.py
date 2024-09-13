import os
import tempfile

from dlt.common import known_env


# dlt settings folder
DOT_DLT = os.environ.get(known_env.DLT_CONFIG_FOLDER, ".dlt")


def get_dlt_project_dir() -> str:
    """The dlt project dir is the current working directory but may be overridden by DLT_PROJECT_DIR env variable."""
    return os.environ.get(known_env.DLT_PROJECT_DIR, ".")


def get_dlt_settings_dir() -> str:
    """Returns a path to dlt settings directory. If not overridden it resides in current working directory

    The name of the setting folder is '.dlt'. The path is current working directory '.' but may be overridden by DLT_PROJECT_DIR env variable.
    """
    return os.path.join(get_dlt_project_dir(), DOT_DLT)


def make_dlt_settings_path(path: str) -> str:
    """Returns path to file in dlt settings folder."""
    return os.path.join(get_dlt_settings_dir(), path)


def get_dlt_data_dir() -> str:
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

    home = _get_user_home_dir()
    if home is None:
        # no home dir - use temp
        return os.path.join(tempfile.gettempdir(), "dlt")
    else:
        # if home directory is available use ~/.dlt/pipelines
        return os.path.join(home, DOT_DLT)


def _get_user_home_dir() -> str:
    return os.path.expanduser("~")
