import os
import tempfile

DOT_DLT = ".dlt"


def get_dlt_project_dir() -> str:
    """Returns the dlt project folder. The path is always relative to the current working directory."""
    return os.path.join(".", DOT_DLT)


def make_dlt_project_path(path: str) -> str:
    """Returns path to file in dlt project folder. The path is always relative to the current working directory"""
    return os.path.join(DOT_DLT, path)


def get_dlt_home_dir() -> str:
    """ Gets default directory where pipelines' data will be stored
        1. in user home directory ~/.dlt/
        2. if current user is root in /var/dlt/
        3. if current user does not have a home directory in /tmp/dlt/
    """
    # getuid not available on Windows
    if hasattr(os, "getuid") and os.geteuid() == 0:
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
