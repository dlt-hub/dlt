import os

from dlt.common.exceptions import DltException
from dlt.common.runtime.exceptions import RunContextNotAvailable


class WorkspaceException(DltException):
    pass


class WorkspaceRunContextNotAvailable(RunContextNotAvailable, WorkspaceException):
    def __init__(self, run_dir: str):
        msg = (
            f"Workspace could not be found in `{run_dir}` (`{os.path.abspath(run_dir)}`). To enable"
            " workspace, create an empty file with the name `.workspace` in `.dlt` settings"
            " folder."
        )
        super().__init__(run_dir, msg)
