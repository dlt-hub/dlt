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


class RuntimeNotAuthenticated(RuntimeException):
    pass


class RuntimeOperationNotAuthorized(WorkspaceException, RuntimeException):
    pass


class WorkspaceIdMismatch(RuntimeOperationNotAuthorized):
    def __init__(self, local_workspace_id: str, remote_workspace_id: str):
        self.local_workspace_id = local_workspace_id
        self.remote_workspace_id = remote_workspace_id
        super().__init__(local_workspace_id, remote_workspace_id)


class LocalWorkspaceIdNotSet(RuntimeOperationNotAuthorized):
    def __init__(self, remote_workspace_id: str):
        self.remote_workspace_id = remote_workspace_id
        super().__init__(remote_workspace_id)
