from typing import List
from dlt.common.typing import TypedDict

# current version of deployment engine
DEPLOYMENT_ENGINE_VERSION = 1


class TDeploymentFileItem(TypedDict, total=False):
    """TypedDict representing a file in the deployment package"""

    relative_path: str
    size_in_bytes: int


class TDeploymentManifest(TypedDict, total=False):
    """TypedDict defining the deployment manifest structure"""

    engine_version: int
    files: List[TDeploymentFileItem]
