from typing import List, Optional

from dlt._workspace.deployment.exceptions import DeploymentValidationError
from dlt._workspace.deployment.typing import TJobsDeploymentManifest

PROVIDER = "example"
PROCESSED_WARNING = "dlt-example-plugin processed the manifest"


class ExampleManifestRejected(DeploymentValidationError):
    pass


def process_manifest(manifest: TJobsDeploymentManifest) -> Optional[List[str]]:
    """Runs the jobs of the deployment modules of this plugin on the example provider."""
    module = manifest["deployment_module"]
    # other tests keep the jobs of their deployment modules
    if not module.startswith("dlt_example_plugin."):
        return None
    if module == "dlt_example_plugin.rejected_deployment":
        raise ExampleManifestRejected(f"dlt-example-plugin rejects the jobs of `{module}`")
    for job_def in manifest["jobs"]:
        job_def.setdefault("require", {})["provider"] = PROVIDER
    return [PROCESSED_WARNING]
