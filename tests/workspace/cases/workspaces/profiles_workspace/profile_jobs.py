"""Jobs that report the profile and the profile configuration they run with."""

from typing import Any, Dict

import dlt
from dlt.hub.run import job

module_pipeline = dlt.pipeline("profile_module", destination="filesystem")


def _describe(pipeline: dlt.Pipeline) -> Dict[str, Any]:
    with pipeline.destination_client() as client:
        bucket_url = client.config.bucket_url  # type: ignore[attr-defined]
    return {
        "profile": dlt.current.run_context().profile,  # type: ignore[attr-defined]
        "pipelines_dir": pipeline.pipelines_dir,
        "bucket_url": bucket_url,
    }


@job
def module_pipeline_probe():
    """Describes the pipeline created at module import."""
    return _describe(module_pipeline)


@job
def job_pipeline_probe():
    """Describes a pipeline created in the job body."""
    return _describe(dlt.pipeline("profile_job", destination="filesystem"))


@job
def config_probe(days: int = dlt.config.value):
    """Returns a job input that each profile configures."""
    return {"profile": dlt.current.run_context().profile, "days": days}  # type: ignore[attr-defined]
