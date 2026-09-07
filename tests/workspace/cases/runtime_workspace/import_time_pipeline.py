"""Module-level pipeline created at import time for auto refresh tests."""

import dlt
from dlt.hub.run import job

module_pipeline = dlt.pipeline("import_time_refresh_probe")


@job
def report_module_pipeline_refresh():
    """Reports refresh mode of the pipeline created at module import."""
    return f"refresh={module_pipeline.refresh}"
