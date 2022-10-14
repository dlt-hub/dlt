# from experiments.pipeline.pipeline import Pipeline

# pipeline = Pipeline()

# def __getattr__(name):
#     if name == 'y':
#         return 3
#     raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
import tempfile

from dlt.common.typing import TSecretValue, Any
from dlt.common.configuration import with_config

from experiments.pipeline.configuration import PipelineConfiguration
from experiments.pipeline.pipeline import Pipeline


# @overload
# def configure(self,
#     pipeline_name: str = None,
#     working_dir: str = None,
#     pipeline_secret: TSecretValue = None,
#     drop_existing_data: bool = False,
#     import_schema_path: str = None,
#     export_schema_path: str = None,
#     destination_name: str = None,
#     log_level: str = "INFO"
# ) -> None:
#     ...


@with_config(spec=PipelineConfiguration, auto_namespace=True)
def configure(pipeline_name: str = None, working_dir: str = None, pipeline_secret: TSecretValue = None, **kwargs: Any) -> Pipeline:
    # if working_dir not provided use temp folder
    if not working_dir:
        working_dir = tempfile.gettempdir()
    return Pipeline(pipeline_name, working_dir, pipeline_secret, kwargs["runtime"])

def run() -> Pipeline:
    return configure().extract()