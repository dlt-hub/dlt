import tempfile
from typing import Union
from importlib import import_module

from dlt.common.typing import TSecretValue, Any
from dlt.common.configuration import with_config
from dlt.load.client_base import DestinationReference

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
def configure(pipeline_name: str = None, working_dir: str = None, pipeline_secret: TSecretValue = None, destination: Union[None, str, DestinationReference] = None, **kwargs: Any) -> Pipeline:
    print(locals())
    print(kwargs["_last_dlt_config"].pipeline_name)
    # if working_dir not provided use temp folder
    if not working_dir:
        working_dir = tempfile.gettempdir()
    # if destination is a str, get destination reference by dynamically importing module from known location
    if isinstance(destination, str):
        destination = import_module(f"dlt.load.{destination}")

    return Pipeline(pipeline_name, working_dir, pipeline_secret, destination, kwargs["runtime"])

def run() -> Pipeline:
    return configure().extract()