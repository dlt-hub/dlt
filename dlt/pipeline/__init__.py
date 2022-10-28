from typing import Union

from dlt.common.typing import TSecretValue, Any
from dlt.common.configuration import with_config
from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationReference, resolve_destination_reference
from dlt.common.pipeline import PipelineContext, get_default_working_dir

from experiments.pipeline.configuration import PipelineConfiguration
from experiments.pipeline.pipeline import Pipeline
from experiments.pipeline.decorators import source, resource


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
def pipeline(
    pipeline_name: str = None,
    working_dir: str = None,
    pipeline_secret: TSecretValue = None,
    destination: Union[None, str, DestinationReference] = None,
    dataset_name: str = None,
    import_schema_path: str = None,
    export_schema_path: str = None,
    always_drop_pipeline: bool = False,
    **kwargs: Any
) -> Pipeline:
    # call without parameters returns current pipeline
    if not locals():
        context = Container()[PipelineContext]
        # if pipeline instance is already active then return it, otherwise create a new one
        if context.is_activated():
            return context.pipeline()

    print(kwargs["_last_dlt_config"].pipeline_name)
    # if working_dir not provided use temp folder
    if not working_dir:
        working_dir = get_default_working_dir()
    destination = resolve_destination_reference(destination)
    # create new pipeline instance
    p = Pipeline(pipeline_name, working_dir, pipeline_secret, destination, dataset_name, import_schema_path, export_schema_path, always_drop_pipeline, kwargs["runtime"])
    # set it as current pipeline
    Container()[PipelineContext].activate(p)

    return p

# setup default pipeline in the container
print("CONTEXT")
Container()[PipelineContext] = PipelineContext(pipeline)


def run(source: Any, destination: Union[None, str, DestinationReference] = None) -> Pipeline:
    destination = resolve_destination_reference(destination)
    return pipeline().run(source=source, destination=destination)
