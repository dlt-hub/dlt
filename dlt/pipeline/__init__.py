from typing import Sequence, Union, cast
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TWriteDisposition

from dlt.common.typing import TSecretValue, Any
from dlt.common.configuration import with_config
from dlt.common.configuration.container import Container
from dlt.common.destination import DestinationReference
from dlt.common.pipeline import PipelineContext, get_default_working_dir

from dlt.pipeline.configuration import PipelineConfiguration
from dlt.pipeline.pipeline import Pipeline
from dlt.extract.decorators import source, resource


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
            return cast(Pipeline, context.pipeline())

    # if working_dir not provided use temp folder
    if not working_dir:
        working_dir = get_default_working_dir()

    destination = DestinationReference.from_name(destination)
    # create new pipeline instance
    p = Pipeline(pipeline_name, working_dir, pipeline_secret, destination, dataset_name, import_schema_path, export_schema_path, always_drop_pipeline, False, kwargs["_runtime"])
    # set it as current pipeline
    Container()[PipelineContext].activate(p)

    return p


def restore(
    pipeline_name: str = None,
    working_dir: str = None,
    pipeline_secret: TSecretValue = None
) -> Pipeline:

    _pipeline_name = pipeline_name
    _working_dir = working_dir

    @with_config(spec=PipelineConfiguration, auto_namespace=True)
    def _restore(
        pipeline_name: str,
        working_dir: str,
        pipeline_secret: TSecretValue,
        always_drop_pipeline: bool = False,
        **kwargs: Any
    ) -> Pipeline:
        # use the outer pipeline name and working dir to override those from config in order to restore the requested state
        pipeline_name = _pipeline_name or pipeline_name
        working_dir = _working_dir or working_dir

        # if working_dir not provided use temp folder
        if not working_dir:
            working_dir = get_default_working_dir()
        # create new pipeline instance
        p = Pipeline(pipeline_name, working_dir, pipeline_secret, None, None, None, None, always_drop_pipeline, True, kwargs["_runtime"])
        # set it as current pipeline
        Container()[PipelineContext].activate(p)
        return p

    return _restore(pipeline_name, working_dir, pipeline_secret)


# setup default pipeline in the container
Container()[PipelineContext] = PipelineContext(pipeline)


def run(
    source: Any,
    destination: Union[None, str, DestinationReference] = None,
    dataset_name: str = None,
    table_name: str = None,
    write_disposition: TWriteDisposition = None,
    columns: Sequence[TColumnSchema] = None,
    schema: Schema = None
) -> None:
    destination = DestinationReference.from_name(destination)
    return pipeline().run(source, destination, dataset_name, table_name, write_disposition, columns, schema)
