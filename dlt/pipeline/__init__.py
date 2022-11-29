from typing import Sequence, cast

from dlt import __version__
from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TWriteDisposition

from dlt.common.typing import TSecretValue, Any
from dlt.common.configuration import with_config
from dlt.common.configuration.container import Container
from dlt.common.configuration.inject import get_orig_args, last_config
from dlt.common.destination import DestinationReference, TDestinationReferenceArg
from dlt.common.pipeline import LoadInfo, PipelineContext, get_default_working_dir

from dlt.pipeline.configuration import PipelineConfiguration, ensure_correct_pipeline_kwargs
from dlt.pipeline.pipeline import Pipeline


@with_config(spec=PipelineConfiguration, auto_namespace=True)
def pipeline(
    pipeline_name: str = None,
    pipelines_dir: str = None,
    pipeline_salt: TSecretValue = None,
    destination: TDestinationReferenceArg = None,
    dataset_name: str = None,
    import_schema_path: str = None,
    export_schema_path: str = None,
    full_refresh: bool = False,
    credentials: Any = None,
    **kwargs: Any
) -> Pipeline:
    ensure_correct_pipeline_kwargs(pipeline, **kwargs)
    # call without arguments returns current pipeline
    orig_args = get_orig_args(**kwargs)  # original (*args, **kwargs)
    # is any of the arguments different from defaults
    has_arguments = bool(orig_args[0]) or any(orig_args[1].values())

    if not has_arguments:
        context = Container()[PipelineContext]
        # if pipeline instance is already active then return it, otherwise create a new one
        if context.is_active():
            return cast(Pipeline, context.pipeline())
        else:
            pass

    # if working_dir not provided use temp folder
    if not pipelines_dir:
        pipelines_dir = get_default_working_dir()

    destination = DestinationReference.from_name(destination or kwargs["destination_name"])
    # create new pipeline instance
    p = Pipeline(
        pipeline_name,
        pipelines_dir,
        pipeline_salt,
        destination,
        dataset_name,
        credentials,
        import_schema_path,
        export_schema_path,
        full_refresh,
        False,
        last_config(**kwargs),
        kwargs["runtime"])
    # set it as current pipeline
    Container()[PipelineContext].activate(p)

    return p


@with_config(spec=PipelineConfiguration, auto_namespace=True)
def attach(
    pipeline_name: str = None,
    pipelines_dir: str = None,
    pipeline_salt: TSecretValue = None,
    full_refresh: bool = False,
    **kwargs: Any
) -> Pipeline:
    ensure_correct_pipeline_kwargs(attach, **kwargs)
    # if working_dir not provided use temp folder
    if not pipelines_dir:
        pipelines_dir = get_default_working_dir()
    # create new pipeline instance
    p = Pipeline(pipeline_name, pipelines_dir, pipeline_salt, None, None, None, None, None, full_refresh, True, last_config(**kwargs), kwargs["runtime"])
    # set it as current pipeline
    Container()[PipelineContext].activate(p)
    return p


# setup default pipeline in the container
Container()[PipelineContext] = PipelineContext(pipeline)


def run(
    data: Any,
    *,
    destination: TDestinationReferenceArg = None,
    dataset_name: str = None,
    credentials: Any = None,
    table_name: str = None,
    write_disposition: TWriteDisposition = None,
    columns: Sequence[TColumnSchema] = None,
    schema: Schema = None
) -> LoadInfo:
    destination = DestinationReference.from_name(destination)
    return pipeline().run(
        data,
        destination=destination,
        dataset_name=dataset_name,
        credentials=credentials,
        table_name=table_name,
        write_disposition=write_disposition,
        columns=columns,
        schema=schema
    )

