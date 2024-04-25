from typing import Sequence, Type, cast, overload
from typing_extensions import TypeVar

from dlt.common.schema import Schema
from dlt.common.schema.typing import TColumnSchema, TWriteDispositionConfig, TSchemaContract

from dlt.common.typing import TSecretValue, Any
from dlt.common.configuration import with_config
from dlt.common.configuration.container import Container
from dlt.common.configuration.inject import get_orig_args, last_config
from dlt.common.destination import TLoaderFileFormat, Destination, TDestinationReferenceArg
from dlt.common.pipeline import LoadInfo, PipelineContext, get_dlt_pipelines_dir

from dlt.pipeline.configuration import PipelineConfiguration, ensure_correct_pipeline_kwargs
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.progress import _from_name as collector_from_name, TCollectorArg, _NULL_COLLECTOR
from dlt.pipeline.warnings import credentials_argument_deprecated

TPipeline = TypeVar("TPipeline", bound=Pipeline, default=Pipeline)


@overload
def pipeline(
    pipeline_name: str = None,
    pipelines_dir: str = None,
    pipeline_salt: TSecretValue = None,
    destination: TDestinationReferenceArg = None,
    staging: TDestinationReferenceArg = None,
    dataset_name: str = None,
    import_schema_path: str = None,
    export_schema_path: str = None,
    full_refresh: bool = False,
    credentials: Any = None,
    progress: TCollectorArg = _NULL_COLLECTOR,
    _impl_cls: Type[TPipeline] = Pipeline,  # type: ignore[assignment]
) -> TPipeline:
    """Creates a new instance of `dlt` pipeline, which moves the data from the source ie. a REST API to a destination ie. database or a data lake.

    #### Note:
    The `pipeline` functions allows you to pass the destination name to which the data should be loaded, the name of the dataset and several other options that govern loading of the data.
    The created `Pipeline` object lets you load the data from any source with `run` method or to have more granular control over the loading process with `extract`, `normalize` and `load` methods.

    Please refer to the following doc pages
    - Write your first pipeline walkthrough: https://dlthub.com/docs/walkthroughs/create-a-pipeline
    - Pipeline architecture and data loading steps: https://dlthub.com/docs/reference
    - List of supported destinations: https://dlthub.com/docs/dlt-ecosystem/destinations

    #### Args:
        pipeline_name (str, optional): A name of the pipeline that will be used to identify it in monitoring events and to restore its state and data schemas on subsequent runs.
        Defaults to the file name of pipeline script with `dlt_` prefix added.

        pipelines_dir (str, optional): A working directory in which pipeline state and temporary files will be stored. Defaults to user home directory: `~/dlt/pipelines/`.

        pipeline_salt (TSecretValue, optional): A random value used for deterministic hashing during data anonymization. Defaults to a value derived from the pipeline name.
        Default value should not be used for any cryptographic purposes.

        destination (str | DestinationReference, optional): A name of the destination to which dlt will load the data, or a destination module imported from `dlt.destination`.
        May also be provided to `run` method of the `pipeline`.

        staging (str | DestinationReference, optional): A name of the destination where dlt will stage the data before final loading, or a destination module imported from `dlt.destination`.
        May also be provided to `run` method of the `pipeline`.

        dataset_name (str, optional): A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. `schema` in relational databases or folder grouping many files.
        May also be provided later to the `run` or `load` methods of the `Pipeline`. If not provided at all then defaults to the `pipeline_name`

        import_schema_path (str, optional): A path from which the schema `yaml` file will be imported on each pipeline run. Defaults to None which disables importing.

        export_schema_path (str, optional): A path where the schema `yaml` file will be exported after every schema change. Defaults to None which disables exporting.

        full_refresh (bool, optional): When set to True, each instance of the pipeline with the `pipeline_name` starts from scratch when run and loads the data to a separate dataset.
        The datasets are identified by `dataset_name_` + datetime suffix. Use this setting whenever you experiment with your data to be sure you start fresh on each run. Defaults to False.

        credentials (Any, optional): Credentials for the `destination` ie. database connection string or a dictionary with google cloud credentials.
        In most cases should be set to None, which lets `dlt` to use `secrets.toml` or environment variables to infer right credentials values.

        progress(str, Collector): A progress monitor that shows progress bars, console or log messages with current information on sources, resources, data items etc. processed in
        `extract`, `normalize` and `load` stage. Pass a string with a collector name or configure your own by choosing from `dlt.progress` module.
        We support most of the progress libraries: try passing `tqdm`, `enlighten` or `alive_progress` or `log` to write to console/log.

    #### Returns:
        Pipeline: An instance of `Pipeline` class with. Please check the documentation of `run` method for information on what to do with it.
    """


@overload
def pipeline() -> Pipeline:  # type: ignore
    """When called without any arguments, returns the recently created `Pipeline` instance.
    If not found, it creates a new instance with all the pipeline options set to defaults."""


@with_config(spec=PipelineConfiguration, auto_pipeline_section=True)
def pipeline(
    pipeline_name: str = None,
    pipelines_dir: str = None,
    pipeline_salt: TSecretValue = None,
    destination: TDestinationReferenceArg = None,
    staging: TDestinationReferenceArg = None,
    dataset_name: str = None,
    import_schema_path: str = None,
    export_schema_path: str = None,
    full_refresh: bool = False,
    credentials: Any = None,
    progress: TCollectorArg = _NULL_COLLECTOR,
    _impl_cls: Type[TPipeline] = Pipeline,  # type: ignore[assignment]
    **kwargs: Any,
) -> TPipeline:
    ensure_correct_pipeline_kwargs(pipeline, **kwargs)
    # call without arguments returns current pipeline
    orig_args = get_orig_args(**kwargs)  # original (*args, **kwargs)
    # is any of the arguments different from defaults
    has_arguments = bool(orig_args[0]) or any(orig_args[1].values())

    credentials_argument_deprecated("pipeline", credentials, destination)

    if not has_arguments:
        context = Container()[PipelineContext]
        # if pipeline instance is already active then return it, otherwise create a new one
        if context.is_active():
            return cast(TPipeline, context.pipeline())
        else:
            pass

    # if working_dir not provided use temp folder
    if not pipelines_dir:
        pipelines_dir = get_dlt_pipelines_dir()

    destination = Destination.from_reference(
        destination or kwargs["destination_type"], destination_name=kwargs["destination_name"]
    )
    staging = Destination.from_reference(
        staging or kwargs.get("staging_type", None),
        destination_name=kwargs.get("staging_name", None),
    )

    progress = collector_from_name(progress)
    # create new pipeline instance
    p = _impl_cls(
        pipeline_name,
        pipelines_dir,
        pipeline_salt,
        destination,
        staging,
        dataset_name,
        credentials,
        import_schema_path,
        export_schema_path,
        full_refresh,
        progress,
        False,
        last_config(**kwargs),
        kwargs["runtime"],
    )
    # set it as current pipeline
    p.activate()
    return p


@with_config(spec=PipelineConfiguration, auto_pipeline_section=True)
def attach(
    pipeline_name: str = None,
    pipelines_dir: str = None,
    pipeline_salt: TSecretValue = None,
    full_refresh: bool = False,
    credentials: Any = None,
    progress: TCollectorArg = _NULL_COLLECTOR,
    **kwargs: Any,
) -> Pipeline:
    """Attaches to the working folder of `pipeline_name` in `pipelines_dir` or in default directory. Requires that valid pipeline state exists in working folder."""
    ensure_correct_pipeline_kwargs(attach, **kwargs)
    # if working_dir not provided use temp folder
    if not pipelines_dir:
        pipelines_dir = get_dlt_pipelines_dir()
    progress = collector_from_name(progress)
    # create new pipeline instance
    p = Pipeline(
        pipeline_name,
        pipelines_dir,
        pipeline_salt,
        None,
        None,
        None,
        credentials,
        None,
        None,
        full_refresh,
        progress,
        True,
        last_config(**kwargs),
        kwargs["runtime"],
    )
    # set it as current pipeline
    p.activate()
    return p


def run(
    data: Any,
    *,
    destination: TDestinationReferenceArg = None,
    staging: TDestinationReferenceArg = None,
    dataset_name: str = None,
    credentials: Any = None,
    table_name: str = None,
    write_disposition: TWriteDispositionConfig = None,
    columns: Sequence[TColumnSchema] = None,
    schema: Schema = None,
    loader_file_format: TLoaderFileFormat = None,
    schema_contract: TSchemaContract = None,
) -> LoadInfo:
    """Loads the data in `data` argument into the destination specified in `destination` and dataset specified in `dataset_name`.

    #### Note:
    This method will `extract` the data from the `data` argument, infer the schema, `normalize` the data into a load package (ie. jsonl or PARQUET files representing tables) and then `load` such packages into the `destination`.

    The data may be supplied in several forms:
    * a `list` or `Iterable` of any JSON-serializable objects ie. `dlt.run([1, 2, 3], table_name="numbers")`
    * any `Iterator` or a function that yield (`Generator`) ie. `dlt.run(range(1, 10), table_name="range")`
    * a function or a list of functions decorated with @dlt.resource ie. `dlt.run([chess_players(title="GM"), chess_games()])`
    * a function or a list of functions decorated with @dlt.source.

    Please note that `dlt` deals with `bytes`, `datetime`, `decimal` and `uuid` objects so you are free to load binary data or documents containing dates.

    #### Execution:
    The `run` method will first use `sync_destination` method to synchronize pipeline state and schemas with the destination. You can disable this behavior with `restore_from_destination` configuration option.
    Next it will make sure that data from the previous is fully processed. If not, `run` method normalizes and loads pending data items.
    Only then the new data from `data` argument is extracted, normalized and loaded.

    #### Args:
        data (Any): Data to be loaded to destination

        destination (str | DestinationReference, optional): A name of the destination to which dlt will load the data, or a destination module imported from `dlt.destination`.
        If not provided, the value passed to `dlt.pipeline` will be used.

        dataset_name (str, optional):A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. `schema` in relational databases or folder grouping many files.
        If not provided, the value passed to `dlt.pipeline` will be used. If not provided at all then defaults to the `pipeline_name`

        credentials (Any, optional): Credentials for the `destination` ie. database connection string or a dictionary with google cloud credentials.
        In most cases should be set to None, which lets `dlt` to use `secrets.toml` or environment variables to infer right credentials values.

        table_name (str, optional): The name of the table to which the data should be loaded within the `dataset`. This argument is required for a `data` that is a list/Iterable or Iterator without `__name__` attribute.
        The behavior of this argument depends on the type of the `data`:
        * generator functions: the function name is used as table name, `table_name` overrides this default
        * `@dlt.resource`: resource contains the full table schema and that includes the table name. `table_name` will override this property. Use with care!
        * `@dlt.source`: source contains several resources each with a table schema. `table_name` will override all table names within the source and load the data into single table.

        write_disposition (TWriteDispositionConfig, optional): Controls how to write data to a table. Accepts a shorthand string literal or configuration dictionary.
        Allowed shorthand string literals: `append` will always add new data at the end of the table. `replace` will replace existing data with new data. `skip` will prevent data from loading. "merge" will deduplicate and merge data based on "primary_key" and "merge_key" hints. Defaults to "append".
        Write behaviour can be further customized through a configuration dictionary. For example, to obtain an SCD2 table provide `write_disposition={"disposition": "merge", "strategy": "scd2"}`.
        Please note that in case of `dlt.resource` the table schema value will be overwritten and in case of `dlt.source`, the values in all resources will be overwritten.

        columns (Sequence[TColumnSchema], optional): A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.

        schema (Schema, optional): An explicit `Schema` object in which all table schemas will be grouped. By default `dlt` takes the schema from the source (if passed in `data` argument) or creates a default one itself.

    Raises:
        PipelineStepFailed when a problem happened during `extract`, `normalize` or `load` steps.
    Returns:
        LoadInfo: Information on loaded data including the list of package ids and failed job statuses. Please not that `dlt` will not raise if a single job terminally fails. Such information is provided via LoadInfo.
    """
    destination = Destination.from_reference(destination, credentials=credentials)
    return pipeline().run(
        data,
        destination=destination,
        staging=staging,
        dataset_name=dataset_name,
        credentials=credentials,
        table_name=table_name,
        write_disposition=write_disposition,
        columns=columns,
        schema=schema,
        loader_file_format=loader_file_format,
        schema_contract=schema_contract,
    )


# plug default tracking module
from dlt.pipeline import trace, track, platform

trace.TRACKING_MODULES = [track, platform]

# setup default pipeline in the container
Container()[PipelineContext] = PipelineContext(pipeline)
