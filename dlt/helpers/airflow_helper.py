import functools
import os
from tempfile import gettempdir
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from tenacity import (
    retry_if_exception,
    wait_exponential,
    stop_after_attempt,
    Retrying,
    RetryCallState,
)

from dlt.common.known_env import DLT_DATA_DIR, DLT_PROJECT_DIR
from dlt.common.exceptions import MissingDependencyException

try:
    from airflow.configuration import conf
    from airflow.models import TaskInstance
    from airflow.utils.task_group import TaskGroup
    from airflow.operators.dummy import DummyOperator  # type: ignore
    from airflow.operators.python import PythonOperator, get_current_context
except ModuleNotFoundError:
    raise MissingDependencyException("Airflow", ["apache-airflow>=2.5"])


import dlt
from dlt.common import logger
from dlt.common.pendulum import pendulum
from dlt.common.runtime.telemetry import with_telemetry

from dlt.common.destination import TLoaderFileFormat
from dlt.common.schema.typing import TWriteDispositionConfig, TSchemaContract

from dlt.common.utils import uniq_id
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.runtime.collector import NULL_COLLECTOR

from dlt.extract import DltSource
from dlt.pipeline.helpers import retry_load
from dlt.pipeline.pipeline import Pipeline
from dlt.pipeline.progress import log
from dlt.pipeline.typing import TPipelineStep


DEFAULT_RETRY_NO_RETRY = Retrying(stop=stop_after_attempt(1), reraise=True)
DEFAULT_RETRY_BACKOFF = Retrying(
    stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=4, max=10), reraise=True
)


class PipelineTasksGroup(TaskGroup):
    """
    Represents a dlt Airflow pipeline task group.
    """

    def __init__(
        self,
        pipeline_name: str,
        use_data_folder: bool = False,
        local_data_folder: str = None,
        use_task_logger: bool = True,
        log_progress_period: float = 30.0,
        buffer_max_items: int = 1000,
        retry_policy: Retrying = DEFAULT_RETRY_NO_RETRY,
        retry_pipeline_steps: Sequence[TPipelineStep] = ("load",),
        fail_task_if_any_job_failed: bool = True,
        abort_task_if_any_job_failed: bool = False,
        wipe_local_data: bool = True,
        save_load_info: bool = False,
        save_trace_info: bool = False,
        **kwargs: Any,
    ) -> None:
        """Creates a task group to which you can add pipeline runs

        The run environment is prepared as follows
        - the .dlt folder (the project folder) is searched under `dags` as configured by Airflow
        - the data folder where pipelines are stored is always unique

        The `data_folder` is available in certain Airflow deployments. In case of Composer, it is a location on the gcs bucket. `use_data_folder` is disabled and should be
        enabled only when needed. The operations on bucket are non-atomic and way slower than on local storage and should be avoided.

        `fail_task_if_any_job_failed` will raise an exception if any of the loading jobs failed permanently and thus fail the current Airflow task.
        This happens **after all dlt loading jobs executed**. See more here: https://dlthub.com/docs/running-in-production/running#failed-jobs

        `abort_task_if_any_job_failed` will abort the other dlt loading jobs and fail the Airflow task in any of the jobs failed. This may put your warehouse in
        inconsistent state so the option is disabled by default.

        The load info and trace info can be optionally saved to the destination. See https://dlthub.com/docs/running-in-production/running#inspect-and-save-the-load-info-and-trace

        Args:
            pipeline_name (str): Name of the task group
            use_data_folder (bool, optional): If well defined 'data' folder is present it will be used. Currently only data folder on Composer is supported. Defaults to False.
            local_data_folder (str, optional): Path to a local folder on worker machine to where to store data. Used if local_data_folder is False or there's not well defined data folder. Defaults to gettempdir.
            use_task_logger (bool, optional): Will redirect dlt logger into task logger. Defaults to True.
            log_progress_period (float, optional): If progress is not configured for a pipeline, the `log` progress is used with a given period. Set 0 to disable. Defaults to 30.0.
            buffer_max_items (int, optional): Maximum number of buffered items. Use 0 to keep dlt built-in limit. Defaults to 1000.
            retry_policy (_type_, optional): Tenacity retry policy. Defaults to no retry.
            retry_pipeline_steps (Sequence[TPipelineStep], optional): Which pipeline steps are eligible for retry. Defaults to ("load", ).
            fail_task_if_any_job_failed (bool, optional): Will fail a task if any of the dlt load jobs failed. Defaults to True.
            wipe_local_data (bool, optional): Will wipe all the data created by pipeline, also in case of exception. Defaults to False.
            save_load_info (bool, optional): Will save extensive load info to the destination. Defaults to False.
            save_trace_info (bool, optional): Will save trace info to the destination. Defaults to False.
        """

        super().__init__(group_id=pipeline_name, **kwargs)
        self._used_names: Dict[str, Any] = {}
        self.use_task_logger = use_task_logger
        self.log_progress_period = log_progress_period
        self.buffer_max_items = buffer_max_items
        self.retry_policy = retry_policy
        self.retry_pipeline_steps = retry_pipeline_steps
        self.fail_task_if_any_job_failed = fail_task_if_any_job_failed
        self.abort_task_if_any_job_failed = abort_task_if_any_job_failed
        self.wipe_local_data = wipe_local_data
        self.save_load_info = save_load_info
        self.save_trace_info = save_trace_info

        # reload providers so config.toml in dags folder is included
        dags_folder = conf.get("core", "dags_folder")

        # set the dlt project folder to dags
        os.environ[DLT_PROJECT_DIR] = dags_folder

        # check if /data mount is available
        if use_data_folder and os.path.exists("/home/airflow/gcs/data"):
            data_dir = os.path.join("/home/airflow/gcs/data", f"dlt_{uniq_id(8)}")
        else:
            # create random path
            data_dir = os.path.join(local_data_folder or gettempdir(), f"dlt_{uniq_id(8)}")
        os.environ[DLT_DATA_DIR] = data_dir

        # delete existing config providers in container, they will get reloaded on next use
        if ConfigProvidersContext in Container():
            del Container()[ConfigProvidersContext]

    def _task_name(self, pipeline: Pipeline, data: Any) -> str:
        """Generate a task name.

        Args:
            pipeline (Pipeline): The pipeline to run.
            data (Any): The data to run the pipeline with.

        Returns:
            str: The name of the task.
        """
        task_name = pipeline.pipeline_name

        if isinstance(data, DltSource):
            resource_names = list(data.selected_resources.keys())
            task_name = data.name + "_" + "-".join(resource_names[:4])

            if len(resource_names) > 4:
                task_name += f"-{len(resource_names)-4}-more"

            num = self._used_names.setdefault(task_name, 0)
            self._used_names[task_name] = num + 1

            if num:
                task_name += f"-{num + 1}"

        return task_name

    def run(
        self,
        pipeline: Pipeline,
        data: Any,
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        loader_file_format: TLoaderFileFormat = None,
        schema_contract: TSchemaContract = None,
        pipeline_name: str = None,
        on_before_run: Callable[[], None] = None,
        **kwargs: Any,
    ) -> PythonOperator:
        """
        Create a task to run the given pipeline with the
        given data in Airflow.

        Args:
            pipeline (Pipeline): The pipeline to run
            data (Any):
                The data to run the pipeline with. If a non-resource
                callable given, it's evaluated during the DAG execution,
                right before the actual pipeline run.
                NOTE: If `on_before_run` is provided, first `on_before_run`
                      is evaluated, and then callable `data`.
            table_name (str, optional): The name of the table to
                which the data should be loaded within the `dataset`.
            write_disposition (TWriteDispositionConfig, optional): Same as
                in `run` command.
            loader_file_format (TLoaderFileFormat, optional):
                The file format the loader will use to create the
                load package.
            schema_contract (TSchemaContract, optional): On override
                for the schema contract settings, this will replace
                the schema contract settings for all tables in the schema.
            pipeline_name (str, optional): The name of the derived pipeline.
            on_before_run (Callable, optional): A callable to be
                executed right before the actual pipeline run.

        Returns:
            PythonOperator: Airflow task instance.
        """
        f = functools.partial(
            self._run,
            pipeline,
            data,
            table_name=table_name,
            write_disposition=write_disposition,
            loader_file_format=loader_file_format,
            schema_contract=schema_contract,
            pipeline_name=pipeline_name,
            on_before_run=on_before_run,
        )
        return PythonOperator(task_id=self._task_name(pipeline, data), python_callable=f, **kwargs)

    def _run(
        self,
        pipeline: Pipeline,
        data: Any,
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        loader_file_format: TLoaderFileFormat = None,
        schema_contract: TSchemaContract = None,
        pipeline_name: str = None,
        on_before_run: Callable[[], None] = None,
    ) -> None:
        """Run the given pipeline with the given data.

        Args:
            pipeline (Pipeline): The pipeline to run
            data (Any):
                The data to run the pipeline with. If a non-resource
                callable given, it's evaluated during the DAG execution,
                right before the actual pipeline run.
                NOTE: If `on_before_run` is provided, first `on_before_run`
                      is evaluated, and then callable `data`.
            table_name (str, optional): The name of the
                table to which the data should be loaded
                within the `dataset`.
            write_disposition (TWriteDispositionConfig, optional):
                Same as in `run` command.
            loader_file_format (TLoaderFileFormat, optional):
                The file format the loader will use to create
                the load package.
            schema_contract (TSchemaContract, optional): On
                override for the schema contract settings,
                this will replace the schema contract settings
                for all tables in the schema.
            pipeline_name (str, optional): The name of the
                derived pipeline.
            on_before_run (Callable, optional): A callable
                to be executed right before the actual pipeline run.
        """
        # activate pipeline
        pipeline.activate()
        # drop local data
        task_pipeline = pipeline.drop(pipeline_name=pipeline_name)

        # use task logger
        if self.use_task_logger:
            ti: TaskInstance = get_current_context()["ti"]  # type: ignore
            logger.LOGGER = ti.log

        # set global number of buffered items
        if dlt.config.get("data_writer.buffer_max_items") is None and self.buffer_max_items > 0:
            dlt.config["data_writer.buffer_max_items"] = self.buffer_max_items
            logger.info(f"Set data_writer.buffer_max_items to {self.buffer_max_items}")

        # enable abort package if job failed
        if self.abort_task_if_any_job_failed:
            dlt.config["load.raise_on_failed_jobs"] = True
            logger.info("Set load.abort_task_if_any_job_failed to True")

        if self.log_progress_period > 0 and task_pipeline.collector == NULL_COLLECTOR:
            task_pipeline.collector = log(log_period=self.log_progress_period, logger=logger.LOGGER)
            logger.info(f"Enabled log progress with period {self.log_progress_period}")

        logger.info(f"Pipeline data in {task_pipeline.working_dir}")

        def log_after_attempt(retry_state: RetryCallState) -> None:
            if not retry_state.retry_object.stop(retry_state):
                logger.error(
                    "Retrying pipeline run due to exception: %s",
                    retry_state.outcome.exception(),
                )

        try:
            if on_before_run is not None:
                on_before_run()

            if callable(data):
                data = data()

            # retry with given policy on selected pipeline steps
            for attempt in self.retry_policy.copy(
                retry=retry_if_exception(
                    retry_load(retry_on_pipeline_steps=self.retry_pipeline_steps)
                ),
                after=log_after_attempt,
            ):
                with attempt:
                    logger.info(
                        "Running the pipeline, attempt=%s" % attempt.retry_state.attempt_number
                    )
                    load_info = task_pipeline.run(
                        data,
                        table_name=table_name,
                        write_disposition=write_disposition,
                        loader_file_format=loader_file_format,
                        schema_contract=schema_contract,
                    )
                    logger.info(str(load_info))
                    # save load and trace
                    if self.save_load_info:
                        logger.info("Saving the load info in the destination")
                        task_pipeline.run(
                            [load_info],
                            table_name="_load_info",
                            loader_file_format=loader_file_format,
                        )
                    if self.save_trace_info:
                        logger.info("Saving the trace in the destination")
                        task_pipeline.run(
                            [task_pipeline.last_trace],
                            table_name="_trace",
                            loader_file_format=loader_file_format,
                        )
                    # raise on failed jobs if requested
                    if self.fail_task_if_any_job_failed:
                        load_info.raise_on_failed_jobs()
        finally:
            # always completely wipe out pipeline folder, in case of success and failure
            if self.wipe_local_data:
                logger.info(f"Removing folder {pipeline.working_dir}")
                task_pipeline._wipe_working_folder()

    @with_telemetry("helper", "airflow_add_run", False, "decompose")
    def add_run(
        self,
        pipeline: Pipeline,
        data: Any,
        *,
        decompose: Literal["none", "serialize", "parallel", "parallel-isolated"] = "none",
        table_name: str = None,
        write_disposition: TWriteDispositionConfig = None,
        loader_file_format: TLoaderFileFormat = None,
        schema_contract: TSchemaContract = None,
        on_before_run: Callable[[], None] = None,
        **kwargs: Any,
    ) -> List[PythonOperator]:
        """Creates a task or a group of tasks to run `data` with `pipeline`

        Creates an Airflow task that extracts, normalizes and loads `data` with the passed pipeline instance `pipeline`. If `data` is a source
        and `decompose` is `serialize` it will decompose the source into disjoint connected components (isolated group of resources) and execute them
        one after another as separate Airflow tasks. The decomposition makes sure that each resource or transformer is extracted only once. It preserves
        the order of resources declared in the source when creating graph of tasks.

        The `kwargs` are passed as arguments to all Airflow task instances created.

        Args:
            pipeline (Pipeline): An instance of pipeline used to run the source
            data (Any):
                Any data supported by `run` method of the pipeline.
                If a non-resource callable given, it's called before
                the load to get the data.
            decompose (Literal["none", "serialize", "parallel"], optional):
                A source decomposition strategy into Airflow tasks:
                    none - no decomposition, default value.
                    serialize - decompose the source into a sequence of Airflow tasks.
                    parallel - decompose the source into a parallel Airflow task group,
                               except the first resource must be completed first.
                               All tasks that are run in parallel share the same pipeline state.
                               If two of them modify the state, part of state may be lost
                    parallel-isolated - decompose the source into a parallel Airflow task group.
                               with the same exception as above. All task have separate pipeline
                               state (via separate pipeline name) but share the same dataset,
                               schemas and tables.
                NOTE: The first component of the source in both parallel models is done first,
                      after that the rest are executed in parallel to each other.
                NOTE: In case the SequentialExecutor is used by Airflow, the tasks
                      will remain sequential despite 'parallel' or 'parallel-isolated' mode.
                      Use another executor (e.g. CeleryExecutor) to make tasks parallel!

                Parallel tasks are executed in different pipelines, all derived from the original
                one, but with the state isolated from each other.
            table_name: (str): The name of the table to which the data should be loaded within the `dataset`
            write_disposition (TWriteDispositionConfig, optional): Same as in `run` command. Defaults to None.
            loader_file_format (Literal["jsonl", "insert_values", "parquet"], optional): The file format the loader will use to create the load package.
                Not all file_formats are compatible with all destinations. Defaults to the preferred file format of the selected destination.
            schema_contract (TSchemaContract, optional): On override for the schema contract settings,
                this will replace the schema contract settings for all tables in the schema. Defaults to None.
            on_before_run (Callable, optional):
                A callable to be executed right before the actual pipeline run.

        Returns:
            Any: Airflow tasks created in order of creation.
        """

        # make sure that pipeline was created after dag was initialized
        if not pipeline.pipelines_dir.startswith(os.environ[DLT_DATA_DIR]):
            raise ValueError(
                "Please create your Pipeline instance after AirflowTasks are created. The dlt"
                " pipelines directory is not set correctly."
            )

        with self:
            # use factory function to make a task, in order to parametrize it
            # passing arguments to task function (_run) is serializing
            # them and running template engine on them
            def make_task(pipeline: Pipeline, data: Any, name: str = None) -> PythonOperator:
                f = functools.partial(
                    self._run,
                    pipeline,
                    data,
                    table_name=table_name,
                    write_disposition=write_disposition,
                    loader_file_format=loader_file_format,
                    schema_contract=schema_contract,
                    pipeline_name=name,
                    on_before_run=on_before_run,
                )
                return PythonOperator(
                    task_id=self._task_name(pipeline, data), python_callable=f, **kwargs
                )

            if decompose == "none":
                # run pipeline as single task
                return [make_task(pipeline, data)]
            elif decompose == "serialize":
                if not isinstance(data, DltSource):
                    raise ValueError("Can only decompose dlt sources")
                if pipeline.dev_mode:
                    raise ValueError("Cannot decompose pipelines with dev_mode set")
                # serialize tasks
                tasks = []
                pt = None
                for source in data.decompose("scc"):
                    nt = make_task(pipeline, source)
                    tasks.append(nt)
                    if pt is not None:
                        pt >> nt
                    pt = nt
                return tasks
            elif decompose == "parallel":
                if not isinstance(data, DltSource):
                    raise ValueError("Can only decompose dlt sources")

                if pipeline.dev_mode:
                    raise ValueError("Cannot decompose pipelines with dev_mode set")

                tasks = []
                sources = data.decompose("scc")
                t_name = self._task_name(pipeline, data)
                start = make_task(pipeline, sources[0])

                # parallel tasks
                for source in sources[1:]:
                    for resource in source.resources.values():
                        if resource.incremental:
                            logger.warn(
                                f"The resource {resource.name} in task {t_name} "
                                "is using incremental loading and may modify the "
                                "state. Resources that modify the state should not "
                                "run in parallel within the single pipeline as the "
                                "state will not be correctly merged. Please use "
                                "'serialize' or 'parallel-isolated' modes instead."
                            )
                            break

                    tasks.append(make_task(pipeline, source))

                end = DummyOperator(task_id=f"{t_name}_end")

                if tasks:
                    start >> tasks >> end
                    return [start] + tasks + [end]

                start >> end
                return [start, end]
            elif decompose == "parallel-isolated":
                if not isinstance(data, DltSource):
                    raise ValueError("Can only decompose dlt sources")

                if pipeline.dev_mode:
                    raise ValueError("Cannot decompose pipelines with dev_mode set")

                # parallel tasks
                tasks = []
                naming = SnakeCaseNamingConvention()
                sources = data.decompose("scc")
                start = make_task(
                    pipeline,
                    sources[0],
                    naming.normalize_identifier(self._task_name(pipeline, sources[0])),
                )

                # parallel tasks
                for source in sources[1:]:
                    # name pipeline the same as task
                    new_pipeline_name = naming.normalize_identifier(
                        self._task_name(pipeline, source)
                    )
                    tasks.append(make_task(pipeline, source, new_pipeline_name))

                t_name = self._task_name(pipeline, data)
                end = DummyOperator(task_id=f"{t_name}_end")

                if tasks:
                    start >> tasks >> end
                    return [start] + tasks + [end]

                start >> end
                return [start, end]
            else:
                raise ValueError(
                    "decompose value must be one of ['none', 'serialize', 'parallel',"
                    " 'parallel-isolated']"
                )


def airflow_get_execution_dates() -> Tuple[pendulum.DateTime, Optional[pendulum.DateTime]]:
    # prefer logging to task logger
    try:
        from airflow.operators.python import get_current_context  # noqa

        context = get_current_context()
        return context["data_interval_start"], context["data_interval_end"]
    except Exception:
        return None, None
