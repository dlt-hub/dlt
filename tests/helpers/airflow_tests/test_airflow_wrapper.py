import os
import pytest
from unittest import mock
from typing import List
from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator, get_current_context
from airflow.models import TaskInstance
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

import dlt
from dlt.common import logger, pendulum
from dlt.common.utils import uniq_id
from dlt.common.normalizers.naming.snake_case import NamingConvention as SnakeCaseNamingConvention

from dlt.helpers.airflow_helper import PipelineTasksGroup, DEFAULT_RETRY_BACKOFF
from dlt.pipeline.exceptions import CannotRestorePipelineException, PipelineStepFailed

from tests.pipeline.utils import load_table_counts
from tests.utils import TEST_STORAGE_ROOT


DEFAULT_DATE = pendulum.datetime(2023, 4, 18, tz="Europe/Berlin")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "max_active_runs": 1,
}


@dlt.source
def mock_data_source():
    @dlt.resource(selected=True)
    def _r_init():
        yield ["-", "x", "!"]

    @dlt.resource(selected=False)
    def _r1():
        yield ["a", "b", "c"]

    @dlt.transformer(data_from=_r1, selected=True)
    def _t1(items, suffix):
        yield list(map(lambda i: i + "_" + suffix, items))

    @dlt.transformer(data_from=_r1)
    def _t2(items, mul):
        yield items * mul

    @dlt.transformer(data_from=_r1)
    def _t3(items, mul):
        for item in items:
            yield item.upper() * mul

    # add something to init
    @dlt.transformer(data_from=_r_init)
    def _t_init_post(items):
        for item in items:
            yield item * 2

    @dlt.resource
    def _r_isolee():
        yield from ["AX", "CV", "ED"]

    return _r_init, _t_init_post, _r1, _t1("POST"), _t2(3), _t3(2), _r_isolee


@dlt.source
def mock_data_single_resource():
    @dlt.resource(selected=True)
    def resource():
        yield ["-", "x", "!"]

    return resource


@dlt.source
def mock_data_incremental_source():
    @dlt.resource
    def resource1(a: str = None, b=None, c=None):
        yield ["s", "a"]

    @dlt.resource
    def resource2(
        updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
            "updated_at", initial_value="1970-01-01T00:00:00Z"
        )
    ):
        yield [{"updated_at": "1970-02-01T00:00:00Z"}]

    return resource1, resource2


@dlt.source(section="mock_data_source_state")
def mock_data_source_state():
    @dlt.resource(selected=True)
    def _r_init():
        dlt.current.source_state()["counter"] = 1
        dlt.current.source_state()["end_counter"] = 1
        yield ["-", "x", "!"]

    @dlt.resource(selected=False)
    def _r1():
        dlt.current.source_state()["counter"] += 1
        dlt.current.resource_state()["counter"] = 1
        yield from ["a", "b", "c"]

    @dlt.transformer(data_from=_r1, selected=True)
    def _t1(items, suffix):
        dlt.current.source_state()["counter"] += 1
        dlt.current.resource_state("_r1")["counter"] += 1
        dlt.current.resource_state()["counter"] = 1
        yield list(map(lambda i: i + "_" + suffix, items))

    @dlt.transformer(data_from=_r1)
    def _t2(items, mul):
        dlt.current.source_state()["counter"] += 1
        dlt.current.resource_state("_r1")["counter"] += 1
        dlt.current.resource_state()["counter"] = 1
        yield items * mul

    @dlt.transformer(data_from=_r1)
    def _t3(items, mul):
        dlt.current.source_state()["counter"] += 1
        dlt.current.resource_state("_r1")["counter"] += 1
        dlt.current.resource_state()["counter"] = 1
        for item in items:
            yield item.upper() * mul

    # add something to init
    @dlt.transformer(data_from=_r_init)
    def _t_init_post(items):
        for item in items:
            yield item * 2

    @dlt.resource
    def _r_isolee():
        dlt.current.source_state()["end_counter"] += 1
        yield from ["AX", "CV", "ED"]

    return _r_init, _t_init_post, _r1, _t1("POST"), _t2(3), _t3(2), _r_isolee


def test_regular_run() -> None:
    # run the pipeline normally
    pipeline_standalone = dlt.pipeline(
        pipeline_name="pipeline_standalone",
        dataset_name="mock_data_" + uniq_id(),
        destination=dlt.destinations.duckdb(credentials=":pipeline:"),
    )
    pipeline_standalone.run(mock_data_source())
    pipeline_standalone_counts = load_table_counts(
        pipeline_standalone, *[t["name"] for t in pipeline_standalone.default_schema.data_tables()]
    )

    tasks_list: List[PythonOperator] = None

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_regular():
        nonlocal tasks_list
        tasks = PipelineTasksGroup(
            "pipeline_dag_regular", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name="pipeline_dag_regular",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=":pipeline:"),
        )
        tasks_list = tasks.add_run(
            pipeline_dag_regular,
            mock_data_source(),
            decompose="none",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )

    dag_def: DAG = dag_regular()
    assert len(tasks_list) == 1
    # composite task name
    assert (
        tasks_list[0].task_id
        == "pipeline_dag_regular.mock_data_source__r_init-_t_init_post-_t1-_t2-2-more"
    )

    dag_def.test()
    # we should be able to attach to pipeline state created within Airflow

    pipeline_dag_regular = dlt.attach(pipeline_name="pipeline_dag_regular")
    pipeline_dag_regular_counts = load_table_counts(
        pipeline_dag_regular,
        *[t["name"] for t in pipeline_dag_regular.default_schema.data_tables()],
    )
    # same data should be loaded
    assert pipeline_dag_regular_counts == pipeline_standalone_counts

    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "pipeline_dag_decomposed.duckdb")

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_decomposed():
        nonlocal tasks_list
        tasks = PipelineTasksGroup(
            "pipeline_dag_decomposed", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        # set duckdb to be outside of pipeline folder which is dropped on each task
        pipeline_dag_decomposed = dlt.pipeline(
            pipeline_name="pipeline_dag_decomposed",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=quackdb_path),
        )
        tasks_list = tasks.add_run(
            pipeline_dag_decomposed,
            mock_data_source(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )

    dag_def = dag_decomposed()
    assert len(tasks_list) == 3
    # task one by one
    assert tasks_list[0].task_id == "pipeline_dag_decomposed.mock_data_source__r_init-_t_init_post"
    assert tasks_list[1].task_id == "pipeline_dag_decomposed.mock_data_source__t1-_t2-_t3"
    assert tasks_list[2].task_id == "pipeline_dag_decomposed.mock_data_source__r_isolee"
    dag_def.test()
    pipeline_dag_decomposed = dlt.attach(pipeline_name="pipeline_dag_decomposed")
    pipeline_dag_decomposed_counts = load_table_counts(
        pipeline_dag_decomposed,
        *[t["name"] for t in pipeline_dag_decomposed.default_schema.data_tables()],
    )
    assert pipeline_dag_decomposed_counts == pipeline_standalone_counts


def test_run() -> None:
    task: PythonOperator = None

    pipeline_standalone = dlt.pipeline(
        pipeline_name="pipeline_standalone",
        dataset_name="mock_data_" + uniq_id(),
        destination=dlt.destinations.duckdb(credentials=":pipeline:"),
    )
    pipeline_standalone.run(mock_data_source())
    pipeline_standalone_counts = load_table_counts(
        pipeline_standalone, *[t["name"] for t in pipeline_standalone.default_schema.data_tables()]
    )

    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "pipeline_dag_regular.duckdb")

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_regular():
        nonlocal task
        tasks = PipelineTasksGroup(
            "pipeline_dag_regular", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        # set duckdb to be outside of pipeline folder which is dropped on each task
        pipeline_dag_regular = dlt.pipeline(
            pipeline_name="pipeline_dag_regular",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=quackdb_path),
        )
        task = tasks.run(pipeline_dag_regular, mock_data_source())

    dag_def: DAG = dag_regular()
    assert task.task_id == "mock_data_source__r_init-_t_init_post-_t1-_t2-2-more"

    dag_def.test()

    pipeline_dag_regular = dlt.attach(pipeline_name="pipeline_dag_regular")
    pipeline_dag_regular_counts = load_table_counts(
        pipeline_dag_regular,
        *[t["name"] for t in pipeline_dag_regular.default_schema.data_tables()],
    )
    assert pipeline_dag_regular_counts == pipeline_standalone_counts

    assert isinstance(task, PythonOperator)


def test_parallel_run():
    pipeline_standalone = dlt.pipeline(
        pipeline_name="pipeline_parallel",
        dataset_name="mock_data_" + uniq_id(),
        destination=dlt.destinations.duckdb(credentials=":pipeline:"),
    )
    pipeline_standalone.run(mock_data_source())
    pipeline_standalone_counts = load_table_counts(
        pipeline_standalone, *[t["name"] for t in pipeline_standalone.default_schema.data_tables()]
    )

    tasks_list: List[PythonOperator] = None

    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "pipeline_dag_parallel.duckdb")

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_parallel():
        nonlocal tasks_list
        tasks = PipelineTasksGroup(
            "pipeline_dag_parallel", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        # set duckdb to be outside of pipeline folder which is dropped on each task
        pipeline_dag_parallel = dlt.pipeline(
            pipeline_name="pipeline_dag_parallel",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=quackdb_path),
        )
        tasks_list = tasks.add_run(
            pipeline_dag_parallel,
            mock_data_source(),
            decompose="parallel",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )

    dag_def = dag_parallel()
    assert len(tasks_list) == 4
    dag_def.test()

    pipeline_dag_parallel = dlt.attach(pipeline_name="pipeline_dag_parallel")
    results = load_table_counts(
        pipeline_dag_parallel,
        *[t["name"] for t in pipeline_dag_parallel.default_schema.data_tables()],
    )

    assert results == pipeline_standalone_counts

    # verify tasks 1-2 in between tasks 0 and 3
    for task in dag_def.tasks[1:3]:
        assert task.downstream_task_ids == set([dag_def.tasks[-1].task_id])
        assert task.upstream_task_ids == set([dag_def.tasks[0].task_id])


def test_parallel_incremental():
    pipeline_standalone = dlt.pipeline(
        pipeline_name="pipeline_parallel",
        dataset_name="mock_data_" + uniq_id(),
        destination=dlt.destinations.duckdb(credentials=":pipeline:"),
    )
    pipeline_standalone.run(mock_data_incremental_source())

    tasks_list: List[PythonOperator] = None

    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "pipeline_dag_parallel.duckdb")

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_parallel():
        nonlocal tasks_list
        tasks = PipelineTasksGroup(
            "pipeline_dag_parallel", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        # set duckdb to be outside of pipeline folder which is dropped on each task
        pipeline_dag_parallel = dlt.pipeline(
            pipeline_name="pipeline_dag_parallel",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=quackdb_path),
        )
        tasks.add_run(
            pipeline_dag_parallel,
            mock_data_incremental_source(),
            decompose="parallel",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )

    with mock.patch("dlt.helpers.airflow_helper.logger.warn") as warn_mock:
        dag_def = dag_parallel()
        dag_def.test()
        warn_mock.assert_has_calls(
            [
                mock.call(
                    "The resource resource2 in task"
                    " mock_data_incremental_source_resource1-resource2 is using incremental loading"
                    " and may modify the state. Resources that modify the state should not run in"
                    " parallel within the single pipeline as the state will not be correctly"
                    " merged. Please use 'serialize' or 'parallel-isolated' modes instead."
                )
            ]
        )


def test_parallel_isolated_run():
    pipeline_standalone = dlt.pipeline(
        pipeline_name="pipeline_parallel",
        dataset_name="mock_data_" + uniq_id(),
        destination=dlt.destinations.duckdb(credentials=":pipeline:"),
    )
    pipeline_standalone.run(mock_data_source())
    pipeline_standalone_counts = load_table_counts(
        pipeline_standalone, *[t["name"] for t in pipeline_standalone.default_schema.data_tables()]
    )

    tasks_list: List[PythonOperator] = None

    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "pipeline_dag_parallel.duckdb")

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_parallel():
        nonlocal tasks_list
        tasks = PipelineTasksGroup(
            "pipeline_dag_parallel", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        # set duckdb to be outside of pipeline folder which is dropped on each task
        pipeline_dag_parallel = dlt.pipeline(
            pipeline_name="pipeline_dag_parallel",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=quackdb_path),
        )
        tasks_list = tasks.add_run(
            pipeline_dag_parallel,
            mock_data_source(),
            decompose="parallel-isolated",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )

    dag_def = dag_parallel()
    assert len(tasks_list) == 4
    dag_def.test()

    results = {}
    snake_case = SnakeCaseNamingConvention()
    for i in range(0, 3):
        pipeline_dag_parallel = dlt.attach(
            pipeline_name=snake_case.normalize_identifier(
                dag_def.tasks[i].task_id.replace("pipeline_dag_parallel.", "")[:-2]
            )
        )
        pipeline_dag_decomposed_counts = load_table_counts(
            pipeline_dag_parallel,
            *[t["name"] for t in pipeline_dag_parallel.default_schema.data_tables()],
        )
        results.update(pipeline_dag_decomposed_counts)

    assert results == pipeline_standalone_counts

    # verify tasks 1-2 in between tasks 0 and 3
    for task in dag_def.tasks[1:3]:
        assert task.downstream_task_ids == set([dag_def.tasks[-1].task_id])
        assert task.upstream_task_ids == set([dag_def.tasks[0].task_id])


def test_parallel_run_single_resource():
    pipeline_standalone = dlt.pipeline(
        pipeline_name="pipeline_parallel",
        dataset_name="mock_data_" + uniq_id(),
        destination=dlt.destinations.duckdb(credentials=":pipeline:"),
    )
    pipeline_standalone.run(mock_data_single_resource())
    pipeline_standalone_counts = load_table_counts(
        pipeline_standalone, *[t["name"] for t in pipeline_standalone.default_schema.data_tables()]
    )

    tasks_list: List[PythonOperator] = None

    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "pipeline_dag_parallel.duckdb")

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_parallel():
        nonlocal tasks_list
        tasks = PipelineTasksGroup(
            "pipeline_dag_parallel", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        # set duckdb to be outside of pipeline folder which is dropped on each task
        pipeline_dag_parallel = dlt.pipeline(
            pipeline_name="pipeline_dag_parallel",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=quackdb_path),
        )
        tasks_list = tasks.add_run(
            pipeline_dag_parallel,
            mock_data_single_resource(),
            decompose="parallel",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )

    dag_def = dag_parallel()
    assert len(tasks_list) == 2
    dag_def.test()
    pipeline_dag_parallel = dlt.attach(pipeline_name="pipeline_dag_parallel")
    pipeline_dag_decomposed_counts = load_table_counts(
        pipeline_dag_parallel,
        *[t["name"] for t in pipeline_dag_parallel.default_schema.data_tables()],
    )
    assert pipeline_dag_decomposed_counts == pipeline_standalone_counts

    assert dag_def.tasks[0].downstream_task_ids == set([dag_def.tasks[1].task_id])
    assert dag_def.tasks[1].upstream_task_ids == set([dag_def.tasks[0].task_id])


# def test_run_with_dag_config()


# def test_decompose_errors() -> None:
#     @dag(
#         schedule=None,
#         start_date=DEFAULT_DATE,
#         catchup=False,
#         default_args=default_args
#     )
#     def dag_fail_3():
#         tasks = PipelineTasksGroup("pipeline_fail_3", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False)

#         pipeline_fail_3 = dlt.pipeline(
#             pipeline_name="pipeline_fail_3", dataset_name="mock_data_" + uniq_id(), destination="duckdb", credentials=":pipeline:")
#         tasks.add_run(pipeline_fail_3, _fail_3, decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)


def test_run_with_retry() -> None:
    retries = 2
    now = pendulum.now()

    @dlt.resource
    def _fail_3():
        nonlocal retries
        retries -= 1
        if retries > 0:
            raise Exception(f"Failed on retry #{retries}")
        yield from "ABC"

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_fail_3():
        # by default we do not retry so this will fail
        tasks = PipelineTasksGroup(
            "pipeline_fail_3", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        pipeline_fail_3 = dlt.pipeline(
            pipeline_name="pipeline_fail_3",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=":pipeline:"),
        )
        tasks.add_run(
            pipeline_fail_3, _fail_3, trigger_rule="all_done", retries=0, provide_context=True
        )

    dag_def: DAG = dag_fail_3()
    ti = get_task_run(dag_def, "pipeline_fail_3.pipeline_fail_3", now)
    with pytest.raises(PipelineStepFailed) as pip_ex:
        # this is the way to get the exception from a task
        ti._run_raw_task()
    assert pip_ex.value.step == "extract"

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_fail_4():
        # by default we do not retry extract so we fail
        tasks = PipelineTasksGroup(
            "pipeline_fail_3",
            retry_policy=DEFAULT_RETRY_BACKOFF,
            local_data_folder=TEST_STORAGE_ROOT,
            wipe_local_data=False,
        )

        pipeline_fail_3 = dlt.pipeline(
            pipeline_name="pipeline_fail_3",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=":pipeline:"),
        )
        tasks.add_run(
            pipeline_fail_3, _fail_3, trigger_rule="all_done", retries=0, provide_context=True
        )

    dag_def = dag_fail_4()
    ti = get_task_run(dag_def, "pipeline_fail_3.pipeline_fail_3", now)
    # will fail on extract
    with pytest.raises(PipelineStepFailed) as pip_ex:
        retries = 2
        ti._run_raw_task()
    assert pip_ex.value.step == "extract"

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_fail_5():
        # this will retry
        tasks = PipelineTasksGroup(
            "pipeline_fail_3",
            retry_policy=DEFAULT_RETRY_BACKOFF,
            retry_pipeline_steps=("load", "extract"),
            local_data_folder=TEST_STORAGE_ROOT,
            wipe_local_data=False,
        )

        pipeline_fail_3 = dlt.pipeline(
            pipeline_name="pipeline_fail_3",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=":pipeline:"),
        )
        tasks.add_run(
            pipeline_fail_3, _fail_3, trigger_rule="all_done", retries=0, provide_context=True
        )

    dag_def = dag_fail_5()
    ti = get_task_run(dag_def, "pipeline_fail_3.pipeline_fail_3", now)
    retries = 2
    ti._run_raw_task()
    assert retries == 0


def test_run_decomposed_with_state_wipe() -> None:
    dataset_name = "mock_data_" + uniq_id()
    pipeline_name = "pipeline_dag_regular_" + uniq_id()

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_regular():
        tasks = PipelineTasksGroup(
            pipeline_name,
            local_data_folder=TEST_STORAGE_ROOT,
            wipe_local_data=True,
            save_load_info=True,
            save_trace_info=True,
        )

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb"
        )
        tasks.add_run(
            pipeline_dag_regular,
            mock_data_source_state(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )

    dag_def: DAG = dag_regular()
    dag_def.test()

    # pipeline local state was destroyed
    with pytest.raises(CannotRestorePipelineException):
        dlt.attach(pipeline_name=pipeline_name)

    pipeline_dag_regular = dlt.pipeline(
        pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb"
    )
    pipeline_dag_regular.sync_destination()
    # print(pipeline_dag_regular.state)
    # now source can attach to state in the pipeline
    post_source = mock_data_source_state()
    # print(post_source.state)
    # end state was increased twice (in init and in isolee at the end)
    assert post_source.state["end_counter"] == 2
    # the source counter was increased in init, _r1 and in 3 transformers * 3 items
    assert post_source.state["counter"] == 1 + 1 + 3 * 3
    # resource counter _r1
    assert post_source._r1.state["counter"] == 1 + 3 * 3
    # each transformer has a counter
    assert post_source._t1.state["counter"] == 1
    assert post_source._t2.state["counter"] == 1
    assert post_source._t2.state["counter"] == 1


def test_run_multiple_sources() -> None:
    dataset_name = "mock_data_" + uniq_id()
    pipeline_name = "pipeline_dag_regular_" + uniq_id()

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_serialize():
        tasks = PipelineTasksGroup(
            pipeline_name, local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=True
        )

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb"
        )
        st_tasks = tasks.add_run(
            pipeline_dag_regular,
            mock_data_source_state(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )
        nst_tasks = tasks.add_run(
            pipeline_dag_regular,
            mock_data_source(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )
        # connect end of first run to a head of a second
        st_tasks[-1] >> nst_tasks[0]

    dag_def: DAG = dag_serialize()
    dag_def.test()

    pipeline_dag_serial = dlt.pipeline(
        pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb"
    )
    pipeline_dag_serial.sync_destination()
    # we should have two schemas
    assert set(pipeline_dag_serial.schema_names) == {"mock_data_source_state", "mock_data_source"}
    counters_st_tasks = load_table_counts(
        pipeline_dag_serial,
        *[t["name"] for t in pipeline_dag_serial.schemas["mock_data_source_state"].data_tables()],
    )
    counters_nst_tasks = load_table_counts(
        pipeline_dag_serial,
        *[t["name"] for t in pipeline_dag_serial.schemas["mock_data_source"].data_tables()],
    )
    # print(counters_st_tasks)
    # print(counters_nst_tasks)
    # this state is confirmed in other test
    assert pipeline_dag_serial.state["sources"]["mock_data_source_state"] == {
        "counter": 11,
        "end_counter": 2,
        "resources": {
            "_r1": {"counter": 10},
            "_t3": {"counter": 1},
            "_t2": {"counter": 1},
            "_t1": {"counter": 1},
        },
    }

    # next DAG does not connect subgraphs

    dataset_name = "mock_data_" + uniq_id()
    pipeline_name = "pipeline_dag_regular_" + uniq_id()

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_parallel():
        tasks = PipelineTasksGroup(
            pipeline_name, local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=True
        )

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb"
        )
        tasks.add_run(
            pipeline_dag_regular,
            mock_data_source_state(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )
        tasks.add_run(
            pipeline_dag_regular,
            mock_data_source(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )
        # do not connect graph

    dag_def = dag_parallel()
    dag_def.test()

    pipeline_dag_parallel = dlt.pipeline(
        pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb"
    )
    pipeline_dag_parallel.sync_destination()
    # we should have two schemas
    assert set(pipeline_dag_parallel.schema_names) == {"mock_data_source_state", "mock_data_source"}
    counters_st_tasks_par = load_table_counts(
        pipeline_dag_parallel,
        *[t["name"] for t in pipeline_dag_parallel.schemas["mock_data_source_state"].data_tables()],
    )
    counters_nst_tasks_par = load_table_counts(
        pipeline_dag_parallel,
        *[t["name"] for t in pipeline_dag_parallel.schemas["mock_data_source"].data_tables()],
    )
    assert counters_st_tasks == counters_st_tasks_par
    assert counters_nst_tasks == counters_nst_tasks_par
    assert pipeline_dag_serial.state["sources"] == pipeline_dag_parallel.state["sources"]

    # here two runs are mixed together

    dataset_name = "mock_data_" + uniq_id()
    pipeline_name = "pipeline_dag_regular_" + uniq_id()

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_mixed():
        tasks = PipelineTasksGroup(
            pipeline_name, local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=True
        )

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb"
        )
        pd_tasks = tasks.add_run(
            pipeline_dag_regular,
            mock_data_source_state(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )
        hb_tasks = tasks.add_run(
            pipeline_dag_regular,
            mock_data_source(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )
        # create almost randomly connected tasks across two runs
        for pd_t, hb_t in zip(pd_tasks, hb_tasks):
            pd_t >> hb_t

    dag_def = dag_mixed()
    dag_def.test()

    pipeline_dag_mixed = dlt.pipeline(
        pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb"
    )
    pipeline_dag_mixed.sync_destination()
    # we should have two schemas
    assert set(pipeline_dag_mixed.schema_names) == {"mock_data_source_state", "mock_data_source"}
    counters_st_tasks_par = load_table_counts(
        pipeline_dag_mixed,
        *[t["name"] for t in pipeline_dag_mixed.schemas["mock_data_source_state"].data_tables()],
    )
    counters_nst_tasks_par = load_table_counts(
        pipeline_dag_mixed,
        *[t["name"] for t in pipeline_dag_mixed.schemas["mock_data_source"].data_tables()],
    )
    assert counters_st_tasks == counters_st_tasks_par
    assert counters_nst_tasks == counters_nst_tasks_par
    assert pipeline_dag_serial.state["sources"] == pipeline_dag_mixed.state["sources"]

    # TODO: here we create two pipelines in two separate task groups


def get_task_run(dag_def: DAG, task_name: str, now: pendulum.DateTime) -> TaskInstance:
    dag_def.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=now,
        run_type=DagRunType.MANUAL,
        data_interval=(now, now),
    )
    dag_def.run(start_date=now, run_at_least_once=True)
    task_def = dag_def.task_dict[task_name]
    return TaskInstance(task=task_def, execution_date=now)


def test_task_already_added():
    """
    Test that the error 'Task id {id} has already been added to the DAG'
    is not happening while adding two same sources.
    """
    tasks_list: List[PythonOperator] = None

    @dag(schedule=None, start_date=pendulum.today(), catchup=False)
    def dag_parallel():
        nonlocal tasks_list

        tasks = PipelineTasksGroup(
            "test_pipeline",
            local_data_folder="_storage",
            wipe_local_data=False,
        )

        source = mock_data_source()

        pipe = dlt.pipeline(
            pipeline_name="test_pipeline",
            dataset_name="mock_data",
            destination=dlt.destinations.duckdb(
                credentials=os.path.join("_storage", "test_pipeline.duckdb")
            ),
        )
        task = tasks.add_run(
            pipe,
            source,
            decompose="none",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )[0]
        assert task.task_id == "test_pipeline.mock_data_source__r_init-_t_init_post-_t1-_t2-2-more"

        task = tasks.add_run(
            pipe,
            source,
            decompose="none",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )[0]
        assert (
            task.task_id == "test_pipeline.mock_data_source__r_init-_t_init_post-_t1-_t2-2-more-2"
        )

        tasks_list = tasks.add_run(
            pipe,
            source,
            decompose="none",
            trigger_rule="all_done",
            retries=0,
            provide_context=True,
        )
        assert (
            tasks_list[0].task_id
            == "test_pipeline.mock_data_source__r_init-_t_init_post-_t1-_t2-2-more-3"
        )

    dag_def = dag_parallel()
    assert len(tasks_list) == 1
    dag_def.test()


def callable_source():
    @dlt.resource
    def test_res():
        context = get_current_context()
        yield [
            {"id": 1, "tomorrow": context["tomorrow_ds"]},
            {"id": 2, "tomorrow": context["tomorrow_ds"]},
            {"id": 3, "tomorrow": context["tomorrow_ds"]},
        ]

    return test_res


def test_run_callable() -> None:
    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "callable_dag.duckdb")

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_regular():
        tasks = PipelineTasksGroup(
            "callable_dag_group", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        call_dag = dlt.pipeline(
            pipeline_name="callable_dag",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=quackdb_path),
        )
        tasks.run(call_dag, callable_source)

    dag_def: DAG = dag_regular()
    dag_def.test()

    pipeline_dag = dlt.attach(pipeline_name="callable_dag")

    with pipeline_dag.sql_client() as client:
        with client.execute_query("SELECT * FROM test_res") as result:
            results = result.fetchall()

            assert len(results) == 3

            for row in results:
                assert row[1] == pendulum.tomorrow().format("YYYY-MM-DD")


def on_before_run():
    context = get_current_context()
    logger.info(f'on_before_run test: {context["tomorrow_ds"]}')


def test_on_before_run() -> None:
    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "callable_dag.duckdb")

    @dag(schedule=None, start_date=DEFAULT_DATE, catchup=False, default_args=default_args)
    def dag_regular():
        tasks = PipelineTasksGroup(
            "callable_dag_group", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False
        )

        call_dag = dlt.pipeline(
            pipeline_name="callable_dag",
            dataset_name="mock_data_" + uniq_id(),
            destination=dlt.destinations.duckdb(credentials=quackdb_path),
        )
        tasks.run(call_dag, mock_data_source, on_before_run=on_before_run)

    dag_def: DAG = dag_regular()

    with mock.patch("dlt.helpers.airflow_helper.logger.info") as logger_mock:
        dag_def.test()
        logger_mock.assert_has_calls(
            [
                mock.call(f'on_before_run test: {pendulum.tomorrow().format("YYYY-MM-DD")}'),
            ]
        )
