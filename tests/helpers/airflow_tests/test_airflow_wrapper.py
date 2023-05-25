import os
import pytest
from typing import List
from airflow import DAG
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State, DagRunState
from airflow.utils.types import DagRunType

import dlt
from dlt.common import pendulum
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.configuration.providers.airflow import AIRFLOW_SECRETS_TOML_VARIABLE_KEY
from dlt.common.utils import uniq_id
from dlt.helpers.airflow_helper import PipelineTasksGroup, DEFAULT_RETRY_BACKOFF
from dlt.pipeline.exceptions import CannotRestorePipelineException, PipelineStepFailed

from tests.utils import preserve_environ, autouse_test_storage, TEST_STORAGE_ROOT
from tests.load.pipeline.utils import load_table_counts
from tests.helpers.airflow_tests.utils import initialize_airflow_db


DEFAULT_DATE = pendulum.datetime(2023, 4, 18, tz='Europe/Berlin')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1
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
        yield items*mul

    @dlt.transformer(data_from=_r1)
    def _t3(items, mul):
        for item in items:
            yield item.upper()*mul

    # add something to init
    @dlt.transformer(data_from=_r_init)
    def _t_init_post(items):
        for item in items:
            yield item*2

    @dlt.resource
    def _r_isolee():
        yield from ["AX", "CV", "ED"]

    return _r_init, _t_init_post, _r1, _t1("POST"), _t2(3), _t3(2), _r_isolee


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
        yield items*mul

    @dlt.transformer(data_from=_r1)
    def _t3(items, mul):
        dlt.current.source_state()["counter"] += 1
        dlt.current.resource_state("_r1")["counter"] += 1
        dlt.current.resource_state()["counter"] = 1
        for item in items:
            yield item.upper()*mul

    # add something to init
    @dlt.transformer(data_from=_r_init)
    def _t_init_post(items):
        for item in items:
            yield item*2

    @dlt.resource
    def _r_isolee():
        dlt.current.source_state()["end_counter"] += 1
        yield from ["AX", "CV", "ED"]

    return _r_init, _t_init_post, _r1, _t1("POST"), _t2(3), _t3(2), _r_isolee


def test_regular_run() -> None:
    # run the pipeline normally
    pipeline_standalone = dlt.pipeline(
        pipeline_name="pipeline_standalone", dataset_name="mock_data_" + uniq_id(), destination="duckdb", credentials=":pipeline:")
    pipeline_standalone.run(mock_data_source())
    pipeline_standalone_counts = load_table_counts(pipeline_standalone, *[t["name"] for t in pipeline_standalone.default_schema.data_tables()])

    tasks_list: List[PythonOperator] = None
    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_regular():
        nonlocal tasks_list
        tasks = PipelineTasksGroup("pipeline_dag_regular", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False)

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name="pipeline_dag_regular", dataset_name="mock_data_" + uniq_id(), destination="duckdb", credentials=":pipeline:")
        tasks_list = tasks.add_run(pipeline_dag_regular, mock_data_source(), decompose="none", trigger_rule="all_done", retries=0, provide_context=True)

    dag_def: DAG = dag_regular()
    assert len(tasks_list) == 1
    # composite task name
    assert tasks_list[0].task_id == "pipeline_dag_regular.mock_data_source__r_init-_t_init_post-_t1-_t2-2-more"

    dag_def.test()
    # we should be able to attach to pipeline state created within Airflow

    pipeline_dag_regular = dlt.attach(pipeline_name="pipeline_dag_regular")
    pipeline_dag_regular_counts = load_table_counts(pipeline_dag_regular, *[t["name"] for t in pipeline_dag_regular.default_schema.data_tables()])
    # same data should be loaded
    assert pipeline_dag_regular_counts == pipeline_standalone_counts

    quackdb_path = os.path.join(TEST_STORAGE_ROOT, "pipeline_dag_decomposed.duckdb")
    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_decomposed():
        nonlocal tasks_list
        tasks = PipelineTasksGroup("pipeline_dag_decomposed", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False)

        # set duckdb to be outside of pipeline folder which is dropped on each task
        pipeline_dag_decomposed = dlt.pipeline(
            pipeline_name="pipeline_dag_decomposed", dataset_name="mock_data_" + uniq_id(), destination="duckdb", credentials=quackdb_path)
        tasks_list = tasks.add_run(pipeline_dag_decomposed, mock_data_source(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)

    dag_def: DAG = dag_decomposed()
    assert len(tasks_list) == 3
    # task one by one
    assert tasks_list[0].task_id == "pipeline_dag_decomposed.mock_data_source__r_init-_t_init_post"
    assert tasks_list[1].task_id == "pipeline_dag_decomposed.mock_data_source__t1-_t2-_t3"
    assert tasks_list[2].task_id == "pipeline_dag_decomposed.mock_data_source__r_isolee"
    dag_def.test()
    pipeline_dag_decomposed = dlt.attach(pipeline_name="pipeline_dag_decomposed")
    pipeline_dag_decomposed_counts = load_table_counts(pipeline_dag_decomposed, *[t["name"] for t in pipeline_dag_decomposed.default_schema.data_tables()])
    assert pipeline_dag_decomposed_counts == pipeline_standalone_counts


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

    @dlt.resource
    def _fail_3():
        nonlocal retries
        retries -= 1
        if retries > 0:
            raise Exception(f"Failed on retry #{retries}")
        yield from "ABC"

    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_fail_3():
        # by default we do not retry so this will fail
        tasks = PipelineTasksGroup("pipeline_fail_3", local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False)

        pipeline_fail_3 = dlt.pipeline(
            pipeline_name="pipeline_fail_3", dataset_name="mock_data_" + uniq_id(), destination="duckdb", credentials=":pipeline:")
        tasks.add_run(pipeline_fail_3, _fail_3, trigger_rule="all_done", retries=0, provide_context=True)

    dag_def: DAG = dag_fail_3()
    # will fail on extract
    with pytest.raises(PipelineStepFailed) as pip_ex:
        dag_def.test()
    assert pip_ex.value.step == "extract"

    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_fail_3():
        # by default we do not retry extract so we fail
        tasks = PipelineTasksGroup("pipeline_fail_3", retry_policy=DEFAULT_RETRY_BACKOFF, local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False)

        pipeline_fail_3 = dlt.pipeline(
            pipeline_name="pipeline_fail_3", dataset_name="mock_data_" + uniq_id(), destination="duckdb", credentials=":pipeline:")
        tasks.add_run(pipeline_fail_3, _fail_3, trigger_rule="all_done", retries=0, provide_context=True)

    dag_def: DAG = dag_fail_3()
    # will fail on extract
    with pytest.raises(PipelineStepFailed) as pip_ex:
        retries = 2
        dag_def.test()
    assert pip_ex.value.step == "extract"

    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_fail_3():
        # this will retry
        tasks = PipelineTasksGroup("pipeline_fail_3", retry_policy=DEFAULT_RETRY_BACKOFF, retry_pipeline_steps=("load", "extract"), local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=False)

        pipeline_fail_3 = dlt.pipeline(
            pipeline_name="pipeline_fail_3", dataset_name="mock_data_" + uniq_id(), destination="duckdb", credentials=":pipeline:")
        tasks.add_run(pipeline_fail_3, _fail_3, trigger_rule="all_done", retries=0, provide_context=True)

    dag_def: DAG = dag_fail_3()
    retries = 2
    dag_def.test()
    assert retries == 0


def test_run_decomposed_with_state_wipe() -> None:

    dataset_name = "mock_data_" + uniq_id()
    pipeline_name = "pipeline_dag_regular_" + uniq_id()

    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_regular():
        tasks = PipelineTasksGroup(pipeline_name, local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=True, save_load_info=True, save_trace_info=True)

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb")
        tasks.add_run(pipeline_dag_regular, mock_data_source_state(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)

    dag_def: DAG = dag_regular()
    dag_def.test()

    # pipeline local state was destroyed
    with pytest.raises(CannotRestorePipelineException):
        dlt.attach(pipeline_name=pipeline_name)

    pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb")
    pipeline_dag_regular.sync_destination()
    # print(pipeline_dag_regular.state)
    # now source can attach to state in the pipeline
    post_source = mock_data_source_state()
    # print(post_source.state)
    # end state was increased twice (in init and in isolee at the end)
    assert post_source.state["end_counter"] == 2
    # the source counter was increased in init, _r1 and in 3 transformers * 3 items
    assert post_source.state["counter"] == 1 + 1 + 3*3
    # resource counter _r1
    assert post_source._r1.state["counter"] == 1 + 3*3
    # each transformer has a counter
    assert post_source._t1.state["counter"] == 1
    assert post_source._t2.state["counter"] == 1
    assert post_source._t2.state["counter"] == 1


def test_run_multiple_sources() -> None:
    dataset_name = "mock_data_" + uniq_id()
    pipeline_name = "pipeline_dag_regular_" + uniq_id()

    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_serialize():
        tasks = PipelineTasksGroup(pipeline_name, local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=True)

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb")
        st_tasks = tasks.add_run(pipeline_dag_regular, mock_data_source_state(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)
        nst_tasks = tasks.add_run(pipeline_dag_regular, mock_data_source(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)
        # connect end of first run to a head of a second
        st_tasks[-1] >> nst_tasks[0]


    dag_def: DAG = dag_serialize()
    dag_def.test()

    pipeline_dag_serial = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb")
    pipeline_dag_serial.sync_destination()
    # we should have two schemas
    assert pipeline_dag_serial.schema_names == ['mock_data_source_state', 'mock_data_source']
    counters_st_tasks = load_table_counts(pipeline_dag_serial, *[t["name"] for t in pipeline_dag_serial.schemas['mock_data_source_state'].data_tables()])
    counters_nst_tasks = load_table_counts(pipeline_dag_serial, *[t["name"] for t in pipeline_dag_serial.schemas['mock_data_source'].data_tables()])
    # print(counters_st_tasks)
    # print(counters_nst_tasks)
    # this state is confirmed in other test
    assert pipeline_dag_serial.state["sources"]["mock_data_source_state"] == {'counter': 11, 'end_counter': 2, 'resources': {'_r1': {'counter': 10}, '_t3': {'counter': 1}, '_t2': {'counter': 1}, '_t1': {'counter': 1}}}

    # next DAG does not connect subgraphs

    dataset_name = "mock_data_" + uniq_id()
    pipeline_name = "pipeline_dag_regular_" + uniq_id()

    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_parallel():
        tasks = PipelineTasksGroup(pipeline_name, local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=True)

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb")
        tasks.add_run(pipeline_dag_regular, mock_data_source_state(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)
        tasks.add_run(pipeline_dag_regular, mock_data_source(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)
        # do not connect graph

    dag_def: DAG = dag_parallel()
    dag_def.test()

    pipeline_dag_parallel = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb")
    pipeline_dag_parallel.sync_destination()
    # we should have two schemas
    assert pipeline_dag_parallel.schema_names == ['mock_data_source_state', 'mock_data_source']
    counters_st_tasks_par = load_table_counts(pipeline_dag_parallel, *[t["name"] for t in pipeline_dag_parallel.schemas['mock_data_source_state'].data_tables()])
    counters_nst_tasks_par = load_table_counts(pipeline_dag_parallel, *[t["name"] for t in pipeline_dag_parallel.schemas['mock_data_source'].data_tables()])
    assert counters_st_tasks == counters_st_tasks_par
    assert counters_nst_tasks == counters_nst_tasks_par
    assert pipeline_dag_serial.state["sources"] == pipeline_dag_parallel.state["sources"]

    # here two runs are mixed together

    dataset_name = "mock_data_" + uniq_id()
    pipeline_name = "pipeline_dag_regular_" + uniq_id()

    @dag(
        schedule=None,
        start_date=DEFAULT_DATE,
        catchup=False,
        default_args=default_args
    )
    def dag_mixed():
        tasks = PipelineTasksGroup(pipeline_name, local_data_folder=TEST_STORAGE_ROOT, wipe_local_data=True)

        pipeline_dag_regular = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb")
        pd_tasks = tasks.add_run(pipeline_dag_regular, mock_data_source_state(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)
        hb_tasks = tasks.add_run(pipeline_dag_regular, mock_data_source(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)
        # create almost randomly connected tasks across two runs
        for pd_t, hb_t in zip(pd_tasks, hb_tasks):
            pd_t >> hb_t

    dag_def: DAG = dag_mixed()
    dag_def.test()

    pipeline_dag_mixed = dlt.pipeline(
            pipeline_name=pipeline_name, dataset_name=dataset_name, destination="duckdb")
    pipeline_dag_mixed.sync_destination()
    # we should have two schemas
    assert pipeline_dag_mixed.schema_names == ['mock_data_source_state', 'mock_data_source']
    counters_st_tasks_par = load_table_counts(pipeline_dag_mixed, *[t["name"] for t in pipeline_dag_mixed.schemas['mock_data_source_state'].data_tables()])
    counters_nst_tasks_par = load_table_counts(pipeline_dag_mixed, *[t["name"] for t in pipeline_dag_mixed.schemas['mock_data_source'].data_tables()])
    assert counters_st_tasks == counters_st_tasks_par
    assert counters_nst_tasks == counters_nst_tasks_par
    assert pipeline_dag_serial.state["sources"] == pipeline_dag_mixed.state["sources"]

    # TODO: here we create two pipelines in two separate task groups