from datetime import datetime

from airflow import DAG

import dlt
from dlt.helpers.airflow.pipeline.pipeline import DltAirflowPipeline
from dlt.helpers.airflow.source.source import DltAirflowSource


@dlt.source
def mock_source():
    @dlt.resource(name="test_data", write_disposition="replace")
    def test_data():
        for i in range(10):
            yield i

    return test_data(),


def test_airflow_dag_creation():
    """
    This code defines two test cases for creating a DAG with a DltAirflowPipeline and DltAirflowSource. The first test case creates a DAG with a single DltAirflowPipeline TaskGroup containing a single
    DltAirflowSource task. The second test case creates a DAG with two connected DltAirflowSource tasks, each having a separate pipeline configuration.
    """
    with DAG(dag_id="test", default_args={"owner": "airflow", "start_date": datetime(2023, 3, 30)}) as dag:
        with DltAirflowPipeline(
            name="test_pipeline",
            destination="dummy",
            dataset_name="test",
            full_refresh=True,
        ):
            DltAirflowSource(
                name="test_source",
                source=mock_source(),
            )

    assert dag is not None
    assert dag.task_dict is not None
    assert len(dag.task_dict) == 1
    assert "test_pipeline.test_source" in dag.task_dict

    with DAG(dag_id="test", default_args={"owner": "airflow", "start_date": datetime(2023, 3, 30)}) as dag:
        DltAirflowSource(
            name="test_source_1",
            source=mock_source(),
            pipeline={
                'pipeline_name': 'dummy_pipeline_1',
                'destination': 'dummy',
                'dataset_name': 'dummy_dataset',
                'full_refresh': False
            }
        ) >> DltAirflowSource(
            name="test_source_2",
            source=mock_source(),
            pipeline={
                'pipeline_name': 'dummy_pipeline_2',
                'destination': 'dummy',
                'dataset_name': 'dummy_dataset',
                'full_refresh': False
            }
        )

    assert dag is not None
    assert dag.task_dict is not None
    assert len(dag.task_dict) == 2
    assert "test_source_1" in dag.task_dict
    assert "test_source_2" in dag.task_dict
