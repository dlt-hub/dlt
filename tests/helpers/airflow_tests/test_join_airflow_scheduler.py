import datetime
from pendulum.tz import UTC
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context  # noqa

import dlt
from dlt.common import pendulum
from dlt.common.utils import uniq_id
from dlt.common.time import ensure_pendulum_date

# flake8: noqa: B008

CATCHUP_BEGIN = pendulum.datetime(2023, 1, 1, tz="Europe/Berlin")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1
}

@dlt.resource()
def existing_incremental(updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental("updated_at", allow_external_schedulers=True)):
    yield {"updated_at": CATCHUP_BEGIN, "state": updated_at.get_state()}


def test_date_coercion() -> None:
    @dag(schedule_interval='@daily',
        start_date=CATCHUP_BEGIN,
        catchup=False,
        default_args=default_args
    )
    def dag_regular():
        # pipeline_dag_regular = dlt.pipeline(
        #     pipeline_name="pipeline_dag_regular", dataset_name="mock_data_" + uniq_id(), destination="dummy")

        @task
        def scheduled() -> None:
            context = get_current_context()

            # DateTime coercion, also must be UTC
            r = existing_incremental()
            state = list(r)[0]
            # updated at is CATCHUP_BEGIN preserving the timezone
            assert state["updated_at"] == CATCHUP_BEGIN
            assert "Europe/Berlin" in str(state["updated_at"].tz)
            # must have UTC timezone
            assert state["state"]["initial_value"] == CATCHUP_BEGIN == context["execution_date"]
            assert state["state"]["initial_value"].tz == UTC
            assert state["state"]["last_value"] == CATCHUP_BEGIN == context["execution_date"]
            assert state["state"]["last_value"].tz == UTC
            # end date
            assert r.incremental._incremental.end_value == context["next_execution_date"]
            assert r.incremental._incremental.end_value.tz == UTC

            # datetime.datetime coercion must be pendulum anyway
            @dlt.resource()
            def incremental_datetime(updated_at = dlt.sources.incremental[datetime.datetime]("updated_at", allow_external_schedulers=True)):
                yield {"updated_at": CATCHUP_BEGIN, "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            # must have UTC timezone
            assert state["state"]["initial_value"] == CATCHUP_BEGIN == context["execution_date"]
            assert state["state"]["initial_value"].tz == UTC

            # datetime.date coercion also works
            @dlt.resource()
            def incremental_datetime(updated_at = dlt.sources.incremental[datetime.date]("updated_at", allow_external_schedulers=True)):
                yield {"updated_at": ensure_pendulum_date(CATCHUP_BEGIN), "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == ensure_pendulum_date(context["execution_date"])
            assert isinstance(state["state"]["initial_value"], datetime.date)

            # coerce to int
            @dlt.resource()
            def incremental_datetime(updated_at = dlt.sources.incremental[int]("updated_at", allow_external_schedulers=True)):
                yield {"updated_at": CATCHUP_BEGIN.int_timestamp, "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == context["execution_date"].int_timestamp
            assert r.incremental._incremental.end_value == context["next_execution_date"].int_timestamp

            # coerce to float
            @dlt.resource()
            def incremental_datetime(updated_at = dlt.sources.incremental[float]("updated_at", allow_external_schedulers=True)):
                yield {"updated_at": CATCHUP_BEGIN.timestamp(), "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == context["execution_date"].timestamp()
            assert r.incremental._incremental.end_value == context["next_execution_date"].timestamp()

            # coerce to str
            @dlt.resource()
            def incremental_datetime(updated_at = dlt.sources.incremental[str]("updated_at", allow_external_schedulers=True)):
                yield {"updated_at": CATCHUP_BEGIN.in_tz("UTC").isoformat(), "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            # must have UTC timezone
            assert state["state"]["initial_value"] == context["execution_date"].in_tz("UTC").isoformat()
            assert r.incremental._incremental.end_value == context["next_execution_date"].in_tz("UTC").isoformat()

        scheduled()

    dag_def: DAG = dag_regular()
    # print(dag_def.get_run_dates(CATCHUP_BEGIN.add(minutes=10)))
    dag_def.test(execution_date=CATCHUP_BEGIN)


def test_no_next_execution_date() -> None:
    now = pendulum.now()

    @dag(schedule=None,
        start_date=CATCHUP_BEGIN,
        catchup=False,
        default_args=default_args
    )
    def dag_no_schedule():
        @task
        def unscheduled():
            context = get_current_context()

            @dlt.resource()
            def incremental_datetime(updated_at = dlt.sources.incremental[datetime.datetime]("updated_at", allow_external_schedulers=True)):
                yield {"updated_at": context["execution_date"], "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == context["execution_date"]
            # end_value is None so state will be used
            assert r.incremental._incremental.end_value is None
            # initial_value very close to now
            delta = context["execution_date"] - now
            assert delta.total_minutes() < 2

            # will be filtered out (now earlier than execution_date)
            @dlt.resource()
            def incremental_datetime(updated_at = dlt.sources.incremental[datetime.datetime]("updated_at", allow_external_schedulers=True)):
                yield {"updated_at": now, "state": updated_at.get_state()}

            r = incremental_datetime()
            assert len(list(r)) == 0

        unscheduled()

    dag_def: DAG = dag_no_schedule()
    dag_def.test()

    now = pendulum.now()

    @dag(schedule_interval='@daily',
        start_date=CATCHUP_BEGIN,
        catchup=True,
        default_args=default_args
    )
    def dag_daily_schedule():
        @task
        def scheduled():
            context = get_current_context()
            @dlt.resource()
            def incremental_datetime(updated_at = dlt.sources.incremental[datetime.datetime]("updated_at", allow_external_schedulers=True)):
                yield {"updated_at": context["execution_date"], "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == context["execution_date"]
            # end_value is None because next_execution_date was in the future!
            assert r.incremental._incremental.end_value is None
            # initial_value very close to now
            delta = context["execution_date"] - now
            assert delta.total_minutes() < 2

        scheduled()
    dag_def = dag_daily_schedule()
    dag_def.test()


def test_scheduler_pipeline_state() -> None:
    pipeline = dlt.pipeline(
            pipeline_name="pipeline_dag_regular", dataset_name="mock_data_" + uniq_id(), destination="duckdb", credentials=":pipeline:")

    @dag(schedule_interval='@daily',
        start_date=CATCHUP_BEGIN,
        catchup=False,
        default_args=default_args
    )
    def dag_regular():

        @task
        def scheduled() -> None:
            r = existing_incremental()
            pipeline.run(r)

        scheduled()

    dag_def: DAG = dag_regular()
    dag_def.test(execution_date=CATCHUP_BEGIN)

    # no source and resource state
    assert "sources" not in pipeline.state

    dag_def.test()
    # state was saved (end date was in the future)
    assert "existing_incremental" in pipeline.state["sources"]["pipeline_dag_regular"]["resources"]

    pipeline = pipeline.drop()

    dag_def.test(execution_date=CATCHUP_BEGIN)
    assert "sources" not in pipeline.state

    @dag(
        schedule=None,
        start_date=CATCHUP_BEGIN,
        catchup=False,
        default_args=default_args
    )
    def dag_no_schedule():

        @task
        def unscheduled() -> None:
            r = existing_incremental()
            pipeline.run(r)

        unscheduled()

    dag_def: DAG = dag_no_schedule()
    dag_def.test(execution_date=CATCHUP_BEGIN)

    # state was saved (end date not specified)
    assert "existing_incremental" in pipeline.state["sources"]["pipeline_dag_regular"]["resources"]
