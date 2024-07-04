import datetime
from pendulum.tz import UTC
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import get_current_context  # noqa
from airflow.utils.state import State, DagRunState
from airflow.utils.types import DagRunType

import dlt
from dlt.common import pendulum
from dlt.common.utils import uniq_id
from dlt.common.time import ensure_pendulum_date

# flake8: noqa: B008

CATCHUP_BEGIN = pendulum.datetime(2023, 1, 1, tz="Europe/Berlin")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


@dlt.resource()
def existing_incremental(
    updated_at: dlt.sources.incremental[pendulum.DateTime] = dlt.sources.incremental(
        "updated_at", allow_external_schedulers=True
    )
):
    yield {"updated_at": CATCHUP_BEGIN, "state": updated_at.get_state()}


def test_date_coercion() -> None:
    @dag(
        schedule_interval="@daily",
        start_date=CATCHUP_BEGIN,
        catchup=False,
        max_active_runs=1,
        default_args=default_args,
    )
    def dag_regular():
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
            assert (
                state["state"]["initial_value"] == CATCHUP_BEGIN == context["data_interval_start"]
            )
            assert state["state"]["initial_value"].tz == UTC
            assert state["state"]["last_value"] == CATCHUP_BEGIN == context["data_interval_start"]
            assert state["state"]["last_value"].tz == UTC
            # end date
            assert r.incremental._incremental.end_value == context["data_interval_end"]
            assert r.incremental._incremental.end_value.tz == UTC
            assert (
                r.incremental._incremental.end_value - state["state"]["initial_value"]
            ) == datetime.timedelta(hours=24)

            # datetime.datetime coercion must be pendulum anyway
            @dlt.resource()
            def incremental_datetime(
                updated_at=dlt.sources.incremental[datetime.datetime](
                    "updated_at", allow_external_schedulers=True
                )
            ):
                yield {"updated_at": CATCHUP_BEGIN, "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            # must have UTC timezone
            assert (
                state["state"]["initial_value"] == CATCHUP_BEGIN == context["data_interval_start"]
            )
            assert state["state"]["initial_value"].tz == UTC

            # datetime.date coercion also works
            @dlt.resource()  # type: ignore[no-redef]
            def incremental_datetime(
                updated_at=dlt.sources.incremental[datetime.date](
                    "updated_at", allow_external_schedulers=True
                )
            ):
                yield {
                    "updated_at": ensure_pendulum_date(CATCHUP_BEGIN),
                    "state": updated_at.get_state(),
                }

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == ensure_pendulum_date(
                context["data_interval_start"]
            )
            assert isinstance(state["state"]["initial_value"], datetime.date)

            # coerce to int
            @dlt.resource()  # type: ignore[no-redef]
            def incremental_datetime(
                updated_at=dlt.sources.incremental[int](
                    "updated_at", allow_external_schedulers=True
                )
            ):
                yield {"updated_at": CATCHUP_BEGIN.int_timestamp, "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == context["data_interval_start"].int_timestamp
            assert (
                r.incremental._incremental.end_value == context["data_interval_end"].int_timestamp
            )

            # coerce to float
            @dlt.resource()  # type: ignore[no-redef]
            def incremental_datetime(
                updated_at=dlt.sources.incremental[float](
                    "updated_at", allow_external_schedulers=True
                )
            ):
                yield {"updated_at": CATCHUP_BEGIN.timestamp(), "state": updated_at.get_state()}

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == context["data_interval_start"].timestamp()
            assert r.incremental._incremental.end_value == context["data_interval_end"].timestamp()

            # coerce to str
            @dlt.resource()  # type: ignore[no-redef]
            def incremental_datetime(
                updated_at=dlt.sources.incremental[str](
                    "updated_at", allow_external_schedulers=True
                )
            ):
                yield {
                    "updated_at": CATCHUP_BEGIN.in_tz("UTC").isoformat(),
                    "state": updated_at.get_state(),
                }

            r = incremental_datetime()
            state = list(r)[0]
            # must have UTC timezone
            assert (
                state["state"]["initial_value"]
                == context["data_interval_start"].in_tz("UTC").isoformat()
            )
            assert (
                r.incremental._incremental.end_value
                == context["data_interval_end"].in_tz("UTC").isoformat()
            )

        scheduled()

    dag_def: DAG = dag_regular()
    # this will correctly simulate data inverval but only because execution_date is a scheduled one
    dag_def.test(execution_date=CATCHUP_BEGIN)
    # print(dag_def.get_run_dates(CATCHUP_BEGIN.add(minutes=10)))


def test_no_next_execution_date() -> None:
    now = pendulum.now()

    @dag(
        schedule=None,
        catchup=False,
        start_date=CATCHUP_BEGIN,
        default_args=default_args,
        max_active_runs=1,
    )
    def dag_no_schedule():
        @task
        def unscheduled():
            context = get_current_context()

            @dlt.resource()
            def incremental_datetime(
                updated_at=dlt.sources.incremental[datetime.datetime](
                    "updated_at", allow_external_schedulers=True
                )
            ):
                yield {
                    "updated_at": context["data_interval_start"],
                    "state": updated_at.get_state(),
                }

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == context["data_interval_start"]
            # end_value is set very close to now (incremental detected manual run and set it to now)
            delta = r.incremental._incremental.end_value - now
            assert delta.total_minutes() < 2
            # initial_value very close to now
            delta = context["data_interval_start"] - now
            assert delta.total_minutes() < 2
            # intervals start and end are the same
            assert context["data_interval_start"] == context["data_interval_end"]

            # will be filtered out (now earlier than data_interval_start)
            @dlt.resource()  # type: ignore[no-redef]
            def incremental_datetime(
                updated_at=dlt.sources.incremental[datetime.datetime](
                    "updated_at", allow_external_schedulers=True
                )
            ):
                yield {
                    "updated_at": now.subtract(hours=1, seconds=1),
                    "state": updated_at.get_state(),
                }

            r = incremental_datetime()
            assert len(list(r)) == 0

        unscheduled()

    now = pendulum.now()

    dag_def: DAG = dag_no_schedule()
    dag_def.create_dagrun(
        state=DagRunState.QUEUED,
        execution_date=now.subtract(hours=1),
        run_type=DagRunType.MANUAL,
    )
    task_def = dag_def.task_dict["unscheduled"]
    ti = TaskInstance(task=task_def, execution_date=now.subtract(hours=1))
    ti.run()
    assert ti.state == State.SUCCESS

    @dag(
        schedule_interval="@daily",
        start_date=CATCHUP_BEGIN,
        catchup=True,
        default_args=default_args,
    )
    def dag_daily_schedule():
        @task
        def scheduled():
            context = get_current_context()

            @dlt.resource()
            def incremental_datetime(
                updated_at=dlt.sources.incremental[datetime.datetime](
                    "updated_at", allow_external_schedulers=True
                )
            ):
                yield {
                    "updated_at": context["data_interval_start"],
                    "state": updated_at.get_state(),
                }

            r = incremental_datetime()
            state = list(r)[0]
            assert state["state"]["initial_value"] == context["data_interval_start"]
            # end_value is set very close to now (incremental detected manual run and set it to now)
            delta = r.incremental._incremental.end_value - now
            assert delta.total_minutes() < 2
            # initial_value very close to now
            delta = context["data_interval_start"] - now
            assert delta.total_minutes() < 2
            # intervals start and end are the same
            assert context["data_interval_start"] == context["data_interval_end"]

        scheduled()

    dag_def = dag_daily_schedule()
    # manually run a scheduled DAG.
    # WARNING: explicit data interval must be specified! the code that infers intervals in create_dagrun produces different results than in docs
    # https://airflow.apache.org/docs/apache-airflow/stable/faq.html#why-next-ds-or-prev-ds-might-not-contain-expected-values
    # "When manually triggering DAG, the schedule will be ignored, and prev_ds == next_ds == ds"
    dag_def.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=now,
        run_type=DagRunType.MANUAL,
        data_interval=(now, now),
    )
    dag_def.run(start_date=now, run_at_least_once=True)
    task_def = dag_def.task_dict["scheduled"]
    ti = TaskInstance(task=task_def, execution_date=now)
    ti.run()
    assert ti.state == State.SUCCESS


def test_scheduler_pipeline_state() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="pipeline_dag_regular",
        dataset_name="mock_data_" + uniq_id(),
        destination=dlt.destinations.duckdb(credentials=":pipeline:"),
    )
    now = pendulum.now()

    @dag(
        schedule_interval="@daily",
        start_date=CATCHUP_BEGIN,
        catchup=False,
        default_args=default_args,
    )
    def dag_regular():
        @task
        def scheduled() -> None:
            r = existing_incremental()
            pipeline.run(r)
            assert r.incremental._incremental.end_value is not None

        scheduled()

    dag_def: DAG = dag_regular()
    dag_def.test(execution_date=CATCHUP_BEGIN)

    # no source and resource state
    assert "sources" not in pipeline.state

    # end of interval in the future - state still not saved
    dag_def.test()
    assert "sources" not in pipeline.state

    # start ==  end interval
    dag_def.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=now,
        run_type=DagRunType.MANUAL,
        data_interval=(now, now),
    )
    dag_def.run(start_date=now, run_at_least_once=True)
    task_def = dag_def.task_dict["scheduled"]
    ti = TaskInstance(task=task_def, execution_date=now)
    ti.run()
    assert ti.state == State.SUCCESS
    assert "sources" not in pipeline.state

    pipeline = pipeline.drop()

    dag_def.test(execution_date=CATCHUP_BEGIN)
    assert "sources" not in pipeline.state

    @dag(schedule=None, start_date=CATCHUP_BEGIN, catchup=False, default_args=default_args)
    def dag_no_schedule():
        @task
        def unscheduled() -> None:
            r = existing_incremental()
            pipeline.run(r)

        unscheduled()

    dag_def = dag_no_schedule()
    dag_def.test(execution_date=CATCHUP_BEGIN)

    # state was saved (end date not specified)
    assert "sources" not in pipeline.state
