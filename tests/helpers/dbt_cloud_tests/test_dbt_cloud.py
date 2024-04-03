import pytest
from dlt.helpers.dbt_cloud import run_dbt_cloud_job, get_dbt_cloud_run_status


@pytest.mark.parametrize("wait_outcome", [False, True])
def test_trigger_run(wait_outcome):
    # Trigger job run and wait for an outcome
    run_status = run_dbt_cloud_job(wait_for_outcome=wait_outcome)
    print(run_status)

    assert run_status.get("id") is not None
    assert not run_status.get("is_error")


@pytest.mark.parametrize("wait_outcome", [False, True])
def test_run_status(wait_outcome):
    # Trigger job run and wait for an outcome
    run_status = run_dbt_cloud_job(wait_for_outcome=False)
    run_status = get_dbt_cloud_run_status(
        run_id=run_status["id"], wait_for_outcome=wait_outcome
    )
    print(run_status)

    assert run_status.get("id") is not None
    assert not run_status.get("is_error")
