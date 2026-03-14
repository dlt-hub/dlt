# Airflow 3 Migration Status

Last updated: 2026-03-13. Tested against AF 3.0.6 and AF 3.1.8 on Python 3.12.

## What is done

### Production code

Version-conditional imports in `dlt/helpers/airflow_helper.py` — no deprecation
warnings on AF3. AF2 imports used when `_AIRFLOW_VERSION < 3`.

| AF2 import | AF3 import |
|---|---|
| `airflow.utils.task_group.TaskGroup` | `airflow.sdk.TaskGroup` |
| `airflow.operators.empty.EmptyOperator` | `airflow.providers.standard.operators.empty.EmptyOperator` |
| `airflow.operators.python.PythonOperator` | `airflow.providers.standard.operators.python.PythonOperator` |
| `airflow.operators.python.get_current_context` | `airflow.sdk.get_current_context` |

Unchanged (work on both): `airflow.configuration.conf`, `airflow.models.TaskInstance`,
`airflow.models.BaseOperator`.

Other production files with inline `get_current_context` imports updated with
try/except AF3-first pattern: `exec_info.py`, `config_providers_context.py`,
`incremental/__init__.py`.

`DltSource.__getattr__` now raises `AttributeError` for dunder names to prevent
`RecursionError` during `copy.deepcopy` (triggered by AF3's DAG serialization).

`airflow_info()` in `exec_info.py` returns `AIRFLOW_TASK_3` when running on AF3.

`RuntimeTaskInstance` in AF3 has no `.log` attribute — the `hasattr(ti, "log")`
guard skips logger redirection. AF3 captures task output via supervisor pipes.

### Test harness

- `BaseOperator` imported from `airflow.models` (works on both)
- `provide_context=True` removed from all test calls (deprecated since AF 2.0, rejected by AF3)
- `schedule_interval` replaced with `schedule` (works on AF2 >= 2.4)
- `tomorrow_ds` replaced with `_get_tomorrow_ds()` helper using `logical_date`
- `create_dagrun` + `TaskInstance.run()` pattern replaced with `run_task()` helper
  in `test_airflow_provider.py` and `test_join_airflow_scheduler.py`
- `dag.test(execution_date=...)` wrapped in `exec_dag_test()` helper
  (`logical_date` on AF3, `execution_date` on AF2)
- `test_run_with_retry` skipped on AF3 (uses `_run_raw_task` pattern)
- Airflow DB now created in test storage (auto-cleaned between tests)
- `pyproject.toml` airflow group widened: `apache-airflow>=2.8.0,<4 ; python_version < '3.14'`
- Version detection via `_get_airflow_version()` using `importlib.metadata` + `packaging.version.Version`

### CI

Currently AF2 only (Python 3.12). Ready for AF3 matrix when blocker is resolved.

## Blockers for AF3 CI

### `dag.test()` broken in AF3

`dag.test()` uses `InProcessTestSupervisor` which communicates with an
in-process API server. The `client.task_instances.start()` call fails with:

```
airflow.sdk.api.client.ServerResponseError: Remote server returned validation error
detail=[ValidationError(loc=['body'], msg='Input should be a valid dictionary or
object to extract fields from', type='model_attributes_type')]
```

This affects ALL tasks run via `dag.test()` — even a simple `lambda: print("hello")`.

- **AF 3.0.x**: `dag.test()` worked briefly (confirmed on 3.0.6 with specific
  dependency versions) but breaks with current transitive dependency resolutions.
  The `InProcessTestSupervisor` API validation fails.
- **AF 3.1.x**: `dag.test()` fails with `AirflowException: Cannot create DagRun
  for DAG because the dag is not serialized`. Known open bug:
  [#60860](https://github.com/apache/airflow/issues/60860),
  [#56657](https://github.com/apache/airflow/issues/56657).
  Was fixed in 3.0.2, reintroduced in 3.1.0.

### `PipelineTasksGroup` architecture

`PipelineTasksGroup` passes `DltSource` via `functools.partial` to
`PythonOperator`. In AF3, the DAG is serialized to JSON and tasks run in
isolated subprocesses via the Task SDK. The `python_callable` is reconstructed
by re-parsing the DAG file — this works for DAG files but not for inline
test DAGs.

Long-term, `PipelineTasksGroup` should not send sources through task boundaries.
Instead, it should pass source names and reconstruct them inside the task.

## What to do when AF3 unblocks

1. Add AF3 matrix entry to `.github/workflows/test_tools_airflow.yml`
2. Pin to the AF3 version that fixes `dag.test()` (likely 3.1.x or 3.2.x)
3. Verify all tests pass — the harness is already AF3-ready
4. Investigate `test_run_with_retry` AF3 equivalent (currently skipped)
