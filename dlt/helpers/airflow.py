from typing import Any

from dlt.common.exceptions import MissingDependencyException

try:
    from airflow.utils.task_group import TaskGroup
except ImportError:
    raise MissingDependencyException("Airflow", ["airflow>=2.0.0"])


# def run(data: Any) -> TaskGroup:
