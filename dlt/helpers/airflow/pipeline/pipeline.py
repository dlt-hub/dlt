from typing import Any

from dlt.common.exceptions import MissingDependencyException

try:
    from airflow.utils.task_group import TaskGroup
except ImportError:
    raise MissingDependencyException("Airflow", ["airflow>=2.0.0"])


class DltAirflowPipeline(TaskGroup):
    """
    Represents a DLT Airflow pipeline task group.
    """

    def __init__(self, name: str, destination: str, dataset_name: str, full_refresh: bool, credentials: Any = None,
                 **kwargs: Any) -> None:
        """
        Initializes a new instance of the DltAirflowPipeline class.

        :param name: The name of the task group.
        :param destination: The destination of the data.
        :param dataset_name: The name of the dataset.
        :param full_refresh: Whether to do a full refresh of the data.
        :param credentials: Any required credentials for accessing the data.
        :param kwargs: Additional arguments to pass to the parent TaskGroup class.
        """

        super().__init__(group_id=name, **kwargs)
        self.name: str = name
        self.destination: str = destination
        self.dataset_name: str = dataset_name
        self.full_refresh: bool = full_refresh
        self.credentials: Any = credentials
