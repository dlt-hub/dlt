from typing import Any

from dlt.common.exceptions import MissingDependencyException

try:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context
    from dlt import pipeline, Pipeline
    from dlt.extract.source import DltSource
except ImportError:
    raise MissingDependencyException("Airflow", ["airflow>=2.0.0"])


class DltAirflowSource(BaseOperator):
    """
    A custom Airflow operator for running pipelines on DltSource objects.
    """

    def __init__(self, name: str, source: DltSource, **kwargs: Any) -> None:
        """
        Initializes a new instance of the DltAirflowSource class.

        Parameters:
        name (str): The task ID of the operator.
        source (DltSource): The DltSource object to be used as the source for the pipeline.
        **kwargs: Additional arguments to be passed to the BaseOperator constructor.
                  This can include a `pipeline` dictionary or Pipeline object.

        Returns:
        None
        """

        # Pop `pipeline` argument from kwargs to prevent passing it to the BaseOperator constructor
        _pipeline = kwargs.pop('pipeline', None)

        # Call the constructor of the BaseOperator class with the given `task_id` and `**kwargs`
        super().__init__(task_id=name, **kwargs)

        # Create a new pipeline object based on the given `pipeline` parameter
        if isinstance(_pipeline, dict):
            self.pipeline = pipeline(**_pipeline)
        elif isinstance(_pipeline, Pipeline):
            self.pipeline = _pipeline
        elif _pipeline is None:
            self.pipeline = pipeline(
                pipeline_name=self.task_group.group_id,
                destination=self.task_group.destination,
                dataset_name=self.task_group.dataset_name,
                full_refresh=self.task_group.full_refresh,
                credentials=self.task_group.credentials
            )
        else:
            raise TypeError(
                "The 'pipeline' parameter must be a Pipeline object or a dictionary specifying pipeline parameters.")

        self._source = source

    def execute(self, context: Context) -> Any:
        """
        Executes the operator.

        Parameters:
        context (Context): The context passed by Airflow.

        Returns:
        None
        """

        return_value = self.pipeline.run(self._source)

        self.log.info(str(return_value))
