from typing import List, Tuple

import dlt

from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.storages import FileStorage


def get_local_pipelines(pipelines_dir: str = None) -> Tuple[str, List[str]]:
    """Get the local pipelines directory and the list of pipeline names in it.

    Args:
        pipelines_dir (str, optional): The local pipelines directory. Defaults to get_dlt_pipelines_dir().

    Returns:
        Tuple[str, List[str]]: The local pipelines directory and the list of pipeline names in it.
    """
    pipelines_dir = pipelines_dir or get_dlt_pipelines_dir()
    storage = FileStorage(pipelines_dir)
    dirs = storage.list_folder_dirs(".", to_root=False)
    return pipelines_dir, dirs


def get_pipeline(pipeline_name: str) -> dlt.Pipeline:
    """Get a pipeline by name.

    Args:
        pipeline_name (str): The name of the pipeline to get.

    Returns:
        dlt.Pipeline: The pipeline.
    """
    return dlt.attach(pipeline_name)
