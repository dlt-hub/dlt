from typing import List, Tuple

from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.storages import FileStorage


def get_local_pipelines(pipelines_dir: str = None) -> Tuple[str, List[str]]:
    pipelines_dir = pipelines_dir or get_dlt_pipelines_dir()
    storage = FileStorage(pipelines_dir)
    dirs = storage.list_folder_dirs(".", to_root=False)
    return pipelines_dir, dirs


def get_pipeline(dlt, pipeline_select):
    pipeline_name = pipeline_select.value[0]
    return dlt.attach(pipeline_name)
