import typing as t

from dataclasses import dataclass

import dlt

from dlt.sources import DltResource, DltSource


@dataclass
class RunnerParams:
    script_path: str
    current_dir: str
    pipeline_workdir: t.Optional[str] = None
    pipeline_name: t.Optional[str] = None
    source_name: t.Optional[str] = None
    args: t.List[str] = None


class PipelineMembers(t.TypedDict):
    pipelines: t.Dict[str, dlt.Pipeline]
    sources: t.Dict[str, t.Union[DltResource, DltSource]]
