import typing as t

import dlt

from dlt.cli import echo as fmt
from dlt.cli.utils import track_command
from dlt.common.cli.runner import RunnerInventory, DltRunnerEnvironment
from dlt.sources import DltResource, DltSource
from typing_extensions import TypedDict


class PipelineMember(TypedDict):
    name: str
    instance: t.Union[dlt.Pipeline, DltResource, DltSource]


class DltInventory(TypedDict):
    pipelines: t.List[PipelineMember]
    sources: t.List[PipelineMember]
    resources: t.List[PipelineMember]


@track_command("run", False)
def run_pipeline_command(
    module: str,
    pipeline: t.Optional[str] = None,
    source: t.Optional[str] = None,
    args: t.Optional[str] = None,
):
    pick_first_pipeline_and_source = not pipeline and not source
    inventory = RunnerInventory(
        module,
        pipeline_name=pipeline,
        source_name=source,
        args=args,
        run_first_pipeline_with_source=pick_first_pipeline_and_source,
    )

    try:
        dlt_environment = DltRunnerEnvironment(inventory=inventory)
        dlt_environment.run()
    except RuntimeError as ex:
        fmt.echo(str(ex))
        return -1
