import os
import typing as t

from dlt.cli import echo as fmt
from dlt.cli.utils import track_command
from dlt.common.cli.runner.runner import PipelineRunner
from dlt.common.cli.runner.types import RunnerParams


@track_command("run", False)
def run_pipeline_command(
    module: str,
    pipeline: t.Optional[str] = None,
    source: t.Optional[str] = None,
    args: t.Optional[str] = None,
):
    params = RunnerParams(
        module,
        pipeline_name=pipeline,
        source_name=source,
        args=args,
    )

    try:
        with PipelineRunner(params=params, current_dir=os.getcwd()) as runner:
            load_info = runner.run()
            fmt.echo("")
            fmt.echo(load_info)
    except RuntimeError as ex:
        fmt.echo(str(ex))
        return -1
