import os
import typing as t

from dlt.cli import echo as fmt
from dlt.cli.utils import track_command
from dlt.common.cli.runner.errors import FriendlyExit, PreflightError, RunnerError
from dlt.common.cli.runner.runner import PipelineRunner
from dlt.common.cli.runner.types import RunnerParams
from dlt.pipeline.exceptions import PipelineStepFailed


@track_command("run", False)
def run_pipeline_command(
    module: str,
    pipeline: t.Optional[str] = None,
    source: t.Optional[str] = None,
    args: t.Optional[t.List[str]] = None,
) -> int:
    params = RunnerParams(
        module,
        current_dir=os.getcwd(),
        pipeline_name=pipeline,
        source_name=source,
        args=args,
    )

    try:
        with PipelineRunner(params=params) as runner:
            load_info = runner.run()
            fmt.echo("")
            # FIXME: provide more useful information
            fmt.echo(load_info)
    except PipelineStepFailed:
        raise
    except RunnerError as ex:
        fmt.echo(ex.message)
        return -1
    except (FriendlyExit, PreflightError):
        fmt.info("Stopping...")

    return 0
