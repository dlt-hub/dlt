import os
from typing import List, Optional

from dlt.cli import echo as fmt
from dlt.cli.utils import track_command
from dlt.common.cli.runner.errors import FriendlyExit, PreflightError, RunnerError
from dlt.common.cli.runner.runner import PipelineRunner
from dlt.common.cli.runner.types import RunnerParams
from dlt.pipeline.exceptions import PipelineStepFailed


@track_command("run", False)
def run_pipeline_command(
    module: str,
    pipeline_name: Optional[str] = None,
    source_name: Optional[str] = None,
    args: Optional[List[str]] = None,
) -> int:
    """Run the given module if any pipeline and sources or resources exist in it

    Args:
        module (str): path to python module or file with dlt artifacts
        pipeline_name (Optional[str]): Pipeline name
        source_name (Optional[str]): Source or resource name
        args (Optiona[List[str]]): List of arguments to `pipeline.run` method

    Returns:
        (int): exit code
    """
    params = RunnerParams(
        module,
        current_dir=os.getcwd(),
        pipeline_name=pipeline_name,
        source_name=source_name,
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
