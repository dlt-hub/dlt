import os
import typing as t

from dlt.cli import echo as fmt
from dlt.common.cli.runner.inquirer import Inquirer
from dlt.common.pipeline import LoadInfo
from dlt.common.cli.runner.errors import FriendlyExit, RunnerError
from dlt.common.cli.runner.pipeline_script import PipelineScript
from dlt.common.cli.runner.types import PipelineMembers, RunnerParams


class PipelineRunner:
    def __init__(self, params: RunnerParams, current_dir: str) -> None:
        self.params = params
        self.current_dir = current_dir
        self.script = PipelineScript(params)

    def __enter__(self) -> t.Self:
        self.check_if_runnable(self.script.pipeline_members)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self) -> LoadInfo:
        try:
            inquirer = Inquirer(self.script.pipeline_members)
            pipeline, resource = inquirer.ask()
        except FriendlyExit:
            fmt.info("Stopping...")
            return

        load_info = pipeline.run(resource(), **self.script.run_arguments)
        return load_info

    def check_if_runnable(self, pipeline_members: PipelineMembers) -> None:
        if not pipeline_members["pipelines"]:
            raise RunnerError(f"No pipelines found in {self.params.script_path}")

        if not pipeline_members["sources"]:
            raise RunnerError(f"Could not find any source or resource {self.params.script_path}")

    def check_workdir(self):
        if self.current_dir != self.script.workdir:
            fmt.warning(
                "Current working directory is different from the pipeline script"
                f" {self.script.workdir}"
            )
