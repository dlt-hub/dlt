import typing as t

from dlt.common.cli.runner.inquirer import Inquirer
from dlt.common.pipeline import LoadInfo
from dlt.common.cli.runner.pipeline_script import PipelineScript
from dlt.common.cli.runner.types import RunnerParams


class PipelineRunner:
    def __init__(self, params: RunnerParams) -> None:
        self.params = params
        self.script = PipelineScript(params)
        self.params.pipeline_workdir = self.script.workdir
        self.inquirer = Inquirer(self.params, self.script.pipeline_members)

    def __enter__(self) -> t.Self:
        self.inquirer.preflight_checks()
        self.inquirer.check_if_runnable()
        self.pipeline, self.resource = self.inquirer.maybe_ask()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def run(self) -> LoadInfo:
        load_info = self.pipeline.run(self.resource(), **self.script.run_arguments)
        return load_info
