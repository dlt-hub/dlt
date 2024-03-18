import typing as t
from typing_extensions import Self
from types import TracebackType
from dlt.common.cli.runner.inquirer import Inquirer
from dlt.common.pipeline import LoadInfo
from dlt.common.cli.runner.pipeline_script import PipelineScript
from dlt.common.cli.runner.types import RunnerParams
from dlt.extract.resource import DltResource


class PipelineRunner:
    def __init__(self, params: RunnerParams) -> None:
        self.params = params
        self.script = PipelineScript(params)
        self.params.pipeline_workdir = self.script.workdir
        self.inquirer = Inquirer(self.params, self.script.pipeline_members)

    def __enter__(self) -> Self:
        self.inquirer.preflight_checks()
        self.inquirer.check_if_runnable()
        self.pipeline, self.resource = self.inquirer.maybe_ask()
        return self

    def __exit__(
        self,
        exc_type: t.Optional[t.Type[BaseException]] = None,
        exc_value: t.Optional[BaseException] = None,
        traceback: t.Optional[TracebackType] = None,
    ) -> None:
        pass

    def run(self) -> LoadInfo:
        data_resource = self.resource
        if isinstance(self.resource, DltResource) and not self.resource._args_bound:
            data_resource = self.resource()

        load_info = self.pipeline.run(data_resource, **self.script.run_arguments)  # type: ignore[arg-type]
        return load_info
