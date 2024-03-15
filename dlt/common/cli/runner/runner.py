import typing as t

from dlt.cli import echo as fmt
from dlt.common.cli.runner.inquirer import Inquirer
from dlt.common.pipeline import LoadInfo
from dlt.common.cli.runner.errors import FriendlyExit, RunnerError
from dlt.common.cli.runner.pipeline_script import PipelineScript
from dlt.common.cli.runner.types import PipelineMembers, RunnerInventory


class PipelineRunner:
    def __init__(self, inventory: RunnerInventory) -> None:
        self.inventory = inventory
        self.script = PipelineScript(inventory)
        self.yolo = self.inventory.pipeline_name is None and self.inventory.source_name is None

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
            raise RunnerError(f"No pipelines found in {self.inventory.script_path}")

        if not pipeline_members["sources"]:
            raise RunnerError(f"Could not find any source or resource {self.inventory.script_path}")
