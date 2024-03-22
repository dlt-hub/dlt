import os
import importlib
import tempfile
import typing as t
import sys

from collections import defaultdict
from types import ModuleType

import dlt

from dlt.cli.utils import parse_init_script
from dlt.common.cli.runner.source_patcher import SourcePatcher
from dlt.common.cli.runner.types import PipelineMembers, RunnerParams
from dlt.sources import DltResource, DltSource


COMMAND_NAME: t.Final[str] = "run"


class PipelineScript:
    """Handles pipeline source code and prepares it to run

    Things it does

    1. Reads the source code,
    2. Stubs all pipeline.run calls,
    3. Creates a temporary module in the location of pipeline file,
    4. Imports rewritten module,
    5. Prepares module name,
    6. Exposes ready to use module instance.
    """

    def __init__(self, params: RunnerParams) -> None:
        self.params = params
        """This means that user didn't specify pipeline name or resource and source name

        And we need to check if there is 1 pipeline and 1 resource or source to run right
        away if there are multiple resources and sources then we need to provide a CLI prompt
        """
        self.workdir = os.path.dirname(os.path.abspath(params.script_path))
        """Directory in which pipeline script lives"""

        # Now we need to patch and store pipeline code
        visitor = parse_init_script(
            COMMAND_NAME,
            self.script_contents,
            self.module_name,
        )
        patcher = SourcePatcher(visitor)
        self.source_code = patcher.patch()

    def load_module(self, script_path: str) -> ModuleType:
        """Loads pipeline module from a given location"""
        spec = importlib.util.spec_from_file_location(self.module_name, script_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[self.module_name] = module
        spec.loader.exec_module(module)
        return module

    @property
    def pipeline_module(self) -> ModuleType:
        with tempfile.NamedTemporaryFile(
            mode="w+",
            dir=self.workdir,
            prefix="pipeline_",
            suffix=".py",
        ) as tm:
            tm.write(self.source_code)
            tm.flush()
            self.module = self.load_module(tm.name)

        return self.module

    @property
    def script_contents(self) -> str:
        """Loads script contents"""
        with open(self.params.script_path, encoding="utf-8") as fp:
            return fp.read()

    @property
    def module_name(self) -> str:
        """Strips extension with path and returns filename as modulename"""
        module_name = self.params.script_path.split(os.sep)[-1]
        if module_name.endswith(".py"):
            module_name = module_name[:-3]

        return module_name

    @property
    def pipeline_members(self) -> PipelineMembers:
        """Inspect the module and return pipelines with resources and sources
        We populate sources, pipelines with relevant instances by their name,
        also put every dlt specific instance under "by_alias" collection.

        It is done because users might pass pipeline, source or resource name
        by their variable names, so this is an extra step to make sure that we
        locate corrent pipeline, source or resource instance before showing an error.
        """
        members: PipelineMembers = defaultdict(dict)  # type: ignore[assignment]
        for name, value in self.pipeline_module.__dict__.items():
            # skip modules and private stuff
            if isinstance(value, ModuleType) or name.startswith("_"):
                continue

            if isinstance(value, dlt.Pipeline):
                members["by_alias"][name] = value
                members["pipelines"][value.pipeline_name] = value

            if isinstance(value, (DltResource, DltSource)):
                members["by_alias"][name] = value
                members["sources"][value.name] = value

        return members

    @property
    def run_arguments(self) -> t.Dict[str, str]:
        run_options: t.Dict[str, str] = {}
        if not self.params.args:
            return run_options

        for arg in self.params.args:
            arg_name, value = arg.split("=")
            run_options[arg_name] = value

        return run_options
