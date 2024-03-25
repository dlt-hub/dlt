import os
import importlib
import typing as t
import sys

from contextlib import contextmanager
from types import ModuleType
from typing_extensions import get_args

import dlt

from dlt.cli import echo as fmt
from dlt.cli.utils import parse_init_script
from dlt.common.cli.runner.errors import RunnerError
from dlt.common.cli.runner.types import PipelineMembers, RunnerParams
from dlt.common.destination.capabilities import TLoaderFileFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.schema.typing import TSchemaEvolutionMode, TWriteDisposition
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

        self.has_pipeline_auto_runs: bool = False
        self.supported_formats = get_args(TLoaderFileFormat)
        self.supported_evolution_modes = get_args(TSchemaEvolutionMode)
        self.supported_write_disposition = get_args(TWriteDisposition)

        # Now we need to patch and store pipeline code
        visitor = parse_init_script(
            COMMAND_NAME,
            self.script_contents,
            self.module_name,
        )
        self.source_code = visitor.source

    def load_module(self, script_path: str) -> ModuleType:
        """Loads pipeline module from a given location"""
        with self.expect_no_pipeline_runs():
            spec = importlib.util.spec_from_file_location(self.module_name, script_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[self.module_name] = module
            spec.loader.exec_module(module)
            if self.has_pipeline_auto_runs:
                raise RunnerError(
                    "Please move all pipeline.run calls inside __main__ or remove them"
                )

            return module

    @contextmanager
    def expect_no_pipeline_runs(self):  # type: ignore[no-untyped-def]
        """Monkey patch pipeline.run during module loading
        Restore it once importing is done
        """
        original_run = dlt.Pipeline.run

        def noop(*args, **kwargs) -> LoadInfo:  # type: ignore
            self.has_pipeline_auto_runs = True

        dlt.Pipeline.run = noop  # type: ignore

        yield

        dlt.Pipeline.run = original_run  # type: ignore

    @property
    def pipeline_module(self) -> ModuleType:
        self.module = self.load_module(self.params.script_path)
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
        We populate sources, pipelines with relevant instances by their variable name.

        Resources and sources must be initialized and bound beforehand.
        """
        members: PipelineMembers = {
            "pipelines": {},
            "sources": {},
        }
        for name, value in self.pipeline_module.__dict__.items():
            # skip modules and private stuff
            if isinstance(value, ModuleType) or name.startswith("_"):
                continue

            if isinstance(value, dlt.Pipeline):
                members["pipelines"][name] = value

            if isinstance(value, DltSource):
                members["sources"][name] = value

            if isinstance(value, DltResource):
                if value._args_bound:
                    members["sources"][name] = value
                else:
                    fmt.echo(fmt.info_style(f"Resource: {value.name} is not bound, skipping."))

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

    def validate_arguments(self) -> None:
        """Validates and checks"""
        supported_args = {
            "destination",
            "staging",
            "credentials",
            "table_name",
            "write_disposition",
            "dataset_name",
            "primary_key",
            "schema_contract",
            "loader_file_format",
        }
        errors = []
        arguments = self.run_arguments
        for arg_name, _ in arguments.items():
            if arg_name not in supported_args:
                errors.append(f"Invalid argument {arg_name}")

        if (
            "write_disposition" in arguments
            and arguments["write_disposition"] not in self.supported_write_disposition
        ):
            errors.append(
                "Invalid write disposition, select one of"
                f" {'|'.join(self.supported_write_disposition)}"
            )
        if (
            "loader_file_format" in arguments
            and arguments["loader_file_format"] not in self.supported_formats
        ):
            errors.append(
                f"Invalid loader file format, select one of {'|'.join(self.supported_formats)}"
            )
        if (
            "schema_contract" in arguments
            and arguments["schema_contract"] not in self.supported_evolution_modes
        ):
            errors.append(
                "Invalid schema_contract mode, select one of"
                f" {'|'.join(self.supported_evolution_modes)}"
            )

        if errors:
            all_errors = "\n".join(errors)
            raise RunnerError(f"One or more arguments are invalid:\n{all_errors}")
