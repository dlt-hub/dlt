import ast
from collections import defaultdict
import inspect
import os
import typing as t
import importlib
from dataclasses import dataclass
from importlib import machinery as im
from importlib import util as iu
from types import ModuleType

import dlt

from dlt.cli import echo as fmt
from dlt.common.configuration.providers.environ import EnvironProvider
from dlt.common.configuration.providers.toml import ConfigTomlProvider, SecretsTomlProvider
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.sources import DltResource, DltSource
from dlt.cli.utils import parse_init_script
from dlt.common.reflection.utils import evaluate_node_literal
from dlt.reflection import names as rn
from dlt.reflection.script_visitor import PipelineScriptVisitor


@dataclass
class RunnerInventory:
    """This class contains parameters passed to command
    Also it provides two convenience methods to load script contents
    and to get module name from provided `script_path`.
    """

    script_path: str
    pipeline_name: t.Optional[str] = None
    source_name: t.Optional[str] = None
    run_first_pipeline_with_source: t.Optional[bool] = False
    args: t.List[str] = None

    @property
    def script_contents(self) -> str:
        """Loads script contents"""
        with open(self.script_path) as fp:
            return fp.read()

    @property
    def module_name(self) -> str:
        """Strips extension with path and returns filename as modulename"""
        module_name = self.script_path.split(os.sep)[-1]
        if module_name.endswith(".py"):
            module_name = module_name[:-3]

        return module_name

    @property
    def run_arguments(self) -> t.Dict[str, str]:
        run_options = {}
        for arg in self.args or []:
            arg_name, value = arg.split("=")
            run_options[arg_name] = value

        return run_options


class PipelineRunner:
    def __init__(self, inventory: RunnerInventory, visitor: PipelineScriptVisitor) -> None:
        self.inventory = inventory
        self.visitor = visitor
        self.raise_if_not_runnable()
        self.pipeline_source = self.strip_pipeline_runs()
        self.module = self.load_module()
        self.run_options = self.inventory.run_arguments
        self.workdir = os.path.dirname(os.path.abspath(self.inventory.script_path))

    def run(self):
        pick_first = not self.inventory.pipeline_name and not self.inventory.source_name
        if pick_first:
            fmt.echo(
                fmt.style(
                    "Pipeline name and source not specified, "
                    "we will pick first pipeline and a source to run",
                    fg="blue",
                )
            )

        pipeline_name = self.inventory.pipeline_name
        resource_name = self.inventory.source_name
        pipelines = list(self.pipelines.values())
        resources = list(self.visitor.known_sources_resources.keys())
        if pick_first:
            resource = getattr(self.module, resources[0])
            pipeline_instance = pipelines[0]
        else:
            resource = getattr(self.module, resource_name)
            pipeline_instance = self.pipelines[pipeline_name]

        setattr(self.module, f"pipeline_{pipeline_name}", pipeline_instance)

        fmt.echo(
            fmt.style("Pipeline workdir", fg="black", bg="blue")
            + f": {pipeline_instance.working_dir}"
        )

        fmt.echo(f"Runtime options for pipeline: {pipeline_instance.pipeline_name}\n")
        for key, val in self.run_options.items():
            fmt.echo(f"    {fmt.style(key, fg='green')}={val}")

        pipeline_instance.activate()
        load_info = pipeline_instance.run(resource(), **self.run_options)
        fmt.echo("")
        fmt.echo(load_info)

    @property
    def run_nodes(self) -> t.List[ast.AST]:
        """Extract pipeline.run nodes"""
        pipeline_runs = self.visitor.known_calls_with_nodes.get(rn.RUN)
        run_nodes = [run_node for _args, run_node in pipeline_runs]
        return run_nodes

    @property
    def sources(self) -> t.List[str]:
        """Returns source and resource names"""
        return self.visitor.known_sources_resources.keys()

    @property
    def pipelines(self) -> t.Dict[str, dlt.Pipeline]:
        pipeline_arguments: t.List[inspect.BoundArguments] = (
            self.visitor.known_calls.get("pipeline") or []
        )
        pipeline_items = defaultdict(dict)
        for item in pipeline_arguments:
            pipeline_options = {}
            for arg_name, bound_value in item.arguments.items():
                if bound_value is not None:
                    if arg_name == "kwargs":
                        pipeline_options.update(bound_value)
                        continue

                    if hasattr(bound_value, "value"):
                        value = bound_value.value
                    else:
                        value = bound_value
                    pipeline_options[arg_name] = value

            pipeline = dlt.pipeline(**pipeline_options)
            pipeline.working_dir = self.workdir
            pipeline_items[pipeline_options["pipeline_name"]] = pipeline

        return pipeline_items

    def strip_pipeline_runs(self) -> str:
        """Removes all pipeline.run nodes and return patched source code"""
        # Copy original source
        script_lines: t.List[str] = self.visitor.source_lines[:]
        stub = '""" run method replaced """'

        def restore_indent(line: str) -> str:
            indent = ""
            for ch in line:
                if ch == " ":
                    indent += " "
            return indent

        for node in self.run_nodes:
            # if it is a one liner
            if node.lineno == node.end_lineno:
                script_lines[node.lineno] = None
            else:
                line_of_code = script_lines[node.lineno - 1]
                script_lines[node.lineno - 1] = restore_indent(line_of_code) + stub
                start = node.lineno + 1
                while start <= node.end_lineno:
                    script_lines[start - 1] = None
                    start += 1

        result = [line.rstrip() for line in script_lines if line]
        return "\n".join(result)

    def load_module(self) -> ModuleType:
        spec = im.ModuleSpec(name=self.inventory.module_name, loader=None)
        module = iu.module_from_spec(spec)
        exec(self.pipeline_source, module.__dict__)
        return module

    def raise_if_not_runnable(self) -> None:
        if not self.visitor.known_calls.get("pipeline"):
            raise RuntimeError("Could not find any pipeline in the given module")

        if not self.visitor.known_sources_resources:
            raise RuntimeError("Could not find any source or resource in the given module")


class DltRunnerEnvironment:
    def __init__(self, inventory: RunnerInventory) -> None:
        self.inventory = inventory
        visitor: PipelineScriptVisitor = parse_init_script(
            "run",
            self.inventory.script_contents,
            self.inventory.module_name,
        )
        self.runner = PipelineRunner(inventory, visitor)

    def run(self) -> None:
        self.runner.run()
