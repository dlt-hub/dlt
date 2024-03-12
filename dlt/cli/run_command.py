from itertools import chain
import os
import inspect as it
import typing as t

from collections import defaultdict
from importlib import util as im
from types import ModuleType

import dlt

from dlt.cli import echo as fmt
from dlt.cli.utils import track_command
from dlt.sources import DltResource, DltSource
from typing_extensions import TypedDict

original_run = dlt.Pipeline.run
def noop(*args, **kwards):
    pass

dlt.Pipeline.run = noop

class PipelineMember(TypedDict):
    name: str
    instance: t.Union[dlt.Pipeline, DltResource, DltSource]


class DltInventory(TypedDict):
    pipelines: t.List[PipelineMember]
    sources: t.List[PipelineMember]
    resources: t.List[PipelineMember]


@track_command("run", False)
def run_pipeline_command(
    module: str,
    pipeline: t.Optional[str] = None,
    source: t.Optional[str] = None,
    args: t.Optional[str] = None,
):
    pick_first_pipeline_and_source = not pipeline and not source
    pipeline_module = load_module(module)

    pipeline_members = extract_dlt_members(pipeline_module)
    errors = validate_mvp_pipeline(pipeline_members)
    if errors:
        for err in errors:
            fmt.echo(err)
        return -1

    run_options = prepare_run_arguments(args)
    dlt.Pipeline.run = original_run
    if pick_first_pipeline_and_source:
        fmt.echo(
            "Neiter of pipeline name or source were specified, "
            "we will pick first pipeline and a source to run"
        )

        pipeline_instance = pipeline_members["pipelines"][0]["instance"]
        if resources := pipeline_members["resources"]:
            data_source = resources[0]["instance"]
        else:
            data_source = pipeline_members["sources"][0]["instance"]

        pipeline_instance.run(data_source(), **run_options)
    else:
        pipeline_instance = get_pipeline_by_name(pipeline, pipeline_members)
        resource_instance = get_resource_by_name(source, pipeline_members)
        pipeline_instance.run(resource_instance(), **run_options)


def load_module(module_path: str) -> ModuleType:
    if not os.path.exists(module_path):
        fmt.echo(f'Module or file "{module_path}" does not exist')
        return -1

    module_name = module_path[:]
    if module_name.endswith(".py"):
        module_name = module_path[:-3]

    spec = im.spec_from_file_location(module_name, module_path)
    module = spec.loader.load_module(module_name)

    return module


def extract_dlt_members(module: ModuleType) -> DltInventory:
    variables: DltInventory = defaultdict(list)
    for name, value in it.getmembers(module):
        # We would like to skip private variables and other modules
        if not it.ismodule(value) and not name.startswith("_"):
            if isinstance(value, dlt.Pipeline):
                variables["pipelines"].append(
                    {
                        "name": value.pipeline_name,
                        "instance": value,
                    }
                )

            if isinstance(value, DltSource):
                variables["sources"].append(
                    {
                        "name": value.name,
                        "instance": value,
                    }
                )

            if isinstance(value, DltResource):
                variables["resources"].append(
                    {
                        "name": value.name,
                        "instance": value,
                    }
                )

    return variables


def validate_mvp_pipeline(pipeline_memebers: DltInventory) -> t.List[str]:
    errors = []
    if not pipeline_memebers.get("pipelines"):
        errors.append("Could not find any pipeline in the given module")

    if not pipeline_memebers.get("resources") and not pipeline_memebers.get("sources"):
        errors.append("Could not find any source or resource in the given module")

    return errors


def prepare_run_arguments(arglist: t.List[str]) -> t.Dict[str, str]:
    run_options = {}
    for arg in arglist:
        arg_name, value = arg.split("=")
        run_options[arg_name] = value

    return run_options


def get_pipeline_by_name(pipeline_name: str, members: DltInventory) -> t.Optional[dlt.Pipeline]:
    for pipeline_item in members["pipelines"]:
        if pipeline_item["name"] == pipeline_name:
            return pipeline_item["instace"]

    return None


def get_resource_by_name(
    resource_name: str, members: DltInventory
) -> t.Optional[t.Union[DltResource, DltSource]]:
    for source_item in chain(members["resources"], members["sources"]):
        if source_item["name"] == resource_name:
            return source_item["instace"]
