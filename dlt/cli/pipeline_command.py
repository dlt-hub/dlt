import yaml
from typing import Any
import dlt
from dlt.cli.exceptions import CliCommandException

from dlt.common import json
from dlt.common.pipeline import resource_state, get_dlt_pipelines_dir, TSourceState
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.common.runners import Venv
from dlt.common.runners.stdout import iter_stdout
from dlt.common.schema.utils import group_tables_by_resource, remove_defaults
from dlt.common.storages.file_storage import FileStorage
from dlt.common.typing import DictStrAny
from dlt.pipeline.helpers import DropCommand
from dlt.pipeline.exceptions import CannotRestorePipelineException

from dlt.cli import echo as fmt

DLT_PIPELINE_COMMAND_DOCS_URL = "https://dlthub.com/docs/reference/command-line-interface"


def pipeline_command(operation: str, pipeline_name: str, pipelines_dir: str, verbosity: int, dataset_name: str = None, destination: TDestinationReferenceArg = None, **command_kwargs: Any) -> None:
    if operation == "list":
        pipelines_dir = pipelines_dir or get_dlt_pipelines_dir()
        storage = FileStorage(pipelines_dir)
        dirs = storage.list_folder_dirs(".", to_root=False)
        if len(dirs) > 0:
            fmt.echo("%s pipelines found in %s" % (len(dirs), fmt.bold(pipelines_dir)))
        else:
            fmt.echo("No pipelines found in %s" % fmt.bold(pipelines_dir))
        for _dir in dirs:
            fmt.secho(_dir, fg="green")
        return

    try:
        p = dlt.attach(pipeline_name=pipeline_name, pipelines_dir=pipelines_dir)
    except CannotRestorePipelineException as e:
        if operation not in {"sync", "drop"}:
            raise
        fmt.warning(str(e))
        if not fmt.confirm("Do you want to attempt to restore the pipeline state from destination?", default=False):
            return
        destination = destination or fmt.text_input(f"Enter destination name for pipeline {fmt.bold(pipeline_name)}")
        dataset_name = dataset_name or fmt.text_input(f"Enter dataset name for pipeline {fmt.bold(pipeline_name)}")
        p = dlt.pipeline(pipeline_name, pipelines_dir, destination=destination, dataset_name=dataset_name)
        p.sync_destination()
        if p.first_run:
            # remote state was not found
            p._wipe_working_folder()
            fmt.error(f"Pipeline {pipeline_name} was not found in dataset {dataset_name} in {destination}")
            return
        if operation == "sync":
            return  # No need to sync again

    fmt.echo("Found pipeline %s in %s" % (fmt.bold(p.pipeline_name), fmt.bold(p.pipelines_dir)))

    if operation == "show":
        from dlt.common.runtime import signals
        from dlt.helpers import streamlit_helper

        with signals.delayed_signals():
            venv = Venv.restore_current()
            for line in iter_stdout(venv, "streamlit", "run", streamlit_helper.__file__, pipeline_name):
                fmt.echo(line)

    if operation == "info":
        state: TSourceState = p.state  # type: ignore
        fmt.echo("Synchronized state:")
        for k, v in state.items():
            if not isinstance(v, dict):
                fmt.echo("%s: %s" % (fmt.style(k, fg="green"), v))
        sources_state = state.get("sources")
        if sources_state:
            fmt.echo()
            fmt.secho("sources:", fg="green")
            if verbosity > 0:
                fmt.echo(json.dumps(sources_state, pretty=True))
            else:
                fmt.echo("Add -v option to see sources state. Note that it could be large.")

        fmt.echo()
        fmt.echo("Local state:")
        for k, v in state["_local"].items():
            if not isinstance(v, dict):
                fmt.echo("%s: %s" % (fmt.style(k, fg="green"), v))
        fmt.echo()
        if p.default_schema_name is None:
            fmt.warning("This pipeline does not have a default schema")
        else:
            is_single_schema = len(p.schema_names) == 1
            for schema_name in  p.schema_names:
                fmt.echo("Resources in schema: %s" % fmt.bold(schema_name))
                schema = p.schemas[schema_name]
                data_tables = {t["name"]: t for t in schema.data_tables()}
                for resource_name, tables in group_tables_by_resource(data_tables).items():
                    res_state_slots = 0
                    if sources_state:
                        source_state = next(iter(sources_state.items()))[1] if is_single_schema else sources_state.get(schema_name)
                        if source_state:
                            resource_state_ = resource_state(resource_name, source_state)
                            res_state_slots = len(resource_state_)
                    fmt.echo("%s with %s table(s) and %s resource state slot(s)" % (fmt.bold(resource_name), fmt.bold(str(len(tables))), fmt.bold(str(res_state_slots))))
        fmt.echo()
        fmt.echo("Working dir content:")
        extracted_files = p.list_extracted_resources()
        if extracted_files:
            fmt.echo("Has %s extracted files ready to be normalized" % fmt.bold(str(len(extracted_files))))
        norm_packages = p.list_normalized_load_packages()
        if norm_packages:
            fmt.echo("Has %s load packages ready to be loaded with following load ids:" % fmt.bold(str(len(norm_packages))))
            for load_id in norm_packages:
                fmt.echo(load_id)
            fmt.echo()
        loaded_packages = p.list_completed_load_packages()
        if loaded_packages:
            fmt.echo("Has %s completed load packages with following load ids:" % fmt.bold(str(len(loaded_packages))))
            for load_id in loaded_packages:
                fmt.echo(load_id)
            fmt.echo()
        trace = p.last_trace
        if trace is None or len(trace.steps) == 0:
            fmt.echo("Pipeline does not have last run trace.")
        else:
            fmt.echo("Pipeline has last run trace. Use 'dlt pipeline %s trace' to inspect " % pipeline_name)

    if operation == "trace":
        trace = p.last_trace
        if trace is None or len(trace.steps) == 0:
            fmt.warning("Pipeline does not have last run trace.")
            return
        fmt.echo(trace.asstr(verbosity))

    if operation == "failed-jobs":
        completed_loads = p.list_completed_load_packages()
        normalized_loads = p.list_normalized_load_packages()
        for load_id in completed_loads + normalized_loads:  # type: ignore
            fmt.echo("Checking failed jobs in load id '%s'" % fmt.bold(load_id))
            failed_jobs = p.list_failed_jobs_in_package(load_id)
            if failed_jobs:
                for failed_job in p.list_failed_jobs_in_package(load_id):
                    fmt.echo("JOB: %s(%s)" % (fmt.bold(failed_job.job_file_info.job_id()), fmt.bold(failed_job.job_file_info.table_name)))
                    fmt.echo("JOB file type: %s" % fmt.bold(failed_job.job_file_info.file_format))
                    fmt.echo("JOB file path: %s" % fmt.bold(failed_job.file_path))
                    if verbosity > 0:
                        fmt.echo(failed_job.asstr(verbosity))
                    fmt.secho(failed_job.failed_message, fg="red")
                    fmt.echo()
            else:
                fmt.echo("No failed jobs found")


    if operation == "sync":
        if fmt.confirm("About to drop the local state of the pipeline and reset all the schemas. The destination state, data and schemas are left intact. Proceed?", default=False):
            fmt.echo("Dropping local state")
            p = p.drop()
            fmt.echo("Restoring from destination")
            p.sync_destination()

    if operation == "load-package":
        load_id = command_kwargs.get('load_id')
        if not load_id:
            packages = sorted(p.list_normalized_load_packages())
            if not packages:
                packages = sorted(p.list_completed_load_packages())
            if not packages:
                raise CliCommandException("pipeline", "There are no load packages for that pipeline")
            load_id = packages[-1]

        package_info = p.get_load_package_info(load_id)
        fmt.echo("Package %s found in %s" % (fmt.bold(load_id), fmt.bold(package_info.package_path)))
        fmt.echo(package_info.asstr(verbosity))
        if len(package_info.schema_update) > 0:
            if verbosity == 0:
                print("Add -v option to see schema update. Note that it could be large.")
            else:
                tables = remove_defaults({"tables": package_info.schema_update})  # type: ignore
                fmt.echo(fmt.bold("Schema update:"))
                fmt.echo(yaml.dump(tables, allow_unicode=True, default_flow_style=False, sort_keys=False))

    if operation == "schema":
        if not p.default_schema_name:
            fmt.warning("Pipeline does not have a default schema")
        else:
            fmt.echo("Found schema with name %s" % fmt.bold(p.default_schema_name))
        schema_str = p.default_schema.to_pretty_yaml(remove_defaults=True)
        fmt.echo(schema_str)

    if operation == "drop":
        drop = DropCommand(p, **command_kwargs)
        if drop.is_empty:
            fmt.echo("Could not select any resources to drop and no resource/source state to reset. Use the command below to inspect the pipeline:")
            fmt.echo(f"dlt pipeline -v {p.pipeline_name} info")
            if len(drop.info["warnings"]):
                fmt.echo("Additional warnings are available")
                for warning in drop.info["warnings"]:
                    fmt.warning(warning)
            return

        fmt.echo("About to drop the following data in dataset %s in destination %s:" % (fmt.bold(drop.info["dataset_name"]), fmt.bold(p.destination.__name__)))
        fmt.echo("%s: %s" % (fmt.style("Selected schema", fg="green"), drop.info["schema_name"]))
        fmt.echo("%s: %s" % (fmt.style("Selected resource(s)", fg="green"), drop.info["resource_names"]))
        fmt.echo("%s: %s" % (fmt.style("Table(s) to drop", fg="green"), drop.info["tables"]))
        fmt.echo("%s: %s" % (fmt.style("Resource(s) state to reset", fg="green"), drop.info["resource_states"]))
        fmt.echo("%s: %s" % (fmt.style("Source state path(s) to reset", fg="green"), drop.info["state_paths"]))
        # for k, v in drop.info.items():
        #     fmt.echo("%s: %s" % (fmt.style(k, fg="green"), v))
        for warning in drop.info["warnings"]:
            fmt.warning(warning)
        if fmt.confirm("Do you want to apply these changes?", default=False):
            drop()
