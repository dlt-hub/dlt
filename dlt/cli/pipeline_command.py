import yaml
import dlt
from dlt.cli.exceptions import CliCommandException

from dlt.common import json
from dlt.common.pipeline import get_dlt_pipelines_dir, TSourceState
from dlt.common.runners import Venv
from dlt.common.runners.stdout import iter_stdout
from dlt.common.schema.utils import remove_defaults
from dlt.common.storages.file_storage import FileStorage

from dlt.cli import echo as fmt


def pipeline_command(operation: str, pipeline_name: str, pipelines_dir: str, verbosity: int, load_id: str = None) -> None:
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

    p = dlt.attach(pipeline_name=pipeline_name, pipelines_dir=pipelines_dir)
    fmt.echo("Found pipeline %s in %s" % (fmt.bold(p.pipeline_name), fmt.bold(p.pipelines_dir)))

    if operation == "show":
        from dlt.helpers import streamlit
        venv = Venv.restore_current()
        for line in iter_stdout(venv, "streamlit", "run", streamlit.__file__, pipeline_name):
            fmt.echo(line)

    if operation == "info":
        state: TSourceState = p.state  # type: ignore
        fmt.echo("Synchronized state:")
        for k, v in state.items():
            if not isinstance(v, dict):
                fmt.echo("%s: %s" % (fmt.style(k, fg="green"), v))
        if state.get("sources"):
            fmt.echo()
            fmt.secho("sources:", fg="green")
            if verbosity > 0:
                fmt.echo(json.dumps(state["sources"], pretty=True))
            else:
                fmt.echo("Add -v option to see sources state. Note that it could be large.")

        fmt.echo()
        fmt.echo("Local state:")
        for k, v in state["_local"].items():
            if not isinstance(v, dict):
                fmt.echo("%s: %s" % (fmt.style(k, fg="green"), v))
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
            p = p.drop()
            p.sync_destination()

    if operation == "load-package":
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
                tables = remove_defaults({"tables": package_info.schema_update})
                fmt.echo(fmt.bold("Schema update:"))
                fmt.echo(yaml.dump(tables, allow_unicode=True, default_flow_style=False, sort_keys=False))

    if operation == "schema":
        if not p.default_schema_name:
            fmt.warning("Pipeline does not have a default schema")
        else:
            fmt.echo("Found schema with name %s" % fmt.bold(p.default_schema_name))
        schema_str = p.default_schema.to_pretty_yaml(remove_defaults=True)
        fmt.echo(schema_str)
