import os
import dlt

from dlt.common import json
from dlt.common.pipeline import get_dlt_pipelines_dir
from dlt.common.runners import Venv
from dlt.common.runners.stdout import iter_stdout
from dlt.common.storages.file_storage import FileStorage

from dlt.cli import echo as fmt
from dlt.pipeline.state import TSourceState


def pipeline_command(operation: str, pipeline_name: str, pipelines_dir: str, verbose: bool) -> None:
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
        if "sources" in state and state["sources"]:
            fmt.echo()
            fmt.secho("sources:", fg="green")
            if verbose:
                fmt.echo(json.dumps(state["sources"], pretty=True))
            else:
                print("Add -v option to see sources state. Note that it could be large.")

        fmt.echo()
        fmt.echo("Local state:")
        for k, v in state["_local"].items():
            if not isinstance(v, dict):
                fmt.echo("%s: %s" % (fmt.style(k, fg="green"), v))

    if operation == "failed_loads":
        completed_loads = p.list_completed_load_packages()
        for load_id in completed_loads:
            fmt.echo("Checking failed jobs in load id '%s'" % fmt.bold(load_id))
            for job, failed_message in p.list_failed_jobs_in_package(load_id):
                fmt.echo("JOB: %s" % fmt.bold(os.path.abspath(job)))
                fmt.secho(failed_message, fg="red")

    if operation == "sync":
        if fmt.confirm("About to drop the local state of the pipeline and reset all the schemas. The destination state, data and schemas are left intact. Proceed?", default=False):
            p = p.drop()
            p.sync_destination()
