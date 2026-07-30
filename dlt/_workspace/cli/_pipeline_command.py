import yaml
from typing import Any, Sequence, Tuple
import dlt

from dlt.common.json import json
from dlt.common.pipeline import TSourceState
from dlt.common.destination.reference import TDestinationReferenceArg
from dlt.common.schema.utils import (
    group_tables_by_resource,
    has_table_seen_data,
    is_complete_column,
    remove_defaults,
)
from dlt.common.storages import PackageStorage

from dlt.extract.state import resource_state
from dlt.pipeline.exceptions import CannotRestorePipelineException
from dlt._workspace.cli import echo as fmt, utils
from dlt._workspace.cli.exceptions import CliCommandException, CliCommandInnerException
from dlt._workspace.cli._urls import DLT_PIPELINE_COMMAND_DOCS_URL  # noqa: F401
from dlt._workspace.cli._pipeline_command_helpers import (
    abort_packages,
    display_package_jobs,
    display_package_row_counts,
    drop_pipeline,
    fail_package_job,
)


def list_pipelines(pipelines_dir: str = None, verbosity: int = 1) -> None:
    """List all pipelines in the given directory, sorted by last run time.

    Args:
        pipelines_dir: Directory containing pipeline folders. If None, uses the default
                      dlt pipelines directory.
        verbosity: Controls output detail level:
                   - 0: Only show count summary
                   - 1+: Show full list with last run times
    """
    pipelines_dir, pipelines, _ = utils.list_local_pipelines(pipelines_dir)

    if len(pipelines) > 0:
        if verbosity == 0:
            fmt.echo(
                "%s pipelines found in %s. Use %s to see the full list."
                % (len(pipelines), fmt.bold(pipelines_dir), fmt.bold("-v"))
            )
            return
        else:
            fmt.echo("%s pipelines found in %s" % (len(pipelines), fmt.bold(pipelines_dir)))
    else:
        fmt.echo("No pipelines found in %s" % fmt.bold(pipelines_dir))
        return

    # pipelines are already sorted by timestamp (newest first) from get_local_pipelines
    for pipeline_info in pipelines:
        name = pipeline_info["name"]
        timestamp = pipeline_info["timestamp"]
        time_str = utils.date_from_timestamp_with_ago(timestamp)
        fmt.echo(
            "%s %s" % (fmt.style(name, fg="green"), fmt.style(f"(last run: {time_str})", fg="cyan"))
        )


def pipeline_command(
    operation: str,
    pipeline_name: str,
    pipelines_dir: str,
    verbosity: int,
    dataset_name: str = None,
    destination: TDestinationReferenceArg = None,
    **command_kwargs: Any,
) -> None:
    if operation == "list":
        list_pipelines(pipelines_dir)
        return

    # we may open the dashboard for a pipeline without checking if it exists
    if operation == "show":
        if not utils.is_hub_available():
            return

        from dlt._workspace.helpers.dashboard.runner import run_dashboard

        run_dashboard(pipeline_name, edit=command_kwargs.get("edit"), pipelines_dir=pipelines_dir)
        return

    try:
        if verbosity > 0:
            fmt.echo("Attaching to pipeline %s" % fmt.bold(pipeline_name))
        pipeline = dlt.attach(pipeline_name=pipeline_name, pipelines_dir=pipelines_dir)
    except CannotRestorePipelineException as exc:
        if operation not in {"sync", "drop"}:
            raise
        fmt.warning(str(exc))
        if not fmt.confirm(
            "Do you want to attempt to restore the pipeline state from destination?",
            default=False,
        ):
            return
        destination = destination or fmt.text_input(
            f"Enter destination name for pipeline {fmt.bold(pipeline_name)}"
        )
        dataset_name = dataset_name or fmt.text_input(
            f"Enter dataset name for pipeline {fmt.bold(pipeline_name)}"
        )
        pipeline = dlt.pipeline(
            pipeline_name,
            pipelines_dir,
            destination=destination,
            dataset_name=dataset_name,
        )
        pipeline.sync_destination()
        if pipeline.first_run:
            # remote state was not found
            pipeline._wipe_working_folder()
            fmt.error(
                f"Pipeline {pipeline_name} was not found in dataset {dataset_name} in {destination}"
            )
            return
        if operation == "sync":
            return  # No need to sync again

    def _display_pending_packages() -> Tuple[Sequence[str], Sequence[str]]:
        extracted_packages = pipeline.list_extracted_load_packages()
        if extracted_packages:
            fmt.echo(
                "Has %s extracted packages ready to be normalized with following load ids:"
                % fmt.bold(str(len(extracted_packages)))
            )
            for load_id in extracted_packages:
                fmt.echo(load_id)
        norm_packages = pipeline.list_normalized_load_packages()
        if norm_packages:
            fmt.echo(
                "Has %s normalized packages ready to be loaded with following load ids:"
                % fmt.bold(str(len(norm_packages)))
            )
            for load_id in norm_packages:
                fmt.echo(load_id)
            # load first (oldest) package
            first_package_info = pipeline.get_load_package_info(norm_packages[0])
            if PackageStorage.is_package_partially_loaded(first_package_info):
                fmt.warning(
                    "This package is partially loaded. Data in the destination may be modified."
                )
            fmt.echo()
        return extracted_packages, norm_packages

    # launch mcp server before outputting to stdout
    if operation == "mcp":
        if not utils.is_hub_available():
            return

        from dlt._workspace.mcp import PipelineMCP

        if command_kwargs["stdio"]:
            transport = "stdio"
        elif command_kwargs.get("sse"):
            transport = "sse"
        else:
            transport = "streamable-http"
        if transport != "stdio":
            fmt.echo("Starting dlt MCP server", err=True)
        mcp = PipelineMCP(pipeline.pipeline_name, command_kwargs["port"])
        mcp.run(transport=transport)

        return

    fmt.echo(
        "Found pipeline %s in %s"
        % (fmt.bold(pipeline.pipeline_name), fmt.bold(pipeline.pipelines_dir))
    )

    if operation == "info":
        state: TSourceState = pipeline.state  # type: ignore
        fmt.echo("Synchronized state:")
        for key, value in state.items():
            if not isinstance(value, dict):
                fmt.echo("%s: %s" % (fmt.style(key, fg="green"), value))
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
        for key, value in state["_local"].items():
            if isinstance(value, dict):
                # show run context id
                if key == "last_run_context":
                    key = "last_run_context['uri']"
                    value = value["uri"]
                else:
                    value = None
            if value is not None:
                fmt.echo("%s: %s" % (fmt.style(key, fg="green"), value))
        fmt.echo()
        if pipeline.default_schema_name is None:
            fmt.warning("This pipeline does not have a default schema")
        else:
            is_single_schema = len(pipeline.schema_names) == 1
            for schema_name in pipeline.schema_names:
                fmt.echo("Resources in schema: %s" % fmt.bold(schema_name))
                schema = pipeline.schemas[schema_name]
                data_tables = {table["name"]: table for table in schema.data_tables()}
                for resource_name, resource_tables in group_tables_by_resource(data_tables).items():
                    res_state_slots = 0
                    if sources_state:
                        source_state = (
                            next(iter(sources_state.items()))[1]
                            if is_single_schema
                            else sources_state.get(schema_name)
                        )
                        if source_state:
                            resource_state_ = resource_state(resource_name, source_state)
                            res_state_slots = len(resource_state_)
                    fmt.echo(
                        "%s with %s table(s) and %s resource state slot(s)"
                        % (
                            fmt.bold(resource_name),
                            fmt.bold(str(len(resource_tables))),
                            fmt.bold(str(res_state_slots)),
                        )
                    )
                    if verbosity > 0:
                        for table in resource_tables:
                            incomplete_columns = len(
                                [
                                    col
                                    for col in table["columns"].values()
                                    if not is_complete_column(col)
                                ]
                            )
                            fmt.echo(
                                "\t%s table %s column(s) %s %s"
                                % (
                                    fmt.bold(table["name"]),
                                    fmt.bold(str(len(table["columns"]))),
                                    (
                                        fmt.style("received data", fg="green")
                                        if has_table_seen_data(table)
                                        else fmt.style("not yet received data", fg="yellow")
                                    ),
                                    (
                                        fmt.style(
                                            f"{incomplete_columns} incomplete column(s)",
                                            fg="yellow",
                                        )
                                        if incomplete_columns > 0
                                        else ""
                                    ),
                                )
                            )
        fmt.echo()
        fmt.echo("Working dir content:")
        _display_pending_packages()
        loaded_packages = pipeline.list_completed_load_packages()
        if loaded_packages:
            fmt.echo(
                "Has %s completed load packages with following load ids:"
                % fmt.bold(str(len(loaded_packages)))
            )
            for load_id in loaded_packages:
                fmt.echo(load_id)
            fmt.echo()
        trace = pipeline.last_trace
        if trace is None or len(trace.steps) == 0:
            fmt.echo("Pipeline does not have last run trace.")
        else:
            fmt.echo(
                "Pipeline has last run trace. Use '%s' to inspect "
                % fmt.cli_cmd(f"pipeline {pipeline_name} trace")
            )

    if operation == "trace":
        trace = pipeline.last_trace
        if trace is None or len(trace.steps) == 0:
            fmt.warning("Pipeline does not have last run trace.")
            return
        fmt.echo(trace.asstr(verbosity))

    if operation == "failed-jobs":
        completed_loads = pipeline.list_completed_load_packages()
        normalized_loads = pipeline.list_normalized_load_packages()
        for load_id in completed_loads + normalized_loads:  # type: ignore
            fmt.echo("Checking failed jobs in load id '%s'" % fmt.bold(load_id))
            failed_jobs = pipeline.list_failed_jobs_in_package(load_id)
            if failed_jobs:
                for failed_job in pipeline.list_failed_jobs_in_package(load_id):
                    fmt.echo(
                        "JOB: %s(%s)"
                        % (
                            fmt.bold(failed_job.job_file_info.job_id()),
                            fmt.bold(failed_job.job_file_info.table_name),
                        )
                    )
                    fmt.echo("JOB file type: %s" % fmt.bold(failed_job.job_file_info.file_format))
                    fmt.echo("JOB file path: %s" % fmt.bold(failed_job.file_path))
                    if verbosity > 0:
                        fmt.echo(failed_job.asstr(verbosity))
                    fmt.secho(failed_job.failed_message, fg="red")
                    fmt.echo()
            else:
                fmt.echo("No failed jobs found")

    if operation in ("abort-packages", "drop-pending-packages"):
        if operation == "drop-pending-packages":
            fmt.warning(
                "drop-pending-packages is deprecated and now aborts packages. Use `%s` instead."
                % fmt.cli_cmd(f"pipeline {pipeline_name} abort-packages")
            )
        abort_packages(pipeline, pipeline_name)

    if operation == "sync":
        if fmt.confirm(
            "About to drop the local state of the pipeline and reset all the schemas. The"
            " destination state, data and schemas are left intact. Proceed?",
            default=False,
        ):
            fmt.echo("Dropping local state")
            pipeline = pipeline.drop()
            fmt.echo("Restoring from destination")
            pipeline.sync_destination()
            if pipeline.first_run:
                # remote state was not found
                pipeline._wipe_working_folder()
                fmt.error(
                    f"Pipeline {pipeline_name} was not found in dataset {dataset_name} in"
                    f" {destination}"
                )
                return

    if operation == "load-package":
        load_id = command_kwargs.get("load_id")
        action = command_kwargs.get("action") or "info"
        job_arg = command_kwargs.get("job")
        if not load_id:
            packages = sorted(pipeline.list_extracted_load_packages())
            if not packages:
                packages = sorted(pipeline.list_normalized_load_packages())
            if not packages:
                packages = sorted(pipeline.list_completed_load_packages())
            if packages:
                load_id = packages[-1]
            elif action != "row-counts":
                raise CliCommandInnerException(
                    "pipeline", "There are no load packages for that pipeline"
                )

        if action == "row-counts":
            if not load_id:
                raise CliCommandInnerException("pipeline", "Provide a load-id to count rows for")
            display_package_row_counts(pipeline, load_id)
            return

        if action == "abort":
            # abort this package and all newer ones; older packages stay intact and loadable
            abort_packages(pipeline, pipeline_name, load_id)
            return

        if action == "fail-job":
            if not job_arg:
                raise CliCommandInnerException(
                    "pipeline",
                    "Provide a job id to fail, e.g. `load-package <load-id> fail-job <job-id>`",
                )
            fail_package_job(pipeline, load_id, job_arg)
            return

        package_info = pipeline.get_load_package_info(load_id)
        fmt.echo(
            "Package %s found in %s" % (fmt.bold(load_id), fmt.bold(package_info.package_path))
        )
        if action == "job":
            if not job_arg:
                raise CliCommandInnerException(
                    "pipeline",
                    "Provide a job name filter, e.g. `load-package <load-id> job <pattern>`",
                )
            display_package_jobs(pipeline, load_id, package_info, job_arg, verbosity)
            return
        fmt.echo(package_info.asstr(verbosity))
        if PackageStorage.is_package_partially_loaded(package_info):
            fmt.warning(PackageStorage.partially_loaded_warning(load_id))
        if len(package_info.schema_update) > 0:
            if verbosity == 0:
                fmt.echo("Add -v option to see schema update. Note that it could be large.")
            else:
                tables = remove_defaults({"tables": package_info.schema_update})
                fmt.echo(fmt.bold("Schema update:"))
                fmt.echo(
                    yaml.dump(
                        tables,
                        allow_unicode=True,
                        default_flow_style=False,
                        sort_keys=False,
                    )
                )

    if operation == "schema":
        if not pipeline.default_schema_name:
            fmt.warning("Pipeline does not have a default schema")
        else:
            fmt.echo("Found schema with name %s" % fmt.bold(pipeline.default_schema_name))
        format_ = command_kwargs.get("format")
        remove_defaults_ = command_kwargs.get("remove_defaults")
        default_schema = pipeline.default_schema
        export = utils.fetch_schema_export(
            default_schema, format_=format_, remove_defaults=remove_defaults_
        )
        fmt.echo(export["content"])

    if operation == "drop":
        drop_pipeline(
            pipeline,
            resources=command_kwargs.get("resources", ()),
            schema_name=command_kwargs.get("schema_name"),
            state_paths=command_kwargs.get("state_paths", ()),
            drop_all=command_kwargs.get("drop_all", False),
            state_only=command_kwargs.get("state_only", False),
        )


def pipeline_command_wrapper(
    operation: str, pipeline_name: str, pipelines_dir: str, verbosity: int, **command_kwargs: Any
) -> None:
    try:
        pipeline_command(operation, pipeline_name, pipelines_dir, verbosity, **command_kwargs)
    except CannotRestorePipelineException as ex:
        fmt.secho(str(ex), err=True, fg="red")
        fmt.secho(
            "Try command %s to restore the pipeline state from destination"
            % fmt.bold(fmt.cli_cmd(f"pipeline {pipeline_name} sync"))
        )
        raise CliCommandException(error_code=-2)
