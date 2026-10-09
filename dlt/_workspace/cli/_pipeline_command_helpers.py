"""Display and action helpers for the `dlt pipeline` CLI command."""

import os
import contextlib
from typing import Iterable, List, Optional, Sequence, Union

import dlt

from dlt.common.destination.exceptions import (
    DestinationUndefinedEntity,
    SqlClientNotAvailable,
)
from dlt.common.jsonpath import TAnyJsonPath
from dlt.common.schema.typing import C_DLT_LOADS_TABLE_LOAD_ID, TSimpleRegex
from dlt.common.schema.utils import get_nested_tables, get_write_disposition, is_nested_table
from dlt.common.storages import PackageStorage, ParsedLoadJobFileName
from dlt.common.storages.exceptions import LoadPackageNotFound
from dlt.common.storages.load_package import LoadPackageInfo, PARTIAL_LOAD_DOCS_URL
from dlt.pipeline.helpers import pipeline_drop
from dlt.pipeline.exceptions import PipelineConfigMissing
from dlt._workspace.cli import echo as fmt
from dlt._workspace.cli.exceptions import CliCommandInnerException


def echo_job_list(label: str, jobs: Sequence[str], indent: int = 2) -> None:
    """Prints a label followed by one bullet per job (by file name). No-op when empty."""
    if not jobs:
        return
    fmt.echo("%s%s:" % (" " * indent, label))
    for job in jobs:
        fmt.echo("%s* %s" % (" " * 2 * indent, os.path.basename(job)))


def display_package_jobs(
    pipeline: dlt.Pipeline,
    load_id: str,
    package_info: LoadPackageInfo,
    job_filter: str,
    verbosity: int,
) -> None:
    """Prints jobs matching `job_filter` across all job states of a package with their
    state, retry count and every exception message recorded across retries."""
    try:
        packages: Optional[PackageStorage] = pipeline._get_load_storage().get_package_storage(
            load_id
        )
    except LoadPackageNotFound:
        # extracted-only package, no loaded/normalized exception history
        packages = None
    matched = [
        job
        for jobs in package_info.jobs.values()
        for job in jobs
        if job_filter in job.job_file_info.file_name()
    ]
    if not matched:
        fmt.echo("No jobs matching '%s' in package %s" % (job_filter, fmt.bold(load_id)))
        return
    for job in matched:
        info = job.job_file_info
        fmt.echo("Job %s (table: %s)" % (fmt.bold(info.job_id()), fmt.bold(info.table_name)))
        fmt.echo("  state: %s  retry count: %d" % (fmt.bold(job.state), info.retry_count))
        if verbosity > 0:
            fmt.echo("  file: %s" % job.file_path)
        exceptions = packages.list_job_exceptions(load_id, info) if packages else []
        if exceptions:
            for retry_no, exc_type, message in exceptions:
                fmt.echo("  attempt %d (%s):" % (retry_no, exc_type))
                fmt.secho(message.strip(), fg="red")
        elif job.failed_message:
            fmt.secho(job.failed_message.strip(), fg="red")
        else:
            fmt.echo("  no exception messages recorded")
        fmt.echo()


def display_package_row_counts(pipeline: dlt.Pipeline, load_id: str) -> None:
    """Counts rows loaded for `load_id` in the destination dataset and reports whether the
    package is recorded as completed in _dlt_loads. Independent of the local working dir."""
    try:
        dataset = pipeline.dataset()
    except PipelineConfigMissing:
        raise CliCommandInnerException(
            "pipeline", "Cannot count rows: pipeline has no destination configured."
        )
    fmt.echo(
        "Row counts for load %s in dataset %s:"
        % (fmt.bold(load_id), fmt.bold(pipeline.dataset_name))
    )

    schema = dataset.schema
    naming = schema.naming
    try:
        loads = (
            dataset.loads_table()
            .where(naming.normalize_identifier(C_DLT_LOADS_TABLE_LOAD_ID), "eq", load_id)
            .fetchall()
        )
    except (DestinationUndefinedEntity, SqlClientNotAvailable, ConnectionError) as ex:
        fmt.warning("Could not read state from the destination: %s" % str(ex))
        return
    # status column is 0 for a completed load
    if loads and loads[0][2] == 0:
        fmt.echo("Package is COMPLETED, loaded at %s" % fmt.bold(str(loads[0][3])))
    else:
        fmt.warning("Package is NOT completed: no entry in _dlt_loads for this load id.")
        fmt.echo("See %s for handling partially loaded packages." % PARTIAL_LOAD_DOCS_URL)

    # include dlt tables so an updated _dlt_pipeline_state row is visible for this load
    try:
        counts = dataset.row_counts(dlt_tables=True, load_id=load_id).fetchall()
    except (DestinationUndefinedEntity, SqlClientNotAvailable, ConnectionError):
        fmt.warning("No tables for this load id are present at the destination.")
        return
    total = 0
    for table_name, count in sorted(counts):
        line = "  %s: %s" % (fmt.bold(table_name), count)
        table = schema.tables.get(table_name)
        if table is not None:
            line += " (write disposition: %s)" % get_write_disposition(schema.tables, table_name)
            if not is_nested_table(table):
                nested = get_nested_tables(schema.tables, table_name, include_self=False)
                if nested:
                    line += ", %d nested tables could also be modified" % len(nested)
        fmt.echo(line)
        total += count
    fmt.echo("  %s: %d rows across %d tables" % (fmt.bold("total"), total, len(counts)))


def abort_packages(pipeline: dlt.Pipeline, pipeline_name: str, load_id: str = None) -> None:
    """Shows the abort plan for `load_id` (or all pending packages), asks for confirmation and
    aborts. State and schemas rewind to the point at which the oldest aborted package started."""
    plan = pipeline.abort_packages(load_id=load_id, dry_run=True)
    if plan is None:
        fmt.echo("No pending packages found. Nothing to abort.")
        return
    package_to_abort = plan["package_to_abort"]
    if package_to_abort:
        head_id = package_to_abort["load_id"]
        fmt.echo("Load package %s will be aborted:" % fmt.bold(head_id))
        echo_job_list("jobs pending with terminal errors", package_to_abort["terminal_jobs"])
        echo_job_list("jobs to mark as failed", package_to_abort["transient_jobs"])
        echo_job_list(
            "interrupted jobs (no recorded outcome) to mark as failed",
            package_to_abort["interrupted_jobs"],
        )
        echo_job_list(
            "jobs already committed to the destination, will complete",
            package_to_abort["committed_jobs"],
        )
    else:
        head_id = None
    if plan["packages_to_delete"]:
        if package_to_abort:
            fmt.echo()
        echo_job_list("Normalized packages to delete:", plan["packages_to_delete"], indent=0)
    if plan["extracted_packages_to_delete"]:
        fmt.echo(
            "Extracted packages to delete: %s" % ", ".join(plan["extracted_packages_to_delete"])
        )
    if head_id and PackageStorage.is_package_partially_loaded(
        pipeline.get_load_package_info(head_id)
    ):
        fmt.echo()
        fmt.warning(
            "Package %s is partially loaded: some of its jobs could already write to the"
            " destination. Aborting will NOT revert data already written, so the"
            " destination may be left in an inconsistent state."
            % fmt.bold(head_id)
        )
        fmt.echo(
            "Inspect what was written first with: %s"
            % fmt.bold(fmt.cli_cmd(f"pipeline {pipeline_name} load-package {head_id} row-counts"))
        )
    fmt.echo()
    fmt.echo(
        "After aborting, local pipeline state and schemas will be restored to the"
        " point at which the oldest aborted package started."
    )
    if fmt.confirm("Proceed?", default=False):
        pipeline.abort_packages(load_id=load_id)
        fmt.echo(
            "Done. Packages aborted, local state and schemas restored to the point"
            " before the aborted load."
        )


def fail_package_job(pipeline: dlt.Pipeline, load_id: str, job_arg: str) -> None:
    """Moves a pending retry job (matched by job id or file name) to failed_jobs, after showing
    its recorded exception and asking for confirmation."""
    pending = pipeline.list_pending_retry_jobs_in_package(load_id)
    if not pending:
        fmt.echo("No pending retry jobs in package %s" % fmt.bold(load_id))
        return
    new_job_names = [
        os.path.basename(file_path) for file_path, folder in pending if folder == "new_jobs"
    ]
    started_job_names = [
        os.path.basename(file_path) for file_path, folder in pending if folder == "started_jobs"
    ]

    def match_job(job_names: List[str]) -> Optional[str]:
        # resolve job argument: try as file_name first, then as job_id
        with contextlib.suppress(Exception):
            parsed_arg = ParsedLoadJobFileName.parse(job_arg)
            for job_name in job_names:
                if job_name == parsed_arg.file_name():
                    return job_name
        for job_name in job_names:
            if ParsedLoadJobFileName.parse(job_name).job_id() == job_arg:
                return job_name
        return None

    matched_file_name = match_job(new_job_names)
    if matched_file_name is None:
        if match_job(started_job_names) is not None:
            raise CliCommandInnerException(
                "pipeline",
                "Job '%s' is interrupted (in started_jobs) and cannot be failed directly."
                " Run the pipeline or `abort` to resolve it." % job_arg,
            )
        raise CliCommandInnerException(
            "pipeline",
            "Job '%s' not found in pending retry jobs for package '%s'" % (job_arg, load_id),
        )

    parsed_job = ParsedLoadJobFileName.parse(matched_file_name)
    load_storage = pipeline._get_load_storage()
    exc_type, exc_msg = load_storage.normalized_packages.get_last_job_exception(load_id, parsed_job)

    fmt.echo("Job: %s (table: %s)" % (fmt.bold(parsed_job.job_id()), parsed_job.table_name))
    fmt.echo("Retry count: %d" % parsed_job.retry_count)
    if exc_type:
        fmt.echo("Exception type: %s" % exc_type)
    if exc_msg:
        fmt.echo("Exception message: %s" % exc_msg.strip())
    if fmt.confirm("Fail this job?", default=False):
        pipeline.fail_pending_job(load_id, matched_file_name)
        fmt.echo("Job %s moved to failed_jobs" % parsed_job.job_id())


def drop_pipeline(
    pipeline: dlt.Pipeline,
    resources: Union[Iterable[Union[str, TSimpleRegex]], str, TSimpleRegex] = (),
    schema_name: str = None,
    state_paths: TAnyJsonPath = (),
    drop_all: bool = False,
    state_only: bool = False,
) -> None:
    """Shows the resources, tables and state that `drop` would remove, asks for confirmation
    and applies the drop."""
    drop_command = pipeline_drop(
        pipeline,
        resources=resources,
        schema_name=schema_name,
        state_paths=state_paths,
        drop_all=drop_all,
        state_only=state_only,
    )
    if drop_command.is_empty:
        fmt.echo(
            "Could not select any resources to drop and no resource/source state to reset. Use"
            " the command below to inspect the pipeline:"
        )
        fmt.echo(fmt.cli_cmd(f"pipeline -v {pipeline.pipeline_name} info"))
        if len(drop_command.info["warnings"]):
            fmt.echo("Additional warnings are available")
            for warning in drop_command.info["warnings"]:
                fmt.warning(warning)
        return

    # drop command will fail if first run but that happens later, so we make sure that last_run_context exists
    if not pipeline.first_run:
        from dlt.common.runtime import run_context

        active_run_dir = os.path.abspath(run_context.active().run_dir)

        if pipeline.last_run_context["run_dir"] != active_run_dir:
            fmt.warning(
                fmt.style(
                    "You should run this from the same directory as the pipeline script (%s),"
                    " where the folder with credentials (%s) is located. Alternatively, you can"
                    " set the required credentials as environment variables."
                    % (
                        pipeline.last_run_context["run_dir"],
                        pipeline.last_run_context["settings_dir"],
                    ),
                    fg="yellow",
                )
            )

    fmt.echo(
        "About to drop the following data in dataset %s in destination %s:"
        % (
            fmt.bold(pipeline.dataset_name),
            fmt.bold(pipeline.destination.destination_name),
        )
    )
    fmt.echo(
        "%s: %s" % (fmt.style("Selected schema", fg="green"), drop_command.info["schema_name"])
    )
    fmt.echo(
        "%s: %s"
        % (
            fmt.style("Selected resource(s)", fg="green"),
            drop_command.info["resource_names"],
        )
    )
    fmt.echo("%s: %s" % (fmt.style("Table(s) to drop", fg="green"), drop_command.info["tables"]))
    fmt.echo(
        "%s: %s"
        % (
            fmt.style("\twith data in destination", fg="green"),
            drop_command.info["tables_with_data"],
        )
    )
    fmt.echo(
        "%s: %s"
        % (
            fmt.style("Resource(s) state to reset", fg="green"),
            drop_command.info["resource_states"],
        )
    )
    fmt.echo(
        "%s: %s"
        % (
            fmt.style("Source state path(s) to reset", fg="green"),
            drop_command.info["state_paths"],
        )
    )
    for warning in drop_command.info["warnings"]:
        fmt.warning(warning)
    if fmt.confirm("Do you want to apply these changes?", default=False):
        drop_command()
