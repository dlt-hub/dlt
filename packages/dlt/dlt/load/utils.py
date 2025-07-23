from typing import List, Set, Iterable, Callable, Optional, Tuple, Sequence
from itertools import groupby

from dlt.common import logger
from dlt.common.storages.load_package import LoadJobInfo, PackageStorage, TPackageJobState
from dlt.common.schema.utils import (
    fill_hints_from_parent_and_clone_table,
    get_nested_tables,
    get_root_table,
    has_table_seen_data,
)
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import TTableSchema
from dlt.common.destination.client import JobClientBase, WithStagingDataset, LoadJob
from dlt.load.configuration import LoaderConfiguration
from dlt.common.destination import DestinationCapabilitiesContext


def get_completed_table_chain(
    schema: Schema,
    all_jobs: Iterable[Tuple[TPackageJobState, ParsedLoadJobFileName]],
    top_merged_table: TTableSchema,
    being_completed_job_id: str = None,
) -> List[TTableSchema]:
    """Gets a table chain starting from the `top_merged_table` containing only tables with completed/failed jobs. None is returned if there's any job that is not completed
    For append and merge write disposition, tables without jobs will be included, providing they have seen data (and were created in the destination)
    Optionally `being_completed_job_id` can be passed that is considered to be completed before job itself moves in storage
    """
    # returns ordered list of tables from parent to nested leaf tables
    table_chain: List[TTableSchema] = []
    # allow for jobless tables for those write disposition
    skip_jobless_table = top_merged_table["write_disposition"] not in (
        "replace",
        "merge",
    )

    # make sure all the jobs for the table chain is completed
    for table in map(
        lambda t: fill_hints_from_parent_and_clone_table(schema.tables, t),
        get_nested_tables(schema.tables, top_merged_table["name"]),
    ):
        table_jobs = PackageStorage.filter_jobs_for_table(all_jobs, table["name"])
        # skip tables that never seen data
        if not has_table_seen_data(table):
            assert len(table_jobs) == 0, f"Tables that never seen data cannot have jobs {table}"
            continue
        # skip jobless tables
        if len(table_jobs) == 0 and skip_jobless_table:
            continue
        else:
            # all jobs must be completed in order for merge to be created
            if any(
                job[0] not in ("failed_jobs", "completed_jobs")
                and job[1].job_id() != being_completed_job_id
                for job in table_jobs
            ):
                return None
        table_chain.append(table)
    # there must be at least table
    assert len(table_chain) > 0
    return table_chain


def init_client(
    job_client: JobClientBase,
    schema: Schema,
    new_jobs: Iterable[ParsedLoadJobFileName],
    expected_update: TSchemaTables,
    truncate_filter: Callable[[str], bool],
    load_staging_filter: Callable[[str], bool],
    drop_tables: Optional[List[TTableSchema]] = None,
    truncate_tables: Optional[List[TTableSchema]] = None,
) -> TSchemaTables:
    """Initializes destination storage including staging dataset if supported

    Will initialize and migrate schema in destination dataset and staging dataset.

    Args:
        job_client (JobClientBase): Instance of destination client
        schema (Schema): The schema as in load package
        new_jobs (Iterable[LoadJobInfo]): List of new jobs
        expected_update (TSchemaTables): Schema update as in load package. Always present even if empty
        truncate_filter (Callable[[str], bool]): A filter that tells which table in destination dataset should be truncated
        load_staging_filter (Callable[[str], bool]): A filter which tell which table in the staging dataset may be loaded into
        drop_tables (Optional[List[TTableSchema]]): List of tables to drop before initializing storage
        truncate_tables (Optional[List[TTableSchema]]): List of tables to truncate before initializing storage

    Returns:
        TSchemaTables: Actual migrations done at destination
    """
    # get dlt/internal tables
    dlt_tables = set(schema.dlt_table_names())

    # tables without data (TODO: normalizer removes such jobs, write tests and remove the line below)
    tables_no_data = set(
        table["name"] for table in schema.data_tables() if not has_table_seen_data(table)
    )
    # get all tables that actually have load jobs with data
    tables_with_jobs = set(job.table_name for job in new_jobs) - tables_no_data

    # get tables to truncate by extending tables with jobs with all their nested tables
    initial_truncate_names = set(t["name"] for t in truncate_tables) if truncate_tables else set()
    truncate_table_names = set(
        _extend_tables_with_table_chain(
            schema,
            tables_with_jobs,
            tables_with_jobs,
            lambda table_name: truncate_filter(table_name)
            or (table_name in initial_truncate_names),
        )
    )

    # get tables to drop
    drop_table_names = {table["name"] for table in drop_tables} if drop_tables else set()
    job_client.verify_schema(only_tables=tables_with_jobs | dlt_tables, new_jobs=new_jobs)
    applied_update = _init_dataset_and_update_schema(
        job_client,
        expected_update,
        tables_with_jobs | dlt_tables,
        truncate_table_names,
        drop_tables=drop_table_names,
    )

    # update the staging dataset if client supports this
    if isinstance(job_client, WithStagingDataset):
        # get staging tables (all data tables that are eligible)
        staging_tables = set(
            _extend_tables_with_table_chain(
                schema, tables_with_jobs, tables_with_jobs, load_staging_filter
            )
        )

        # if there are tables to drop, we should also drop them in the staging dataset
        if staging_tables or drop_table_names:
            with job_client.with_staging_dataset():
                _init_dataset_and_update_schema(
                    job_client,
                    expected_update,
                    staging_tables | {schema.version_table_name},  # keep only schema version
                    staging_tables,  # all eligible tables must be also truncated
                    staging_info=True,
                    drop_tables=drop_table_names,  # try to drop all the same tables on staging
                )

    return applied_update


def _init_dataset_and_update_schema(
    job_client: JobClientBase,
    expected_update: TSchemaTables,
    update_tables: Iterable[str],
    truncate_tables: Iterable[str] = None,
    staging_info: bool = False,
    drop_tables: Iterable[str] = None,
) -> TSchemaTables:
    staging_text = "for staging dataset" if staging_info else ""
    logger.info(
        f"Client for {job_client.config.destination_type} will start initialize storage"
        f" {staging_text}"
    )
    if drop_tables and job_client.is_storage_initialized():
        if hasattr(job_client, "drop_tables"):
            logger.info(
                f"Client for {job_client.config.destination_type} will drop tables"
                f" {drop_tables} {staging_text}"
            )
            job_client.drop_tables(*drop_tables, delete_schema=True)
        else:
            logger.warning(
                f"Client for {job_client.config.destination_type} does not implement drop table."
                f" Following tables {drop_tables} will not be dropped {staging_text}"
            )

    job_client.initialize_storage()

    logger.info(
        f"Client for {job_client.config.destination_type} will update schema to package schema"
        f" {staging_text}"
    )
    applied_update = job_client.update_stored_schema(
        only_tables=update_tables, expected_update=expected_update
    )
    if truncate_tables:
        logger.info(
            f"Client for {job_client.config.destination_type} will truncate tables {staging_text}"
        )

    job_client.initialize_storage(truncate_tables=truncate_tables)
    return applied_update


def _extend_tables_with_table_chain(
    schema: Schema,
    tables: Iterable[str],
    tables_with_jobs: Iterable[str],
    include_table_filter: Callable[[str], bool] = lambda t: True,
) -> Iterable[str]:
    """Extend 'tables` with all their children and filter out tables that do not have jobs (in `tables_with_jobs`),
    haven't seen data or are not included by `include_table_filter`.
    Note that for root tables with replace and merge, the filter for tables that do not have jobs

    Returns an unordered set of table names and their nested tables
    """
    result: Set[str] = set()
    for table_name in tables:
        top_job_table = get_root_table(schema.tables, table_name)
        # for replace and merge write dispositions we should include tables
        # without jobs in the table chain, because nested tables may need
        # processing due to changes in the root table
        skip_jobless_table = top_job_table["write_disposition"] not in (
            "replace",
            "merge",
        )
        for table in map(
            lambda t: fill_hints_from_parent_and_clone_table(schema.tables, t),
            get_nested_tables(schema.tables, top_job_table["name"]),
        ):
            chain_table_name = table["name"]
            table_has_job = chain_table_name in tables_with_jobs
            # table that never seen data are skipped as they will not be created
            # also filter out tables
            # NOTE: this will ie. eliminate all non iceberg tables on ATHENA destination from staging (only iceberg needs that)
            if not has_table_seen_data(table) or not include_table_filter(chain_table_name):
                continue
            # if there's no job for the table and we are in append then skip
            if not table_has_job and skip_jobless_table:
                continue
            result.add(chain_table_name)
    return result


def get_available_worker_slots(
    config: LoaderConfiguration,
    capabilities: DestinationCapabilitiesContext,
    running_jobs: Sequence[LoadJob],
) -> int:
    """
    Returns the number of available worker slots
    """
    parallelism_strategy = config.parallelism_strategy or capabilities.loader_parallelism_strategy

    # find real max workers value
    max_workers = 1 if parallelism_strategy == "sequential" else config.workers
    if mp := capabilities.max_parallel_load_jobs:
        max_workers = min(max_workers, mp)

    return max(0, max_workers - len(running_jobs))


def filter_new_jobs(
    file_names: Sequence[str],
    capabilities: DestinationCapabilitiesContext,
    config: LoaderConfiguration,
    running_jobs: Sequence[LoadJob],
    available_slots: int,
) -> Sequence[str]:
    """Filters the list of new jobs to adhere to max_workers and parallellism strategy"""
    """NOTE: in the current setup we only filter based on settings for the final destination"""
    """Support for differentiating staging destination jobs might come in the future if we need it"""

    # nothing to do
    if not file_names:
        return file_names

    # config can overwrite destination settings, if nothing is set, code below defaults to parallel
    parallelism_strategy = config.parallelism_strategy or capabilities.loader_parallelism_strategy

    # regular sequential works on all jobs
    eligible_jobs = file_names

    # we must ensure there only is one job per table
    if parallelism_strategy == "table-sequential":
        # TODO later: this whole code block is a bit inefficient for long lists of jobs
        # better would be to keep a list of loadjobinfos in the loader which we can iterate

        # find table names of all currently running jobs
        running_tables = {j._parsed_file_name.table_name for j in running_jobs}
        new_jobs: List[str] = []

        for job in eligible_jobs:
            if (table_name := ParsedLoadJobFileName.parse(job).table_name) not in running_tables:
                running_tables.add(table_name)
                new_jobs.append(job)
            # exit loop if we have enough
            if len(new_jobs) >= available_slots:
                break

        return new_jobs

    else:
        return eligible_jobs[:available_slots]
