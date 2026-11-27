from contextlib import contextmanager
from typing import cast, List, Set, Iterable, Iterator, Callable, Optional, Tuple, Sequence

from dlt.common import logger
from dlt.common.storages.load_package import PackageStorage, TPackageJobState
from dlt.common.schema.utils import (
    fill_hints_from_parent_and_clone_table,
    get_nested_tables,
    get_root_table,
    get_write_disposition,
    has_table_seen_data,
)
from dlt.common.storages.load_storage import ParsedLoadJobFileName
from dlt.common.schema import Schema, TSchemaTables
from dlt.common.schema.typing import TTableSchema
from dlt.common.destination.client import JobClientBase, WithStagingDataset, LoadJob
from dlt.common.destination.exceptions import (
    DestinationException,
    DestinationSchemaTampered,
    DestinationUndefinedEntity,
    SchemaUpdateError,
)
from dlt.destinations.job_client_impl import SqlJobClientWithStagingDataset
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
    drop_staging_filter: Callable[[TTableSchema], bool],
    drop_tables: Optional[List[TTableSchema]] = None,
    truncate_tables: Optional[List[TTableSchema]] = None,
    truncate_staging_merge_tables: bool = True,
) -> Tuple[TSchemaTables, Set[str], Set[str]]:
    """Initializes destination storage including staging dataset if supported

    Will initialize and migrate schema in destination dataset and staging dataset.

    Args:
        job_client (JobClientBase): Instance of destination client
        schema (Schema): The schema as in load package
        new_jobs (Iterable[LoadJobInfo]): List of new jobs
        expected_update (TSchemaTables): Schema update as in load package. Always present even if empty
        truncate_filter (Callable[[str], bool]): A filter that tells if table should be truncated
        load_staging_filter (Callable[[str], bool]): A filter which tell if table may be loaded into
        drop_staging_filter (Callable[[str], bool]): A filter which tell if table may be dropped
        drop_tables (Optional[List[TTableSchema]]): List of tables to drop before initializing storage
        truncate_tables (Optional[List[TTableSchema]]): List of tables to truncate before initializing storage
        truncate_staging_merge_tables (bool): If `False`, dlt keeps the staging tables of `merge`
            tables. The staging tables of other dispositions are always truncated

    Returns:
        Tuple[TSchemaTables, Set[str], Set[str]]: Actual migrations done at destination and the
        names of tables actually dropped and truncated in the destination dataset due to the
        load package state.
    """
    dlt_tables = set(schema.dlt_table_names())
    tables_with_jobs, initial_truncate_names, truncate_table_names, drop_table_names = (
        _resolve_table_names(
            schema=schema,
            new_jobs=new_jobs,
            truncate_filter=truncate_filter,
            drop_staging_filter=drop_staging_filter,
            drop_tables=drop_tables,
            truncate_tables=truncate_tables,
        )
    )
    job_client.verify_schema(only_tables=tables_with_jobs | dlt_tables, new_jobs=new_jobs)
    # forced migration (re)creates schema-known truncate targets before truncation. targets
    # with old-convention names (after a naming change) are not in the schema
    update_tables = (
        tables_with_jobs | dlt_tables | (initial_truncate_names & set(schema.tables.keys()))
    )
    applied_dropped = _drop_tables_if_possible(job_client, drop_table_names)
    applied_update, applied_truncated = _init_dataset_and_update_schema(
        job_client=job_client,
        expected_update=expected_update,
        update_tables=update_tables,
        truncate_tables=truncate_table_names,
        initial_truncate_tables=initial_truncate_names,
        # force schema update if tables dropped or truncated via refresh
        force=bool(drop_table_names or initial_truncate_names),
    )

    # update the staging dataset if client supports this
    if isinstance(job_client, WithStagingDataset):
        _prepare_staging_for_init_client(
            job_client=cast(SqlJobClientWithStagingDataset, job_client),
            schema=schema,
            expected_update=expected_update,
            load_staging_filter=load_staging_filter,
            tables_with_jobs=tables_with_jobs,
            drop_table_names=drop_table_names,
            truncate_staging_merge_tables=truncate_staging_merge_tables,
        )

    return applied_update, applied_dropped, applied_truncated


def _resolve_table_names(
    schema: Schema,
    new_jobs: Iterable[ParsedLoadJobFileName],
    truncate_filter: Callable[[str], bool],
    drop_staging_filter: Callable[[TTableSchema], bool],
    drop_tables: Optional[List[TTableSchema]],
    truncate_tables: Optional[List[TTableSchema]],
) -> Tuple[Set[str], Set[str], Set[str], Set[str]]:
    """Resolves table names for `init_client` from the load package.

    Returns names of: tables with jobs, tables to truncate due to package state (these already
    contain child tables), tables to truncate by extending tables with jobs with their nested
    tables, and tables to drop (these are not in the schema and come from the package state).
    """
    tables_with_jobs = set(job.table_name for job in new_jobs)
    initial_truncate_names = (
        set(t["name"] for t in truncate_tables if drop_staging_filter(t))
        if truncate_tables
        else set()
    )
    truncate_table_names = _get_table_chains(schema, tables_with_jobs, truncate_filter)
    drop_table_names = (
        {table["name"] for table in drop_tables if drop_staging_filter(table)}
        if drop_tables
        else set()
    )
    return tables_with_jobs, initial_truncate_names, truncate_table_names, drop_table_names


def _drop_tables_if_possible(
    job_client: JobClientBase, drop_tables: Iterable[str], staging_text: str = ""
) -> Set[str]:
    """Drops `drop_tables` if storage is initialized and client supports it.

    Returns names of dropped tables.
    """
    if not drop_tables or not job_client.is_storage_initialized():
        return set()
    if not hasattr(job_client, "drop_tables"):
        logger.warning(
            f"Client for {job_client.config.destination_type} does not implement drop table."
            f" Following tables {drop_tables} will not be dropped {staging_text}"
        )
        return set()
    logger.info(
        f"Client for {job_client.config.destination_type} will drop tables"
        f" {drop_tables} {staging_text}"
    )
    job_client.drop_tables(*drop_tables, delete_schema=True)
    return set(drop_tables)


def _prepare_staging_for_init_client(
    job_client: SqlJobClientWithStagingDataset,
    schema: Schema,
    expected_update: TSchemaTables,
    load_staging_filter: Callable[[str], bool],
    tables_with_jobs: set[str],
    drop_table_names: set[str],
    truncate_staging_merge_tables: bool = True,
) -> None:
    """Initializes the staging dataset of `job_client` and syncs its schema.

    Creates or updates every data table eligible for staging (per `load_staging_filter`,
    including nested tables of each table chain) plus the version table. Only tables with
    jobs in this load are truncated.

    WARNING: for append disposition, only tables with actual jobs are truncated - nested
    tables without jobs retain stale data from previous loads. The staging dataset thus does
    not reflect the current load package exactly. When we add a mode where all jobs go
    through staging, we may need to truncate all staging tables instead.

    If no tables are eligible for staging but `drop_table_names` is not empty, stale copies of
    the dropped tables are dropped from the staging dataset, only if it already exists: neither
    the dataset nor the version table is created, the next load initializes it like the first one.

    Args:
        job_client: Client with a staging dataset.
        schema: Full load schema; trimmed to staged tables while the staging dataset is active.
        expected_update: Schema update expected to be applied on the destination.
        load_staging_filter: Returns True if a table is materialized in the staging dataset.
        tables_with_jobs: Tables that have jobs in this load.
        drop_table_names: Tables to drop. Dropped on staging as well.
        truncate_staging_merge_tables: If False, tables with merge disposition are not
            truncated, since only merge may keep data from earlier loads. Replace and athena
            iceberg copy the whole staging table.
    """
    staging_tables_with_jobs = _get_table_chains(schema, tables_with_jobs, load_staging_filter)
    # include data tables eligible for staging whether or not they have jobs
    staging_data_tables = _get_table_chains(schema, schema.data_table_names(), load_staging_filter)
    all_staging_tables = staging_data_tables | staging_tables_with_jobs
    staging_tables_to_truncate = staging_tables_with_jobs
    if not truncate_staging_merge_tables:
        staging_tables_to_truncate = {
            name
            for name in staging_tables_with_jobs
            if get_write_disposition(schema.tables, name) != "merge"
        }

    if all_staging_tables:
        # trim the schema to tables that will be materialized
        job_client_schema = job_client.schema
        with job_client.with_staging_dataset():
            job_client.schema = _trim_schema_to_tables(schema, all_staging_tables)
            try:
                # try to drop all the same tables on staging
                _drop_tables_if_possible(job_client, drop_table_names, "for staging dataset")
                _init_dataset_and_update_schema(
                    job_client,
                    expected_update,
                    all_staging_tables | {schema.version_table_name},
                    # only truncate tables with jobs in this load
                    truncate_tables=staging_tables_to_truncate,
                    staging_info=True,
                    force=bool(drop_table_names),
                )
            finally:
                job_client.schema = job_client_schema

    elif drop_table_names:
        # nothing to materialize, only clean up stale staging data.
        with job_client.with_staging_dataset():
            _drop_tables_if_possible(job_client, drop_table_names, "for staging dataset")


@contextmanager
def _raise_as_schema_update_error(
    job_client: JobClientBase, table_names: Iterable[str], staging_dataset: bool
) -> Iterator[None]:
    """Converts a destination error from applied DDL into `SchemaUpdateError`.

    `retry_schema_update` retries this error.
    `table_names` is empty when the DDL creates the dataset itself.
    """
    try:
        yield
    except (DestinationSchemaTampered, DestinationUndefinedEntity):
        # a tampered schema or a genuinely missing entity are terminal and must surface as-is
        raise
    except DestinationException as schema_ex:
        raise SchemaUpdateError.from_cause(
            job_client.schema.name,
            table_names,
            schema_ex,
            staging_dataset=staging_dataset,
        ) from schema_ex


def _init_dataset_and_update_schema(
    job_client: JobClientBase,
    expected_update: TSchemaTables,
    update_tables: Iterable[str],
    *,
    truncate_tables: Set[str] = None,
    initial_truncate_tables: Set[str] = None,
    staging_info: bool = False,
    force: bool = False,
) -> Tuple[TSchemaTables, Set[str]]:
    """Initializes storage, applies schema update and truncates. Does not drop tables.

    Returns the applied schema update and the subset of `initial_truncate_tables` actually
    truncated.
    """
    applied_truncated: Set[str] = set()
    staging_text = "for staging dataset" if staging_info else ""
    logger.info(
        f"Client for {job_client.config.destination_type} will start initialize storage"
        f" {staging_text}"
    )
    with _raise_as_schema_update_error(job_client, (), staging_info):
        job_client.initialize_storage()

    logger.info(
        f"Client for {job_client.config.destination_type} will update schema to package schema"
        f" {staging_text}"
    )
    with _raise_as_schema_update_error(job_client, update_tables, staging_info):
        applied_update = job_client.update_stored_schema(
            only_tables=update_tables,
            expected_update=expected_update,
            force=force,
            # the stored hash covers the whole schema so tables without jobs need their new columns
            include_existing_tables=True,
        )
    if truncate_tables or initial_truncate_tables:
        if initial_truncate_tables:
            truncate_tables = initial_truncate_tables | (truncate_tables or set())
        logger.info(
            f"Client for {job_client.config.destination_type} will truncate tables {staging_text}"
        )
        job_client.initialize_storage(truncate_tables=truncate_tables)
        # report only tables truncated due to the load package state, not the regular
        # truncation of replace tables that happens on every load
        applied_truncated = (initial_truncate_tables or set()) & truncate_tables

    return applied_update, applied_truncated


def _trim_schema_to_tables(schema: Schema, table_names: Iterable[str]) -> Schema:
    """Clones `schema` keeping only `table_names` and dlt tables, with version and hash bumped
    to the trimmed content.
    """
    keep_tables = set(table_names) | set(schema.dlt_table_names())
    trimmed_schema = schema.clone()
    for name in list(trimmed_schema.tables):
        if name not in keep_tables:
            del trimmed_schema.tables[name]
    trimmed_schema._bump_version()
    return trimmed_schema


def _get_table_chains(
    schema: Schema, tables: Iterable[str], include_table_filter: Callable[[str], bool]
) -> Set[str]:
    """Extends `tables` with their nested tables, treating all of them as having jobs"""
    tables = set(tables)
    return set(_extend_tables_with_table_chain(schema, tables, tables, include_table_filter))


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
    """Filters the list of new jobs to adhere to max_workers and parallelism strategy"""
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
