from typing import List
import pytest

from dlt.common.destination.exceptions import DestinationCapabilitiesException
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.utils import resolve_merge_strategy, resolve_replace_strategy
from dlt.common.schema.typing import (
    TLoaderMergeStrategy,
    TLoaderReplaceStrategy,
    TWriteDisposition,
)
from dlt.common.schema.utils import new_table, new_column
from dlt.common.storages.load_package import LoadJobInfo, LoadPackageInfo, TPackageJobState

from tests.load.utils import DestinationTestConfiguration


def get_load_package_jobs(
    package: LoadPackageInfo, state: TPackageJobState, table_name: str, file_format: str = ""
) -> List[LoadJobInfo]:
    completed_jobs = package.jobs[state]
    return [
        job
        for job in completed_jobs
        if job.job_file_info.table_name == table_name and job.file_path.endswith(file_format)
    ]


def get_sample_table(
    destination_config: DestinationTestConfiguration, write_disposition: TWriteDisposition
) -> PreparedTableSchema:
    """Returns a sample table created according to destination config used ie. to infer expected merge strategy"""
    return new_table(  # type: ignore[return-value]
        "sample_table",
        write_disposition=write_disposition,
        resource="sample_table",
        table_format=destination_config.table_format,
        file_format=destination_config.file_format,
        columns=[new_column("col1", "bigint")],
    )


def skip_if_unsupported_replace_strategy(
    destination_config: DestinationTestConfiguration, replace_strategy: TLoaderReplaceStrategy
):
    """Skip test if destination does not support the given replace strategy."""

    if not resolve_replace_strategy(
        get_sample_table(destination_config, "replace"),
        replace_strategy,
        destination_config.raw_capabilities(),
    ):
        pytest.skip(
            f"Destination {destination_config.name} does not support the replace strategy"
            f" {replace_strategy}"
        )


def skip_if_unsupported_merge_strategy(
    destination_config: DestinationTestConfiguration,
    merge_strategy: TLoaderMergeStrategy,
) -> None:
    sample_table = get_sample_table(destination_config, "merge")
    sample_table["x-merge-strategy"] = merge_strategy  # type: ignore[typeddict-unknown-key]
    try:
        resolve_merge_strategy(
            {"sample_table": sample_table}, sample_table, destination_config.raw_capabilities()
        )
    except DestinationCapabilitiesException:
        pytest.skip(
            f"`{merge_strategy}` merge strategy not supported for `{destination_config.name}`"
            " destination."
        )
