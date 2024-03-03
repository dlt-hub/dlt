"""Tests extended layout placeholders for filesystem destinations."""
import datetime
import inspect
import os
from typing import Iterator, Dict

import pytest

import dlt
from dlt.common.configuration.inject import with_config
from dlt.common.storages import FileStorage, FilesystemConfiguration
from dlt.common.typing import DictStrAny
from dlt.common.utils import uniq_id
from dlt.destinations import filesystem
from tests.common.configuration.utils import environment
from tests.load.pipeline.utils import (
    destinations_configs,
)
from tests.load.utils import DestinationTestConfiguration
from tests.pipeline.utils import assert_load_info
from tests.utils import (
    preserve_environ,
    autouse_test_storage,
    clean_test_storage,
    init_test_logging,
)


@with_config(spec=FilesystemConfiguration, sections=("destination", "filesystem"))
def get_config(config: FilesystemConfiguration = None) -> FilesystemConfiguration:
    return config


@pytest.mark.usefixtures("preserve_environ", "autouse_test_storage", "environment")
@pytest.fixture(autouse=True)
def storage() -> FileStorage:
    return clean_test_storage(init_normalize=True, init_loader=True)


@pytest.fixture(scope="module", autouse=True)
def logger_autouse() -> None:
    init_test_logging()


NORMALIZED_FILES = [
    "event_user.839c6e6b514e427687586ccc65bf133f.0.jsonl",
    "event_loop_interrupted.839c6e6b514e427687586ccc65bf133f.0.jsonl",
]

ALL_LAYOUTS = (
    None,
    "{schema_name}/{table_name}/{load_id}.{file_id}.{ext}",  # New default layout with schema.
    "{schema_name}.{table_name}.{load_id}.{file_id}.{ext}",  # Classic layout.
    "{table_name}88{load_id}-u-{file_id}.{ext}",  # Default layout with strange separators.
    "{table_name}88{load_id}-u-{file_id}.{ext}",  # Default layout with strange separators.
)


@pytest.mark.parametrize("layout", ALL_LAYOUTS)
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
)
def test_integration_partition_on_resource_metadata(
    layout: str, default_buckets_env: str, destination_config: DestinationTestConfiguration
) -> None:
    """
    Integration test to check whether custom partitioning layout works for both literal and
    callback partition values, based on resource metadata.
    """
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)

    @dlt.resource(
        standalone=True,
        columns=[
            {"name": "event_date", "data_type": "date"},
            {"name": "category", "data_type": "text"},
            {"name": "count", "data_type": "bigint"},
        ],
    )
    def resource() -> Iterator[DictStrAny]:
        start_date = datetime.date(2024, 1, 1)
        yield from [
            {
                "count": i,
                "category": chr(i % 3 + ord("A")),  # Cycles through A, B and C.
                "event_date": start_date + datetime.timedelta(days=i),
            }
            for i in range(100)
        ]

    destination_config.destination = filesystem(layout_placeholders={"category": 3})  # type: ignore[assignment]
    print(destination_config.destination.config_params)

    pipeline = destination_config.setup_pipeline(
        f"{inspect.currentframe().f_code.co_name}_{uniq_id()}", full_refresh=True
    )

    info = pipeline.run(resource())
    assert_load_info(info)

    print(info)


@pytest.mark.parametrize("layout", ALL_LAYOUTS)
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
)
def test_integration_partition_on_resource_parameters(
    layout: str, default_buckets_env: str, destination_config: DestinationTestConfiguration
) -> None:
    """
    Integration test to check whether custom partitioning layout works for both literal and
    callback partition values, based on resource parameters.
    """
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)

    @dlt.resource(
        standalone=True,
        columns=[
            {"name": "event_date", "data_type": "date"},
            {"name": "category", "data_type": "text"},
            {"name": "count", "data_type": "bigint"},
        ],
    )
    def resource() -> Iterator[DictStrAny]:
        start_date = datetime.date(2024, 1, 1)
        yield from [
            {
                "count": i,
                "category": chr(i % 3 + ord("A")),  # Cycles through A, B and C.
                "event_date": start_date + datetime.timedelta(days=i),
            }
            for i in range(100)
        ]

    pytest.skip("Not implemented yet.")
