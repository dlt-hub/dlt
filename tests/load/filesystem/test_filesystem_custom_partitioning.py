"""Tests extended layout placeholders for filesystem destinations."""
import os
from typing import Iterator, Dict

import pytest

import dlt
from dlt.common.configuration.inject import with_config
from dlt.common.storages import FileStorage, FilesystemConfiguration
from dlt.common.utils import uniq_id
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
)


@pytest.mark.parametrize("layout", ALL_LAYOUTS)
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(all_buckets_filesystem_configs=True),
)
def test_resource_arguments_to_partitions(
    layout: str, default_buckets_env: str, destination_config: DestinationTestConfiguration
) -> None:
    if layout:
        os.environ["DESTINATION__FILESYSTEM__LAYOUT"] = layout
    else:
        os.environ.pop("DESTINATION__FILESYSTEM__LAYOUT", None)

    @dlt.resource(standalone=True)
    def test_resource(arg: int, kwarg: str = "example_column") -> Iterator[Dict[str, int]]:
        yield from [{kwarg: i} for i in range(arg)]

    destination_config

    pipeline = destination_config.setup_pipeline(
        f"test_resource_arguments_to_partitions_{uniq_id()}", full_refresh=True
    )

    info = pipeline.run(test_resource(10, kwarg="example_column"))
    assert_load_info(info)
