import pytest
import os
import shutil

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    SFTP_BUCKET,
    MEMORY_BUCKET,
)

from tests.pipeline.utils import load_table_counts, assert_records_as_set
from dlt.common.destination.exceptions import DestinationUndefinedEntity


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        bucket_exclude=[SFTP_BUCKET, MEMORY_BUCKET],
    ),  # TODO: make SFTP work
    ids=lambda x: x.name,
)
@pytest.mark.parametrize("disable_dlt_folders", [True, False])
def test_read_interfaces_filesystem(
    destination_config: DestinationTestConfiguration,
    disable_dlt_folders: bool,
) -> None:
    p = destination_config.setup_pipeline(
        "test_read_interfaces_filesystem",
        dataset_name="test_read_interfaces_filesystem",
    )

    if disable_dlt_folders:
        os.environ["DESTINATION__FILESYSTEM__EXPERIMENTAL_EXCLUDE_DLT_TABLES"] = "true"

    # load three items
    p.run([1, 2, 3], table_name="items", write_disposition="append")

    def _check_dlt_folders(
        expected_loads: int, expected_pipeline_state: int, expected_version: int
    ):
        if not disable_dlt_folders:
            # check presence of dlt_state and dlt_version folders
            assert load_table_counts(p, "_dlt_loads") == {"_dlt_loads": expected_loads}
            assert load_table_counts(p, "_dlt_pipeline_state") == {
                "_dlt_pipeline_state": expected_pipeline_state
            }
            assert load_table_counts(p, "_dlt_version") == {"_dlt_version": expected_version}
        else:
            # folders do not exist and raise if accessed
            with pytest.raises(DestinationUndefinedEntity):
                load_table_counts(p, "_dlt_loads")
            with pytest.raises(DestinationUndefinedEntity):
                load_table_counts(p, "_dlt_pipeline_state")
            with pytest.raises(DestinationUndefinedEntity):
                load_table_counts(p, "_dlt_version")

    # check dlt folders, and items table content
    _check_dlt_folders(expected_loads=1, expected_pipeline_state=1, expected_version=1)
    assert load_table_counts(p, "items") == {"items": 3}
    assert_records_as_set(
        p.dataset().items["value"].df().to_dict(orient="records"),
        [
            {"value": 1},
            {"value": 2},
            {"value": 3},
        ],
    )
    # drop local pipeline working and recreate pipeline
    shutil.rmtree(p.working_dir)
    p = destination_config.setup_pipeline(
        "test_read_interfaces_filesystem",
        dataset_name="test_read_interfaces_filesystem",
    )

    # load three more items, see that append works for both cases
    p.run([4, 5, 6], table_name="items", write_disposition="append")
    _check_dlt_folders(expected_loads=2, expected_pipeline_state=1, expected_version=1)
    assert load_table_counts(p, "items") == {"items": 6}
    assert_records_as_set(
        p.dataset().items["value"].df().to_dict(orient="records"),
        [
            {"value": 1},
            {"value": 2},
            {"value": 3},
            {"value": 4},
            {"value": 5},
            {"value": 6},
        ],
    )
    # drop local pipeline working and recreate pipeline
    shutil.rmtree(p.working_dir)
    p = destination_config.setup_pipeline(
        "test_read_interfaces_filesystem",
        dataset_name="test_read_interfaces_filesystem",
    )

    # replace also works in this mode
    p.run([7, 8, 9], table_name="items", write_disposition="replace")
    _check_dlt_folders(expected_loads=3, expected_pipeline_state=2, expected_version=2)
    assert load_table_counts(p, "items") == {"items": 3}
    assert_records_as_set(
        p.dataset().items["value"].df().to_dict(orient="records"),
        [
            {"value": 7},
            {"value": 8},
            {"value": 9},
        ],
    )
