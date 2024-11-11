# NOTE: these should be run with all destinations enabled
# NOTE: these are very rudimentary tests, they should be extended

from typing import Any

from tests.load.utils import destinations_configs, DEFAULT_BUCKETS, DestinationTestConfiguration


def test_empty_destinations_configs():
    configs = destinations_configs()
    assert len(configs) == 0


def _assert_name_uniqueness(configs: Any) -> None:
    identifiers = [config[0][0].name for config in configs]
    print(identifiers)
    assert len(identifiers) == len(set(identifiers)), "Identifier uniqueness violated"


def test_enable_filesystem_configs():
    # enable local filesystem configs
    configs = destinations_configs(local_filesystem_configs=True)
    _assert_name_uniqueness(configs)
    assert len(configs) == 3

    # enable all buckets configs
    configs = destinations_configs(all_buckets_filesystem_configs=True)
    _assert_name_uniqueness(configs)
    assert len(configs) == len(DEFAULT_BUCKETS) == 7  # hardcoded for now

    # enable with delta tables
    configs = destinations_configs(
        all_buckets_filesystem_configs=True, table_format_filesystem_configs=True
    )
    _assert_name_uniqueness(configs)
    assert (
        len(configs) == len(DEFAULT_BUCKETS) * 2 == 7 * 2
    )  # we have delta now, so double the expected amount


def test_uniqueness_of_names():
    configs = destinations_configs(
        default_sql_configs=True,
        default_vector_configs=True,
        default_staging_configs=True,
        all_staging_configs=True,
        all_buckets_filesystem_configs=True,
    )
    _assert_name_uniqueness(configs)
