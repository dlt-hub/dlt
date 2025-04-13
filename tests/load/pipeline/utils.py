import pytest

from tests.load.utils import DestinationTestConfiguration


def skip_if_unsupported_replace_strategy(
    destination_config: DestinationTestConfiguration, replace_strategy: str
):
    """Skip test if destination does not support the given replace strategy."""
    supported_replace_strategies = (
        destination_config.raw_capabilities().supported_replace_strategies
    )
    # hardcoded exclusions (that require table schema and replace selector to be used, here it is not available)
    is_athena = destination_config.destination_type == "athena"
    is_filesystem = destination_config.destination_type == "filesystem"
    is_open_table = destination_config.force_iceberg or destination_config.table_format

    if (is_athena or is_filesystem) and not is_open_table:
        supported_replace_strategies = ["truncate-and-insert"]

    if is_athena and is_open_table:
        supported_replace_strategies = ["insert-from-staging"]

    if replace_strategy not in supported_replace_strategies:
        pytest.skip(
            f"Destination {destination_config.name} does not support the replace strategy"
            f" {replace_strategy}"
        )
