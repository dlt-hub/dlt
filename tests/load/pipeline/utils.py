import pytest

from tests.load.utils import DestinationTestConfiguration


def skip_if_unsupported_replace_strategy(
    destination_config: DestinationTestConfiguration, replace_strategy: str
):
    """Skip test if destination does not support the given replace strategy."""
    supported_replace_strategies = (
        destination_config.raw_capabilities().supported_replace_strategies
    )
    # hardcoded exclusions (that require table schema and selector to be used, here it is not available)
    if (
        destination_config.destination_type == "athena"
        and not destination_config.force_iceberg
        and destination_config.table_format != "iceberg"
    ):
        supported_replace_strategies = ["truncate-and-insert"]

    if replace_strategy not in supported_replace_strategies:
        pytest.skip(
            f"Destination {destination_config.name} does not support the replace strategy"
            f" {replace_strategy}"
        )
