import pytest

from tests.load.utils import DestinationTestConfiguration

REPLACE_STRATEGIES = ["truncate-and-insert", "insert-from-staging", "staging-optimized"]


def skip_if_unsupported_replace_strategy(
    destination_config: DestinationTestConfiguration, replace_strategy: str
):
    """Skip test if destination does not support the given replace strategy."""
    supported_replace_strategies = (
        destination_config.raw_capabilities().supported_replace_strategies
    )
    if not supported_replace_strategies or replace_strategy not in supported_replace_strategies:
        pytest.skip(
            f"Destination {destination_config.name} does not support the replace strategy"
            f" {replace_strategy}"
        )
