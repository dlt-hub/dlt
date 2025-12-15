"""Test that ConfigSectionContext is properly restored in worker processes."""

import os
from typing import Tuple, ClassVar

import pytest

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext, ConfigSectionContext
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.runners.configuration import PoolRunnerConfiguration
from dlt.common.runners.pool_runner import create_pool


@configspec
class SectionedTestConfig(BaseConfiguration):
    """A test configuration that uses a specific section."""

    test_value: str = "default"

    __section__: ClassVar[str] = "test_section"


def _worker_resolve_config() -> Tuple[str, Tuple[str, ...]]:
    """Worker function that resolves a config value using ConfigSectionContext.

    Returns:
        Tuple of (resolved_value, sections_from_context)
    """
    section_ctx = Container()[ConfigSectionContext]
    config = resolve_configuration(SectionedTestConfig())

    return config.test_value, section_ctx.sections


@pytest.mark.parametrize("start_method", ["spawn", "fork"])
@pytest.mark.parametrize("use_section_context", [True, False])
def test_config_section_context_restored_in_worker(
    start_method: str, use_section_context: bool
) -> None:
    """Test that ConfigSectionContext is properly restored in worker processes.

    This test verifies that ConfigSectionContext is correctly serialized and restored
    in worker processes, allowing config resolution to use the correct sections.
    When no ConfigSectionContext is set, workers should use the default empty sections.
    """
    # Set up environment variables with section-specific values
    os.environ["MY_SECTION__TEST_SECTION__TEST_VALUE"] = "sectioned_value"
    os.environ["TEST_SECTION__TEST_VALUE"] = "non_sectioned_value"

    container = Container()

    if use_section_context:
        # Set up ConfigSectionContext in main process
        section_context = ConfigSectionContext(
            pipeline_name=None,
            sections=("my_section",),
        )
        container[ConfigSectionContext] = section_context
    else:
        # Ensure no ConfigSectionContext is in container
        del container[ConfigSectionContext]

    # Create process pool with multiple workers
    # Using multiple workers ensures we're actually testing cross-process behavior
    config = PoolRunnerConfiguration(
        pool_type="process",
        workers=4,
        start_method=start_method,
    )

    with create_pool(config) as pool:
        # Submit multiple tasks to ensure we're using worker processes
        futures = [pool.submit(_worker_resolve_config) for _ in range(4)]
        results = [f.result() for f in futures]

        # All workers should have the same ConfigSectionContext
        result_value, result_sections = results[0]

    if use_section_context:
        # Verify that ConfigSectionContext was restored correctly
        assert result_sections == ("my_section",), (
            f"Expected sections ('my_section',) but got {result_sections}. "
            "ConfigSectionContext was not properly restored in worker process."
        )
        # Verify that config resolution used the correct sections
        assert result_value == "sectioned_value", (
            f"Expected 'sectioned_value' but got '{result_value}'. "
            "Config resolution did not use the restored ConfigSectionContext sections."
        )
    else:
        # Without section context, should use default empty sections
        assert result_sections == (), (
            f"Expected empty sections () but got {result_sections}. "
            "ConfigSectionContext should have default empty sections when not set."
        )
        # Verify that config resolution used the non-sectioned value
        assert result_value == "non_sectioned_value", (
            f"Expected 'non_sectioned_value' but got '{result_value}'. "
            "Config resolution should use non-sectioned value when no ConfigSectionContext is set."
        )
