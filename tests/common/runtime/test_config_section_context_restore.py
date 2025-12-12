"""Test that ConfigSectionContext is properly restored in spawned worker processes."""

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


def test_config_section_context_restored_in_spawn_worker() -> None:
    """Test that ConfigSectionContext is properly restored when using spawn method.

    This test verifies that ConfigSectionContext is correctly serialized and restored
    in worker processes, allowing config resolution to use the correct sections.
    """
    # Set up environment variable with section-specific value
    os.environ["MY_SECTION__TEST_SECTION__TEST_VALUE"] = "sectioned_value"
    os.environ["TEST_SECTION__TEST_VALUE"] = "non_sectioned_value"  # Should not be used

    # Set up ConfigSectionContext in main process
    section_context = ConfigSectionContext(
        pipeline_name=None,
        sections=("my_section",),
    )

    # Store it in container
    container = Container()
    container[ConfigSectionContext] = section_context

    # Create process pool with spawn method and multiple workers
    # Using multiple workers ensures we're actually testing cross-process behavior
    config = PoolRunnerConfiguration(
        pool_type="process",
        workers=4,
        start_method="spawn",
    )

    with create_pool(config) as pool:
        # Submit multiple tasks to ensure we're using worker processes
        futures = [pool.submit(_worker_resolve_config) for _ in range(4)]
        results = [f.result() for f in futures]

        # All workers should have the same ConfigSectionContext
        result_value, result_sections = results[0]

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


def test_config_section_context_with_pipeline_name() -> None:
    pipeline_name = "test_pipeline"
    os.environ[f"{pipeline_name.upper()}__MY_SECTION__TEST_SECTION__TEST_VALUE"] = (
        "pipeline_sectioned_value"
    )
    os.environ["MY_SECTION__TEST_SECTION__TEST_VALUE"] = "sectioned_value"

    section_context = ConfigSectionContext(
        pipeline_name=pipeline_name,
        sections=("my_section",),
    )

    container = Container()
    container[ConfigSectionContext] = section_context

    config = PoolRunnerConfiguration(
        pool_type="process",
        workers=4,
        start_method="spawn",
    )

    with create_pool(config) as pool:
        futures = [pool.submit(_worker_resolve_config) for _ in range(4)]
        results = [f.result() for f in futures]

        # Verify all workers got the same context
        for result_value, result_sections in results:
            assert result_sections == ("my_section",)
            # Should prefer pipeline-specific value
            assert result_value == "pipeline_sectioned_value"


def test_config_section_context_empty_sections() -> None:
    os.environ["TEST_SECTION__TEST_VALUE"] = "non_sectioned_value"

    # ConfigSectionContext with empty sections
    section_context = ConfigSectionContext(
        pipeline_name=None,
        sections=(),
    )

    container = Container()
    container[ConfigSectionContext] = section_context

    config = PoolRunnerConfiguration(
        pool_type="process",
        workers=4,
        start_method="spawn",
    )

    with create_pool(config) as pool:
        futures = [pool.submit(_worker_resolve_config) for _ in range(4)]
        results = [f.result() for f in futures]

        # Verify all workers got empty sections
        for result_value, result_sections in results:
            assert result_sections == (), "Empty sections should be preserved"
            assert result_value == "non_sectioned_value", "Should use non-sectioned value"
