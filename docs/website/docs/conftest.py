import os

import pytest

from dlt.common.configuration.container import Container

# patch which providers to enable
from dlt.common.configuration.providers import (
    ConfigTomlProvider,
    EnvironProvider,
    SecretsTomlProvider,
)
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)


@pytest.fixture(autouse=True, scope="session")
def test_config_providers() -> None:
    """Creates set of config providers where tomls are loaded from tests/.dlt"""
    config_root = "./.dlt"
    ctx = ConfigProvidersContext()
    ctx.providers.clear()
    ctx.add_provider(EnvironProvider())
    ctx.add_provider(
        SecretsTomlProvider(project_dir=config_root, add_global_config=False)
    )
    ctx.add_provider(
        ConfigTomlProvider(project_dir=config_root, add_global_config=False)
    )
    # replace in container
    Container()[ConfigProvidersContext] = ctx
    # extras work when container updated
    ctx.add_extras()


def pytest_configure(config):
    # push sentry to ci
    os.environ[
        "RUNTIME__SENTRY_DSN"
    ] = "https://6f6f7b6f8e0f458a89be4187603b55fe@o1061158.ingest.sentry.io/4504819859914752"
