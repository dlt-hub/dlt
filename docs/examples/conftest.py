import sys
import os
import pytest
from unittest.mock import patch

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    ConfigTomlProvider,
    EnvironProvider,
    SecretsTomlProvider,
    StringTomlProvider,
)
from dlt.common.configuration.specs.pluggable_run_context import (
    PluggableRunContext,
)
from dlt.common.utils import set_working_dir

from tests.utils import (
    patch_home_dir,
    autouse_test_storage,
    preserve_environ,
    duckdb_pipeline_location,
    wipe_pipeline,
)


@pytest.fixture(autouse=True)
def setup_secret_providers(request):
    """Creates set of config providers where tomls are loaded from tests/.dlt"""
    secret_dir = os.path.abspath("./.dlt")
    dname = os.path.dirname(request.module.__file__)
    config_dir = dname + "/.dlt"

    # inject provider context so the original providers are restored at the end
    def _initial_providers(self):
        return [
            EnvironProvider(),
            SecretsTomlProvider(settings_dir=secret_dir),
            ConfigTomlProvider(settings_dir=config_dir),
        ]

    with set_working_dir(dname), patch(
        "dlt.common.runtime.run_context.RunContext.initial_providers",
        _initial_providers,
    ):
        Container()[PluggableRunContext].reload_providers()

        try:
            sys.path.insert(0, dname)
            yield
        finally:
            sys.path.pop(0)


def pytest_configure(config):
    # push sentry to ci
    os.environ["RUNTIME__SENTRY_DSN"] = (
        "https://6f6f7b6f8e0f458a89be4187603b55fe@o1061158.ingest.sentry.io/4504819859914752"
    )
