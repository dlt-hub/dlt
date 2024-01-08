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
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
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
    secret_dir = "./.dlt"
    dname = os.path.dirname(request.module.__file__)
    config_dir = dname + "/.dlt"

    # inject provider context so the original providers are restored at the end
    def _initial_providers():
        return [
            EnvironProvider(),
            SecretsTomlProvider(project_dir=secret_dir, add_global_config=False),
            ConfigTomlProvider(project_dir=config_dir, add_global_config=False),
        ]

    glob_ctx = ConfigProvidersContext()
    glob_ctx.providers = _initial_providers()

    with set_working_dir(dname), Container().injectable_context(glob_ctx), patch(
        "dlt.common.configuration.specs.config_providers_context.ConfigProvidersContext.initial_providers",
        _initial_providers,
    ):
        # extras work when container updated
        glob_ctx.add_extras()
        yield


def pytest_configure(config):
    # push sentry to ci
    os.environ["RUNTIME__SENTRY_DSN"] = (
        "https://6f6f7b6f8e0f458a89be4187603b55fe@o1061158.ingest.sentry.io/4504819859914752"
    )
