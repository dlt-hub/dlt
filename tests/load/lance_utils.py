from __future__ import annotations

from contextlib import contextmanager
import os
from typing import TYPE_CHECKING, Iterator, Optional

import pytest

from tests.utils import get_test_storage_root, get_test_worker_id, get_test_worker_idx

if TYPE_CHECKING:
    from tests.load.utils import DestinationTestConfiguration


_LANCE_REST_SERVER_ACTIVE = False


class LanceRestServerConfig:
    """Configuration for ephemeral in-process Lance REST server fixture.

    Port is based on xdist worker index to avoid conflicts when running tests in parallel.
    """

    HOST = "127.0.0.1"
    BASE_PORT = 2333
    CATALOG_TYPE_ENV = "DESTINATION__CATALOG_TYPE"
    CREDENTIALS_URI_ENV = "DESTINATION__CREDENTIALS__URI"

    @classmethod
    def needs_fixture(cls, destination_config: Optional[DestinationTestConfiguration]) -> bool:
        return (
            destination_config is not None
            and destination_config.destination_type == "lance"
            and destination_config.env_vars is not None
            and destination_config.env_vars.get(cls.CATALOG_TYPE_ENV) == "rest"
        )

    @classmethod
    def get_port(cls) -> int:
        return cls.BASE_PORT + get_test_worker_idx()

    @classmethod
    def get_url(cls) -> str:
        return f"http://{cls.HOST}:{cls.get_port()}"

    @classmethod
    def get_destination_test_configuration_env_vars(cls) -> dict[str, str]:
        return {
            cls.CATALOG_TYPE_ENV: "rest",
            cls.CREDENTIALS_URI_ENV: cls.get_url(),
        }


def get_lance_namespace_name() -> str:
    # isolate per xdist worker — Lance __manifest writes conflict on S3
    return f"dlt_lance_root_{get_test_worker_id()}"


def extract_destination_test_configuration(
    request: pytest.FixtureRequest,
) -> Optional[DestinationTestConfiguration]:
    """Extract first destination test configuration from pytest fixture context, if available."""
    if "destination_config" in request.fixturenames:
        return request.getfixturevalue("destination_config")

    from tests.load.utils import DestinationTestConfiguration

    callspec = getattr(request.node, "callspec", None)
    if callspec is None:
        return None

    return next(
        (
            param
            for param in callspec.params.values()
            if isinstance(param, DestinationTestConfiguration)
        ),
        None,
    )


@contextmanager
def maybe_lance_rest_server(
    destination_config: Optional[DestinationTestConfiguration],
) -> Iterator[None]:
    """Starts ephemeral in-process Lance REST server if not already active and `destination_config` needs it."""
    global _LANCE_REST_SERVER_ACTIVE

    # NOTE: the module-scoped fixture may have already started the server for the entire module, in
    # which case we can skip starting it again for the auto-used function-scoped fixture
    if _LANCE_REST_SERVER_ACTIVE or not LanceRestServerConfig.needs_fixture(destination_config):
        yield
        return

    from lance.namespace import RestAdapter
    from tests.load.utils import FILE_BUCKET

    root = os.path.join(get_test_storage_root(), FILE_BUCKET, get_lance_namespace_name())
    _LANCE_REST_SERVER_ACTIVE = True
    try:
        with RestAdapter(
            "dir",
            namespace_properties={"root": root},
            host=LanceRestServerConfig.HOST,
            port=LanceRestServerConfig.get_port(),
        ):
            yield
    finally:
        _LANCE_REST_SERVER_ACTIVE = False


@pytest.fixture(scope="function", autouse=True)
def lance_rest_server(request: pytest.FixtureRequest) -> Iterator[None]:
    """Starts function-scoped ephemeral in-process Lance REST server if needed."""
    with maybe_lance_rest_server(extract_destination_test_configuration(request)):
        yield


@pytest.fixture(scope="module")
def module_lance_rest_server(
    destination_config: Optional[DestinationTestConfiguration],
) -> Iterator[None]:
    """Starts module-scoped ephemeral in-process Lance REST server if needed."""
    with maybe_lance_rest_server(destination_config):
        yield
