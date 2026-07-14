from __future__ import annotations

from contextlib import contextmanager
import os
from typing import Iterator, List, Optional
import pytest

from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.storages.configuration import FilesystemConfiguration
from dlt.common.storages.fsspec_filesystem import fsspec_from_config

from tests.load.utils import (
    FILE_BUCKET,
    OBJECT_STORE_RS_BUCKETS,
    DestinationTestConfiguration,
    get_lance_namespace_name,
)
from tests.utils import get_test_storage_root, get_test_worker_idx


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


def lance_rest_destination_configs() -> List[DestinationTestConfiguration]:
    """REST namespace lance configs backed by the in-process test REST server.

    Only local storage-backed REST namespaces are included: `RestAdapter` (our test REST
    Namespace server) does not vend credentials, so cloud-bucket REST namespaces are excluded.
    """
    return [
        DestinationTestConfiguration(
            destination_type="lance",
            extra_info=f"rest-{FilesystemConfiguration.parse_protocol(bucket)}",
            env_vars=LanceRestServerConfig.get_destination_test_configuration_env_vars(),
        )
        for bucket in OBJECT_STORE_RS_BUCKETS
        if bucket == FILE_BUCKET
    ]


@pytest.fixture(scope="session", autouse=True)
def cleanup_lance_namespace_root() -> Iterator[None]:
    """Deletes this session's lance namespace root from remote buckets."""
    yield

    name = get_lance_namespace_name()
    for bucket in OBJECT_STORE_RS_BUCKETS:
        # local files live in the test storage and are cleaned with it
        if bucket == FILE_BUCKET:
            continue
        try:
            cfg = resolve_configuration(
                FilesystemConfiguration(bucket_url=bucket), sections=("destination", "filesystem")
            )
            fs, path = fsspec_from_config(cfg)
            root = f"{path}/{name}"
            if fs.exists(root):
                fs.rm(root, recursive=True)
        except Exception:
            # best effort: no credentials or bucket not used in this session
            pass


def extract_destination_test_configuration(
    request: pytest.FixtureRequest,
) -> Optional[DestinationTestConfiguration]:
    """Extract first destination test configuration from pytest fixture context, if available."""
    if "destination_config" in request.fixturenames:
        return request.getfixturevalue("destination_config")

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

    # guard against re-entry if the server is already running for the current test
    if _LANCE_REST_SERVER_ACTIVE or not LanceRestServerConfig.needs_fixture(destination_config):
        yield
        return

    from lance.namespace import RestAdapter

    root = os.path.join(get_test_storage_root(), FILE_BUCKET, get_lance_namespace_name())
    _LANCE_REST_SERVER_ACTIVE = True
    try:
        with RestAdapter(
            "dir",
            namespace_client_properties={"root": root},
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
