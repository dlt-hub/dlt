import multiprocessing
import platform
from typing import Any, Mapping
import requests
import pytest
import logging
from os import environ

from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import EnvironProvider, DictionaryProvider
from dlt.common.configuration.utils import serialize_value
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import BaseConfiguration, RunConfiguration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.logger import init_logging_from_config
from dlt.common.storages import FileStorage
from dlt.common.schema import Schema
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.typing import StrAny


TEST_STORAGE_ROOT = "_storage"
ALL_DESTINATIONS = ["bigquery", "redshift", "postgres"]
# ALL_DESTINATIONS = ["redshift"]

# add test dictionary provider
def TEST_DICT_CONFIG_PROVIDER():
    providers_context = Container()[ConfigProvidersContext]
    try:
        return providers_context[DictionaryProvider.NAME]
    except KeyError:
        provider = DictionaryProvider()
        providers_context.add_provider(provider)
        return provider


class MockHttpResponse():
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 300:
            raise requests.HTTPError(response=self)


def write_version(storage: FileStorage, version: str) -> None:
    storage.save(VersionedStorage.VERSION_FILE, str(version))


def delete_test_storage() -> None:
    storage = FileStorage(TEST_STORAGE_ROOT)
    if storage.has_folder(""):
        storage.delete_folder("", recursively=True)


@pytest.fixture()
def test_storage() -> FileStorage:
    return clean_test_storage()


@pytest.fixture(autouse=True)
def autouse_test_storage() -> FileStorage:
    return clean_test_storage()


@pytest.fixture(scope="module", autouse=True)
def preserve_environ() -> None:
    saved_environ = environ.copy()
    yield
    environ.clear()
    environ.update(saved_environ)


def init_logger(C: RunConfiguration = None) -> None:
    if not hasattr(logging, "health"):
        if not C:
            C = resolve_configuration(RunConfiguration())
        init_logging_from_config(C)


def clean_test_storage(init_normalize: bool = False, init_loader: bool = False, mode: str = "t") -> FileStorage:
    storage = FileStorage(TEST_STORAGE_ROOT, mode, makedirs=True)
    storage.delete_folder("", recursively=True, delete_ro=True)
    storage.create_folder(".")
    if init_normalize:
        from dlt.common.storages import NormalizeStorage
        NormalizeStorage(True)
    if init_loader:
        from dlt.common.storages import LoadStorage
        LoadStorage(True, "jsonl", LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
    return storage


def add_config_to_env(config: BaseConfiguration) ->  None:
    # write back default values in configuration back into environment
    return add_config_dict_to_env(dict(config), config.__namespace__)


def add_config_dict_to_env(dict_: Mapping[str, Any], namespace: str = None, overwrite_keys: bool = False) -> None:
    for k, v in dict_.items():
        env_key = EnvironProvider.get_key_name(k, namespace)
        if env_key not in environ or overwrite_keys:
            if v is not None:
                environ[env_key] = serialize_value(v)


def create_schema_with_name(schema_name) -> Schema:
    schema = Schema(schema_name)
    return schema


def assert_no_dict_key_starts_with(d: StrAny, key_prefix: str) -> None:
    assert all(not key.startswith(key_prefix) for key in d.keys())


skipifspawn = pytest.mark.skipif(
    multiprocessing.get_start_method() != "fork", reason="process fork not supported"
)

skipifpypy = pytest.mark.skipif(
    platform.python_implementation() == "PyPy", reason="won't run in PyPy interpreter"
)

skipifnotwindows = pytest.mark.skipif(
    platform.system() != "Windows", reason="runs only on windows"
)
