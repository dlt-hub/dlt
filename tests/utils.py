import os
import sys
import multiprocessing
import platform
import requests
import pytest
from os import environ
from typing import Iterator
from unittest.mock import patch

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import DictionaryProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContext
from dlt.common.runtime.logger import init_logging
from dlt.common.runtime.telemetry import start_telemetry, stop_telemetry
from dlt.common.storages import FileStorage
from dlt.common.schema import Schema
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.typing import StrAny
from dlt.common.utils import custom_environ, uniq_id
from dlt.common.pipeline import PipelineContext

TEST_STORAGE_ROOT = "_storage"

# destination configs
ALL_DESTINATIONS = dlt.config.get("ALL_DESTINATIONS", list) or ["duckdb", "bigquery", "redshift", "postgres", "snowflake"]
ALL_LOCAL_DESTINATIONS = set(ALL_DESTINATIONS).intersection("postgres", "duckdb")


def TEST_DICT_CONFIG_PROVIDER():
    # add test dictionary provider
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
        storage.delete_folder("", recursively=True, delete_ro=True)


@pytest.fixture()
def test_storage() -> FileStorage:
    return clean_test_storage()


@pytest.fixture(autouse=True)
def autouse_test_storage() -> FileStorage:
    return clean_test_storage()


@pytest.fixture(scope="function", autouse=True)
def preserve_environ() -> None:
    saved_environ = environ.copy()
    yield
    environ.clear()
    environ.update(saved_environ)


@pytest.fixture(autouse=True)
def duckdb_pipeline_location() -> None:
    with custom_environ({"DESTINATION__DUCKDB__CREDENTIALS": ":pipeline:"}):
        yield


@pytest.fixture(autouse=True)
def patch_home_dir() -> None:
    with patch("dlt.common.configuration.paths._get_user_home_dir") as _get_home_dir:
        _get_home_dir.return_value = os.path.abspath(TEST_STORAGE_ROOT)
        yield


@pytest.fixture(autouse=True)
def patch_random_home_dir() -> None:
    global_dir = os.path.join(TEST_STORAGE_ROOT, "global_" + uniq_id())
    os.makedirs(global_dir, exist_ok=True)
    with patch("dlt.common.configuration.paths._get_user_home_dir") as _get_home_dir:
        _get_home_dir.return_value = os.path.abspath(global_dir)
        yield


@pytest.fixture(autouse=True)
def unload_modules() -> None:
    """Unload all modules inspected in this tests"""
    prev_modules = dict(sys.modules)
    yield
    mod_diff = set(sys.modules.keys()) - set(prev_modules.keys())
    for mod in mod_diff:
        del sys.modules[mod]


@pytest.fixture(autouse=True)
def wipe_pipeline() -> Iterator[None]:
    container = Container()
    if container[PipelineContext].is_active():
        container[PipelineContext].deactivate()
    yield
    if container[PipelineContext].is_active():
        # take existing pipeline
        p = dlt.pipeline()
        p._wipe_working_folder()
        # deactivate context
        container[PipelineContext].deactivate()


def init_test_logging(c: RunConfiguration = None) -> None:
    if not c:
        c = resolve_configuration(RunConfiguration())
    init_logging(c)


def start_test_telemetry(c: RunConfiguration = None):
    stop_telemetry()
    if not c:
        c = resolve_configuration(RunConfiguration())
    start_telemetry(c)


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

skipifwindows = pytest.mark.skipif(
    platform.system() == "Windows", reason="does not runs on windows"
)
