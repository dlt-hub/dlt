import multiprocessing
import os
import platform
import sys
from os import environ
from typing import Any, Iterable, Iterator, List, Literal, Union, get_args
from unittest.mock import patch

import pytest
import requests
from requests import Response

import dlt
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import DictionaryProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)
from dlt.common.pipeline import PipelineContext
from dlt.common.runtime.logger import init_logging
from dlt.common.runtime.telemetry import start_telemetry, stop_telemetry
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.typing import StrAny, TDataItem
from dlt.common.utils import custom_environ, uniq_id
from dlt.common.pipeline import PipelineContext, SupportsPipeline

TEST_STORAGE_ROOT = "_storage"


# destination constants
IMPLEMENTED_DESTINATIONS = {
    "athena",
    "duckdb",
    "bigquery",
    "redshift",
    "postgres",
    "snowflake",
    "filesystem",
    "weaviate",
    "dummy",
    "motherduck",
    "mssql",
    "qdrant",
    "synapse",
}
NON_SQL_DESTINATIONS = {"filesystem", "weaviate", "dummy", "motherduck", "qdrant"}
SQL_DESTINATIONS = IMPLEMENTED_DESTINATIONS - NON_SQL_DESTINATIONS

# exclude destination configs (for now used for athena and athena iceberg separation)
EXCLUDED_DESTINATION_CONFIGURATIONS = set(
    dlt.config.get("EXCLUDED_DESTINATION_CONFIGURATIONS", list) or set()
)


# filter out active destinations for current tests
ACTIVE_DESTINATIONS = set(dlt.config.get("ACTIVE_DESTINATIONS", list) or IMPLEMENTED_DESTINATIONS)

ACTIVE_SQL_DESTINATIONS = SQL_DESTINATIONS.intersection(ACTIVE_DESTINATIONS)
ACTIVE_NON_SQL_DESTINATIONS = NON_SQL_DESTINATIONS.intersection(ACTIVE_DESTINATIONS)

# sanity checks
assert len(ACTIVE_DESTINATIONS) >= 0, "No active destinations selected"

for destination in NON_SQL_DESTINATIONS:
    assert destination in IMPLEMENTED_DESTINATIONS, f"Unknown non sql destination {destination}"

for destination in SQL_DESTINATIONS:
    assert destination in IMPLEMENTED_DESTINATIONS, f"Unknown sql destination {destination}"

for destination in ACTIVE_DESTINATIONS:
    assert destination in IMPLEMENTED_DESTINATIONS, f"Unknown active destination {destination}"


# possible TDataItem types
TDataItemFormat = Literal["json", "pandas", "arrow", "arrow-batch"]
ALL_DATA_ITEM_FORMATS = get_args(TDataItemFormat)
"""List with TDataItem formats: json, arrow table/batch / pandas"""


def TEST_DICT_CONFIG_PROVIDER():
    # add test dictionary provider
    providers_context = Container()[ConfigProvidersContext]
    try:
        return providers_context[DictionaryProvider.NAME]
    except KeyError:
        provider = DictionaryProvider()
        providers_context.add_provider(provider)
        return provider


class MockHttpResponse(Response):
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 300:
            raise requests.HTTPError(response=self)


class MockPipeline(SupportsPipeline):
    def __init__(self, pipeline_name: str, first_run: bool) -> None:
        self.pipeline_name = pipeline_name
        self.first_run = first_run


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
def preserve_environ() -> Iterator[None]:
    saved_environ = environ.copy()
    yield
    environ.clear()
    environ.update(saved_environ)


@pytest.fixture(autouse=True)
def duckdb_pipeline_location() -> Iterator[None]:
    with custom_environ({"DESTINATION__DUCKDB__CREDENTIALS": ":pipeline:"}):
        yield


@pytest.fixture(autouse=True)
def patch_home_dir() -> Iterator[None]:
    with patch("dlt.common.configuration.paths._get_user_home_dir") as _get_home_dir:
        _get_home_dir.return_value = os.path.abspath(TEST_STORAGE_ROOT)
        yield


@pytest.fixture(autouse=True)
def patch_random_home_dir() -> Iterator[None]:
    global_dir = os.path.join(TEST_STORAGE_ROOT, "global_" + uniq_id())
    os.makedirs(global_dir, exist_ok=True)
    with patch("dlt.common.configuration.paths._get_user_home_dir") as _get_home_dir:
        _get_home_dir.return_value = os.path.abspath(global_dir)
        yield


@pytest.fixture(autouse=True)
def unload_modules() -> Iterator[None]:
    """Unload all modules inspected in this tests"""
    prev_modules = dict(sys.modules)
    yield
    mod_diff = set(sys.modules.keys()) - set(prev_modules.keys())
    for mod in mod_diff:
        del sys.modules[mod]


@pytest.fixture(autouse=True)
def wipe_pipeline() -> Iterator[None]:
    """Wipes pipeline local state and deactivates it"""
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


def data_to_item_format(
    item_format: TDataItemFormat, data: Union[Iterator[TDataItem], Iterable[TDataItem]]
) -> Any:
    """Return the given data in the form of pandas, arrow table/batch or json items"""
    if item_format == "json":
        return data

    import pandas as pd
    from dlt.common.libs.pyarrow import pyarrow as pa

    # Make dataframe from the data
    df = pd.DataFrame(list(data))
    if item_format == "pandas":
        return [df]
    elif item_format == "arrow":
        return [pa.Table.from_pandas(df)]
    elif item_format == "arrow-batch":
        return [pa.RecordBatch.from_pandas(df)]
    else:
        raise ValueError(f"Unknown item format: {item_format}")


def init_test_logging(c: RunConfiguration = None) -> None:
    if not c:
        c = resolve_configuration(RunConfiguration())
    init_logging(c)


def start_test_telemetry(c: RunConfiguration = None):
    stop_telemetry()
    if not c:
        c = resolve_configuration(RunConfiguration())
    start_telemetry(c)


def clean_test_storage(
    init_normalize: bool = False, init_loader: bool = False, mode: str = "t"
) -> FileStorage:
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


def skip_if_not_active(destination: str) -> None:
    assert destination in IMPLEMENTED_DESTINATIONS, f"Unknown skipped destination {destination}"
    if destination not in ACTIVE_DESTINATIONS:
        pytest.skip(f"{destination} not in ACTIVE_DESTINATIONS", allow_module_level=True)


def is_running_in_github_fork() -> bool:
    """Check if executed by GitHub Actions, in a repo fork."""
    is_github_actions = os.environ.get("GITHUB_ACTIONS") == "true"
    is_fork = os.environ.get("IS_FORK") == "true"  # custom var set by us in the workflow's YAML
    return is_github_actions and is_fork


skipifspawn = pytest.mark.skipif(
    multiprocessing.get_start_method() != "fork", reason="process fork not supported"
)

skipifpypy = pytest.mark.skipif(
    platform.python_implementation() == "PyPy", reason="won't run in PyPy interpreter"
)

skipifnotwindows = pytest.mark.skipif(platform.system() != "Windows", reason="runs only on windows")

skipifwindows = pytest.mark.skipif(
    platform.system() == "Windows", reason="does not runs on windows"
)

skipifgithubfork = pytest.mark.skipif(
    is_running_in_github_fork(), reason="Skipping test because it runs on a PR coming from fork"
)
