import contextlib
from http import HTTPStatus
import http.server
import multiprocessing
import os
import platform
import threading
import sys
from functools import partial
from os import environ
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal, Optional, Union, get_args, List
from unittest.mock import patch

import pytest
import requests
from requests import Response

import dlt
from dlt.common import known_env
from dlt.common.runtime import telemetry
from dlt.common.configuration.container import Container
from dlt.common.configuration.providers import (
    DictionaryProvider,
    EnvironProvider,
    SecretsTomlProvider,
    ConfigTomlProvider,
)
from dlt.common.configuration.providers.provider import ConfigProvider
from dlt.common.configuration.resolve import resolve_configuration
from dlt.common.configuration.specs import RuntimeConfiguration, PluggableRunContext, configspec
from dlt.common.configuration.specs.config_providers_context import ConfigProvidersContainer
from dlt.common.configuration.specs.pluggable_run_context import (
    RunContextBase,
)
from dlt.common.pipeline import PipelineContext, SupportsPipeline
from dlt.common.runtime.run_context import DOT_DLT, RunContext
from dlt.common.runtime.telemetry import stop_telemetry
from dlt.common.schema import Schema
from dlt.common.schema.typing import TTableFormat
from dlt.common.storages import FileStorage
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.typing import StrAny, TDataItem, PathLike
from dlt.common.utils import set_working_dir

TEST_STORAGE_ROOT = "_storage"

ALL_DESTINATIONS = dlt.config.get("ALL_DESTINATIONS", list) or [
    "duckdb",
]

LOCAL_POSTGRES_CREDENTIALS = "postgresql://loader:loader@localhost:5432/dlt_data"


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
    "lancedb",
    "destination",
    "synapse",
    "databricks",
    "clickhouse",
    "dremio",
    "sqlalchemy",
    "ducklake",
}
NON_SQL_DESTINATIONS = {
    "filesystem",
    "weaviate",
    "dummy",
    "qdrant",
    "lancedb",
    "destination",
}

try:
    # register additional destinations from _addons.py which must be placed in the same folder
    # as tests
    from _addons import register_destinations  # type: ignore[import-not-found]

    register_destinations(IMPLEMENTED_DESTINATIONS, NON_SQL_DESTINATIONS)
except ImportError:
    pass

SQL_DESTINATIONS = IMPLEMENTED_DESTINATIONS - NON_SQL_DESTINATIONS

# exclude destination configs (for now used for athena and athena iceberg separation)
EXCLUDED_DESTINATION_CONFIGURATIONS = set(
    dlt.config.get("EXCLUDED_DESTINATION_CONFIGURATIONS", list) or set()
)


# filter out active destinations for current tests
ACTIVE_DESTINATIONS = set(dlt.config.get("ACTIVE_DESTINATIONS", list) or IMPLEMENTED_DESTINATIONS)

ACTIVE_SQL_DESTINATIONS = SQL_DESTINATIONS.intersection(ACTIVE_DESTINATIONS)
ACTIVE_NON_SQL_DESTINATIONS = NON_SQL_DESTINATIONS.intersection(ACTIVE_DESTINATIONS)

# filter out active table formats for current tests
IMPLEMENTED_TABLE_FORMATS = set(get_args(TTableFormat))
ACTIVE_TABLE_FORMATS = set(
    dlt.config.get("ACTIVE_TABLE_FORMATS", list) or IMPLEMENTED_TABLE_FORMATS
)

# sanity checks
assert len(ACTIVE_DESTINATIONS) >= 0, "No active destinations selected"

for destination in NON_SQL_DESTINATIONS:
    assert destination in IMPLEMENTED_DESTINATIONS, f"Unknown non sql destination {destination}"

for destination in SQL_DESTINATIONS:
    assert destination in IMPLEMENTED_DESTINATIONS, f"Unknown sql destination {destination}"

for destination in ACTIVE_DESTINATIONS:
    assert destination in IMPLEMENTED_DESTINATIONS, f"Unknown active destination {destination}"

TPythonTableFormat = Literal["pandas", "arrow-table", "arrow-batch"]
"""Possible arrow item formats"""

TestDataItemFormat = Literal["object", "pandas", "arrow-table", "arrow-batch"]
ALL_TEST_DATA_ITEM_FORMATS = get_args(TestDataItemFormat)
"""List with TDataItem formats: object, arrow table/batch / pandas"""


def TEST_DICT_CONFIG_PROVIDER():
    # add test dictionary provider
    providers_context = Container()[PluggableRunContext].providers
    try:
        return providers_context[DictionaryProvider.NAME]
    except KeyError:
        provider = DictionaryProvider()
        providers_context.add_provider(provider)
        return provider


class PublicCDNHandler(http.server.SimpleHTTPRequestHandler):
    @classmethod
    def factory(cls, *args, directory: Path) -> "PublicCDNHandler":
        return cls(*args, directory=directory)

    def __init__(self, *args, directory: Optional[Path] = None):
        super().__init__(*args, directory=str(directory) if directory else None)

    def list_directory(self, path: Union[str, PathLike]) -> None:
        self.send_error(HTTPStatus.FORBIDDEN, "Directory listing is forbidden")
        return None


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
def autouse_test_storage(request) -> Optional[FileStorage]:
    # skip storage cleanup if module level fixture is activated
    if not request.keywords.get("skip_autouse_test_storage", False):
        return clean_test_storage()
    return None


@pytest.fixture(scope="function", autouse=True)
def preserve_environ() -> Iterator[None]:
    yield from _preserve_environ()


@pytest.fixture(autouse=True)
def auto_test_run_context() -> Iterator[None]:
    """Creates a run context that points to TEST_STORAGE_ROOT (_storage)"""
    yield from create_test_run_context()


@pytest.fixture(autouse=True, scope="module")
def auto_module_test_storage(request) -> FileStorage:
    request.keywords["skip_autouse_test_storage"] = True
    return clean_test_storage()


@pytest.fixture(autouse=True, scope="module")
def preserve_module_environ() -> Iterator[None]:
    yield from _preserve_environ()


@pytest.fixture(autouse=True, scope="module")
def auto_module_test_run_context(auto_module_test_storage) -> Iterator[None]:
    yield from create_test_run_context()


@pytest.fixture
def public_http_server():
    """
    A simple HTTP server serving files from the current directory.
    Used to simulate public CDN. It allows only file access, directory listing is forbidden.
    """
    httpd = http.server.ThreadingHTTPServer(
        ("localhost", 8189),
        partial(
            PublicCDNHandler.factory, directory=Path.cwd().joinpath("tests/common/storages/samples")
        ),
    )
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()
    try:
        yield httpd
    finally:
        # always close
        httpd.shutdown()
        server_thread.join()
        httpd.server_close()


def create_test_run_context() -> Iterator[None]:
    # this plugs active context
    ctx = PluggableRunContext()
    mock = MockableRunContext.from_context(ctx.context)
    mock._local_dir = os.path.abspath(TEST_STORAGE_ROOT)
    mock._global_dir = mock._data_dir = os.path.join(mock._local_dir, DOT_DLT)
    ctx_plug = Container()[PluggableRunContext]
    cookie = ctx_plug.push_context()
    try:
        ctx_plug.reload(mock)
        yield
    finally:
        assert ctx_plug is Container()[PluggableRunContext], "PluggableRunContext was replaced"
        ctx_plug.pop_context(cookie)


def _preserve_environ() -> Iterator[None]:
    saved_environ = environ.copy()
    # delta-rs sets those keys without updating environ and there's no
    # method to refresh environ
    known_environ = {
        key_: saved_environ.get(key_)
        for key_ in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION",
            "AWS_SESSION_TOKEN",
        ]
    }
    try:
        yield
    finally:
        environ.clear()
        environ.update(saved_environ)
        for key_, value_ in known_environ.items():
            if value_ is not None or key_ not in environ:
                environ[key_] = value_ or ""
            else:
                del environ[key_]


@pytest.fixture(autouse=True)
def preserve_run_context() -> Iterator[None]:
    """Restores initial run context when test completes"""
    ctx_plug = Container()[PluggableRunContext]
    cookie = ctx_plug.push_context()
    try:
        yield
    finally:
        assert ctx_plug is Container()[PluggableRunContext], "PluggableRunContext was replaced"
        ctx_plug.pop_context(cookie)


class MockableRunContext(RunContext):
    @property
    def name(self) -> str:
        return self._name

    @property
    def global_dir(self) -> str:
        return self._global_dir

    @property
    def run_dir(self) -> str:
        return os.environ.get(known_env.DLT_PROJECT_DIR, self._run_dir)

    @property
    def local_dir(self) -> str:
        return os.environ.get(known_env.DLT_LOCAL_DIR, self._local_dir)

    # @property
    # def settings_dir(self) -> str:
    #     return self._settings_dir

    @property
    def data_dir(self) -> str:
        return os.environ.get(known_env.DLT_DATA_DIR, self._data_dir)

    _name: str
    _global_dir: str
    _run_dir: str
    _settings_dir: str
    _data_dir: str
    _local_dir: str

    @classmethod
    def from_context(cls, ctx: RunContextBase) -> "MockableRunContext":
        cls_ = cls(ctx.run_dir)
        cls_._name = ctx.name
        cls_._global_dir = ctx.global_dir
        cls_._run_dir = ctx.run_dir
        cls_._settings_dir = ctx.settings_dir
        cls_._data_dir = ctx.data_dir
        cls_._local_dir = ctx.local_dir
        cls_._runtime_config = ctx.runtime_config
        return cls_


@pytest.fixture(autouse=True)
def auto_unload_modules() -> Iterator[None]:
    """Unload all modules inspected in this tests"""
    prev_modules = dict(sys.modules)
    yield
    mod_diff = set(sys.modules.keys()) - set(prev_modules.keys())
    for mod in mod_diff:
        del sys.modules[mod]


@pytest.fixture(autouse=True)
def deactivate_pipeline(preserve_environ) -> Iterator[None]:
    """Deactivates pipeline. Local state is not removed"""
    container = Container()
    if container[PipelineContext].is_active():
        container[PipelineContext].deactivate()
    yield
    if container[PipelineContext].is_active():
        # take existing pipeline
        # NOTE: no more needed. test storage is wiped fully when test starts
        # p = dlt.pipeline()
        # p._wipe_working_folder()
        # deactivate context
        container[PipelineContext].deactivate()


@pytest.fixture(autouse=True)
def setup_secret_providers_to_current_module(request):
    """Creates set of config providers where secrets are loaded from cwd()/.dlt and
    configs are loaded from the .dlt/ in the same folder as module being tested
    """
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

    with (
        set_working_dir(dname),
        patch(
            "dlt.common.runtime.run_context.RunContext.initial_providers",
            _initial_providers,
        ),
    ):
        Container()[PluggableRunContext].reload_providers()

        try:
            sys.path.insert(0, dname)
            yield
        finally:
            sys.path.pop(0)


def data_to_item_format(
    item_format: TestDataItemFormat, data: Union[Iterator[TDataItem], Iterable[TDataItem]]
) -> Any:
    """Return the given data in the form of pandas, arrow table/batch or json items"""
    if item_format == "object":
        return data

    import pandas as pd
    from dlt.common.libs.pyarrow import pyarrow as pa

    # Make dataframe from the data
    df = pd.DataFrame(list(data))
    if item_format == "pandas":
        return [df]
    elif item_format == "arrow-table":
        return [pa.Table.from_pandas(df)]
    elif item_format == "arrow-batch":
        return [pa.RecordBatch.from_pandas(df)]
    else:
        raise ValueError(f"Unknown item format: {item_format}")


def data_item_length(data: TDataItem) -> int:
    import pandas as pd
    from dlt.common.libs.pyarrow import pyarrow as pa

    if isinstance(data, list):
        # If data is a list, check if it's a list of supported data types
        if all(isinstance(item, (list, pd.DataFrame, pa.Table, pa.RecordBatch)) for item in data):
            return sum(data_item_length(item) for item in data)
        # If it's a list but not a list of supported types, treat it as a single list object
        else:
            return len(data)
    elif isinstance(data, pd.DataFrame):
        return len(data.index)
    elif isinstance(data, pa.Table) or isinstance(data, pa.RecordBatch):
        return data.num_rows
    else:
        raise TypeError("Unsupported data type.")


def arrow_item_from_pandas(
    df: Any,
    object_format: TPythonTableFormat,
) -> Any:
    from dlt.common.libs.pyarrow import pyarrow as pa

    if object_format == "pandas":
        return df
    elif object_format == "arrow-table":
        return pa.Table.from_pandas(df)
    elif object_format == "arrow-batch":
        return pa.RecordBatch.from_pandas(df)
    raise ValueError("Unknown item type: " + object_format)


def arrow_item_from_table(
    table: Any,
    object_format: TPythonTableFormat,
) -> Any:
    if object_format == "pandas":
        return table.to_pandas()
    elif object_format == "arrow-table":
        return table
    elif object_format == "arrow-batch":
        return table.to_batches()[0]
    raise ValueError("Unknown item type: " + object_format)


def init_test_logging(c: RuntimeConfiguration = None) -> None:
    ctx = Container()[PluggableRunContext].context
    ctx.initialize_runtime(c)


@configspec
class SentryLoggerConfiguration(RuntimeConfiguration):
    pipeline_name: str = "logger"
    sentry_dsn: str = (
        "https://6f6f7b6f8e0f458a89be4187603b55fe@o1061158.ingest.sentry.io/4504819859914752"
    )


def start_test_telemetry(c: RuntimeConfiguration = None):
    stop_telemetry()
    if not c:
        c = resolve_configuration(RuntimeConfiguration())
        c.dlthub_telemetry = True
    ctx = Container()[PluggableRunContext].context
    ctx.initialize_runtime(c)


@pytest.fixture
def temporary_telemetry() -> Iterator[RuntimeConfiguration]:
    c = SentryLoggerConfiguration()
    start_test_telemetry(c)
    try:
        yield c
    finally:
        stop_telemetry()


@pytest.fixture
def disable_temporary_telemetry() -> Iterator[None]:
    try:
        yield
    finally:
        # force stop telemetry
        telemetry._TELEMETRY_STARTED = True
        stop_telemetry()
        from dlt.common.runtime import anon_tracker

        assert anon_tracker._ANON_TRACKER_ENDPOINT is None


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

        LoadStorage(True, LoadStorage.ALL_SUPPORTED_FILE_FORMATS)
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
    is_github_actions = is_running_in_github_ci()
    is_fork = os.environ.get("IS_FORK") == "true"  # custom var set by us in the workflow's YAML
    return is_github_actions and is_fork


def is_running_in_github_ci() -> bool:
    """Check if executed by GitHub Actions"""
    return os.environ.get("GITHUB_ACTIONS") == "true"


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

skipifgithubci = pytest.mark.skipif(
    is_running_in_github_ci(), reason="This test does not work on github CI"
)


@contextlib.contextmanager
def reset_providers(settings_dir: str) -> Iterator[ConfigProvidersContainer]:
    """Context manager injecting standard set of providers where toml providers are initialized from `settings_dir`"""
    return _reset_providers(settings_dir)


def _reset_providers(settings_dir: str) -> Iterator[ConfigProvidersContainer]:
    yield from _inject_providers(
        [
            EnvironProvider(),
            SecretsTomlProvider(settings_dir=settings_dir),
            ConfigTomlProvider(settings_dir=settings_dir),
        ]
    )


@contextlib.contextmanager
def inject_providers(providers: List[ConfigProvider]) -> Iterator[ConfigProvidersContainer]:
    return _inject_providers(providers)


def _inject_providers(providers: List[ConfigProvider]) -> Iterator[ConfigProvidersContainer]:
    container = Container()
    ctx = ConfigProvidersContainer(initial_providers=providers)
    try:
        old_providers = container[PluggableRunContext].providers
        container[PluggableRunContext].providers = ctx
        yield ctx
    finally:
        container[PluggableRunContext].providers = old_providers
