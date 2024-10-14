import contextlib
import multiprocessing
import os
import platform
import sys
from os import environ
from typing import Any, Iterable, Iterator, Literal, Union, get_args, List
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
    SupportsRunContext,
)
from dlt.common.pipeline import LoadInfo, PipelineContext, SupportsPipeline
from dlt.common.runtime.run_context import DOT_DLT, RunContext
from dlt.common.runtime.telemetry import start_telemetry, stop_telemetry
from dlt.common.schema import Schema
from dlt.common.storages import FileStorage
from dlt.common.storages.versioned_storage import VersionedStorage
from dlt.common.typing import DictStrAny, StrAny, TDataItem
from dlt.common.utils import custom_environ, set_working_dir, uniq_id

TEST_STORAGE_ROOT = "_storage"

ALL_DESTINATIONS = dlt.config.get("ALL_DESTINATIONS", list) or [
    "duckdb",
]


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
}
NON_SQL_DESTINATIONS = {
    "filesystem",
    "weaviate",
    "dummy",
    "qdrant",
    "lancedb",
    "destination",
}
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

    @classmethod
    def from_context(cls, ctx: SupportsRunContext) -> "MockableRunContext":
        cls_ = cls(ctx.run_dir)
        cls_._name = ctx.name
        cls_._global_dir = ctx.global_dir
        cls_._run_dir = ctx.run_dir
        cls_._settings_dir = ctx.settings_dir
        cls_._data_dir = ctx.data_dir
        return cls_


@pytest.fixture(autouse=True)
def patch_home_dir() -> Iterator[None]:
    ctx = PluggableRunContext()
    mock = MockableRunContext.from_context(ctx.context)
    mock._global_dir = mock._data_dir = os.path.join(os.path.abspath(TEST_STORAGE_ROOT), DOT_DLT)
    ctx.context = mock

    with Container().injectable_context(ctx):
        yield


@pytest.fixture(autouse=True)
def patch_random_home_dir() -> Iterator[None]:
    ctx = PluggableRunContext()
    mock = MockableRunContext.from_context(ctx.context)
    mock._global_dir = mock._data_dir = os.path.abspath(
        os.path.join(TEST_STORAGE_ROOT, "global_" + uniq_id(), DOT_DLT)
    )
    ctx.context = mock

    os.makedirs(ctx.context.global_dir, exist_ok=True)
    with Container().injectable_context(ctx):
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
def wipe_pipeline(preserve_environ) -> Iterator[None]:
    """Wipes pipeline local state and deactivates it"""
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
    if not c:
        c = resolve_configuration(RuntimeConfiguration())
    Container()[PluggableRunContext].initialize_runtime(c)


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
    start_telemetry(c)


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


def assert_load_info(info: LoadInfo, expected_load_packages: int = 1) -> None:
    """Asserts that expected number of packages was loaded and there are no failed jobs"""
    assert len(info.loads_ids) == expected_load_packages
    # all packages loaded
    assert all(package.state == "loaded" for package in info.load_packages) is True
    # Explicitly check for no failed job in any load package. In case a terminal exception was disabled by raise_on_failed_jobs=False
    info.raise_on_failed_jobs()


def load_table_counts(p: dlt.Pipeline, *table_names: str) -> DictStrAny:
    """Returns row counts for `table_names` as dict"""
    with p.sql_client() as c:
        query = "\nUNION ALL\n".join(
            [
                f"SELECT '{name}' as name, COUNT(1) as c FROM {c.make_qualified_table_name(name)}"
                for name in table_names
            ]
        )
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}


def assert_query_data(
    p: dlt.Pipeline,
    sql: str,
    table_data: List[Any],
    schema_name: str = None,
    info: LoadInfo = None,
) -> None:
    """Asserts that query selecting single column of values matches `table_data`. If `info` is provided, second column must contain one of load_ids in `info`"""
    with p.sql_client(schema_name=schema_name) as c:
        with c.execute_query(sql) as cur:
            rows = list(cur.fetchall())
            assert len(rows) == len(table_data)
            for r, d in zip(rows, table_data):
                row = list(r)
                # first element comes from the data
                assert row[0] == d
                # the second is load id
                if info:
                    assert row[1] in info.loads_ids


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
