from dataclasses import dataclass
from typing import Any, Iterator, Optional, cast

import pytest
from pytest_mock import MockerFixture
from zerobus import IPCCompression
from zerobus.sdk.shared import NonRetriableException, ZerobusException

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.destination.client import LoadJob, TLoadJobState
from dlt.common.destination.exceptions import (
    DestinationInvalidFileFormat,
    WriteDispositionNotSupported,
)
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.schema.utils import new_table
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import LoadJobTerminalException, LoadJobTransientException
from dlt.destinations.impl.databricks.configuration import (
    DatabricksClientConfiguration,
    DatabricksZerobusConfiguration,
    DatabricksZerobusCredentials,
)
from dlt.destinations.impl.databricks.databricks import (
    DatabricksClient,
    DatabricksLoadJob,
    DatabricksZerobusJsonlLoadJob,
    DatabricksZerobusParquetLoadJob,
)
from dlt.destinations.impl.databricks.databricks_adapter import INSERT_API_HINT
from dlt.destinations.impl.databricks.typing import TDatabricksInsertApi
from tests.load.utils import yield_client


pytestmark = pytest.mark.essential


@pytest.fixture(scope="function")
def client() -> Iterator[DatabricksClient]:
    dataset_name = "test_" + uniq_id()
    yield from cast(
        Iterator[DatabricksClient],
        # skip entering the client to avoid starting the Databricks cluster, which takes multiple
        # minutes and is not necessary for these tests
        yield_client("databricks", dataset_name=dataset_name, enter_client=False),
    )


@pytest.mark.parametrize(
    ("config_insert_api", "table_insert_api", "expected_insert_api"),
    [
        ("copy_into", None, "copy_into"),
        ("zerobus", None, "zerobus"),
        ("zerobus", "copy_into", "copy_into"),
        ("copy_into", "zerobus", "zerobus"),
    ],
)
def test_databricks_client_prepare_load_table_resolves_insert_api(
    client: DatabricksClient,
    config_insert_api: TDatabricksInsertApi,
    table_insert_api: Optional[TDatabricksInsertApi],
    expected_insert_api: TDatabricksInsertApi,
) -> None:
    client.config.insert_api = config_insert_api
    table = new_table("items", write_disposition="append")
    if table_insert_api is not None:
        table[INSERT_API_HINT] = table_insert_api  # type: ignore[typeddict-unknown-key]
    client.schema.update_table(table)

    prepared_table = client.prepare_load_table("items")
    prepared_dlt_table = client.prepare_load_table(client.schema.version_table_name)

    assert prepared_table[INSERT_API_HINT] == expected_insert_api  # type: ignore[typeddict-item]
    # dlt tables should disregard `insert_api` configuration and always use `copy_into`
    assert prepared_dlt_table[INSERT_API_HINT] == "copy_into"  # type: ignore[typeddict-item]


def test_databricks_client_verify_schema_zerobus_file_format(client: DatabricksClient) -> None:
    """Asserts exception is raised if `zerobus` insert API is used with `model` file format."""

    table = new_table("items", write_disposition="append")
    table[INSERT_API_HINT] = "zerobus"  # type: ignore[typeddict-unknown-key]
    client.schema.update_table(table)

    with pytest.raises(DestinationInvalidFileFormat) as exc_info:
        client.verify_schema(
            ["items"],
            [ParsedLoadJobFileName.parse("items.1.1.model")],
        )

    assert exc_info.value.file_format == "model"


@pytest.mark.parametrize("write_disposition", ("replace", "merge"))
def test_databricks_client_verify_schema_zerobus_write_disposition(
    client: DatabricksClient,
    write_disposition: TWriteDisposition,
) -> None:
    """Asserts exception is raised if `zerobus` insert API is used with non-`append` write disposition."""

    table = new_table("items", write_disposition=write_disposition)
    table[INSERT_API_HINT] = "zerobus"  # type: ignore[typeddict-unknown-key]
    client.schema.update_table(table)

    with pytest.raises(WriteDispositionNotSupported) as exc_info:
        client.verify_schema(["items"])

    assert exc_info.value.write_disposition == write_disposition


def test_databricks_client_verify_schema_zerobus_requires_config(
    client: DatabricksClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Asserts Zerobus configuration must be present when `zerobus` insert API is used."""

    table = new_table("items", write_disposition="append")
    table[INSERT_API_HINT] = "zerobus"  # type: ignore[typeddict-unknown-key]
    client.schema.update_table(table)
    monkeypatch.setattr(client.config, "zerobus", None)  # temporarily remove Zerobus configuration

    with pytest.raises(ConfigurationValueError, match="Zerobus configuration is required"):
        client.verify_schema(["items"])


@pytest.mark.parametrize(
    ("insert_api", "file_extension", "expected_class", "expected_exception_match"),
    [
        (None, "jsonl", DatabricksLoadJob, None),
        ("copy_into", "parquet", DatabricksLoadJob, None),
        ("copy_into", "reference", DatabricksLoadJob, None),
        ("zerobus", "jsonl", DatabricksZerobusJsonlLoadJob, None),
        ("zerobus", "parquet", DatabricksZerobusParquetLoadJob, None),
        ("zerobus", "reference", None, "does not support using a staging destination"),
    ],
)
def test_databricks_client_get_load_job_class(
    client: DatabricksClient,
    insert_api: Optional[TDatabricksInsertApi],
    file_extension: str,
    expected_class: Optional[type[LoadJob]],
    expected_exception_match: Optional[str],
) -> None:
    table_name = "foo"
    table = new_table(table_name, write_disposition="append")
    if insert_api is not None:
        table[INSERT_API_HINT] = insert_api  # type: ignore[typeddict-unknown-key]
    client.schema.update_table(table)

    prepared_table = client.prepare_load_table(table_name)
    file_path = f"{table_name}.1.1.{file_extension}"

    if expected_exception_match is None:
        assert client.get_load_job_class(prepared_table, file_path) is expected_class
    else:
        with pytest.raises(LoadJobTerminalException, match=expected_exception_match):
            client.get_load_job_class(prepared_table, file_path)


def test_databricks_zerobus_load_job_calls_create_arrow_stream_with_expected_args(
    mocker: MockerFixture,
) -> None:
    @dataclass
    class FakeSqlClient:
        def make_qualified_table_name(self, table_name: str, quote: bool = False) -> str:
            return "catalog.schema.items"

    @dataclass
    class FakeJobClient:
        sql_client: FakeSqlClient

    @dataclass
    class FakeZerobusSdk:
        create_arrow_stream: Any

    create_arrow_stream = mocker.Mock()
    zerobus_config = DatabricksZerobusConfiguration(
        credentials=DatabricksZerobusCredentials(
            client_id="client-id", client_secret="client-secret"
        ),
        stream_options={"ipc_compression": "LZ4_FRAME", "max_inflight_batches": 32},
    )
    job = DatabricksZerobusParquetLoadJob(
        "/tmp/items.1.1.parquet",
        DatabricksClientConfiguration(zerobus=zerobus_config),
        {},
    )
    job._job_client = cast(Any, FakeJobClient(sql_client=FakeSqlClient()))
    job._load_table = {"name": "items"}
    job.zerobus_sdk = FakeZerobusSdk(create_arrow_stream=create_arrow_stream)
    job._arrow_schema = object()

    job._create_stream()

    args, kwargs = create_arrow_stream.call_args
    create_arrow_stream.assert_called_once()
    assert args == (
        "catalog.schema.items",
        job._arrow_schema,
        "client-id",
        "client-secret",
    )
    assert kwargs["options"].ipc_compression == IPCCompression.LZ4_FRAME
    assert kwargs["options"].max_inflight_batches == 32


@pytest.mark.parametrize(
    ("zerobus_exception", "expected_exception", "expected_state"),
    [
        pytest.param(
            ZerobusException("retriable failure"),
            LoadJobTransientException,
            "retry",
            id="retriable",
        ),
        pytest.param(
            NonRetriableException("terminal failure"),
            LoadJobTerminalException,
            "failed",
            id="non-retriable",
        ),
    ],
)
def test_databricks_zerobus_load_job_error_handling(
    mocker: MockerFixture,
    zerobus_exception: ZerobusException,
    expected_exception: type[Exception],
    expected_state: TLoadJobState,
) -> None:
    class FakeJobClient:
        def prepare_load_job_execution(self, job: LoadJob) -> None:
            pass

        def grant_zerobus_permissions(self, table_name: str) -> None:
            pass

        def __enter__(self) -> "FakeJobClient":
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

    job = DatabricksZerobusParquetLoadJob(
        "/tmp/items.1.1.parquet",
        DatabricksClientConfiguration(zerobus=DatabricksZerobusConfiguration()),
        {},
    )
    job._load_table = {"name": "items"}
    mocker.patch.object(job, "_create_stream", side_effect=zerobus_exception)

    job.run_managed(FakeJobClient(), None)  # type: ignore[arg-type]

    assert isinstance(job.exception(), expected_exception)
    assert job.exception().__cause__ is zerobus_exception
    assert job.state() == expected_state
