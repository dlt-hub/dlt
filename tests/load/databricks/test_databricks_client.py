from typing import Iterator, Optional, cast

import pytest

from dlt.common.configuration.exceptions import ConfigurationValueError
from dlt.common.destination.client import LoadJob
from dlt.common.destination.exceptions import (
    DestinationInvalidFileFormat,
    WriteDispositionNotSupported,
)
from dlt.common.schema.typing import TWriteDisposition
from dlt.common.schema.utils import new_table
from dlt.common.storages.load_package import ParsedLoadJobFileName
from dlt.common.utils import uniq_id
from dlt.destinations.exceptions import LoadJobTerminalException
from dlt.destinations.impl.databricks.databricks_adapter import INSERT_API_HINT
from dlt.destinations.impl.databricks.databricks import (
    DatabricksClient,
    DatabricksLoadJob,
    DatabricksZerobusJsonlLoadJob,
    DatabricksZerobusParquetLoadJob,
)
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
    insert_api: Optional[str],
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
