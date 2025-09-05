from typing import cast
import pytest

import dlt
from dlt.common.destination.client import SupportsOpenTables
from dlt.common.destination.exceptions import (
    OpenTableClientNotAvailable,
    DestinationUndefinedEntity,
)

from tests.load.utils import (
    ABFS_BUCKET,
    destinations_configs,
    DestinationTestConfiguration,
    FILE_BUCKET,
    AZ_BUCKET,
    AWS_BUCKET,
    GCS_BUCKET,
)
from tests.pipeline.utils import assert_table_counts
from dlt.destinations.dataset.dataset import ReadableDBAPIDataset


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format=("delta", "iceberg"),
        # using specific buckets
        bucket_subset=(FILE_BUCKET, AZ_BUCKET, AWS_BUCKET, GCS_BUCKET, ABFS_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_get_open_table_location(destination_config: DestinationTestConfiguration) -> None:
    """tests that get_open_table_location returns the correct location."""
    pipeline = destination_config.setup_pipeline("open_tables", dev_mode=True)

    # test location for a table
    table_name = "test_table"

    # run pipeline first, then get client using context manager
    pipeline.run([1, 2, 3], table_name=table_name)

    with pipeline.destination_client() as client:
        assert isinstance(client, SupportsOpenTables)
        location = client.get_open_table_location(destination_config.table_format, table_name)

        # location should be a URL with the correct protocol
        protocol = destination_config.bucket_url.split("://")[0]
        if protocol == "file":
            assert location.startswith("file://")
        else:
            location.startswith(destination_config.bucket_url)
        # location should include the table name
        assert table_name in location
        assert location.endswith("/")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        all_buckets_filesystem_configs=True,
        local_filesystem_configs=True,
        bucket_subset=(FILE_BUCKET, AZ_BUCKET, AWS_BUCKET, GCS_BUCKET, ABFS_BUCKET),
    ),
    ids=lambda x: x.name,
)
@pytest.mark.parametrize(
    "layout",
    (
        "{table_name}/{load_id}.{file_id}.{ext}",
        "{table_name}.{load_id}.{file_id}.{ext}",
        "{table_name}/{load_id}/{file_id}.{ext}",
        "{table_name}.{load_id}/{file_id}.{ext}",
    ),
)
def test_open_table_location_native(
    destination_config: DestinationTestConfiguration, layout: str
) -> None:
    # note that data dir is native path fs path
    pipeline = destination_config.setup_pipeline("open_tables", dev_mode=True)
    pipeline.destination.config_params["layout"] = layout

    # run pipeline first, then get client using context manager
    pipeline.run([1, 2, 3], table_name="digits", loader_file_format="parquet")
    pipeline.run(["A", "B", "C", "D"], table_name="letters", loader_file_format="csv")

    with pipeline.destination_client() as client:
        assert isinstance(client, SupportsOpenTables)
        location = client.get_open_table_location(None, "digits")
        # location must be url
        protocol = destination_config.bucket_url.split("://")[0]
        if protocol == "file":
            assert location.startswith("file://")
        else:
            location.startswith(destination_config.bucket_url)
        is_folder = layout.startswith("{table_name}/")
        if is_folder:
            assert location.endswith("digits/")
        else:
            assert location.endswith("digits.")
        assert_table_counts(pipeline, expected_counts={"digits": 3, "letters": 4})


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format=("delta", "iceberg"),
        # using specific buckets
        bucket_subset=(FILE_BUCKET, AZ_BUCKET, AWS_BUCKET, GCS_BUCKET, ABFS_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_is_open_table(destination_config: DestinationTestConfiguration) -> None:
    """tests that is_open_table correctly identifies table formats."""
    pipeline = destination_config.setup_pipeline("open_tables", dev_mode=True)

    # create a resource with the specific table format
    @dlt.resource(table_format=destination_config.table_format)
    def formatted_table():
        yield {"id": 1, "value": "test"}

    # create a resource without table format
    @dlt.resource
    def unformatted_table():
        yield {"id": 1, "value": "test"}

    # run the pipeline to create the tables
    pipeline.run([formatted_table(), unformatted_table()])

    # get client using context manager
    with pipeline.destination_client() as client:
        assert isinstance(client, SupportsOpenTables)

        # test is_open_table for the tables
        assert client.is_open_table(destination_config.table_format, "formatted_table")
        assert not client.is_open_table(destination_config.table_format, "unknown_table")

        # test with incorrect format
        wrong_format = "iceberg" if destination_config.table_format == "delta" else "delta"
        assert not client.is_open_table(wrong_format, "formatted_table")  # type: ignore[arg-type]

        # if destinations allows for any other table formats, tests this
        prepared_table = client.prepare_load_table("unformatted_table")
        if prepared_table.get("table_format") not in ("delta", "iceberg"):
            assert not client.is_open_table(destination_config.table_format, "unformatted_table")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format=("delta", "iceberg"),
        # using specific buckets
        bucket_subset=(FILE_BUCKET, AZ_BUCKET, AWS_BUCKET, GCS_BUCKET, ABFS_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_get_open_table_catalog(destination_config: DestinationTestConfiguration) -> None:
    """tests that get_open_table_catalog returns the correct catalog."""
    pipeline = destination_config.setup_pipeline("open_tables", dev_mode=True)

    # run pipeline first
    pipeline.run([1, 2, 3], table_name="test_table")

    # get client using context manager
    with pipeline.destination_client() as client:
        assert isinstance(client, SupportsOpenTables)

        if destination_config.table_format == "iceberg":
            # get catalog for iceberg
            catalog = client.get_open_table_catalog("iceberg", "test_catalog")

            # verify it's an iceberg catalog
            from dlt.common.libs.pyiceberg import IcebergCatalog

            assert isinstance(catalog, IcebergCatalog)
            assert catalog.name == "test_catalog"

            # should have the dataset namespace
            # list_namespaces returns a list of tuples, so check if dataset name is in any of them
            namespaces = catalog.list_namespaces()
            dataset_found = any(pipeline.dataset_name in ns for ns in namespaces)
            assert dataset_found
        else:
            # delta doesn't support catalog
            from dlt.common.destination.exceptions import OpenTableCatalogNotSupported

            with pytest.raises(OpenTableCatalogNotSupported):
                client.get_open_table_catalog("delta")


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format=("delta", "iceberg"),
        # using specific buckets
        bucket_subset=(FILE_BUCKET, AZ_BUCKET, AWS_BUCKET, GCS_BUCKET, ABFS_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_load_open_table(destination_config: DestinationTestConfiguration) -> None:
    """tests that load_open_table loads the correct table."""
    pipeline = destination_config.setup_pipeline("open_tables", dev_mode=True)

    # create a table with the desired format and schema
    @dlt.resource(
        table_format=destination_config.table_format,
        columns={"id": {"data_type": "bigint"}, "value": {"data_type": "text"}},
    )
    def open_table():
        for i in range(5):
            yield {"id": i, "value": f"value_{i}"}

    # run the pipeline to create the table
    pipeline.run(open_table())

    # get client using context manager
    with pipeline.destination_client() as client:
        assert isinstance(client, SupportsOpenTables)

        # load the table
        table = client.load_open_table(destination_config.table_format, "open_table")

        # verify the table has the correct type and properties
        if destination_config.table_format == "delta":
            from dlt.common.libs.deltalake import DeltaTable

            assert isinstance(table, DeltaTable)

            # verify schema has expected columns
            delta_schema = table.schema()
            column_names = [field.name for field in delta_schema.fields]
            assert "id" in column_names
            assert "value" in column_names

            # verify data
            pa_table = table.to_pyarrow_table()
            assert pa_table.num_rows == 5
            assert "id" in pa_table.column_names
            assert "value" in pa_table.column_names

        elif destination_config.table_format == "iceberg":
            from dlt.common.libs.pyiceberg import IcebergTable

            assert isinstance(table, IcebergTable)

            # check schema
            iceberg_schema = table.schema()
            field_names = [field.name for field in iceberg_schema.fields]
            assert "id" in field_names
            assert "value" in field_names

            # check data
            arrow_table = table.scan().to_arrow()
            assert len(arrow_table) == 5
            assert "id" in arrow_table.column_names
            assert "value" in arrow_table.column_names

        # remove table from storage
        client.drop_tables("open_table")  # type: ignore[attr-defined]

        with pytest.raises(DestinationUndefinedEntity):
            client.load_open_table(destination_config.table_format, "open_table")

    # test non existing table, raises due to not being present in dlt schema
    with pytest.raises(DestinationUndefinedEntity):
        client.load_open_table(destination_config.table_format, "non_existing_table")

    # test open table client
    dataset_ = pipeline.dataset()
    assert dataset_.open_table_client.get_open_table_location(
        destination_config.table_format, "open_table"
    )


def test_table_client_not_available() -> None:
    """Test that an appropriate error is raised when trying to access open_table_client on a destination that doesn't support it."""
    pipeline = dlt.pipeline("test_table_client_not_available", destination="duckdb")

    # Run the pipeline to create the table
    pipeline.run([1, 2, 3], table_name="simple_table")

    dataset_ = pipeline.dataset()

    # This should work because SQL client is available
    assert dataset_.sql_client is not None

    with pytest.raises(OpenTableClientNotAvailable):
        dataset_.open_table_client


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format=("delta", "iceberg"),
        # using specific buckets
        bucket_subset=(FILE_BUCKET, AZ_BUCKET, AWS_BUCKET, GCS_BUCKET, ABFS_BUCKET),
    ),
    ids=lambda x: x.name,
)
def test_table_operations(destination_config: DestinationTestConfiguration) -> None:
    """tests operations like reading data from open tables."""
    pipeline = destination_config.setup_pipeline("open_tables", dev_mode=True)

    # create and populate a table
    @dlt.resource(table_format=destination_config.table_format)
    def data_table():
        yield [{"id": i, "name": f"name_{i}"} for i in range(10)]

    # run the pipeline to create the table
    pipeline.run(data_table())

    # get client using context manager
    with pipeline.destination_client() as client:
        assert isinstance(client, SupportsOpenTables)

        # load the table
        table = client.load_open_table(destination_config.table_format, "data_table")

        # perform operations based on format
        if destination_config.table_format == "delta":
            # read data
            pa_table = table.to_pyarrow_table()
            assert pa_table.num_rows == 10

            # check column values
            id_column = pa_table.column("id").to_pylist()
            name_column = pa_table.column("name").to_pylist()
            assert sorted(id_column) == list(
                range(10)
            )  # sorted to handle potential order differences
            assert sorted(name_column) == sorted([f"name_{i}" for i in range(10)])

            # check table version
            assert table.version() == 0

        elif destination_config.table_format == "iceberg":
            # read data
            arrow_table = table.scan().to_arrow()
            assert len(arrow_table) == 10

            # check column values
            id_column = arrow_table.column("id").to_pylist()
            name_column = arrow_table.column("name").to_pylist()
            assert sorted(id_column) == list(
                range(10)
            )  # sorted to handle potential order differences
            assert sorted(name_column) == sorted([f"name_{i}" for i in range(10)])

            # check table metadata
            assert table.metadata.format_version == 2


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        table_format_filesystem_configs=True,
        with_table_format=("delta", "iceberg"),
        # using specific buckets
        bucket_subset=(FILE_BUCKET,),
    ),
    ids=lambda x: x.name,
)
def test_open_table_format_not_supported(destination_config: DestinationTestConfiguration) -> None:
    """tests that appropriate error is raised when table format doesn't match."""
    pipeline = destination_config.setup_pipeline("open_tables", dev_mode=True)

    # create a resource with delta table format
    @dlt.resource(table_format="delta")
    def delta_table():
        yield {"id": 1}

    # create a resource with iceberg table format
    @dlt.resource(table_format="iceberg")
    def iceberg_table():
        yield {"id": 1}

    # run the pipeline to create the tables
    pipeline.run([delta_table(), iceberg_table()])

    # get client using context manager
    with pipeline.destination_client() as client:
        assert isinstance(client, SupportsOpenTables)

        # try to load table with incorrect format
        wrong_table = (
            "delta_table" if destination_config.table_format == "iceberg" else "iceberg_table"
        )

        from dlt.common.destination.exceptions import OpenTableFormatNotSupported

        with pytest.raises(OpenTableFormatNotSupported):
            client.load_open_table(destination_config.table_format, wrong_table)


@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        bucket_subset=(FILE_BUCKET,),
    ),
    ids=lambda x: x.name,
)
def test_native_table_not_open_table(destination_config: DestinationTestConfiguration) -> None:
    """tests that native tables are not recognized as open tables."""
    pipeline = destination_config.setup_pipeline("open_tables", dev_mode=True)

    # create a native table
    @dlt.resource(table_format="native")
    def native_table():
        yield {"id": 1}

    # run the pipeline to create the table
    pipeline.run(native_table())

    # get client using context manager
    with pipeline.destination_client() as client:
        assert isinstance(client, SupportsOpenTables)

        # check it's not recognized as an open table
        assert not client.is_open_table("delta", "native_table")
        assert not client.is_open_table("iceberg", "native_table")

        # try loading as open table should fail
        from dlt.common.destination.exceptions import OpenTableFormatNotSupported

        with pytest.raises(OpenTableFormatNotSupported):
            client.load_open_table("delta", "native_table")
