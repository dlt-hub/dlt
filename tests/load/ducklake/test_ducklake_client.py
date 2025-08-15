import dlt
from dlt.destinations.impl.ducklake.configuration import DuckLakeCredentials


def test_default_ducklake_configuration() -> None:
    expected_ducklake_name = "ducklake"
    expected_ducklake = "duckdb://"
    expected_catalog = expected_ducklake
    expected_bucket_url = expected_ducklake_name

    cred = DuckLakeCredentials()

    assert cred.ducklake_name == expected_ducklake_name
    assert cred.ducklake.to_native_representation() == expected_ducklake
    assert cred.catalog.to_native_representation() == expected_catalog
    assert cred.storage.bucket_url == expected_bucket_url



def test_can_write() -> None:
    destination = dlt.destinations.ducklake()

    pipeline = dlt.pipeline("ducklake_test", destination=destination)
    pipeline.run([{"foo": 1}, {"foo": 2}], table_name="table_foo")

    dataset = pipeline.dataset()

    assert False
