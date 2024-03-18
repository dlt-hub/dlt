import pytest

from dlt.destinations.impl.clickhouse.utils import (
    convert_storage_to_http_scheme,
    render_s3_table_function,
)


def test_convert_s3_url_to_http() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.s3.amazonaws.com/path/to/file.txt"
    assert convert_storage_to_http_scheme(s3_url) == expected_http_url


def test_convert_s3_url_to_https() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.s3.amazonaws.com/path/to/file.txt"
    assert convert_storage_to_http_scheme(s3_url, use_https=True) == expected_https_url


def test_convert_gs_url_to_http() -> None:
    gs_url: str = "gs://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.storage.googleapis.com/path/to/file.txt"
    assert convert_storage_to_http_scheme(gs_url) == expected_http_url
    gcs_url = "gcs://my-bucket/path/to/file.txt"
    expected_http_url = "http://my-bucket.storage.googleapis.com/path/to/file.txt"
    assert convert_storage_to_http_scheme(gcs_url) == expected_http_url


def test_convert_gs_url_to_https() -> None:
    gs_url: str = "gs://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.storage.googleapis.com/path/to/file.txt"
    assert convert_storage_to_http_scheme(gs_url, use_https=True) == expected_https_url
    gcs_url = "gcs://my-bucket/path/to/file.txt"
    expected_https_url = "https://my-bucket.storage.googleapis.com/path/to/file.txt"
    assert convert_storage_to_http_scheme(gcs_url, use_https=True) == expected_https_url


def test_convert_s3_url_to_http_with_region() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.s3-us-west-2.amazonaws.com/path/to/file.txt"
    assert convert_storage_to_http_scheme(s3_url, region="us-west-2") == expected_http_url


def test_convert_s3_url_to_https_with_region() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.s3-us-east-1.amazonaws.com/path/to/file.txt"
    assert (
        convert_storage_to_http_scheme(s3_url, use_https=True, region="us-east-1")
        == expected_https_url
    )


def test_convert_s3_url_to_http_with_endpoint() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.s3.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_to_http_scheme(s3_url, endpoint="s3.custom-endpoint.com")
        == expected_http_url
    )


def test_convert_s3_url_to_https_with_endpoint() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.s3.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_to_http_scheme(s3_url, use_https=True, endpoint="s3.custom-endpoint.com")
        == expected_https_url
    )


def test_convert_gs_url_to_http_with_endpoint() -> None:
    gs_url: str = "gs://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_to_http_scheme(gs_url, endpoint="custom-endpoint.com") == expected_http_url
    )
    gcs_url = "gcs://my-bucket/path/to/file.txt"
    expected_http_url = "http://my-bucket.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_to_http_scheme(gcs_url, endpoint="custom-endpoint.com")
        == expected_http_url
    )


def test_convert_gs_url_to_https_with_endpoint() -> None:
    gs_url: str = "gs://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_to_http_scheme(gs_url, use_https=True, endpoint="custom-endpoint.com")
        == expected_https_url
    )
    gcs_url = "gcs://my-bucket/path/to/file.txt"
    expected_https_url = "https://my-bucket.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_to_http_scheme(gcs_url, use_https=True, endpoint="custom-endpoint.com")
        == expected_https_url
    )


def test_render_with_credentials_jsonl() -> None:
    url = "https://example.com/data.jsonl"
    access_key_id = "test_access_key"
    secret_access_key = "test_secret_key"
    file_format = "jsonl"
    expected_output = """s3('https://example.com/data.jsonl','test_access_key','test_secret_key','JSONEachRow')"""
    assert (
        render_s3_table_function(url, access_key_id, secret_access_key, file_format)  # type: ignore[arg-type]
        == expected_output
    )


def test_render_with_credentials_parquet() -> None:
    url = "https://example.com/data.parquet"
    access_key_id = "test_access_key"
    secret_access_key = "test_secret_key"
    file_format = "parquet"
    expected_output = """s3('https://example.com/data.parquet','test_access_key','test_secret_key','Parquet')"""
    assert (
        render_s3_table_function(url, access_key_id, secret_access_key, file_format)  # type: ignore[arg-type]
        == expected_output
    )


def test_render_without_credentials() -> None:
    url = "https://example.com/data.jsonl"
    file_format = "jsonl"
    expected_output = """s3('https://example.com/data.jsonl',NOSIGN,'JSONEachRow')"""
    assert render_s3_table_function(url, file_format=file_format) == expected_output  # type: ignore[arg-type]



def test_render_invalid_file_format() -> None:
    url = "https://example.com/data.unknown"
    access_key_id = "test_access_key"
    secret_access_key = "test_secret_key"
    file_format = "unknown"
    with pytest.raises(ValueError) as excinfo:
        render_s3_table_function(url, access_key_id, secret_access_key, file_format)  # type: ignore[arg-type]
    assert "Clickhouse s3/gcs staging only supports 'parquet' and 'jsonl'." == str(excinfo.value)


def test_invalid_url_format() -> None:
    with pytest.raises(Exception) as exc_info:
        convert_storage_to_http_scheme("invalid-url")
    assert str(exc_info.value) == "Error converting storage URL to HTTP protocol: 'invalid-url'"


def test_render_missing_url() -> None:
    with pytest.raises(TypeError) as excinfo:
        render_s3_table_function()  # type: ignore
    assert "missing 1 required positional argument: 'url'" in str(excinfo.value)
