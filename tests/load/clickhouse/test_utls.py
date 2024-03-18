import pytest

from dlt.destinations.impl.clickhouse.utils import convert_storage_url_to_http


def test_convert_s3_url_to_http() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.s3.amazonaws.com/path/to/file.txt"
    assert convert_storage_url_to_http(s3_url) == expected_http_url


def test_convert_s3_url_to_https() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.s3.amazonaws.com/path/to/file.txt"
    assert convert_storage_url_to_http(s3_url, use_https=True) == expected_https_url


def test_convert_gs_url_to_http() -> None:
    gs_url: str = "gs://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.storage.googleapis.com/path/to/file.txt"
    assert convert_storage_url_to_http(gs_url) == expected_http_url
    gcs_url = "gcs://my-bucket/path/to/file.txt"
    expected_http_url = "http://my-bucket.storage.googleapis.com/path/to/file.txt"
    assert convert_storage_url_to_http(gcs_url) == expected_http_url


def test_convert_gs_url_to_https() -> None:
    gs_url: str = "gs://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.storage.googleapis.com/path/to/file.txt"
    assert convert_storage_url_to_http(gs_url, use_https=True) == expected_https_url
    gcs_url = "gcs://my-bucket/path/to/file.txt"
    expected_https_url = "https://my-bucket.storage.googleapis.com/path/to/file.txt"
    assert convert_storage_url_to_http(gcs_url, use_https=True) == expected_https_url


def test_convert_s3_url_to_http_with_region() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.s3-us-west-2.amazonaws.com/path/to/file.txt"
    assert convert_storage_url_to_http(s3_url, region="us-west-2") == expected_http_url


def test_convert_s3_url_to_https_with_region() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.s3-us-east-1.amazonaws.com/path/to/file.txt"
    assert (
        convert_storage_url_to_http(s3_url, use_https=True, region="us-east-1")
        == expected_https_url
    )


def test_convert_s3_url_to_http_with_endpoint() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.s3.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_url_to_http(s3_url, endpoint="s3.custom-endpoint.com") == expected_http_url
    )


def test_convert_s3_url_to_https_with_endpoint() -> None:
    s3_url: str = "s3://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.s3.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_url_to_http(s3_url, use_https=True, endpoint="s3.custom-endpoint.com")
        == expected_https_url
    )


def test_convert_gs_url_to_http_with_endpoint() -> None:
    gs_url: str = "gs://my-bucket/path/to/file.txt"
    expected_http_url: str = "http://my-bucket.custom-endpoint.com/path/to/file.txt"
    assert convert_storage_url_to_http(gs_url, endpoint="custom-endpoint.com") == expected_http_url
    gcs_url = "gcs://my-bucket/path/to/file.txt"
    expected_http_url = "http://my-bucket.custom-endpoint.com/path/to/file.txt"
    assert convert_storage_url_to_http(gcs_url, endpoint="custom-endpoint.com") == expected_http_url


def test_convert_gs_url_to_https_with_endpoint() -> None:
    gs_url: str = "gs://my-bucket/path/to/file.txt"
    expected_https_url: str = "https://my-bucket.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_url_to_http(gs_url, use_https=True, endpoint="custom-endpoint.com")
        == expected_https_url
    )
    gcs_url = "gcs://my-bucket/path/to/file.txt"
    expected_https_url = "https://my-bucket.custom-endpoint.com/path/to/file.txt"
    assert (
        convert_storage_url_to_http(gcs_url, use_https=True, endpoint="custom-endpoint.com")
        == expected_https_url
    )


def test_invalid_url_format() -> None:
    with pytest.raises(Exception) as exc_info:
        convert_storage_url_to_http("invalid-url")
    assert str(exc_info.value) == "Error converting storage URL to HTTP protocol: 'invalid-url'"
