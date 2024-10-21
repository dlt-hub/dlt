import pytest

from dlt.destinations.impl.clickhouse.utils import convert_storage_to_http_scheme

# mark all tests as essential, do not remove
pytestmark = pytest.mark.essential


def test_convert_storage_scheme() -> None:
    # with https
    assert (
        convert_storage_to_http_scheme(
            url="s3://blah",
            use_https=True,
        )
        == "https://blah.s3.amazonaws.com/"
    )

    # without https
    assert (
        convert_storage_to_http_scheme(
            url="s3://blah",
            use_https=False,
        )
        == "http://blah.s3.amazonaws.com/"
    )

    # with region
    assert (
        convert_storage_to_http_scheme(url="s3://blah", use_https=True, region="europe5")
        == "https://blah.s3-europe5.amazonaws.com/"
    )

    # does not respect region and changes to google apis endpoint
    assert (
        convert_storage_to_http_scheme(url="gcs://blah", use_https=True, region="europe5")
        == "https://blah.storage.googleapis.com/"
    )

    # with subfolder
    assert (
        convert_storage_to_http_scheme(url="s3://blah/bli/bluh", use_https=True, region="europe5")
        == "https://blah.s3-europe5.amazonaws.com/bli/bluh"
    )

    # with endpoint
    assert (
        convert_storage_to_http_scheme(
            url="s3://blah/bli/bluh",
            use_https=True,
            region="europe5",
            endpoint="http://digital-ocean.com",
        )
        == "https://blah.digital-ocean.com/bli/bluh"
    )
