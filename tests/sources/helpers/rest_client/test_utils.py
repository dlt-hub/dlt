import pytest
from dlt.sources.helpers.rest_client.utils import join_url


@pytest.mark.parametrize(
    "my_base_url, path, expected",
    [
        # Normal cases
        (
            "http://example.com",
            "path/to/resource",
            "http://example.com/path/to/resource",
        ),
        (
            "http://example.com/",
            "/path/to/resource",
            "http://example.com/path/to/resource",
        ),
        (
            "http://example.com/",
            "path/to/resource",
            "http://example.com/path/to/resource",
        ),
        (
            "http://example.com",
            "//path/to/resource",
            "http://example.com/path/to/resource",
        ),
        (
            "http://example.com///",
            "//path/to/resource",
            "http://example.com/path/to/resource",
        ),
        # Trailing and leading slashes
        ("http://example.com/", "/", "http://example.com/"),
        ("http://example.com", "/", "http://example.com/"),
        ("http://example.com/", "///", "http://example.com/"),
        ("http://example.com", "///", "http://example.com/"),
        ("/", "path/to/resource", "/path/to/resource"),
        ("/", "/path/to/resource", "/path/to/resource"),
        # Empty strings
        ("", "", ""),
        (
            "",
            "http://example.com/path/to/resource",
            "http://example.com/path/to/resource",
        ),
        ("", "path/to/resource", "path/to/resource"),
        ("http://example.com", "", "http://example.com"),
        # Query parameters and fragments
        (
            "http://example.com",
            "path/to/resource?query=123",
            "http://example.com/path/to/resource?query=123",
        ),
        (
            "http://example.com/",
            "path/to/resource#fragment",
            "http://example.com/path/to/resource#fragment",
        ),
        # Special characters in the path
        (
            "http://example.com",
            "/path/to/resource with spaces",
            "http://example.com/path/to/resource with spaces",
        ),
        ("http://example.com", "/path/with/中文", "http://example.com/path/with/中文"),
        # Protocols and subdomains
        ("https://sub.example.com", "path", "https://sub.example.com/path"),
        ("ftp://example.com", "/path", "ftp://example.com/path"),
        # Missing protocol in base_url
        ("example.com", "path", "example.com/path"),
    ],
)
def test_join_url(my_base_url, path, expected):
    assert join_url(my_base_url, path) == expected


@pytest.mark.parametrize(
    "my_base_url, path, exception",
    [
        (None, "path", ValueError),
        ("http://example.com", None, AttributeError),
        (123, "path", AttributeError),
        ("http://example.com", 123, AttributeError),
    ],
)
def test_join_url_invalid_input_types(my_base_url, path, exception):
    with pytest.raises(exception):
        join_url(my_base_url, path)
