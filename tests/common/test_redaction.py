import time
from typing import Callable

import pytest

from dlt.common.redaction import (
    is_sensitive_param,
    normalize_param_name,
    redact_secrets,
    redact_text,
    redact_url,
)

REDACT_FUNCTIONS = [pytest.param(redact_url, id="url"), pytest.param(redact_text, id="text")]


@pytest.mark.parametrize(
    "name",
    [
        "x-api-key",
        "X_API_KEY",
        "xapikey",
        "subscription-key",
        "Subscription_Key",
        "refresh_token",
        "Refresh-Token",
        "client-secret",
        "clientsecret",
        "api.key",
        "sig",
        "X-Amz-Signature",
        "X-Amz-Security-Token",
        "X-Goog-Credential",
        "Proxy-Authorization",
        # sentry-python denylist
        "auth",
        "credentials",
        "passwd",
        "mysql_pwd",
        "private_key",
        "sessionid",
        "PHPSESSID",
        "connect.sid",
        "X-CSRFToken",
        "x_forwarded_for",
    ],
)
def test_sensitive_param_names_match_after_normalization(name: str) -> None:
    assert is_sensitive_param(name)


@pytest.mark.parametrize(
    "name",
    ["private_token", "AUTH-TOKEN", "sas_token", "app_secret", "db_password", "url_signature"],
)
def test_sensitive_param_names_match_by_suffix(name: str) -> None:
    assert is_sensitive_param(name)


@pytest.mark.parametrize(
    "name",
    [
        "page",
        "limit",
        "sort_key",
        "partition_key",
        "keyword",
        "token_url",
        "monkey",
        "next_page_token",
        "pageToken",
        "continuation_token",
        "nextToken",
    ],
)
def test_ordinary_param_names_are_not_sensitive(name: str) -> None:
    assert not is_sensitive_param(name)


def test_custom_sensitive_params_replace_the_defaults() -> None:
    assert normalize_param_name(" My.Param-Name_ ") == "myparamname"
    assert is_sensitive_param("My-Param", sensitive={"my_param"})
    # the defaults and the suffix rule no longer apply
    assert not is_sensitive_param("api_key", sensitive={"my_param"})
    assert not is_sensitive_param("private_token", sensitive={"my_param"})


@pytest.mark.parametrize(
    "url,expected",
    [
        pytest.param(
            "sftp://alice:hunter2@host:2222/data",
            "sftp://alice@host:2222/data",
            id="sftp_password",
        ),
        pytest.param(
            "https://:hunter2@host/path", "https://***@host/path", id="password_without_user"
        ),
        pytest.param(
            "sftp://:hunter2@host/path", "sftp://host/path", id="sftp_password_without_user"
        ),
        pytest.param(
            "https://ghp_abc123@api.github.com/repos",
            "https://***@api.github.com/repos",
            id="http_token_as_user",
        ),
        pytest.param(
            "postgresql://u:p#ss@h:5432/db", "postgresql://u@h:5432/db", id="password_with_hash"
        ),
        pytest.param("postgresql://u:p/s?s@h/db", "postgresql://u@h/db", id="password_with_slash"),
        pytest.param("sftp://u:p@ss@h/db", "sftp://u@h/db", id="password_with_at"),
        pytest.param(
            "mssql://u:p@ss:w0rd@h:1433/db", "mssql://u@h:1433/db", id="password_with_at_and_colon"
        ),
        pytest.param(
            "postgresql://u:p/s@s@h/db", "postgresql://u@h/db", id="password_with_slash_and_at"
        ),
        pytest.param(
            "https://u:p@ss@h/x?e=a@b.com",
            "https://***@h/x?e=a@b.com",
            id="password_with_at_and_email_in_query",
        ),
        pytest.param(
            "https://u:pw@host/x?e=a@b.com", "https://***@host/x?e=a@b.com", id="email_in_query"
        ),
        pytest.param("sftp://u:1234567/abc@h/x", "sftp://u@h/x", id="digits_too_long_for_a_port"),
        pytest.param("duckdb:///C:/a?key=1", "duckdb:///C:/a?key=***", id="empty_authority_kept"),
        pytest.param(
            "/i?a=1&page={page}&q=a%20b&token=t",
            "/i?a=1&page={page}&q=a%20b&token=***",
            id="other_params_kept_verbatim",
        ),
        pytest.param(
            "/items?api_key=x via https://h", "/items?api_key=*** via https://h", id="mixed_text"
        ),
        pytest.param(
            "https://h/cb#access_token=abc&state=1",
            "https://h/cb#access_token=***&state=1",
            id="params_in_fragment",
        ),
        pytest.param("https://h/x?api%5Fkey=s", "https://h/x?api%5Fkey=***", id="quoted_name"),
        pytest.param(
            "abfss://container@account.dfs.core.windows.net/path",
            "abfss://container@account.dfs.core.windows.net/path",
            id="abfss_container_kept",
        ),
        pytest.param(
            "https://account.blob.core.windows.net/c/f.csv?sv=2022-11-02&sp=r&sig=s3cr3t",
            "https://account.blob.core.windows.net/c/f.csv?sv=2022-11-02&sp=r&sig=***",
            id="azure_sas",
        ),
        pytest.param(
            "https://b.s3.amazonaws.com/k?X-Amz-Credential=AKID&X-Amz-Signature=abc&X-Amz-Expires=60",
            "https://b.s3.amazonaws.com/k?X-Amz-Credential=***&X-Amz-Signature=***&X-Amz-Expires=60",
            id="s3_presigned",
        ),
        pytest.param(
            "s3://AKID:SECRET@bucket/path/file.parquet",
            "s3://AKID@bucket/path/file.parquet",
            id="s3_fsspec_userinfo",
        ),
        pytest.param(
            "https://api.example.com/items?subscription-key=s&page=2#frag",
            "https://api.example.com/items?subscription-key=***&page=2#frag",
            id="query_and_fragment",
        ),
        pytest.param(
            "https://u:p@api.example.com/?private_token=t",
            "https://***@api.example.com/?private_token=***",
            id="password_and_query",
        ),
        pytest.param(
            "/items?x-api-key=k&page=2", "/items?x-api-key=***&page=2", id="path_without_scheme"
        ),
        pytest.param("rest:https://u:pw@host/ns", "rest:https://***@host/ns", id="prefixed_url"),
        pytest.param(
            "dir:s3://AKID:SECRET@bucket/p", "dir:s3://AKID@bucket/p", id="prefixed_bucket"
        ),
        pytest.param(
            "db://mydb|us-east-1|https://u:pw@host",
            "db://mydb|us-east-1|https://***@host",
            id="joined_parts",
        ),
        pytest.param(
            "copied to sftp://a:b@h/p and https://h/x?token=t",
            "copied to sftp://a@h/p and https://h/x?token=***",
            id="urls_in_text",
        ),
        pytest.param(
            "azureBlobStorage('https://acc.blob/c?sig=S','x')",
            "azureBlobStorage('https://acc.blob/c?sig=***','x')",
            id="quoted_url_keeps_surrounding_text",
        ),
    ],
)
@pytest.mark.parametrize("redact", REDACT_FUNCTIONS)
def test_redact(redact: Callable[[str], str], url: str, expected: str) -> None:
    assert redact(url) == expected


@pytest.mark.parametrize(
    "url",
    [
        pytest.param("postgresql://example.com:5432", id="host_and_port"),
        pytest.param("s3://bucket/path?versionId=3", id="ordinary_query"),
        pytest.param("file:///home/user/data", id="file_url"),
        pytest.param("C:\\Users\\data\\file.csv", id="windows_path"),
        pytest.param("relative/path/file.csv", id="relative_path"),
        pytest.param(":memory:", id="memory_marker"),
        pytest.param("orders", id="plain_name"),
        pytest.param("http://[::1", id="invalid_url"),
        pytest.param("https://host:443/p?email=a@b.com", id="port_and_at_in_query"),
        pytest.param("sftp://host:22/p and mail a@b", id="port_and_at_in_text"),
        pytest.param("https://h/?next_page_token=a&pageToken=b", id="pagination_cursors"),
        pytest.param("hf_datasets_sample_table_2024", id="table_with_token_prefix"),
        pytest.param("s3://bucket/AKIA/data.parquet", id="short_value_with_token_prefix"),
        pytest.param("rapidapi" + "0" * 32, id="token_prefix_inside_a_word"),
        pytest.param("sk_live_short", id="too_short_for_a_token"),
    ],
)
@pytest.mark.parametrize("redact", REDACT_FUNCTIONS)
def test_redact_keeps_strings_without_credentials(redact: Callable[[str], str], url: str) -> None:
    assert redact(url) == url


@pytest.mark.parametrize(
    "text,expected",
    [
        pytest.param(
            '{"auth": {"token": "t0k3n"}, "base_url": "h"}',
            '{"auth": {"token": "***"}, "base_url": "h"}',
            id="json",
        ),
        pytest.param(
            "{'password': 'hun\\'ter2', 'host': 'h'}",
            "{'password': '***', 'host': 'h'}",
            id="dict_repr",
        ),
        pytest.param('{"api_key":"k","page":2}', '{"api_key":"***","page":2}', id="compact_json"),
        pytest.param(
            '{"token": "a\\"b", "x": "y"}', '{"token": "***", "x": "y"}', id="escaped_quote"
        ),
        pytest.param('say "hi": "there"', 'say "hi": "there"', id="ordinary_quoted_pair"),
    ],
)
@pytest.mark.parametrize("redact", REDACT_FUNCTIONS)
def test_redact_quoted_pairs(redact: Callable[[str], str], text: str, expected: str) -> None:
    assert redact(text) == expected


@pytest.mark.parametrize(
    "url,expected",
    [
        pytest.param("sftp://u:p w@h/x", "sftp://u@h/x", id="password_with_space"),
        pytest.param("sftp://u:p@s s@h/db", "sftp://u@h/db", id="password_with_at_and_space"),
        pytest.param(
            "db://mydb|us-east-1|https://u:p w@host",
            "db://mydb|us-east-1|https://***@host",
            id="joined_parts_password_with_space",
        ),
    ],
)
def test_redact_url_password_with_spaces(url: str, expected: str) -> None:
    assert redact_url(url) == expected


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("https://host:443x failed; mail admin@x.com", id="port_typo_then_email"),
        pytest.param("request to https://api:v2 down, contact support@x.com", id="sentence"),
        pytest.param("loaded from sftp://h:data then notified ops@corp.io", id="url_then_email"),
    ],
)
def test_redact_text_keeps_text_up_to_a_later_at(text: str) -> None:
    assert redact_text(text) == text
    # a single url may hold spaces in its password, so text is not kept
    assert redact_url(text) != text


def test_redact_secrets() -> None:
    headers = {"Authorization": "Bearer t", "Cookie": "c", "Accept": "application/json"}
    assert redact_secrets(headers) == {
        "Authorization": "***",
        "Cookie": "***",
        "Accept": "application/json",
    }
    body = {
        "grant_type": "client_credentials",
        "client_secret": "s",
        "items": [{"refresh_token": "r", "id": 1}, "https://u:pw@h/x"],
    }
    assert redact_secrets(body) == {
        "grant_type": "client_credentials",
        "client_secret": "***",
        "items": [{"refresh_token": "***", "id": 1}, "https://***@h/x"],
    }
    assert redact_secrets("a=1&token=t") == "a=1&token=***"
    assert redact_secrets(None) is None
    assert redact_secrets(5) == 5
    # containers under a secret key are walked, plain values are masked, None stays
    assert redact_secrets({"credentials": {"password": "p", "user": "u"}, "token": None}) == {
        "credentials": {"password": "***", "user": "u"},
        "token": None,
    }
    assert redact_secrets(("a", {"secret": "s"})) == ("a", {"secret": "***"})


@pytest.mark.parametrize(
    "token",
    [
        pytest.param("ghp_" + "a1B2" * 9, id="github_classic"),
        pytest.param("github_pat_" + "A1_b" * 12, id="github_fine_grained"),
        pytest.param("glpat-" + "x1Y2z3" * 4, id="gitlab"),
        pytest.param("xoxb-123456789012-1234567890123-AbCdEfGh", id="slack_bot"),
        pytest.param("xapp-1-A0123456789-1234567890-abcdef", id="slack_app"),
        pytest.param("sk_live_" + "4eC39HqLyjWDarjtT1zdp7dc", id="stripe_secret"),
        pytest.param("rk_test_" + "4eC39HqLyjWDarjtT1zdp7dc", id="stripe_restricted"),
        pytest.param("sk-ant-api03-" + "abc_DEF-123" * 3, id="anthropic"),
        pytest.param("sk-proj-" + "abc_DEF-123" * 3, id="openai_project"),
        pytest.param("hf_" + "AbCdEfGhIjKlMnOpQrStUvWxYz1234", id="hugging_face"),
        pytest.param("npm_" + "a1B2c3" * 6, id="npm"),
        pytest.param("pypi-" + "AgEIcHlwaS5vcmc" * 4, id="pypi"),
        pytest.param("shpat_" + "0123456789abcdef" * 2, id="shopify"),
        pytest.param("dapi" + "0123456789abcdef" * 2, id="databricks"),
        pytest.param("AKIAIOSFODNN7EXAMPLE", id="aws_access_key_id"),
        pytest.param("AIzaSyD-" + "a" * 32, id="google_api_key"),
        pytest.param(
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0",
            id="jwt",
        ),
    ],
)
@pytest.mark.parametrize("redact", REDACT_FUNCTIONS)
def test_redact_known_token_formats(redact: Callable[[str], str], token: str) -> None:
    # a token on its own, inside text and in a path or a query parameter of any name
    assert redact(token) == "***"
    assert redact(f"Bearer {token} expired") == "Bearer *** expired"
    assert redact(f"https://h/hooks/{token}/x?id={token}") == "https://h/hooks/***/x?id=***"


@pytest.mark.parametrize(
    "text",
    [
        pytest.param("A" * 100_000, id="long_word"),
        pytest.param("https://u:" + "x" * 100_000, id="user_info_without_at"),
        pytest.param("?a=b&" * 20_000, id="many_params"),
        pytest.param("https://u:" + "p @" * 30_000 + "x", id="password_spaces_and_ats"),
        pytest.param('"a": "' * 20_000, id="unclosed_quoted_pairs"),
        pytest.param('"key":"' + "\\x" * 50_000, id="escapes_without_closing_quote"),
    ],
)
@pytest.mark.parametrize("redact", REDACT_FUNCTIONS)
def test_redact_scales_linearly(redact: Callable[[str], str], text: str) -> None:
    # a regex that backtracks over every start position takes seconds here
    started = time.monotonic()
    redact(text)
    assert time.monotonic() - started < 1.0
