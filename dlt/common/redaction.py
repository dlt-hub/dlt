import re
from typing import Any, FrozenSet, Iterable, Mapping, Match, Optional, Pattern
from urllib.parse import unquote_plus


SENSITIVE_PARAMS = frozenset(
    {
        # api keys, tokens and passwords, names proposed in
        # https://github.com/dlt-hub/dlt/issues/4560
        "apikey",
        "xapikey",
        "key",
        "subscriptionkey",
        "token",
        "accesstoken",
        "refreshtoken",
        "secret",
        "clientsecret",
        "password",
        "pwd",
        # signed urls of Azure, AWS and GCS:
        # https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas
        # https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        # https://cloud.google.com/storage/docs/access-control/signed-urls
        "sig",
        "signature",
        "xamzsignature",
        "xamzcredential",
        "xamzsecuritytoken",
        "xgoogsignature",
        "xgoogcredential",
        # http auth headers
        "authorization",
        "proxyauthorization",
        "cookie",
        "setcookie",
        # `DEFAULT_DENYLIST` of sentry-python:
        # https://github.com/getsentry/sentry-python/blob/master/sentry_sdk/scrubber.py
        "auth",
        "credentials",
        "passwd",
        "mysqlpwd",
        "privatekey",
        "session",
        "sessionid",
        "usersession",
        "aiohttpsession",
        "connectsid",
        "phpsessid",
        "symfony",
        "csrf",
        "csrftoken",
        "xcsrftoken",
        "xsrf",
        "xsrftoken",
        "xforwardedfor",
    }
)
"""Normalized names of query parameters, headers and keys that carry secrets, see
`normalize_param_name`."""

SENSITIVE_PARAM_SUFFIXES = ("token", "secret", "password", "signature", "apikey")
"""Endings of normalized parameter names that carry secrets, ie. `private_token`."""

NON_SENSITIVE_PARAM_SUFFIXES = ("pagetoken", "nexttoken", "continuationtoken", "synctoken")
"""Endings of normalized parameter names that carry pagination cursors, not secrets."""

SECRET_VALUE_PATTERNS = (
    # GitHub classic and fine-grained tokens
    r"gh[pousr]_[A-Za-z0-9]{36,}",
    r"github_pat_[A-Za-z0-9_]{40,}",
    # GitLab personal access tokens
    r"glpat-[A-Za-z0-9_-]{20,}",
    # Slack bot, user and app tokens
    r"xox[abprs]-[A-Za-z0-9-]{10,}",
    r"xapp-[A-Za-z0-9-]{10,}",
    # Stripe secret and restricted keys
    r"[sr]k_(?:live|test)_[A-Za-z0-9]{16,}",
    # Anthropic and OpenAI project keys
    r"sk-ant-[A-Za-z0-9_-]{20,}",
    r"sk-proj-[A-Za-z0-9_-]{20,}",
    # Hugging Face, npm and PyPI tokens
    r"hf_[A-Za-z0-9]{30,}",
    r"npm_[A-Za-z0-9]{36,}",
    r"pypi-[A-Za-z0-9_-]{50,}",
    # Shopify access tokens
    r"shp(?:at|ss|ca|pa)_[a-fA-F0-9]{32,}",
    # Databricks personal access tokens
    r"dapi[a-f0-9]{32,}",
    # AWS access key ids and Google API keys
    r"(?:AKIA|ASIA)[A-Z0-9]{16,}",
    r"AIza[A-Za-z0-9_-]{35,}",
    # JSON web tokens, ie. bearer tokens
    r"eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]*",
)
"""Known API key and token formats: the prefix published by the provider followed by the shape of
the rest of the value. Values matching them are redacted wherever they appear. The rule set of
gitleaks (https://github.com/gitleaks/gitleaks/blob/master/config/gitleaks.toml) is a broader
reference.
"""

HTTP_SCHEMES = frozenset({"http", "https", "ws", "wss"})
"""Schemes whose user name is redacted together with the password, it often holds a token."""

REDACTED = "***"


def normalize_param_name(name: str) -> str:
    """Lowercases `name` and drops separators, so `X-Api-Key` and `x_api_key` give `xapikey`."""
    return re.sub(r"[\s._-]", "", name.lower())


def is_sensitive_param(name: str, sensitive: Optional[Iterable[str]] = None) -> bool:
    """Tells if query parameter `name` carries a secret.

    Without `sensitive`, a name is sensitive when it normalizes to one of `SENSITIVE_PARAMS` or
    ends with one of `SENSITIVE_PARAM_SUFFIXES` but not with one of
    `NON_SENSITIVE_PARAM_SUFFIXES`. With `sensitive`, only those names are, compared in
    normalized form.
    """
    normalized_sensitive = (
        frozenset(normalize_param_name(s) for s in sensitive) if sensitive else None
    )
    return _is_sensitive_normalized(normalize_param_name(name), normalized_sensitive)


def _is_sensitive_normalized(normalized: str, sensitive: Optional[FrozenSet[str]]) -> bool:
    if sensitive is not None:
        return normalized in sensitive
    if normalized in SENSITIVE_PARAMS:
        return True
    return normalized.endswith(SENSITIVE_PARAM_SUFFIXES) and not normalized.endswith(
        NON_SENSITIVE_PARAM_SUFFIXES
    )


def _url_userinfo_pattern(password_char: str, host_end: str) -> Pattern[str]:
    return re.compile(
        # a scheme starts only after a non-scheme character, so a long word is scanned once
        r"(?<![A-Za-z0-9+.-])(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://"
        r"(?P<user>[^\s:/?#@|\"'`<>]*)"
        # `host:port/...` is not a password, ports have at most 5 digits
        r"(?::(?!\d{1,5}(?:[/?#]|$))"
        # the password runs to the first `@` and may hold `/`, `?` or `#`; it continues over further
        # `@`s (ie. `p@ss`) only within the authority, so an email in the query is not swallowed
        rf"(?P<password>(?:(?!://){password_char})*(?:@(?:(?![/?#]){password_char})*)*?))?"
        # the host follows the last `@` of the authority
        rf"@(?=[^@\s/?#]*(?:{host_end}|$))"
    )


_URL_USERINFO = _url_userinfo_pattern(r"[^@]", r"[/?#]")
"""User info of a `scheme://` url in a string that is a single url: the password may hold spaces."""

_URL_USERINFO_IN_TEXT = _url_userinfo_pattern(r"[^@\s]", r"[/?#\s]")
"""User info of a `scheme://` url anywhere in free text: the password holds no whitespace, so text
up to a later `@` (ie. an email) is never taken for a password."""

_QUERY_PARAM = re.compile(r"(?<=[?&#])(?P<name>[^=&#?\s|\"'`<>]+)=(?P<value>[^&#\s|\"'`<>]*)")
"""A `name=value` pair following `?`, `&` or `#`, anywhere in a string."""

_QUOTED_PAIR = re.compile(
    r"(?P<q>[\"'])(?P<name>[^\"'\\\s:]{1,64})(?P=q)\s*:\s*"
    r"(?P<vq>[\"'])(?P<value>(?:\\.|(?!(?P=vq))[^\\])*)(?P=vq)"
)
"""A `"name": "value"` pair of a json document or a python dict repr, anywhere in a string."""

_SECRET_VALUE = re.compile(r"(?<![A-Za-z0-9_-])(?:" + "|".join(SECRET_VALUE_PATTERNS) + ")")
"""A value in one of `SECRET_VALUE_PATTERNS`, not preceded by a token character."""


def redact_url(url: str, sensitive: Optional[Iterable[str]] = None) -> str:
    """Removes passwords from `url` and replaces values of secret query parameters with `***`.

    `url` is a single url, ie. a bucket url, a connection string or a request url, so its password
    may hold any character, spaces included. Prefixed and joined urls, ie. `rest:https://...` or
    `db://a|https://...`, are handled too. Use `redact_text` for free text that holds urls.

    The user name stays, `abfss` urls hold the container there, except for http(s) urls where it
    is replaced with `***` as it often holds a token. Query parameters are secret as decided by
    `is_sensitive_param`, as are quoted `"name": "value"` pairs of json or dict reprs. Values in one
    of `SECRET_VALUE_PATTERNS`, ie. `ghp_...` GitHub tokens, are replaced with `***`. Everything
    else in `url` is kept as is.

    Args:
        url (str): Url to redact.
        sensitive (Optional[Iterable[str]]): Names of secret query parameters, replacing the
            default ones.

    Returns:
        str: `url` without credentials.
    """
    return _redact(url, sensitive, _URL_USERINFO)


def redact_text(text: str, sensitive: Optional[Iterable[str]] = None) -> str:
    """Redacts credentials in urls inside free text, ie. an exception message or a log line.

    Works like `redact_url` but a password may not hold whitespace: in text, `sftp://h:x then
    a@b.com` would otherwise lose everything up to the email. Query parameters without a scheme,
    ie. `/items?api_key=...`, and values in `SECRET_VALUE_PATTERNS` are redacted anywhere.

    Args:
        text (str): Text to redact.
        sensitive (Optional[Iterable[str]]): Names of secret query parameters, replacing the
            default ones.

    Returns:
        str: `text` without credentials.
    """
    return _redact(text, sensitive, _URL_USERINFO_IN_TEXT)


def _redact(value: str, sensitive: Optional[Iterable[str]], userinfo: Pattern[str]) -> str:
    normalized_sensitive = (
        frozenset(normalize_param_name(s) for s in sensitive) if sensitive else None
    )

    def _redact_param(m: Match[str]) -> str:
        name = normalize_param_name(unquote_plus(m.group("name")))
        if _is_sensitive_normalized(name, normalized_sensitive):
            return f"{m.group('name')}={REDACTED}"
        return m.group(0)

    if "://" in value:
        value = userinfo.sub(_redact_userinfo, value)

    def _redact_quoted(m: Match[str]) -> str:
        if _is_sensitive_normalized(normalize_param_name(m.group("name")), normalized_sensitive):
            return m.group(0)[: m.start("value") - m.start()] + REDACTED + m.group("vq")
        return m.group(0)

    if "=" in value:
        value = _QUERY_PARAM.sub(_redact_param, value)
    if ":" in value and ('"' in value or "'" in value):
        value = _QUOTED_PAIR.sub(_redact_quoted, value)
    return _SECRET_VALUE.sub(REDACTED, value)


def _redact_userinfo(m: Match[str]) -> str:
    scheme, user, password = m.group("scheme", "user", "password")
    if scheme.lower() in HTTP_SCHEMES:
        return f"{scheme}://{REDACTED}@" if user or password else f"{scheme}://"
    if password is None:
        return m.group(0)
    return f"{scheme}://{user}@" if user else f"{scheme}://"


def redact_secrets(value: Any) -> Any:
    """Redacts secrets in `value`, ie. request params, headers or a json body.

    Strings are redacted with `redact_text`. In mappings, plain values of keys that
    `is_sensitive_param` tells are secret are replaced with `***`. Mappings, lists and tuples are
    visited recursively, also under secret keys, other values are returned unchanged.
    """
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, Mapping):
        return {
            k: (
                REDACTED
                if isinstance(k, str)
                and is_sensitive_param(k)
                and v is not None
                and not isinstance(v, (Mapping, list, tuple))
                else redact_secrets(v)
            )
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(v) for v in value]
    if isinstance(value, tuple):
        return tuple(redact_secrets(v) for v in value)
    return value
