from typing import Optional, Set
from urllib.parse import urlsplit, parse_qsl, urlencode, urlunsplit

SENSITIVE_PARAMS = {
    "api_key",
    "token",
    "key",
    "access_token",
    "apikey",
    "api-key",
    "access-token",
    "secret",
    "password",
    "pwd",
    "client_secret",
}


def sanitize_url(url: str, sensitive: Optional[Set[str]] = None) -> str:
    """Sanitizes a URL by replacing sensitive query parameters with '***'.

    Args:
        url: The URL to sanitize
        sensitive: Set of parameter names to redact (if not provided,
            uses the default set: SENSITIVE_PARAMS)

    Returns:
        str: The sanitized URL
    """
    if not sensitive:
        sensitive = SENSITIVE_PARAMS

    parsed = urlsplit(url)
    qs = parse_qsl(parsed.query, keep_blank_values=True)
    qs = [(k, "***" if k.lower() in {s.lower() for s in sensitive} else v) for k, v in qs]
    new_query = urlencode(qs, safe="*")  # Don't encode asterisks
    return urlunsplit(parsed._replace(query=new_query))
