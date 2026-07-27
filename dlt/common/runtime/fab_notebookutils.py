"""Azure SDK TokenCredential backed by Microsoft Fabric NotebookUtils."""

import base64
import threading
import time
from typing import Any, Dict, NamedTuple, Optional

from dlt.common import logger
from dlt.common.configuration.exceptions import ConfigurationException
from dlt.common.json import json

_REFRESH_BUFFER_SECONDS = 300
_FALLBACK_LIFETIME_SECONDS = 3600

_AUDIENCE_ALIASES: Dict[str, str] = {
    "sql": "https://database.windows.net/",
}


class _AccessToken(NamedTuple):
    token: str
    expires_on: int


def _decode_jwt_expiry(token: str) -> Optional[int]:
    """Extract the `exp` claim from a JWT payload without signature verification."""
    try:
        payload = token.split(".")[1]
        remainder = len(payload) % 4
        if remainder:
            payload += "=" * (4 - remainder)
        claims = json.loadb(base64.urlsafe_b64decode(payload))
        return int(claims["exp"])
    except (IndexError, KeyError, ValueError, TypeError):
        return None


def resolve_fab_notebookutils_get_token() -> Any:
    """Return the Fabric NotebookUtils `getToken` callable, trying the modern API first.

    Raises:
        ConfigurationException: When not running in a Microsoft Fabric runtime, or when the
            runtime exposes no known credential API.
    """
    try:
        import notebookutils  # type: ignore[import-not-found]
    except ImportError:
        raise ConfigurationException(
            "NotebookUtils credential API is not available."
            " It requires the Microsoft Fabric runtime."
        )

    for path in ("credentials.getToken", "mssparkutils.credentials.getToken"):
        obj: Any = notebookutils
        try:
            for attr in path.split("."):
                obj = getattr(obj, attr)
            if callable(obj):
                return obj
        except AttributeError:
            continue

    raise ConfigurationException(
        "NotebookUtils credential API is not available. It requires the Microsoft Fabric runtime."
    )


def is_fab_notebookutils_available() -> bool:
    """True when the NotebookUtils credential API can be reached (Microsoft Fabric runtime)."""
    try:
        resolve_fab_notebookutils_get_token()
        return True
    except ConfigurationException:
        return False


class FabNotebookUtilsCredential:
    """Azure SDK `TokenCredential` backed by Microsoft Fabric NotebookUtils.

    Authenticates as the identity the Fabric notebook runs under. Only usable inside the
    Fabric runtime; `notebookutils` is imported lazily, never at module load time.

    Args:
        audience (str): Token audience. Accepts NotebookUtils resource keys (`storage`,
            `keyvault`, `pbi`, `kusto`) or a full resource URI. The alias `sql` expands to
            `https://database.windows.net/`.

    Example:
        >>> sql_cred = FabNotebookUtilsCredential("sql")
        >>> vault_cred = FabNotebookUtilsCredential("keyvault")
    """

    def __init__(self, audience: str) -> None:
        self._audience = _AUDIENCE_ALIASES.get(audience, audience)
        self._lock = threading.Lock()
        self._cached: Optional[_AccessToken] = None
        self._get_token_fn: Optional[Any] = None

    def get_token(self, *scopes: str, **kwargs: Any) -> _AccessToken:
        """Acquire a token for the configured audience.

        Implements the Azure SDK `TokenCredential` protocol. `scopes` is accepted for protocol
        compatibility but ignored: the token is always acquired for the audience bound at
        construction.
        """
        with self._lock:
            now = int(time.time())
            if self._cached is not None and self._cached.expires_on - now > _REFRESH_BUFFER_SECONDS:
                return self._cached

            if self._get_token_fn is None:
                self._get_token_fn = resolve_fab_notebookutils_get_token()

            token_str: str = self._get_token_fn(self._audience)
            expires_on = _decode_jwt_expiry(token_str)
            if expires_on is None:
                expires_on = now + _FALLBACK_LIFETIME_SECONDS

            self._cached = _AccessToken(token=token_str, expires_on=expires_on)
            logger.info(
                "Acquired Fabric NotebookUtils token for audience %s (expires in %d s)",
                self._audience,
                max(0, expires_on - now),
            )
            return self._cached

    def close(self) -> None:
        """Part of the Azure SDK credential protocol. No-op: no session is held between tokens."""

    def __enter__(self) -> "FabNotebookUtilsCredential":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
