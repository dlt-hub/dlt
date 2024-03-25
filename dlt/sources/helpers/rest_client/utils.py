from typing import Tuple

from dlt.common import logger
from dlt.extract.source import DltSource


def join_url(base_url: str, path: str) -> str:
    if base_url is None:
        raise ValueError("Base URL must be provided or set to an empty string.")

    if base_url == "":
        return path

    if path == "":
        return base_url

    # Normalize the base URL
    base_url = base_url.rstrip("/")
    if not base_url.endswith("/"):
        base_url += "/"

    return base_url + path.lstrip("/")


def check_connection(
    source: DltSource,
    *resource_names: str,
) -> Tuple[bool, str]:
    try:
        list(source.with_resources(*resource_names).add_limit(1))
        return (True, "")
    except Exception as e:
        logger.error(f"Error checking connection: {e}")
        return (False, str(e))
