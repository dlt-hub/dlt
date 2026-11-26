from typing import Tuple

from dlt.common import logger
from dlt.extract.source import DltSource
from dlt.sources.helpers.rest_client.utils import join_url

__all__ = ["join_url", "check_connection"]


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
