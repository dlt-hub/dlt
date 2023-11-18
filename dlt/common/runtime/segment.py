"""dltHub telemetry using Segment"""

# several code fragments come from https://github.com/RasaHQ/rasa/blob/main/rasa/telemetry.py
import os
import sys
import multiprocessing
import atexit
import base64
import requests
import platform
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, Optional
from dlt.common.configuration.paths import get_dlt_data_dir

from dlt.common.runtime import logger

from dlt.common.configuration.specs import RunConfiguration
from dlt.common.runtime.exec_info import exec_info_names, in_continuous_integration
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.utils import uniq_id
from dlt.version import __version__, DLT_PKG_NAME

TEventCategory = Literal["pipeline", "command", "helper"]

_THREAD_POOL: ThreadPoolExecutor = None
_SESSION: requests.Session = None
_WRITE_KEY: str = None
_SEGMENT_REQUEST_TIMEOUT = (1.0, 1.0)  # short connect & send timeouts
_SEGMENT_ENDPOINT = "https://api.segment.io/v1/track"
_SEGMENT_CONTEXT: DictStrAny = None


def init_segment(config: RunConfiguration) -> None:
    assert config.dlthub_telemetry_segment_write_key, "dlthub_telemetry_segment_write_key not present in RunConfiguration"

    # create thread pool to send telemetry to segment
    global _THREAD_POOL, _WRITE_KEY, _SESSION
    if not _THREAD_POOL:
        _THREAD_POOL = ThreadPoolExecutor(1)
        _SESSION = requests.Session()
        # flush pool on exit
        atexit.register(_at_exit_cleanup)
    # store write key
    key_bytes = (config.dlthub_telemetry_segment_write_key + ":").encode("ascii")
    _WRITE_KEY = base64.b64encode(key_bytes).decode("utf-8")
    # cache the segment context
    _default_context_fields()


def disable_segment() -> None:
    _at_exit_cleanup()


def track(
    event_category: TEventCategory,
    event_name: str,
    properties: DictStrAny
) -> None:
    """Tracks a telemetry event.

    The segment event name will be created as "{event_category}_{event_name}

    Args:
        event_category: Category of the event: pipeline or cli
        event_name: Name of the event.
        properties: Dictionary containing the event's properties.
    """
    if properties is None:
        properties = {}

    properties.update({
        "event_category": event_category,
        "event_name": event_name
    })

    try:
        _send_event(f"{event_category}_{event_name}", properties, _default_context_fields())
    except Exception as e:
        logger.debug(f"Skipping telemetry reporting: {e}")
        raise


def before_send(event: DictStrAny) -> Optional[DictStrAny]:
    """Called before sending event. Does nothing, patch this function in the module for custom behavior"""
    return event


def _at_exit_cleanup() -> None:
    global _THREAD_POOL, _SESSION, _WRITE_KEY, _SEGMENT_CONTEXT
    if _THREAD_POOL:
        _THREAD_POOL.shutdown(wait=True)
        _THREAD_POOL = None
        _SESSION.close()
        _SESSION = None
        _WRITE_KEY = None
        _SEGMENT_CONTEXT = None


def _segment_request_header(write_key: str) -> StrAny:
    """Use a segment write key to create authentication headers for the segment API.

    Args:
        write_key: Authentication key for segment.

    Returns:
        Authentication headers for segment.
    """
    return {
        "Authorization": "Basic {}".format(write_key),
        "Content-Type": "application/json",
    }


def get_anonymous_id() -> str:
    """Creates or reads a anonymous user id"""
    home_dir = get_dlt_data_dir()
    if not os.path.isdir(home_dir):
        os.makedirs(home_dir, exist_ok=True)
    anonymous_id_file = os.path.join(home_dir, ".anonymous_id")
    if not os.path.isfile(anonymous_id_file):
        anonymous_id = uniq_id()
        with open(anonymous_id_file, "w", encoding="utf-8") as f:
            f.write(anonymous_id)
    else:
        with open(anonymous_id_file, "r", encoding="utf-8") as f:
            anonymous_id = f.read()
    return anonymous_id


def _segment_request_payload(
    event_name: str,
    properties: StrAny,
    context: StrAny
) -> DictStrAny:
    """Compose a valid payload for the segment API.

    Args:
        event_name: Name of the event.
        properties: Values to report along the event.
        context: Context information about the event.

    Returns:
        Valid segment payload.
    """
    return {
        "anonymousId": get_anonymous_id(),
        "event": event_name,
        "properties": properties,
        "context": context,
    }


def _default_context_fields() -> DictStrAny:
    """Return a dictionary that contains the default context values.

    Return:
        A new context containing information about the runtime environment.
    """
    global _SEGMENT_CONTEXT

    if not _SEGMENT_CONTEXT:
        # Make sure to update the example in docs/docs/telemetry/telemetry.mdx
        # if you change / add context
        _SEGMENT_CONTEXT = {
            "os": {"name": platform.system(), "version": platform.release()},
            "ci_run": in_continuous_integration(),
            "python": sys.version.split(" ")[0],
            "library": {"name": DLT_PKG_NAME, "version": __version__},
            "cpu": multiprocessing.cpu_count(),
            "exec_info": exec_info_names()
        }

    # avoid returning the cached dict --> caller could modify the dictionary...
    # usually we would use `lru_cache`, but that doesn't return a dict copy and
    # doesn't work on inner functions, so we need to roll our own caching...
    return _SEGMENT_CONTEXT.copy()


def _send_event(
    event_name: str,
    properties: StrAny,
    context: StrAny
) -> None:
    """Report the contents segment of an event to the /track Segment endpoint.

    Args:
        event_name: Name of the event.
        properties: Values to report along the event.
        context: Context information about the event.
    """
    # formulate payload and process in before send
    payload = before_send(_segment_request_payload(event_name, properties, context))
    # skip empty payloads
    if not payload:
        logger.debug("Skipping request to external service: payload was filtered out.")
        return

    if not _WRITE_KEY:
        # If _WRITE_KEY is empty or `None`, telemetry has not been enabled
        logger.debug("Skipping request to external service: telemetry key not set.")
        return

    headers = _segment_request_header(_WRITE_KEY)

    def _future_send() -> None:
        # import time
        # start_ts = time.time()
        resp = _SESSION.post(_SEGMENT_ENDPOINT, headers=headers, json=payload, timeout=_SEGMENT_REQUEST_TIMEOUT)
        # print(f"SENDING TO Segment done {resp.status_code} {time.time() - start_ts} {base64.b64decode(_WRITE_KEY)}")
        # handle different failure cases
        if resp.status_code != 200:
            logger.debug(
                f"Segment telemetry request returned a {resp.status_code} response. "
                f"Body: {resp.text}"
            )
        else:
            data = resp.json()
            if not data.get("success"):
                logger.debug(
                    f"Segment telemetry request returned a failure. Response: {data}"
                )

    _THREAD_POOL.submit(_future_send)
