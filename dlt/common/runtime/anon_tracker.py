"""dltHub telemetry using using anonymous tracker"""

# several code fragments come from https://github.com/RasaHQ/rasa/blob/main/rasa/telemetry.py
import os

import atexit
import base64
import requests
from typing import Literal, Optional
from dlt.common.configuration.paths import get_dlt_data_dir

from dlt.common import logger
from dlt.common.managed_thread_pool import ManagedThreadPool
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.runtime.exec_info import get_execution_context, TExecutionContext
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.utils import uniq_id
from dlt.version import __version__

TEventCategory = Literal["pipeline", "command", "helper"]

_THREAD_POOL: ManagedThreadPool = ManagedThreadPool(1)
_SESSION: requests.Session = None
_WRITE_KEY: str = None
_REQUEST_TIMEOUT = (1.0, 1.0)  # short connect & send timeouts
_ANON_TRACKER_ENDPOINT: str = None
_TRACKER_CONTEXT: TExecutionContext = None


def init_anon_tracker(config: RunConfiguration) -> None:
    if config.dlthub_telemetry_endpoint is None:
        raise ValueError("dlthub_telemetry_endpoint not specified in RunConfiguration")

    if config.dlthub_telemetry_endpoint == "https://api.segment.io/v1/track":
        assert (
            config.dlthub_telemetry_segment_write_key
        ), "dlthub_telemetry_segment_write_key not present in RunConfiguration"

    global _WRITE_KEY, _SESSION, _ANON_TRACKER_ENDPOINT
    # create thread pool to send telemetry to anonymous tracker
    if not _SESSION:
        _SESSION = requests.Session()
        # flush pool on exit
        atexit.register(_at_exit_cleanup)
    # store write key if present
    if config.dlthub_telemetry_segment_write_key:
        key_bytes = (config.dlthub_telemetry_segment_write_key + ":").encode("ascii")
        _WRITE_KEY = base64.b64encode(key_bytes).decode("utf-8")
    # store endpoint
    _ANON_TRACKER_ENDPOINT = config.dlthub_telemetry_endpoint
    # cache the tracker context
    _default_context_fields()


def disable_anon_tracker() -> None:
    _at_exit_cleanup()
    atexit.unregister(_at_exit_cleanup)


def track(event_category: TEventCategory, event_name: str, properties: DictStrAny) -> None:
    """Tracks a telemetry event.

    The tracker event name will be created as "{event_category}_{event_name}

    Args:
        event_category: Category of the event: pipeline or cli
        event_name: Name of the event.
        properties: Dictionary containing the event's properties.
    """
    if properties is None:
        properties = {}

    properties.update({"event_category": event_category, "event_name": event_name})

    try:
        _send_event(f"{event_category}_{event_name}", properties, _default_context_fields())
    except Exception as e:
        logger.debug(f"Skipping telemetry reporting: {e}")
        raise


def before_send(event: DictStrAny) -> Optional[DictStrAny]:
    """Called before sending event. Does nothing, patch this function in the module for custom behavior"""
    return event


def _at_exit_cleanup() -> None:
    global _SESSION, _WRITE_KEY, _TRACKER_CONTEXT, _ANON_TRACKER_ENDPOINT
    if _SESSION:
        _THREAD_POOL.stop(True)
        _SESSION.close()
        _SESSION = None
    _ANON_TRACKER_ENDPOINT = None
    _WRITE_KEY = None
    _TRACKER_CONTEXT = None


def _tracker_request_header(write_key: str) -> StrAny:
    """Use a segment write key to create authentication headers for the segment API.

    Args:
        write_key: Authentication key for segment.

    Returns:
        Authentication headers for segment.
    """
    headers = {"Content-Type": "application/json"}
    if write_key:
        headers["Authorization"] = "Basic {}".format(write_key)
    return headers


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


def _create_request_payload(event_name: str, properties: StrAny, context: StrAny) -> DictStrAny:
    """Compose a valid payload for the tracker.

    Args:
        event_name: Name of the event.
        properties: Values to report along the event.
        context: Context information about the event.

    Returns:
        Valid tracker payload.
    """
    return {
        "anonymousId": get_anonymous_id(),
        "event": event_name,
        "properties": properties,
        "context": context,
    }


def _default_context_fields() -> TExecutionContext:
    """Return a dictionary that contains the default context values.

    Return:
        A new context containing information about the runtime environment.
    """
    global _TRACKER_CONTEXT

    if not _TRACKER_CONTEXT:
        # Make sure to update the example in docs/docs/telemetry/telemetry.mdx
        # if you change / add context
        _TRACKER_CONTEXT = get_execution_context()

    # avoid returning the cached dict --> caller could modify the dictionary...
    # usually we would use `lru_cache`, but that doesn't return a dict copy and
    # doesn't work on inner functions, so we need to roll our own caching...
    return _TRACKER_CONTEXT.copy()


def _send_event(event_name: str, properties: StrAny, context: StrAny) -> None:
    """Report the contents of an event to the tracker endpoint.

    Args:
        event_name: Name of the event.
        properties: Values to report along the event.
        context: Context information about the event.
    """
    # formulate payload and process in before send
    payload = before_send(_create_request_payload(event_name, properties, context))
    # skip empty payloads
    if not payload:
        logger.debug("Skipping request to external service: payload was filtered out.")
        return

    if _ANON_TRACKER_ENDPOINT is None:
        logger.debug("Skipping request to external service: telemetry endpoint not set.")
        return

    headers = _tracker_request_header(_WRITE_KEY)

    def _future_send() -> None:
        # import time
        # start_ts = time.time_ns()
        resp = _SESSION.post(
            _ANON_TRACKER_ENDPOINT, headers=headers, json=payload, timeout=_REQUEST_TIMEOUT
        )
        # end_ts = time.time_ns()
        # elapsed_time = (end_ts - start_ts) / 10e6
        # print(f"SENDING TO TRACKER done: {elapsed_time}ms Status: {resp.status_code}")
        # handle different failure cases
        if resp.status_code not in [200, 204]:
            logger.debug(
                f"Tracker request returned a {resp.status_code} response. Body: {resp.text}"
            )
        else:
            if resp.status_code == 200:
                # parse the response if available
                data = resp.json()
                if not data.get("success"):
                    logger.debug(f"Tracker telemetry request returned a failure. Response: {data}")

    _THREAD_POOL.thread_pool.submit(_future_send)
