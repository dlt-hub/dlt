from typing import Any
import os
import pytest
import logging
from unittest.mock import patch

from dlt.common import logger
from dlt.common.runtime.sentry import _get_sentry_log_level
from dlt.common.runtime.segment import get_anonymous_id, track, disable_segment
from dlt.common.typing import DictStrAny, StrStr
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration
from dlt.version import DLT_PKG_NAME, __version__

from tests.common.runtime.utils import mock_image_env, mock_github_env, mock_pod_env
from tests.common.configuration.utils import environment
from tests.utils import preserve_environ, skipifspawn, skipifwindows, init_test_logging, start_test_telemetry


@configspec
class SentryLoggerConfiguration(RunConfiguration):
    pipeline_name: str = "logger"
    sentry_dsn: str = "https://6f6f7b6f8e0f458a89be4187603b55fe@o1061158.ingest.sentry.io/4504819859914752"
    dlthub_telemetry_segment_write_key: str = "TLJiyRkGVZGCi2TtjClamXpFcxAA1rSB"


@configspec(init=True)
class SentryLoggerCriticalConfiguration(SentryLoggerConfiguration):
    log_level: str = "CRITICAL"


def test_sentry_log_level() -> None:
    sll = _get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="CRITICAL"))
    assert sll._handler.level == logging._nameToLevel["CRITICAL"]
    sll = _get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="ERROR"))
    assert sll._handler.level == logging._nameToLevel["ERROR"]
    sll = _get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="WARNING"))
    assert sll._handler.level == logging._nameToLevel["WARNING"]
    sll = _get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="INFO"))
    assert sll._handler.level == logging._nameToLevel["WARNING"]


@pytest.mark.forked
def test_sentry_init(environment: StrStr) -> None:
    with patch("dlt.common.runtime.sentry.before_send", _mock_before_send):
        mock_image_env(environment)
        mock_pod_env(environment)
        init_test_logging(SentryLoggerConfiguration())
        start_test_telemetry(SentryLoggerConfiguration())
        SENT_ITEMS.clear()
        try:
            1 / 0
        except ZeroDivisionError:
            logger.exception("DIV")
        # message sent
        assert len(SENT_ITEMS) == 1


@pytest.mark.forked
def test_track_segment_event() -> None:
    mock_github_env(os.environ)
    mock_pod_env(os.environ)

    props = {"destination_name": "duckdb", "elapsed_time": 1.23123, "success": True}
    with patch("dlt.common.runtime.segment.before_send", _mock_before_send):
        start_test_telemetry(SentryLoggerConfiguration())
        track("pipeline", "run", props)
        # this will send stuff
        disable_segment()
    event = SENT_ITEMS[0]
    assert event["anonymousId"] == get_anonymous_id()
    assert event["event"] == "pipeline_run"
    assert props.items() <= event["properties"].items()
    assert event["properties"]["event_category"] == "pipeline"
    assert event["properties"]["event_name"] == "run"
    # verify context
    context = event["context"]
    assert context["library"] == {"name": DLT_PKG_NAME, "version": __version__}
    assert isinstance(context["cpu"], int)
    assert isinstance(context["ci_run"], bool)
    assert isinstance(context["exec_info"], list)
    assert ["kubernetes", "codespaces"] <= context["exec_info"]


def test_cleanup(environment: StrStr) -> None:
    # this must happen after all forked tests (problems with tests teardowns in other tests)
    pass


SENT_ITEMS = []
def _mock_before_send(event: DictStrAny, _unused_hint: Any = None) -> DictStrAny:
    # print(event)
    SENT_ITEMS.append(event)
    return event