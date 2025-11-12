from typing import Any, Union
from contextlib import nullcontext as does_not_raise
import os
import pytest
import logging
import base64
from unittest.mock import patch, Mock

from pytest_mock import MockerFixture

import dlt
from dlt.common import logger
from dlt.common.runtime.anon_tracker import get_anonymous_id, track, disable_anon_tracker
from dlt.common.runtime.exec_info import get_execution_context
from dlt.common.typing import DictStrAny, DictStrStr
from dlt.common.schema import Schema
from dlt.common.utils import digest128
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RuntimeConfiguration
from dlt.version import DLT_PKG_NAME, __version__

from tests.common.runtime.utils import mock_image_env, mock_github_env, mock_pod_env
from tests.common.configuration.utils import environment
from tests.utils import (
    preserve_environ,
    SentryLoggerConfiguration,
    disable_temporary_telemetry,
    init_test_logging,
    start_test_telemetry,
    deactivate_pipeline,
)
from dlt.common.runtime.telemetry import with_telemetry


@configspec
class SentryLoggerCriticalConfiguration(SentryLoggerConfiguration):
    log_level: str = "CRITICAL"


def test_sentry_init(environment: DictStrStr, disable_temporary_telemetry) -> None:
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


def test_sentry_log_level() -> None:
    from dlt.common.runtime.sentry import _get_sentry_log_level

    sll = _get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="CRITICAL"))
    assert sll._handler.level == logging._nameToLevel["CRITICAL"]
    sll = _get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="ERROR"))
    assert sll._handler.level == logging._nameToLevel["ERROR"]
    sll = _get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="WARNING"))
    assert sll._handler.level == logging._nameToLevel["WARNING"]
    sll = _get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="INFO"))
    assert sll._handler.level == logging._nameToLevel["WARNING"]


@pytest.mark.parametrize(
    "endpoint, write_key, expectation",
    [
        (
            "https://api.segment.io/v1/track",
            "TLJiyRkGVZGCi2TtjClamXpFcxAA1rSB",
            does_not_raise(),
        ),
        (
            "https://telemetry.scalevector.ai",
            None,
            does_not_raise(),
        ),
        (
            "https://telemetry-tracker.services4758.workers.dev/",
            None,
            does_not_raise(),
        ),
    ],
)
def test_telemetry_endpoint(
    endpoint, write_key, expectation, disable_temporary_telemetry: RuntimeConfiguration
) -> None:
    from dlt.common.runtime import anon_tracker

    with expectation:
        anon_tracker.init_anon_tracker(
            RuntimeConfiguration(
                dlthub_telemetry_endpoint=endpoint, dlthub_telemetry_segment_write_key=write_key
            )
        )

    assert anon_tracker._ANON_TRACKER_ENDPOINT == endpoint
    if write_key is None:
        assert anon_tracker._WRITE_KEY is None
    else:
        assert base64.b64decode(anon_tracker._WRITE_KEY.encode("ascii")).decode() == write_key + ":"


@pytest.mark.parametrize(
    "endpoint, write_key, expectation",
    [
        (
            "https://api.segment.io/v1/track",
            None,
            pytest.raises(AssertionError),
        ),
        (
            None,
            "TLJiyRkGVZGCi2TtjClamXpFcxAA1rSB",
            pytest.raises(ValueError),
        ),
    ],
)
def test_telemetry_endpoint_exceptions(
    endpoint, write_key, expectation, disable_temporary_telemetry: RuntimeConfiguration
) -> None:
    from dlt.common.runtime import anon_tracker

    with expectation:
        anon_tracker.init_anon_tracker(
            RuntimeConfiguration(
                dlthub_telemetry_endpoint=endpoint, dlthub_telemetry_segment_write_key=write_key
            )
        )


def test_track_anon_event(mocker: MockerFixture, disable_temporary_telemetry) -> None:
    from dlt.common.runtime import anon_tracker

    mock_github_env(os.environ)
    mock_pod_env(os.environ)
    SENT_ITEMS.clear()
    config = SentryLoggerConfiguration()

    props = {"destination_name": "duckdb", "elapsed_time": 712.23123, "success": True}
    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)
        requests_post = mocker.patch(
            "dlt.common.runtime.anon_tracker.requests.post",
            return_value=Mock(status_code=204),
        )
        track("pipeline", "run", props)
        # this will send stuff
        disable_anon_tracker()

    event = SENT_ITEMS[0]
    # requests were really called
    requests_post.assert_called_once_with(
        config.dlthub_telemetry_endpoint,
        headers=anon_tracker._tracker_request_header(None),
        json=event,
        timeout=anon_tracker._REQUEST_TIMEOUT,
    )
    # was actually delivered
    assert requests_post.return_value.status_code == 204

    assert event["anonymousId"] == get_anonymous_id()
    assert event["event"] == "pipeline_run"
    assert props.items() <= event["properties"].items()
    assert event["properties"]["event_category"] == "pipeline"
    assert event["properties"]["event_name"] == "run"
    assert event["properties"]["destination_name"] == "duckdb"
    assert event["properties"]["elapsed_time"] == 712.23123
    # verify context
    context = event["context"]
    assert context["library"] == {"name": DLT_PKG_NAME, "version": __version__}
    print(context)
    # we assume plus is not installed
    assert "dlthub" not in context
    assert isinstance(context["cpu"], int)
    assert isinstance(context["ci_run"], bool)
    assert isinstance(context["exec_info"], list)
    assert ["kubernetes", "codespaces"] <= context["exec_info"]
    assert context["run_context"] == "dlt"


def test_forced_anon_tracker() -> None:
    from dlt.common.runtime import anon_tracker

    if anon_tracker._ANON_TRACKER_ENDPOINT is not None:
        disable_anon_tracker()
    assert anon_tracker._ANON_TRACKER_ENDPOINT is None

    with anon_tracker.always_track():
        assert anon_tracker._ANON_TRACKER_ENDPOINT is not None

    assert anon_tracker._ANON_TRACKER_ENDPOINT is None


def test_execution_context_with_plugin() -> None:
    import sys

    # move working dir so dlthub mock is importable and appears in settings
    plus_path = os.path.dirname(__file__)
    sys.path.append(plus_path)
    try:
        context = get_execution_context()
        # has plugin info
        assert context["dlthub"] == {"name": "dlthub", "version": "1.7.1"}
    finally:
        sys.path.remove(plus_path)


@pytest.mark.parametrize(
    "schema",
    [Schema("my_schema"), "str_schema", None],
)
@pytest.mark.parametrize(
    "success",
    [True, False],
)
def test_on_first_dataset_access(
    schema: Union[Schema, str, None], success: bool, monkeypatch, disable_temporary_telemetry
) -> None:
    pipeline = dlt.pipeline("test_on_first_dataset_access", destination="duckdb")

    if not success:
        monkeypatch.setattr(dlt, "dataset", Mock(side_effect=RuntimeError("fake_error")))

    mock_github_env(os.environ)
    mock_pod_env(os.environ)
    SENT_ITEMS.clear()
    config = SentryLoggerConfiguration()

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)
        # first access should always trigger telemetry
        # second access should NOT trigger telemetry
        if not success:
            with pytest.raises(RuntimeError):
                pipeline.dataset(schema)
            with pytest.raises(RuntimeError):
                pipeline.dataset(schema)
        else:
            pipeline.dataset(schema)
            pipeline.dataset(schema)
        disable_anon_tracker()

    # should have exactly 1 event despite two dataset method calls
    assert len(SENT_ITEMS) == 1
    event = SENT_ITEMS[0]

    assert event["event"] == "pipeline_access_dataset"
    assert event["properties"]["event_category"] == "pipeline"
    assert event["properties"]["event_name"] == "access_dataset"
    assert event["properties"]["success"] == success
    assert event["properties"]["destination_name"] == pipeline.destination.destination_name
    assert event["properties"]["destination_type"] == pipeline.destination.destination_type
    assert event["properties"]["dataset_name_hash"] == digest128(pipeline.dataset_name)
    assert event["properties"]["default_schema_name_hash"] is None
    requested_schema_name_hash = None
    if isinstance(schema, Schema):
        requested_schema_name_hash = digest128(schema.name)
    elif isinstance(schema, str):
        requested_schema_name_hash = digest128(schema)
    assert event["properties"]["requested_schema_name_hash"] == requested_schema_name_hash


@pytest.mark.parametrize(
    "case_name, behavior, expected_success",
    [
        ("non-int return -> success", "return_non_int", True),
        ("int 0 return -> success", "return_zero", True),
        ("int non-zero return -> failure", "return_one", False),
        ("exception -> failure", "raise", False),
    ],
    ids=[
        "return-non-int-success",
        "return-int-zero-success",
        "return-int-nonzero-failure",
        "raise-exception-failure",
    ],
)
def test_with_telemetry_track_after_various_outcomes(
    case_name: str,
    behavior: str,
    expected_success: bool,
    mocker: MockerFixture,
    disable_temporary_telemetry,
) -> None:
    # init test telemetry and capture outgoing events
    mock_github_env(os.environ)
    mock_pod_env(os.environ)
    SENT_ITEMS.clear()
    config = SentryLoggerConfiguration()

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)
        mocker.patch(
            "dlt.common.runtime.anon_tracker.requests.post",
            return_value=Mock(status_code=204),
        )

        # decorate a function so that 'x' and 'y' are included as props and an extra const prop
        @with_telemetry("command", "cmd", False, "x", "y", extra_const="value", case=case_name)
        def _fn(x: Any, y: Any) -> Any:
            # implement different behaviors
            if behavior == "return_non_int":
                return "ok"
            if behavior == "return_zero":
                return 0
            if behavior == "return_one":
                return 1
            if behavior == "raise":
                raise ValueError("boom")
            return None

        # call the function and handle potential exception so we can flush telemetry
        if behavior == "raise":
            with pytest.raises(ValueError):
                _fn("X", 7)
        else:
            _fn("X", 7)

        # flush and collect the event
        disable_anon_tracker()

    # exactly one event was recorded
    assert len(SENT_ITEMS) == 1
    event = SENT_ITEMS[0]

    # event name and basic structure
    assert event["event"] == "command_cmd"
    props = event["properties"]
    assert props["event_category"] == "command"
    assert props["event_name"] == "cmd"

    # verify automatic props
    assert isinstance(props["elapsed"], (int, float))
    assert props["elapsed"] >= 0
    assert props["success"] is expected_success

    # verify captured arg values and extra kwargs
    assert props["x"] == "X"
    assert props["y"] == 7
    assert props["extra_const"] == "value"
    assert props["case"] == case_name


@pytest.mark.parametrize(
    "case_name, behavior",
    [
        ("before: return value", "return"),
        ("before: raise exception", "raise"),
    ],
    ids=["track-before-return", "track-before-raise"],
)
def test_with_telemetry_track_before_emits_once_with_success(
    case_name: str, behavior: str, mocker: MockerFixture, disable_temporary_telemetry
) -> None:
    # init test telemetry and capture outgoing events
    mock_github_env(os.environ)
    mock_pod_env(os.environ)
    SENT_ITEMS.clear()
    config = SentryLoggerConfiguration()

    with patch("dlt.common.runtime.anon_tracker.before_send", _mock_before_send):
        start_test_telemetry(config)
        mocker.patch(
            "dlt.common.runtime.anon_tracker.requests.post",
            return_value=Mock(status_code=204),
        )

        # when tracked before, success should always be True regardless of function outcome
        @with_telemetry("command", "before_cmd", True, "x", ignored_param="ignored")
        def _fn(x: Any) -> Any:
            if behavior == "return":
                return 1  # would be failure in after-mode but should be success here
            raise RuntimeError("fail early")

        if behavior == "raise":
            with pytest.raises(RuntimeError):
                _fn(123)
        else:
            _fn(123)

        disable_anon_tracker()

    # a single event is recorded with success True
    assert len(SENT_ITEMS) == 1
    event = SENT_ITEMS[0]
    assert event["event"] == "command_before_cmd"
    props = event["properties"]
    assert props["event_category"] == "command"
    assert props["event_name"] == "before_cmd"
    assert props["success"] is True
    # verify included props
    assert props["x"] == 123
    assert props["ignored_param"] == "ignored"
    assert isinstance(props["elapsed"], (int, float)) and props["elapsed"] >= 0


def test_cleanup(environment: DictStrStr) -> None:
    # this must happen after all forked tests (problems with tests teardowns in other tests)
    pass


SENT_ITEMS = []


def _mock_before_send(event: DictStrAny, _unused_hint: Any = None) -> DictStrAny:
    # print(event)
    SENT_ITEMS.append(event)
    return event
