import io
import logging
import sys
from importlib.metadata import version as pkg_version

import pytest
from pytest_mock import MockerFixture

from dlt.common import logger
from dlt.common.runtime import exec_info
from dlt.common.logger import is_logging
from dlt.common.typing import StrStr, DictStrStr
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RuntimeConfiguration

from tests.common.runtime.utils import mock_image_env, mock_github_env, mock_pod_env
from tests.common.configuration.utils import environment
from tests.utils import preserve_environ, init_test_logging


@configspec
class PureBasicConfiguration(RuntimeConfiguration):
    pipeline_name: str = "logger"


@configspec
class JsonLoggerConfiguration(PureBasicConfiguration):
    log_format: str = "JSON"


# @pytest.mark.skip
def test_version_extract(environment: DictStrStr) -> None:
    version = exec_info.dlt_version_info("logger")
    # assert version["dlt_version"].startswith(code_version)
    lib_version = pkg_version("dlt")
    assert version == {"dlt_version": lib_version, "pipeline_name": "logger"}
    # mock image info available in container
    mock_image_env(environment)
    version = exec_info.dlt_version_info(None)
    assert version == {
        "dlt_version": lib_version,
        "commit_sha": "192891",
        "image_version": "scale/v:112",
    }


def test_pod_info_extract(environment: DictStrStr) -> None:
    pod_info = exec_info.kube_pod_info()
    assert pod_info == {}
    mock_pod_env(environment)
    pod_info = exec_info.kube_pod_info()
    assert pod_info == {
        "kube_node_name": "node_name",
        "kube_pod_name": "pod_name",
        "kube_pod_namespace": "namespace",
    }


def test_github_info_extract(environment: DictStrStr) -> None:
    mock_github_env(environment)
    github_info = exec_info.github_info()
    assert github_info == {
        "github_user": "rudolfix",
        "github_repository": "dlt-hub/beginners-workshop-2022",
        "github_repository_owner": "dlt-hub",
    }
    mock_github_env(environment)
    del environment["GITHUB_USER"]
    github_info = exec_info.github_info()
    assert github_info == {
        "github_user": "dlt-hub",
        "github_repository": "dlt-hub/beginners-workshop-2022",
        "github_repository_owner": "dlt-hub",
    }


def test_logger_defaults_to_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    test_logger = logger._create_logger(
        logger_name="dlt_test_default_stderr",
        level="INFO",
        fmt="{levelname}|{message}",
        component="test",
        version={},
    )

    owned_handlers = [
        handler for handler in test_logger.handlers if isinstance(handler, logger._DltStreamHandler)
    ]

    assert len(owned_handlers) == 1
    assert owned_handlers[0].stream is sys.stderr
    assert test_logger.propagate is False

    test_logger.info("DEFAULT_STDERR_TEST")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "INFO|DEFAULT_STDERR_TEST\n"


def test_logger_outputs_to_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    test_logger = logger._create_logger(
        logger_name="dlt_test_stdout",
        level="INFO",
        fmt="{levelname}|{message}",
        component="test",
        version={},
        log_output="stdout",
    )

    owned_handlers = [
        handler for handler in test_logger.handlers if isinstance(handler, logger._DltStreamHandler)
    ]

    assert len(owned_handlers) == 1
    assert owned_handlers[0].stream is sys.stdout
    assert test_logger.propagate is False

    test_logger.info("STDOUT_TEST")

    captured = capsys.readouterr()
    assert captured.out == "INFO|STDOUT_TEST\n"
    assert captured.err == ""


def test_logger_propagates_to_parent(caplog: pytest.LogCaptureFixture) -> None:
    test_logger = logger._create_logger(
        logger_name="dlt_test_propagate",
        level="INFO",
        fmt="{levelname}|{message}",
        component="test",
        version={},
        log_output="propagate",
    )

    assert test_logger.propagate is True

    with caplog.at_level("INFO"):
        test_logger.info("PROPAGATE_TEST")

    matching_records = [
        record
        for record in caplog.records
        if record.name == "dlt_test_propagate" and record.getMessage() == "PROPAGATE_TEST"
    ]
    assert len(matching_records) == 1


def test_logger_propagation_preserves_levels(
    caplog: pytest.LogCaptureFixture,
) -> None:
    test_logger = logger._create_logger(
        logger_name="dlt_test_propagation_levels",
        level="INFO",
        fmt="{levelname}|{message}",
        component="test",
        version={},
        log_output="propagate",
    )

    with caplog.at_level("INFO"):
        test_logger.info("LEVEL_INFO")
        test_logger.warning("LEVEL_WARNING")
        test_logger.error("LEVEL_ERROR")

    received = [
        (record.levelname, record.getMessage())
        for record in caplog.records
        if record.name == "dlt_test_propagation_levels"
    ]
    assert received == [
        ("INFO", "LEVEL_INFO"),
        ("WARNING", "LEVEL_WARNING"),
        ("ERROR", "LEVEL_ERROR"),
    ]


@pytest.mark.forked
def test_text_logger_init(environment: DictStrStr, mocker: MockerFixture) -> None:
    mock_image_env(environment)
    mock_pod_env(environment)
    c = PureBasicConfiguration()
    c.log_level = "INFO"
    init_test_logging(c)
    assert logger.LOGGER is not None
    assert logger.LOGGER.name == "dlt"

    # logs on info level
    logger_spy = mocker.spy(logger.LOGGER, "info")
    logger.metrics("test health", extra={"metrics": "props"})
    logger_spy.assert_called_once_with("test health", extra={"metrics": "props"}, stacklevel=1)

    logger_spy.reset_mock()
    logger.metrics("test", extra={"metrics": "props"})
    logger_spy.assert_called_once_with("test", extra={"metrics": "props"}, stacklevel=1)

    logger.warning("Warning message here")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("DIV")


def test_propagate_removes_dlt_handler_without_duplicate(
    caplog: pytest.LogCaptureFixture,
) -> None:
    test_logger = logger._create_logger(
        logger_name="dlt_test_switch_to_propagate",
        level="INFO",
        fmt="{levelname}|{message}",
        component="test",
        version={},
        log_output="stderr",
    )
    assert (
        sum(isinstance(handler, logger._DltStreamHandler) for handler in test_logger.handlers) == 1
    )

    reinitialized = logger._create_logger(
        logger_name="dlt_test_switch_to_propagate",
        level="INFO",
        fmt="{levelname}|{message}",
        component="test",
        version={},
        log_output="propagate",
    )

    assert reinitialized is test_logger
    assert reinitialized.propagate is True
    assert not any(
        isinstance(handler, logger._DltStreamHandler) for handler in reinitialized.handlers
    )

    with caplog.at_level("INFO"):
        reinitialized.info("SWITCHED_TO_PROPAGATE")

    received = [
        record
        for record in caplog.records
        if record.name == "dlt_test_switch_to_propagate"
        and record.getMessage() == "SWITCHED_TO_PROPAGATE"
    ]
    assert len(received) == 1


def test_external_handler_survives_reinitialization() -> None:
    name = "dlt_test_external_handler"
    test_logger = logging.getLogger(name)

    external_stream = io.StringIO()
    external_handler = logging.StreamHandler(external_stream)
    external_formatter = logging.Formatter("EXTERNAL:{message}", style="{")
    external_handler.setFormatter(external_formatter)
    test_logger.addHandler(external_handler)

    try:
        for mode in ("stderr", "stdout", "propagate"):
            reinitialized = logger._create_logger(
                logger_name=name,
                level="INFO",
                fmt="{levelname}|{message}",
                component="test",
                version={},
                log_output=mode,
            )

            assert reinitialized is test_logger
            assert external_handler in test_logger.handlers
            assert external_handler.formatter is external_formatter
            assert external_handler.stream is external_stream

        test_logger.info("HANDLER_KEPT")
        assert external_stream.getvalue() == "EXTERNAL:HANDLER_KEPT\n"
    finally:
        test_logger.removeHandler(external_handler)
        external_handler.close()


def test_reinitialization_keeps_single_dlt_handler(
    capsys: pytest.CaptureFixture[str],
) -> None:
    name = "dlt_test_reinitialization"
    previous_handler = None

    for mode in ("stderr", "stderr", "stdout", "stdout", "propagate", "stderr"):
        test_logger = logger._create_logger(
            logger_name=name,
            level="INFO",
            fmt="{levelname}|{message}",
            component="test",
            version={},
            log_output=mode,
        )
        owned = [
            handler
            for handler in test_logger.handlers
            if isinstance(handler, logger._DltStreamHandler)
        ]

        assert len(owned) == (0 if mode == "propagate" else 1)
        if owned and previous_handler is not None:
            assert owned[0] is previous_handler

        previous_handler = owned[0] if owned else None

    test_logger.info("REINIT_ONCE")
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "INFO|REINIT_ONCE\n"


@pytest.mark.forked
def test_json_logger_init(environment: DictStrStr) -> None:
    from dlt.common.runtime import json_logging

    mock_image_env(environment)
    mock_pod_env(environment)
    init_test_logging(JsonLoggerConfiguration())
    # correct component was set
    assert json_logging.COMPONENT_NAME == "logger"
    logger.metrics("test health", extra={"metrics": "props"})
    logger.metrics("test", extra={"metrics": "props"})
    logger.warning("Warning message here")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("DIV")


@pytest.mark.forked
def test_double_log_init(environment: DictStrStr, mocker: MockerFixture) -> None:
    # comment out @pytest.mark.forked and use -s option to see the log messages
    mock_image_env(environment)
    mock_pod_env(environment)

    # logging is enabled somewhere earlier...
    # assert not is_logging()
    # from regular logger
    init_test_logging(PureBasicConfiguration())
    assert is_logging()

    # normal logger
    handler_spy = mocker.spy(logger.LOGGER.handlers[0].stream, "write")  # type: ignore[attr-defined]
    logger.error("test warning", extra={"metrics": "props"})
    msg = handler_spy.call_args_list[0][0][0]
    assert "|dlt|test_logging.py|test_double_log_init:" in msg
    assert 'test warning: "props"' in msg
    assert "ERROR" in msg

    # to json
    init_test_logging(JsonLoggerConfiguration())
    logger.error("test json warning", extra={"metrics": "props"})
    assert (
        '"msg":"test json warning","type":"log","logger":"dlt"'
        in handler_spy.call_args_list[1][0][0]
    )

    # to regular
    init_test_logging(PureBasicConfiguration())
    logger.error("test warning", extra={"metrics": "props"})

    # to json with name
    init_test_logging(JsonLoggerConfiguration())
    logger.error("test json warning", extra={"metrics": "props"})
    assert (
        '"msg":"test json warning","type":"log","logger":"dlt"'
        in handler_spy.call_args_list[3][0][0]
    )
    assert logger.LOGGER.name == "dlt"


@pytest.mark.forked
def test_logger_isEnabledFor(environment: DictStrStr) -> None:
    import logging

    c = PureBasicConfiguration()
    c.log_level = "INFO"
    init_test_logging(c)

    # Test that isEnabledFor properly proxied in dlt.common.logger
    # and not raises TypeError
    assert logger.isEnabledFor(logging.INFO)
    assert logger.isEnabledFor(logging.DEBUG) is False


def test_cleanup(environment: DictStrStr) -> None:
    # this must happen after all forked tests (problems with tests teardowns in other tests)
    pass
