import pytest
import logging
import json_logging
from os import environ
from importlib.metadata import version as pkg_version

from dlt.common import logger, sleep
from dlt.common.typing import StrStr
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration

from tests.utils import preserve_environ, skipifspawn


@configspec
class PureBasicConfiguration(RunConfiguration):
    pipeline_name: str = "logger"


@configspec
class JsonLoggerConfiguration(PureBasicConfiguration):
    log_format: str = "JSON"


@configspec
class SentryLoggerConfiguration(JsonLoggerConfiguration):
    sentry_dsn: str = "http://user:pass@localhost/818782"


@configspec(init=True)
class SentryLoggerCriticalConfiguration(SentryLoggerConfiguration):
    log_level: str = "CRITICAL"


@pytest.fixture(scope="function")
def environment() -> StrStr:
    environ.clear()

    return environ


# @pytest.mark.skip
def test_version_extract(environment: StrStr) -> None:
    version = logger._extract_version_info(PureBasicConfiguration())
    # assert version["dlt_version"].startswith(code_version)
    lib_version = pkg_version("python-dlt")
    assert version == {'dlt_version': lib_version, 'pipeline_name': 'logger'}
    # mock image info available in container
    _mock_image_env(environment)
    version = logger._extract_version_info(PureBasicConfiguration())
    assert version == {'dlt_version': lib_version, 'commit_sha': '192891', 'pipeline_name': 'logger', 'image_version': 'scale/v:112'}


def test_pod_info_extract(environment: StrStr) -> None:
    pod_info = logger._extract_pod_info()
    assert pod_info == {}
    _mock_pod_env(environment)
    pod_info = logger._extract_pod_info()
    assert pod_info == {'kube_node_name': 'node_name', 'kube_pod_name': 'pod_name', 'kube_pod_namespace': 'namespace'}


def test_github_info_extract(environment: StrStr) -> None:
    _mock_github_env(environment)
    github_info = logger._extract_github_info()
    assert github_info == {"github_user": "rudolfix", "github_repository": "dlt-hub/beginners-workshop-2022", "github_repository_owner": "dlt-hub"}
    _mock_github_env(environment)
    del environment["GITHUB_USER"]
    github_info = logger._extract_github_info()
    assert github_info == {"github_user": "dlt-hub", "github_repository": "dlt-hub/beginners-workshop-2022", "github_repository_owner": "dlt-hub"}



@pytest.mark.forked
def test_text_logger_init(environment: StrStr) -> None:
    _mock_image_env(environment)
    _mock_pod_env(environment)
    logger.init_logging_from_config(PureBasicConfiguration())
    logger.metrics("progress", "test health", extra={"metrics": "props"})
    logger.metrics("progress", "test", extra={"metrics": "props"})
    logger.warning("Warning message here")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("DIV")


@pytest.mark.forked
def test_json_logger_init(environment: StrStr) -> None:
    _mock_image_env(environment)
    _mock_pod_env(environment)
    logger.init_logging_from_config(JsonLoggerConfiguration)
    # correct component was set
    json_logging.COMPONENT_NAME = "logger"
    logger.metrics("progress", "test health", extra={"metrics": "props"})
    logger.metrics("progress", "test", extra={"metrics": "props"})
    logger.warning("Warning message here")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("DIV")


def test_sentry_log_level() -> None:
    sll = logger._get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="CRITICAL"))
    assert sll._handler.level == logging._nameToLevel["CRITICAL"]
    sll = logger._get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="ERROR"))
    assert sll._handler.level == logging._nameToLevel["ERROR"]
    sll = logger._get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="WARNING"))
    assert sll._handler.level == logging._nameToLevel["WARNING"]
    sll = logger._get_sentry_log_level(SentryLoggerCriticalConfiguration(log_level="INFO"))
    assert sll._handler.level == logging._nameToLevel["WARNING"]


@skipifspawn
@pytest.mark.forked
def test_sentry_init(environment: StrStr) -> None:
    _mock_image_env(environment)
    _mock_pod_env(environment)
    logger.init_logging_from_config(SentryLoggerConfiguration())
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("DIV")
    sleep(1)


def test_cleanup(environment: StrStr) -> None:
    # this must happen after all forked tests (problems with tests teardowns in other tests)
    pass


def _mock_image_env(environment: StrStr) -> None:
    environment["COMMIT_SHA"] = "192891"
    environment["IMAGE_VERSION"] = "scale/v:112"


def _mock_pod_env(environment: StrStr) -> None:
    environment["KUBE_NODE_NAME"] = "node_name"
    environment["KUBE_POD_NAME"] = "pod_name"
    environment["KUBE_POD_NAMESPACE"] = "namespace"


def _mock_github_env(environment: StrStr) -> None:
    environment["GITHUB_USER"] = "rudolfix"
    environment["GITHUB_REPOSITORY"] = "dlt-hub/beginners-workshop-2022"
    environment["GITHUB_REPOSITORY_OWNER"] = "dlt-hub"
