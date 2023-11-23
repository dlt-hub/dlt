import pytest
from importlib.metadata import version as pkg_version

from dlt.common import logger
from dlt.common.runtime import exec_info
from dlt.common.runtime.logger import is_logging
from dlt.common.typing import StrStr, DictStrStr
from dlt.common.configuration import configspec
from dlt.common.configuration.specs import RunConfiguration

from tests.common.runtime.utils import mock_image_env, mock_github_env, mock_pod_env
from tests.common.configuration.utils import environment
from tests.utils import preserve_environ, init_test_logging


@configspec
class PureBasicConfiguration(RunConfiguration):
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
    version = exec_info.dlt_version_info("logger")
    assert version == {
        "dlt_version": lib_version,
        "commit_sha": "192891",
        "pipeline_name": "logger",
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


@pytest.mark.forked
def test_text_logger_init(environment: DictStrStr) -> None:
    mock_image_env(environment)
    mock_pod_env(environment)
    init_test_logging(PureBasicConfiguration())
    logger.metrics("test health", extra={"metrics": "props"})
    logger.metrics("test", extra={"metrics": "props"})
    logger.warning("Warning message here")
    try:
        1 / 0
    except ZeroDivisionError:
        logger.exception("DIV")


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
def test_double_log_init(environment: DictStrStr) -> None:
    mock_image_env(environment)
    mock_pod_env(environment)

    # logging is enabled somewhere earlier...
    # assert not is_logging()
    # from regular logger
    init_test_logging(PureBasicConfiguration())
    assert is_logging()
    # caplog does not capture the formatted output of loggers below
    # so I'm not able to test the exact output
    # comment out @pytest.mark.forked and use -s option to see the log messages
    # logger.LOGGER.propagate = True
    logger.error("test warning", extra={"metrics": "props"})
    # normal logger
    # to json
    init_test_logging(JsonLoggerConfiguration())
    logger.error("test json warning", extra={"metrics": "props"})
    # to regular
    init_test_logging(PureBasicConfiguration())
    logger.error("test warning", extra={"metrics": "props"})
    # to json
    init_test_logging(JsonLoggerConfiguration())
    logger.error("test warning", extra={"metrics": "props"})


def test_cleanup(environment: DictStrStr) -> None:
    # this must happen after all forked tests (problems with tests teardowns in other tests)
    pass
