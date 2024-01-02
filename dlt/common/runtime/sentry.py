import logging
from typing import Optional

from dlt.common.exceptions import MissingDependencyException

try:
    import sentry_sdk
    from sentry_sdk.transport import HttpTransport
    from sentry_sdk.integrations.logging import LoggingIntegration
except ModuleNotFoundError:
    raise MissingDependencyException(
        "sentry telemetry",
        ["sentry-sdk"],
        "Please install sentry-sdk if you have `sentry_dsn` set in your RuntimeConfiguration",
    )

from dlt.common.typing import DictStrAny, Any, StrAny
from dlt.common.configuration.specs import RunConfiguration
from dlt.common.runtime.exec_info import dlt_version_info, kube_pod_info, github_info


def init_sentry(config: RunConfiguration) -> None:
    version = dlt_version_info(config.pipeline_name)
    sys_ver = version["dlt_version"]
    release = sys_ver + "_" + version.get("commit_sha", "")
    _SentryHttpTransport.timeout = config.request_timeout
    # TODO: ignore certain loggers ie. dbt loggers
    # https://docs.sentry.io/platforms/python/guides/logging/
    sentry_sdk.init(
        config.sentry_dsn,
        before_send=before_send,
        traces_sample_rate=1.0,
        # disable tornado, boto3, sql alchemy etc.
        auto_enabling_integrations=False,
        integrations=[_get_sentry_log_level(config)],
        release=release,
        transport=_SentryHttpTransport,
    )
    # add version tags
    for k, v in version.items():
        sentry_sdk.set_tag(k, v)
    # add kubernetes tags
    pod_tags = kube_pod_info()
    for k, v in pod_tags.items():
        sentry_sdk.set_tag(k, v)
    # add github info
    github_tags = github_info()
    for k, v in github_tags.items():
        sentry_sdk.set_tag(k, v)
    if "github_user" in github_tags:
        sentry_sdk.set_user({"username": github_tags["github_user"]})


def disable_sentry() -> None:
    # init without parameters disables sentry
    sentry_sdk.init()


def before_send(event: DictStrAny, _unused_hint: Optional[StrAny] = None) -> Optional[DictStrAny]:
    """Called by sentry before sending event. Does nothing, patch this function in the module for custom behavior"""
    return event


class _SentryHttpTransport(HttpTransport):
    timeout: float = 0

    def _get_pool_options(self, *a: Any, **kw: Any) -> DictStrAny:
        rv = HttpTransport._get_pool_options(self, *a, **kw)
        rv["timeout"] = self.timeout
        return rv


def _get_sentry_log_level(config: RunConfiguration) -> LoggingIntegration:
    log_level = logging._nameToLevel[config.log_level]
    event_level = logging.WARNING if log_level <= logging.WARNING else log_level
    return LoggingIntegration(
        level=logging.INFO,  # Capture info and above as breadcrumbs
        event_level=event_level,  # Send errors as events
    )
