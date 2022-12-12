import binascii
from os.path import isfile, join
from pathlib import Path
from typing import Any, Optional, Tuple, IO

from dlt.common.utils import encoding_for_mode, main_module_file_path, reveal_pseudo_secret
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.configuration.exceptions import ConfigFileNotFoundException


@configspec
class RunConfiguration(BaseConfiguration):
    pipeline_name: Optional[str] = None
    sentry_dsn: Optional[str] = None  # keep None to disable Sentry
    slack_incoming_hook: Optional[str] = None
    prometheus_port: Optional[int] = None  # keep None to disable Prometheus
    log_format: str = '{asctime}|[{levelname:<21}]|{process}|{name}|{filename}|{funcName}:{lineno}|{message}'
    log_level: str = "INFO"
    request_timeout: Tuple[int, int] = (15, 300)  # default request timeout for all http clients
    config_files_storage_path: str = "/run/config/"

    def on_resolved(self) -> None:
        # generate pipeline name from the entry point script name
        if not self.pipeline_name:
            self.pipeline_name = get_default_pipeline_name(main_module_file_path())
        if self.slack_incoming_hook:
            # it may be obfuscated base64 value
            # TODO: that needs to be removed ASAP
            try:
                self.slack_incoming_hook = reveal_pseudo_secret(self.slack_incoming_hook, b"dlt-runtime-2022")
            except binascii.Error:
                # just keep the original value
                pass

    def has_configuration_file(self, name: str) -> bool:
        return isfile(self.get_configuration_file_path(name))

    def open_configuration_file(self, name: str, mode: str) -> IO[Any]:
        path = self.get_configuration_file_path(name)
        if not self.has_configuration_file(name):
            raise ConfigFileNotFoundException(path)
        return open(path, mode, encoding=encoding_for_mode(mode))

    def get_configuration_file_path(self, name: str) -> str:
        return join(self.config_files_storage_path, name)


def get_default_pipeline_name(entry_point_file: str) -> str:
    if entry_point_file:
        entry_point_file = Path(entry_point_file).stem
    return "dlt_" + (entry_point_file or "pipeline")
