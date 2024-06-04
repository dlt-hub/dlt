import binascii
from os.path import isfile, join
from pathlib import Path
from typing import Any, ClassVar, Optional, IO
from dlt.common.typing import TSecretStrValue

from dlt.common.utils import encoding_for_mode, main_module_file_path, reveal_pseudo_secret
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.configuration.exceptions import ConfigFileNotFoundException


@configspec
class RunConfiguration(BaseConfiguration):
    pipeline_name: Optional[str] = None
    sentry_dsn: Optional[str] = None  # keep None to disable Sentry
    slack_incoming_hook: Optional[TSecretStrValue] = None
    dlthub_telemetry: bool = True  # enable or disable dlthub telemetry
    dlthub_telemetry_endpoint: Optional[str] = "https://telemetry.scalevector.ai"
    dlthub_telemetry_segment_write_key: Optional[str] = None
    log_format: str = "{asctime}|[{levelname:<21}]|{process}|{thread}|{name}|{filename}|{funcName}:{lineno}|{message}"
    log_level: str = "WARNING"
    request_timeout: float = 60
    """Timeout for http requests"""
    request_max_attempts: int = 5
    """Max retry attempts for http clients"""
    request_backoff_factor: float = 1
    """Multiplier applied to exponential retry delay for http requests"""
    request_max_retry_delay: float = 300
    """Maximum delay between http request retries"""
    config_files_storage_path: str = "/run/config/"
    """Platform connection"""
    dlthub_dsn: Optional[TSecretStrValue] = None

    __section__: ClassVar[str] = "runtime"

    def on_resolved(self) -> None:
        # generate pipeline name from the entry point script name
        if not self.pipeline_name:
            self.pipeline_name = get_default_pipeline_name(main_module_file_path())
        if self.slack_incoming_hook:
            # it may be obfuscated base64 value
            # TODO: that needs to be removed ASAP
            try:
                self.slack_incoming_hook = TSecretStrValue(
                    reveal_pseudo_secret(self.slack_incoming_hook, b"dlt-runtime-2022")
                )
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
