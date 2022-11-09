from os.path import isfile, join
from typing import Any, Optional, Tuple, IO

from dlt.common.utils import encoding_for_mode, entry_point_file_stem
from dlt.common.configuration.specs.base_configuration import BaseConfiguration, configspec
from dlt.common.configuration.exceptions import ConfigFileNotFoundException


@configspec
class RunConfiguration(BaseConfiguration):
    pipeline_name: Optional[str] = None
    sentry_dsn: Optional[str] = None  # keep None to disable Sentry
    prometheus_port: Optional[int] = None  # keep None to disable Prometheus
    log_format: str = '{asctime}|[{levelname:<21}]|{process}|{name}|{filename}|{funcName}:{lineno}|{message}'
    log_level: str = "INFO"
    request_timeout: Tuple[int, int] = (15, 300)  # default request timeout for all http clients
    config_files_storage_path: str = "/run/config/"

    def on_resolved(self) -> None:
        # generate pipeline name from the entry point script name
        if not self.pipeline_name:
            self.pipeline_name = "dlt_" + (entry_point_file_stem() or "pipeline")

    def has_configuration_file(self, name: str) -> bool:
        return isfile(self.get_configuration_file_path(name))

    def open_configuration_file(self, name: str, mode: str) -> IO[Any]:
        path = self.get_configuration_file_path(name)
        if not self.has_configuration_file(name):
            raise ConfigFileNotFoundException(path)
        return open(path, mode, encoding=encoding_for_mode(mode))

    def get_configuration_file_path(self, name: str) -> str:
        return join(self.config_files_storage_path, name)
