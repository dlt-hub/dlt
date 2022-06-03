from typing import Optional, Tuple

DEVELOPMENT_CONFIG_FILES_STORAGE_PATH = "_storage/config/%s"
PRODUCTION_CONFIG_FILES_STORAGE_PATH = "/run/config/%s"

class BasicConfiguration:
    NAME: str = None  # the name of the component, must be supplied
    SENTRY_DSN: Optional[str] = None  # keep None to disable Sentry
    PROMETHEUS_PORT: Optional[int] = None  # keep None to disable Prometheus
    LOG_FORMAT: str = '{asctime}|[{levelname:<21}]|{process}|{name}|{filename}|{funcName}:{lineno}|{message}'
    LOG_LEVEL: str = "DEBUG"
    IS_DEVELOPMENT_CONFIG: bool = True
    REQUEST_TIMEOUT: Tuple[int, int] = (15, 300)  # default request timeout for all http clients
    CONFIG_FILES_STORAGE_PATH: str = DEVELOPMENT_CONFIG_FILES_STORAGE_PATH

    @classmethod
    def check_integrity(cls) -> None:
        # if CONFIG_FILES_STORAGE_PATH not overwritten and we are in production mode
        if cls.CONFIG_FILES_STORAGE_PATH == DEVELOPMENT_CONFIG_FILES_STORAGE_PATH and not cls.IS_DEVELOPMENT_CONFIG:
            # set to mount where config files will be present
            cls.CONFIG_FILES_STORAGE_PATH = PRODUCTION_CONFIG_FILES_STORAGE_PATH
