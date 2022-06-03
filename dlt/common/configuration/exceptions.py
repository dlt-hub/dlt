from typing import Iterable, Union

from dlt.common.exceptions import DltException


class ConfigurationException(DltException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class ConfigEntryMissingException(ConfigurationException):
    """thrown when not all required config elements are present"""

    def __init__(self, missing_set: Iterable[str]) -> None:
        self.missing_set = missing_set
        super().__init__('Missing config keys: ' + str(missing_set))


class ConfigEnvValueCannotBeCoercedException(ConfigurationException):
    """thrown when value from ENV cannot be coerced to hinted type"""

    def __init__(self, attr_name: str, env_value: str, hint: type) -> None:
        self.attr_name = attr_name
        self.env_value = env_value
        self.hint = hint
        super().__init__('env value %s cannot be coerced into type %s in attr %s' % (env_value, str(hint), attr_name))


class ConfigIntegrityException(ConfigurationException):
    """thrown when value from ENV cannot be coerced to hinted type"""

    def __init__(self, attr_name: str, env_value: str, info: Union[type, str]) -> None:
        self.attr_name = attr_name
        self.env_value = env_value
        self.info = info
        super().__init__('integrity error for attr %s with value %s. %s.' % (attr_name, env_value, info))


class ConfigFileNotFoundException(ConfigurationException):
    """thrown when configuration file cannot be found in config folder"""

    def __init__(self, path: str) -> None:
        super().__init__(f"Missing config file in {path}")
