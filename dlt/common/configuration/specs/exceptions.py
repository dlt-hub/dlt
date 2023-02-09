from typing import Any, Type
from dlt.common.configuration.exceptions import ConfigurationException


class SpecException(ConfigurationException):
    pass


class NativeValueError(SpecException, ValueError):
    def __init__(self, spec: Type[Any], native_value: str, msg: str) -> None:
        self.spec = spec
        self.native_value = native_value
        super().__init__(msg)


class InvalidConnectionString(NativeValueError):
    def __init__(self, spec: Type[Any], native_value: str, driver: str):
        driver = driver or "driver"
        msg = f"The expected representation for {spec.__name__} is a standard database connection string with the following format: {driver}://username:password@host:port/database."
        super().__init__(spec, native_value, msg)


class InvalidServicesJson(NativeValueError):
    def __init__(self, spec: Type[Any], native_value: str):
        msg = f"The expected representation for {spec.__name__} is a string with serialized services.json file, where at least 'project_id', 'private_key' and 'client_email` keys are present"
        super().__init__(spec, native_value, msg)
