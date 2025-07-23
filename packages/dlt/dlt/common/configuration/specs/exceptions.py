from typing import Any, Type
from dlt.common.configuration.exceptions import ConfigurationException


class SpecException(ConfigurationException):
    pass


class OAuth2ScopesRequired(SpecException):
    def __init__(self, spec: type) -> None:
        self.spec = spec
        super().__init__(
            "Scopes are required to retrieve refresh_token. Use `openid` scope for a token without"
            " any permissions to resources."
        )


class NativeValueError(SpecException, ValueError):
    def __init__(self, spec: Type[Any], native_value: str, msg: str) -> None:
        self.spec = spec
        self.native_value = native_value
        super().__init__(msg)


class InvalidConnectionString(NativeValueError):
    def __init__(self, spec: Type[Any], native_value: str, driver: str):
        driver = driver or "driver"
        msg = (
            f"The expected representation for `{spec.__name__}` is a standard database connection"
            f" string with the following format: `{driver}://username:password@host:port/database`"
        )
        super().__init__(spec, native_value, msg)


class InvalidGoogleNativeCredentialsType(NativeValueError):
    def __init__(self, spec: Type[Any], native_value: Any):
        msg = (
            f"Credentials `{spec.__name__}` accept a string with serialized credentials json file"
            " or an instance of `Credentials` object from Google.* namespace. The value passed is"
            f" of type `{type(native_value)}`"
        )
        super().__init__(spec, native_value, msg)


class InvalidGoogleServicesJson(NativeValueError):
    def __init__(self, spec: Type[Any], native_value: Any):
        msg = (
            f"The expected representation for `{spec.__name__}` is a string with serialized service"
            " account credentials, where at least `project_id`, `private_key` and `client_email`"
            " keys are present"
        )
        super().__init__(spec, native_value, msg)


class InvalidGoogleOauth2Json(NativeValueError):
    def __init__(self, spec: Type[Any], native_value: Any):
        msg = (
            f"The expected representation for `{spec.__name__}` is a string with serialized oauth2"
            " user info and may be wrapped in 'install'/'web' node - depending of oauth2 app type."
        )
        super().__init__(spec, native_value, msg)


class InvalidBoto3Session(NativeValueError):
    def __init__(self, spec: Type[Any], native_value: Any):
        msg = (
            f"The expected representation for `{spec.__name__}` is and instance of boto3.Session"
            " containing credentials"
        )
        super().__init__(spec, native_value, msg)


class ObjectStoreRsCredentialsException(ConfigurationException):
    pass


class UnsupportedAuthenticationMethodException(ConfigurationException):
    pass
