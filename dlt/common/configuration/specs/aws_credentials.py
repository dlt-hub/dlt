from typing import Optional, Dict, Any

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import CredentialsConfiguration, CredentialsWithDefault, configspec
from dlt.common.configuration.specs.exceptions import InvalidBoto3Session
from dlt import version


@configspec
class AwsCredentialsWithoutDefaults(CredentialsConfiguration):
    # credentials without boto implementation
    aws_access_key_id: str = None
    aws_secret_access_key: TSecretStrValue = None
    aws_session_token: Optional[TSecretStrValue] = None
    profile_name: Optional[str] = None
    region_name: Optional[str] = None

    def to_s3fs_credentials(self) -> Dict[str, Optional[str]]:
        """Dict of keyword arguments that can be passed to s3fs"""
        return dict(
            key=self.aws_access_key_id,
            secret=self.aws_secret_access_key,
            token=self.aws_session_token,
            profile=self.profile_name
        )

    def to_native_representation(self) -> Dict[str, Optional[str]]:
        """Return a dict that can be passed as kwargs to boto3 session"""
        return dict(self)


@configspec
class AwsCredentials(AwsCredentialsWithoutDefaults, CredentialsWithDefault):

    def on_partial(self) -> None:
        # Try get default credentials
        session = self._to_session()
        if self._from_session(session) and not self.is_partial():
            self.resolve()

    def _to_session(self) -> Any:
        try:
            import boto3
        except ImportError:
            raise MissingDependencyException(self.__class__.__name__, [f"{version.DLT_PKG_NAME}[s3]"])
        return boto3.Session(**self.to_native_representation())

    def _from_session(self, session: Any) -> Any:
        """Sets the credentials properties from boto3 `session` and return session's credentials if found"""
        import boto3
        assert isinstance(session, boto3.Session)
        self.profile_name = session.profile_name
        self.region_name = session.region_name
        default = session.get_credentials()
        if not default:
            return None
        self.aws_access_key_id = default.access_key
        self.aws_secret_access_key = default.secret_key
        self.aws_session_token = default.token
        return default

    def to_native_credentials(self) -> Optional[Any]:
        return self._to_session().get_credentials()

    def parse_native_representation(self, native_value: Any) -> None:
        """Import external boto session"""
        try:
            import boto3
            if isinstance(native_value, boto3.Session):
                if self._from_session(native_value):
                    self.__is_resolved__ = True
            else:
                raise InvalidBoto3Session(self.__class__, native_value)
        except ImportError:
            pass
