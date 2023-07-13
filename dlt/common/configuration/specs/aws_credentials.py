from typing import Optional, TYPE_CHECKING, Dict, Any

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import CredentialsConfiguration, CredentialsWithDefault, configspec
from dlt import version


@configspec
class AwsCredentialsWithoutDefaults(CredentialsConfiguration):
    # credentials without boto implementation
    aws_access_key_id: str = None
    aws_secret_access_key: TSecretStrValue = None
    aws_session_token: Optional[TSecretStrValue] = None
    aws_profile: Optional[str] = None

    def to_s3fs_credentials(self) -> Dict[str, Optional[str]]:
        """Dict of keyword arguments that can be passed to s3fs"""
        return dict(
            key=self.aws_access_key_id,
            secret=self.aws_secret_access_key,
            token=self.aws_session_token,
            profile=self.aws_profile
        )

    def to_native_representation(self) -> Dict[str, Optional[str]]:
        """Return a dict that can be passed as kwargs to boto3 session"""
        d = dict(self)
        d['profile_name'] = d.pop('aws_profile')  # boto3 argument doesn't match env var name
        return d


@configspec
class AwsCredentials(AwsCredentialsWithoutDefaults, CredentialsWithDefault):

    def on_partial(self) -> None:
        # Try get default credentials
        session = self._to_session()
        self.aws_profile = session.profile_name
        default = session.get_credentials()
        if not default:
            return None
        self.aws_access_key_id = default.access_key
        self.aws_secret_access_key = default.secret_key
        self.aws_session_token = default.token
        if not self.is_partial():
            self.resolve()

    def _to_session(self) -> Any:
        try:
            import boto3
        except ImportError:
            raise MissingDependencyException(self.__class__.__name__, [f"{version.DLT_PKG_NAME}[s3]"])
        return boto3.Session(**self.to_native_representation())

    def to_native_credentials(self) -> Optional[Any]:
        return self._to_session().get_credentials()
