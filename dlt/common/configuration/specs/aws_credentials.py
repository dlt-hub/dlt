from typing import Optional, TYPE_CHECKING, Dict

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import CredentialsConfiguration, CredentialsWithDefault, configspec
from dlt import version
from dlt. common.configuration.specs.aws_credentials_pure import AwsCredentialsPure

if TYPE_CHECKING:
    from botocore.credentials import Credentials
    from boto3 import Session


@configspec
class AwsCredentials(AwsCredentialsPure):

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

    def _to_session(self) -> "Session":
        try:
            import boto3
        except ImportError:
            raise MissingDependencyException(self.__class__.__name__, [f"{version.DLT_PKG_NAME}[s3]"])
        return boto3.Session(**self.to_native_representation())

    def to_native_credentials(self) -> Optional["Credentials"]:
        return self._to_session().get_credentials()
