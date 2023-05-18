from typing import Optional, TYPE_CHECKING

from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import CredentialsConfiguration, CredentialsWithDefault, configspec

if TYPE_CHECKING:
    from botocore.credentials import Credentials


@configspec
class AwsCredentials(CredentialsConfiguration, CredentialsWithDefault):
    access_key_id: str = None
    access_key_secret: TSecretStrValue = None
    session_token: Optional[TSecretStrValue] = None
    profile_name: Optional[str] = None

    def on_partial(self) -> None:
        # Try get default credentials
        default = self.to_native_credentials()
        if not default:
            return None
        self.session_token = default.token
        self.access_key_id = default.access_key
        self.access_key_secret = default.secret_key
        if not self.is_partial():
            self.resolve()

    def to_native_credentials(self) -> Optional["Credentials"]:
        import boto3
        return boto3.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.access_key_secret,
            aws_session_token=self.session_token,
            profile_name=self.profile_name
        ).get_credentials()
