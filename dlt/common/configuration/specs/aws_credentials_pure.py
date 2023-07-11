from typing import Optional, TYPE_CHECKING, Dict

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs import CredentialsConfiguration, CredentialsWithDefault, configspec
from dlt import version


@configspec
class AwsCredentialsPure(CredentialsConfiguration, CredentialsWithDefault):
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
