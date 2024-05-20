from typing import Optional, Dict

from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    configspec,
)
from dlt.common.typing import TSecretStrValue, DictStrAny


@configspec
class HMACCredentials(CredentialsConfiguration):
    aws_access_key_id: str = None
    aws_secret_access_key: TSecretStrValue = None
    aws_session_token: Optional[TSecretStrValue] = None
    profile_name: Optional[str] = None
    region_name: Optional[str] = None
    endpoint_url: Optional[str] = None

    def to_s3fs_credentials(self) -> Dict[str, Optional[str]]:
        """Dict of keyword arguments that can be passed to s3fs"""
        credentials: DictStrAny = dict(
            key=self.aws_access_key_id,
            secret=self.aws_secret_access_key,
            token=self.aws_session_token,
            profile=self.profile_name,
            endpoint_url=self.endpoint_url,
        )
        if self.region_name:
            credentials["client_kwargs"] = {"region_name": self.region_name}
        return credentials

    def to_native_representation(self) -> Dict[str, Optional[str]]:
        """Return a dict that can be passed as kwargs to boto3 session"""
        return dict(self)

    def to_session_credentials(self) -> Dict[str, str]:
        return dict(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
        )
