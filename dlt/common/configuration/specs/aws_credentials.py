from typing import Optional, Dict, Any

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue, DictStrAny
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    CredentialsWithDefault,
    configspec,
)
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


@configspec
class AwsCredentials(AwsCredentialsWithoutDefaults, CredentialsWithDefault):
    def on_partial(self) -> None:
        # Try get default credentials
        session = self._to_botocore_session()
        if self._from_session(session) and not self.is_partial():
            self.resolve()

    def to_session_credentials(self) -> Dict[str, str]:
        """Return configured or new aws session token"""
        if self.aws_session_token and self.aws_access_key_id and self.aws_secret_access_key:
            return dict(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
            )
        sess = self._to_botocore_session()
        client = sess.create_client("sts")
        token = client.get_session_token()
        return dict(
            aws_access_key_id=token["Credentials"]["AccessKeyId"],
            aws_secret_access_key=token["Credentials"]["SecretAccessKey"],
            aws_session_token=token["Credentials"]["SessionToken"],
        )

    def _to_botocore_session(self) -> Any:
        try:
            import botocore.session
        except ModuleNotFoundError:
            raise MissingDependencyException(
                self.__class__.__name__, [f"{version.DLT_PKG_NAME}[s3]"]
            )

        # taken from boto3 Session
        session = botocore.session.get_session()
        if self.profile_name is not None:
            session.set_config_variable("profile", self.profile_name)

        if self.aws_access_key_id or self.aws_secret_access_key or self.aws_session_token:
            session.set_credentials(
                self.aws_access_key_id, self.aws_secret_access_key, self.aws_session_token
            )
        if self.region_name is not None:
            session.set_config_variable("region", self.region_name)
        return session

    def _from_session(self, session: Any) -> Any:
        """Sets the credentials properties from botocore or boto3 `session` and return session's credentials if found"""
        import botocore.session

        if not isinstance(session, botocore.session.Session):
            # assume this is boto3 session
            session = session._session
        # NOTE: we do not set profile name from boto3 session
        # we either pass it explicitly in `_to_session` so we know it is identical
        # this is what boto3 does: return self._session.profile or 'default' which is obviously wrong (returning default when there's no session)
        self.region_name = session.get_config_variable("region")
        default = session.get_credentials()
        if not default:
            return None
        self.aws_access_key_id = default.access_key
        self.aws_secret_access_key = TSecretStrValue(default.secret_key)
        self.aws_session_token = TSecretStrValue(default.token)
        return default

    def to_native_credentials(self) -> Optional[Any]:
        return self._to_botocore_session().get_credentials()

    def parse_native_representation(self, native_value: Any) -> None:
        """Import external boto3 session"""
        try:
            if self._from_session(native_value):
                self.__is_resolved__ = True
        except Exception:
            raise InvalidBoto3Session(self.__class__, native_value)
