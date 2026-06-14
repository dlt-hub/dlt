from typing import Optional, Dict, Any, cast

from dlt.common.utils import without_none
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TSecretStrValue, DictStrAny, Self
from dlt.common.configuration.specs import (
    CredentialsConfiguration,
    CredentialsWithDefault,
    configspec,
)
from dlt.common.configuration.specs.mixins import WithObjectStoreRsCredentials, WithPyicebergConfig
from dlt.common.configuration.specs.exceptions import (
    InvalidBoto3Session,
    ObjectStoreRsCredentialsException,
)
from dlt import version


@configspec
class AwsCredentialsWithoutDefaults(
    CredentialsConfiguration, WithObjectStoreRsCredentials, WithPyicebergConfig
):
    # credentials without boto implementation
    aws_access_key_id: str = None
    aws_secret_access_key: TSecretStrValue = None
    aws_session_token: Optional[TSecretStrValue] = None
    profile_name: Optional[str] = None
    region_name: Optional[str] = None
    endpoint_url: Optional[str] = None
    s3_url_style: Optional[str] = None
    """Only needed for duckdb sql_client s3 access, for minio this needs to be set to path for example."""
    aws_sts_token_expiration_hours: float = 24.0
    """Lifetime in hours of session tokens minted via STS `get_session_token`. Minted tokens do not auto-refresh."""

    def to_s3fs_credentials(self) -> Dict[str, Optional[str]]:
        """Dict of keyword arguments that can be passed to s3fs"""
        sess_creds = self.to_session_credentials()
        credentials: DictStrAny = dict(
            key=sess_creds["aws_access_key_id"],
            secret=sess_creds["aws_secret_access_key"],
            token=sess_creds["aws_session_token"],
            profile=self.profile_name,
            endpoint_url=self.endpoint_url,
        )
        if self.region_name:
            credentials["client_kwargs"] = {"region_name": self.region_name}
        if self.endpoint_url:
            # NOTE: we need to make checksum validation optional for boto to work with s3 compat mode
            # https://www.beginswithdata.com/2025/05/14/aws-s3-tools-with-gcs/
            credentials["config_kwargs"] = {
                "response_checksum_validation": "when_required",
                "request_checksum_calculation": "when_required",
            }
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

    def to_object_store_rs_credentials(self) -> Dict[str, str]:
        return self._to_object_store_rs_credentials(self.to_session_credentials())

    def _to_object_store_rs_credentials(self, sess_creds: Dict[str, str]) -> Dict[str, str]:
        """Create object store credentials from `sess_creds`"""
        # https://docs.rs/object_store/latest/object_store/aws
        # NOTE: delta rs will set the values below in env variables of the current process
        # https://github.com/delta-io/delta-rs/blob/bdf1c4e765ca457e49d4fa53335d42736220f57f/rust/src/storage/s3.rs#L257
        creds = cast(
            Dict[str, str],
            without_none(
                dict(
                    aws_access_key_id=sess_creds.get("aws_access_key_id"),
                    aws_secret_access_key=sess_creds.get("aws_secret_access_key"),
                    aws_session_token=sess_creds.get("aws_session_token"),
                    region=self.region_name,
                    endpoint_url=self.endpoint_url,
                )
            ),
        )
        if not self.endpoint_url:  # AWS S3
            if not self.region_name:
                raise ObjectStoreRsCredentialsException(
                    "`object_store` Rust crate requires AWS region when using AWS S3."
                )
        else:  # S3-compatible, e.g. MinIO
            if self.endpoint_url.startswith("http://"):
                creds["aws_allow_http"] = "true"
        return creds

    def to_pyiceberg_fileio_config(self) -> Dict[str, Any]:
        sess_creds = self.to_session_credentials()
        return {
            "s3.access-key-id": sess_creds["aws_access_key_id"],
            "s3.secret-access-key": sess_creds["aws_secret_access_key"],
            "s3.session-token": sess_creds["aws_session_token"],
            "s3.region": self.region_name,
            "s3.endpoint": self.endpoint_url,
            "s3.connect-timeout": 300,
        }

    @classmethod
    def from_pyiceberg_fileio_config(cls, file_io: Dict[str, Any]) -> Self:
        credentials: Self = cls()
        credentials.aws_access_key_id = file_io.get("s3.access-key-id")
        credentials.aws_secret_access_key = file_io.get("s3.secret-access-key")
        credentials.aws_session_token = file_io.get("s3.session-token")
        credentials.region_name = file_io.get("s3.region")
        credentials.endpoint_url = file_io.get("s3.endpoint")
        return credentials


@configspec
class AwsCredentials(AwsCredentialsWithoutDefaults, CredentialsWithDefault):
    def on_partial(self) -> None:
        # try to resolve credentials from botocore's default provider chain
        session = self._to_botocore_session()
        session = self._from_session(session)
        if session.get_credentials():
            self._set_default_credentials(session)
            self.resolve()

    def to_session_credentials(self) -> Dict[str, str]:
        if self.has_default_credentials():
            frozen = self.default_credentials().get_credentials().get_frozen_credentials()
            return dict(
                aws_access_key_id=frozen.access_key,
                aws_secret_access_key=frozen.secret_key,
                aws_session_token=frozen.token,
            )
        return super().to_session_credentials()

    def to_s3fs_credentials(self) -> Dict[str, Optional[str]]:
        # on default credentials omit static key/secret/token so s3fs resolves and refreshes
        # via its own aiobotocore default chain
        return self._strip_on_default(super().to_s3fs_credentials(), "key", "secret", "token")

    def to_object_store_rs_credentials(self) -> Dict[str, str]:
        # only plain refreshable creds (EC2 IMDS / ECS) can be resolved and refreshed by
        # object_store itself, so we hand over without freezing them. deferred creds
        # (IRSA/SSO/assume-role) and static creds cannot, so they are passed frozen.
        # external sessions are always frozen (see `is_external_session`)
        if (
            self.has_default_credentials()
            and not self.is_external_session()
            and self._default_credentials_self_refresh()
        ):
            return self._to_object_store_rs_credentials({})
        return super().to_object_store_rs_credentials()

    def _default_credentials_self_refresh(self) -> bool:
        try:
            from botocore.credentials import (
                RefreshableCredentials,
                DeferredRefreshableCredentials,
            )
        except ImportError:
            return False
        current = self.default_credentials().get_credentials()
        return isinstance(current, RefreshableCredentials) and not isinstance(
            current, DeferredRefreshableCredentials
        )

    def to_pyiceberg_fileio_config(self) -> Dict[str, Any]:
        # on default credentials omit static s3 keys so pyarrow's S3FileSystem resolves and
        # refreshes via the AWS default chain. region/endpoint/timeout are preserved
        return self._strip_on_default(
            super().to_pyiceberg_fileio_config(),
            "s3.access-key-id",
            "s3.secret-access-key",
            "s3.session-token",
        )

    def is_external_session(self) -> bool:
        """Tells if default credentials come from a boto3/botocore session passed by the user"""
        return getattr(self, "_external_session", False)

    def _strip_on_default(self, config: Dict[str, Any], *secret_keys: str) -> Dict[str, Any]:
        """Removes `secret_keys` from `config` for refreshable default credentials so the
        consumer resolves and refreshes them via its own default chain. Static config and
        external sessions (see `is_external_session`) are kept frozen.
        """
        if self.has_default_credentials() and not self.is_external_session():
            for k in secret_keys:
                config.pop(k, None)
        return config

    def to_sts_credentials(self) -> Dict[str, str]:
        """Return session credentials with a session token, generating one via STS if needed."""
        sess_creds = self.to_session_credentials()
        if sess_creds["aws_session_token"]:
            return sess_creds
        sess = self._to_botocore_session()
        client = sess.create_client("sts")
        token = client.get_session_token(
            DurationSeconds=int(self.aws_sts_token_expiration_hours * 3600)
        )
        return dict(
            aws_access_key_id=token["Credentials"]["AccessKeyId"],
            aws_secret_access_key=token["Credentials"]["SecretAccessKey"],
            aws_session_token=token["Credentials"]["SessionToken"],
        )

    def _to_botocore_session(self) -> Any:
        """Creates a botocore session out of the credentials. When default credentials are
        available, returns the stored session to preserve botocore's credential provider chain.
        """
        if self.has_default_credentials():
            return self.default_credentials()

        try:
            import botocore.session
            from botocore.config import Config
        except ModuleNotFoundError:
            raise MissingDependencyException(
                self.__class__.__name__, [f"{version.DLT_PKG_NAME}[s3]"]
            )

        session = botocore.session.get_session()
        if self.profile_name is not None:
            session.set_config_variable("profile", self.profile_name)
        # set credentials if explicitly present
        if self.aws_access_key_id and self.aws_secret_access_key:
            session.set_credentials(
                self.aws_access_key_id, self.aws_secret_access_key, self.aws_session_token
            )
        if self.region_name is not None:
            session.set_config_variable("region", self.region_name)

        if self.endpoint_url:
            cfg = Config(
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            )
            session.set_default_client_config(cfg)
        return session

    def _from_session(self, session: Any) -> Any:
        """Gets boto3 session from botocore and tries to set region_name"""
        import botocore.session

        if not isinstance(session, botocore.session.Session):
            # assume this is boto3 session
            session = session._session
        # NOTE: we do not set profile name from boto3 session
        # we either pass it explicitly in `_to_session` so we know it is identical
        # this is what boto3 does: return self._session.profile or 'default' which is obviously wrong (returning default when there's no session)
        self.region_name = session.get_config_variable("region")
        return session

    def to_native_credentials(self) -> Optional[Any]:
        return self._to_botocore_session().get_credentials()

    def parse_native_representation(self, native_value: Any) -> None:
        """Import external boto3 or botocore session"""
        try:
            session = self._from_session(native_value)
            if session.get_credentials():
                self._set_default_credentials(session)
                # mark as external so we never strip its keys for refresh-capable consumers
                self._external_session = True
                self.__is_resolved__ = True
        except Exception:
            raise InvalidBoto3Session(self.__class__, native_value)

    @classmethod
    def from_session(cls, botocore_session: Any) -> "AwsCredentials":
        self = cls()
        self.parse_native_representation(botocore_session)
        return self
