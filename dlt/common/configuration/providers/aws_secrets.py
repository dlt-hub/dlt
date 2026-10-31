import threading
from typing import Any, Dict, Optional, Sequence, Set, Tuple

from dlt.common import logger
from dlt.common.configuration.specs import AwsCredentials
from dlt.common.configuration.exceptions import ConfigProviderException
from dlt.common.exceptions import MissingDependencyException
from dlt import version

from .vault import VaultDocProvider, normalize_key
from .provider import get_key_name

SECRET_NAME_SEPARATOR = "/"


class AwsSecretsManagerProvider(VaultDocProvider):
    def __init__(
        self,
        credentials: AwsCredentials,
        only_secrets: bool = True,
        only_toml_fragments: bool = True,
        list_secrets: bool = False,
        secret_name_prefix: str = "dlt/",
    ) -> None:
        """Initialize an AWS Secrets Manager Provider to access secrets stored in AWS Secrets Manager

        Args:
            credentials: AWS credentials to access Secrets Manager
            only_secrets: When True, only keys with secret hint types will be looked up
            only_toml_fragments: When True, only load known TOML fragments and ignore other lookups
            list_secrets: When True, list all secrets upfront to optimize vault access by
                          avoiding lookups for non-existent secrets. Requires additional
                          API calls and the secretsmanager:ListSecrets permission.
            secret_name_prefix: Prepended verbatim to all secret names and used to filter
                                secret listing so dlt secrets are namespaced in a vault shared
                                with other services. Defaults to `dlt/`, set to an empty string
                                to look up unprefixed secret names.
        """
        self.credentials = credentials
        self.secret_name_prefix = secret_name_prefix
        self._client: Any = None
        self._client_lock = threading.Lock()
        super().__init__(only_secrets, only_toml_fragments, list_secrets)

    @staticmethod
    def get_key_name(key: str, *sections: str) -> str:
        """Makes key name for the secret by joining normalized components with `/`

        AWS secret names may contain letters, numerals and `/_+=.@-` characters. Punctuation
        other than `-` and `_` is removed from name components so the separator cannot appear
        inside them.
        """
        normalized_sections = [normalize_key(section) for section in sections if section]
        return get_key_name(normalize_key(key), SECRET_NAME_SEPARATOR, *normalized_sections)

    @property
    def name(self) -> str:
        return "AWS Secrets Manager"

    @property
    def locations(self) -> Sequence[str]:
        if self.credentials:
            location = (
                self.credentials.profile_name or self.credentials.aws_access_key_id or "default"
            )
            if self.credentials.region_name:
                location = f"{location}@{self.credentials.region_name}"
            if self.secret_name_prefix:
                location = f"{location}:{self.secret_name_prefix}"
            return [location]
        else:
            return super().locations

    def _get_client(self) -> Any:
        """Creates a botocore secretsmanager client on first use, client creation is not thread-safe"""
        with self._client_lock:
            if self._client is None:
                try:
                    from botocore.exceptions import NoRegionError
                except ModuleNotFoundError:
                    raise MissingDependencyException(
                        "AwsSecretsManagerProvider",
                        [f"{version.DLT_PKG_NAME}[s3]"],
                        "We need botocore to create the client for AWS Secrets Manager",
                    )

                session = self.credentials._to_botocore_session()
                try:
                    self._client = session.create_client("secretsmanager")
                except NoRegionError:
                    raise ConfigProviderException(
                        self.name,
                        "AWS region could not be determined. Set `region_name` in"
                        " `providers.aws_secrets.credentials`, the `AWS_DEFAULT_REGION`"
                        " environment variable or in your AWS profile.",
                    )
            return self._client

    @staticmethod
    def _client_error_info(error: Any) -> Tuple[str, str]:
        error_doc = error.response.get("Error", {})
        return error_doc.get("Code", "Unknown"), error_doc.get("Message", "Unknown error")

    def _look_vault(self, full_key: str, hint: type) -> Optional[str]:
        client = self._get_client()

        from botocore.exceptions import ClientError

        secret_id = self.secret_name_prefix + full_key
        try:
            response = client.get_secret_value(SecretId=secret_id)
        except ClientError as error:
            error_code, error_message = self._client_error_info(error)
            if error_code == "ResourceNotFoundException":
                return None
            elif error_code in ("AccessDeniedException", "UnrecognizedClientException"):
                logger.warning(
                    "dlt does not have secretsmanager:GetSecretValue permission for"
                    f" {secret_id} or the secret does not exist in AWS Secrets Manager:"
                    f" {error_message}[{error_code}]"
                )
                return None
            elif error_code == "InvalidRequestException":
                logger.warning(
                    f"Unable to read {secret_id}, the secret may be scheduled for deletion:"
                    f" {error_message}[{error_code}]"
                )
                return None
            elif error_code in ("ValidationException", "InvalidParameterException"):
                logger.warning(f"Unable to read {secret_id}: {error_message}[{error_code}]")
                return None
            elif error_code == "DecryptionFailure":
                logger.warning(
                    f"Cannot decrypt {secret_id}, kms:Decrypt permission for the KMS key that"
                    f" encrypts the secret is required: {error_message}[{error_code}]"
                )
                return None
            raise
        secret_string = response.get("SecretString")
        if secret_string is not None:
            return secret_string  # type: ignore[no-any-return]
        secret_binary = response.get("SecretBinary")
        if secret_binary is not None:
            try:
                return secret_binary.decode("utf-8")  # type: ignore[no-any-return]
            except UnicodeDecodeError:
                logger.warning(f"Secret {secret_id} contains binary data that is not valid utf-8")
        return None

    def _list_vault(self) -> Set[str]:
        """Lists secret names under `secret_name_prefix`, prefix is stripped from returned keys"""
        client = self._get_client()

        from botocore.exceptions import ClientError

        available_keys: Set[str] = set()
        paginate_args: Dict[str, Any] = {}
        if self.secret_name_prefix:
            # the name filter is a case-sensitive match on the beginning of the full secret name
            paginate_args["Filters"] = [{"Key": "name", "Values": [self.secret_name_prefix]}]
        try:
            for page in client.get_paginator("list_secrets").paginate(**paginate_args):
                for secret in page.get("SecretList", []):
                    name = secret.get("Name", "")
                    # defensive check, the server-side filter above already matches the prefix
                    if not name.startswith(self.secret_name_prefix):
                        continue
                    key = name[len(self.secret_name_prefix) :]
                    if key:
                        available_keys.add(key)
        except ClientError as error:
            error_code, error_message = self._client_error_info(error)
            if error_code == "AccessDeniedException":
                raise ConfigProviderException(
                    self.name,
                    "Cannot list secrets: dlt does not have the secretsmanager:ListSecrets"
                    " permission. Secret listing is required when list_secrets=True to optimize"
                    " vault access by skipping lookups for non-existent secrets. Error:"
                    f" {error_message} [{error_code}]",
                )
            else:
                raise ConfigProviderException(
                    self.name,
                    "Failed to list secrets in AWS Secrets Manager. Secret listing is required"
                    " when list_secrets=True to optimize vault access by skipping lookups for"
                    f" non-existent secrets. Error: {error_message} [{error_code}]",
                )
        logger.info(
            f"Listed {len(available_keys)} secrets from AWS Secrets Manager"
            + (f" under prefix {self.secret_name_prefix}" if self.secret_name_prefix else "")
        )
        return available_keys
