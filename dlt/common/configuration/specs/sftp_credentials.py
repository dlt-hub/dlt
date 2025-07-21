from typing import Any, Dict, Optional, Annotated

from dlt.common.typing import TSecretStrValue, DictStrAny, SocketLike
from dlt.common.configuration.specs.base_configuration import (
    CredentialsConfiguration,
    configspec,
    NotResolved,
)


@configspec
class SFTPCredentials(CredentialsConfiguration):
    """Credentials for SFTP filesystem, compatible with fsspec SFTP protocol.

    Authentication is attempted in the following order of priority:

        - `key_filename` may contain OpenSSH public certificate paths
          as well as regular private-key paths; when files ending in `-cert.pub` are found, they are assumed to match
          a private key, and both components will be loaded.

        - Any key found through an SSH agent: any “id_rsa”, “id_dsa”, or “id_ecdsa” key discoverable in ~/.ssh/.

        - Plain username/password authentication, if a password was provided.

        - If a private key requires a password to unlock it, and a password is provided, that password will be used to
          attempt to unlock the key.

    For more information about parameters:
    https://docs.paramiko.org/en/3.3/api/client.html#paramiko.client.SSHClient.connect
    """

    sftp_port: Optional[int] = 22
    sftp_username: Optional[str] = None
    sftp_password: Optional[TSecretStrValue] = None
    sftp_key_filename: Optional[str] = None
    sftp_key_passphrase: Optional[TSecretStrValue] = None
    sftp_timeout: Optional[float] = None
    sftp_banner_timeout: Optional[float] = None
    sftp_auth_timeout: Optional[float] = None
    sftp_channel_timeout: Optional[float] = None
    sftp_allow_agent: Optional[bool] = True
    sftp_look_for_keys: Optional[bool] = True
    sftp_compress: Optional[bool] = False
    # Runtime-only socket; cannot be loaded from env var, skip configspec.
    sftp_sock: Annotated[Optional[SocketLike], NotResolved()] = None
    sftp_gss_auth: Optional[bool] = False
    sftp_gss_kex: Optional[bool] = False
    sftp_gss_deleg_creds: Optional[bool] = True
    sftp_gss_host: Optional[str] = None
    sftp_gss_trust_dns: Optional[bool] = True

    def to_fsspec_credentials(self) -> Dict[str, Any]:
        """Return a dict that can be passed to fsspec SFTP/SSHClient.connect method."""

        credentials: Dict[str, Any] = {
            "port": self.sftp_port,
            "username": self.sftp_username,
            "password": self.sftp_password,
            "key_filename": self.sftp_key_filename,
            "passphrase": self.sftp_key_passphrase,
            "timeout": self.sftp_timeout,
            "banner_timeout": self.sftp_banner_timeout,
            "auth_timeout": self.sftp_auth_timeout,
            "channel_timeout": self.sftp_channel_timeout,
            "allow_agent": self.sftp_allow_agent,
            "look_for_keys": self.sftp_look_for_keys,
            "compress": self.sftp_compress,
            "sock": self.sftp_sock,
            "gss_auth": self.sftp_gss_auth,
            "gss_kex": self.sftp_gss_kex,
            "gss_deleg_creds": self.sftp_gss_deleg_creds,
            "gss_host": self.sftp_gss_host,
            "gss_trust_dns": self.sftp_gss_trust_dns,
        }

        return credentials
