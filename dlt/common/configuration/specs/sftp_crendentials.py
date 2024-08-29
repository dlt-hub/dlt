from typing import Any, Dict, Optional

from dlt.common.typing import TSecretValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec


@configspec
class SFTPCredentials(CredentialsConfiguration):
    # TODO: separate config and secrets
    sftp_port: TSecretValue = None
    sftp_username: TSecretValue = None
    sftp_password: Optional[TSecretValue] = None
    sftp_key_filename: Optional[TSecretValue] = None  # path to the private key file
    sftp_key_passphrase: Optional[TSecretValue] = None  # passphrase for the private key

    def to_fsspec_credentials(self) -> Dict[str, Any]:
        """Return a dict that can be passed to fsspec/sftp"""

        # fsspec/sftp (ssh_args) args:
        # - hostname, port=22, username=None, password=None, pkey=None, key_filename=None, timeout=None, allow_agent=True, look_for_keys=True, compress=False, sock=None, gss_auth=False, gss_kex=False, gss_deleg_creds=True, gss_host=None, banner_timeout=None, auth_timeout=None, channel_timeout=None, gss_trust_dns=True, passphrase=None, disabled_algorithms=None, transport_factory=None, auth_strategy=None
        # link: https://docs.paramiko.org/en/3.3/api/client.html#paramiko.client.SSHClient.connect

        return dict(
            port=self.sftp_port,
            username=self.sftp_username,
            password=self.sftp_password,
            key_filename=self.sftp_key_filename,
            passphrase=self.sftp_key_passphrase,
        )
