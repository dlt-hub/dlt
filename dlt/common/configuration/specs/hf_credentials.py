from typing import Any, Dict, Optional
from dlt.common.configuration.specs.base_configuration import (
    CredentialsConfiguration,
    configspec,
)


@configspec
class HfCredentials(CredentialsConfiguration):
    """Credentials for HF filesystem, compatible with fsspec HF protocol at huggingface_hub.HfFileSystem.

    Authentication is attempted in the following order of priority:

        - the `token` argument

        - the `HF_TOKEN` environment variable

        - a local token created with the `hf auth login` command

    For more information about parameters:
    https://huggingface.co/docs/huggingface_hub/en/quick-start#login-command
    """

    token: Optional[str] = None
    endpoint: Optional[str] = None

    def to_hffs_credentials(self) -> Dict[str, Any]:
        """Return a dict that can be passed to fsspec huggingface_hub.HfFileSystem."""

        credentials: Dict[str, Any] = {
            "token": self.token,
            "endpoint": self.endpoint,
        }

        return credentials
