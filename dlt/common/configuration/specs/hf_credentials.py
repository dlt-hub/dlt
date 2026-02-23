from typing import Any, Dict, Optional
from dlt.common.configuration.specs.base_configuration import (
    CredentialsConfiguration,
    configspec,
)


@configspec
class HfCredentials(CredentialsConfiguration):
    """Credentials for HF filesystem, compatible with fsspec HF protocol at huggingface_hub.HfFileSystem.

    Authentication is attempted in the following order of priority:
        - the `hf_token` argument
        - the `HF_TOKEN` environment variable
        - a local token created with the `hf auth login` command

    For more information about parameters:
    https://huggingface.co/docs/huggingface_hub/en/quick-start#login-command
    """

    hf_token: Optional[str] = None
    """Hugging Face authentication token. Leave empty to attempt authentication through `HF_TOKEN` environment variable or locally saved token."""
    hf_endpoint: Optional[str] = None
    """Hugging Face API endpoint. Leave empty to use the default https://huggingface.co."""

    def to_hffs_credentials(self) -> Dict[str, Any]:
        """Return a dict that can be passed to fsspec huggingface_hub.HfFileSystem."""
        return {
            "token": self.hf_token,
            "endpoint": self.hf_endpoint,
        }

    def to_hf_api_credentials(self) -> Dict[str, Any]:
        """Return a dict that can be passed to huggingface_hub.HfApi."""
        return {
            "token": self.hf_token,
            "endpoint": self.hf_endpoint,
        }
