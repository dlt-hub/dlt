from dlt.cli.reference import SupportsCliCommand
from dlt.cli.exceptions import CliCommandException

DEFAULT_VERIFIED_SOURCES_REPO = "https://github.com/dlt-hub/verified-sources.git"
DEFAULT_VIBE_SOURCES_REPO = "https://github.com/dlt-hub/vibe-hub.git"


__all__ = [
    "SupportsCliCommand",
    "CliCommandException",
    "DEFAULT_VERIFIED_SOURCES_REPO",
    "DEFAULT_VIBE_SOURCES_REPO",
]
