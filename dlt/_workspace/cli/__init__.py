from dlt.common.configuration.plugins import SupportsCliCommand

from dlt._workspace.cli.exceptions import CliCommandException

DEFAULT_VERIFIED_SOURCES_REPO = "https://github.com/dlt-hub/verified-sources.git"


__all__ = [
    "SupportsCliCommand",
    "CliCommandException",
    "DEFAULT_VERIFIED_SOURCES_REPO",
]
