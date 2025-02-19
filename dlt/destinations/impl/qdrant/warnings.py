import warnings

from dlt.common.warnings import Dlt100DeprecationWarning


def location_on_credentials_deprecated(arg: str) -> None:
    warnings.warn(
        f"Usage of `{arg}` option on Qdrant credentials is deprecated. Please set the `{arg}` on"
        f' Qdrant config. for example using toml section:\n[destination.qdrant]\n{arg}="value"\n',
        Dlt100DeprecationWarning,
        stacklevel=1,
    )
