import warnings

from dlt.common.warnings import Dlt100DeprecationWarning


def uri_on_credentials_deprecated() -> None:
    warnings.warn(
        "Usage of `uri` argument on lance db credentials is deprecated. Please set the `lance_uri`"
        " on lance db config. for example using toml"
        ' section:\n[destination.lancedb]\nuri="path/db.lancedb"\n',
        Dlt100DeprecationWarning,
        stacklevel=1,
    )
