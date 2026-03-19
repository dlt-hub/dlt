import warnings

from dlt.common.warnings import Dlt100DeprecationWarning


def uri_on_credentials_deprecated() -> None:
    warnings.warn(
        "Usage of `uri` argument on lance credentials is deprecated. Please set the `lance_uri`"
        " on lance config. for example using toml"
        ' section:\n[destination.lance]\nuri="path/db.lancedb"\n',
        Dlt100DeprecationWarning,
        stacklevel=1,
    )
