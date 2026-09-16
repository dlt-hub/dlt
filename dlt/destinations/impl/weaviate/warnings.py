import warnings

from dlt.common.warnings import Dlt100DeprecationWarning


def batch_option_without_v4_equivalent_deprecated(arg: str) -> None:
    warnings.warn(
        f"Usage of `{arg}` option on Weaviate config is deprecated and has no effect. The"
        " weaviate-client v4 batch API manages this internally. Remove it from"
        " `[destination.weaviate]`.",
        Dlt100DeprecationWarning,
        stacklevel=1,
    )
