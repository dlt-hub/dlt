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


def vectorizer_renamed_deprecated(old_name: str, new_name: str) -> None:
    warnings.warn(
        f"Weaviate vectorizer `{old_name}` was renamed to `{new_name}`. Update"
        f' `[destination.weaviate]\nvectorizer="{new_name}"` and the matching `module_config`'
        " key.",
        Dlt100DeprecationWarning,
        stacklevel=1,
    )
