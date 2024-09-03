import typing as t
import warnings

from dlt.common.warnings import Dlt04DeprecationWarning


def full_refresh_argument_deprecated(caller_name: str, full_refresh: t.Optional[bool]) -> None:
    """full_refresh argument is replaced with dev_mode"""
    if full_refresh is None:
        return

    warnings.warn(
        f"The `full_refresh` argument to {caller_name} is deprecated and will be removed in a"
        f" future version. Use `dev_mode={full_refresh}` instead which will have the same effect.",
        Dlt04DeprecationWarning,
        stacklevel=2,
    )
