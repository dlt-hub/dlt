"""A collection of dltHub Features"""
from typing import Any


__found__ = False
__exception__ = None


try:
    from dlthub import transformation, runner
    from . import current
    from . import data_quality

    __found__ = True
    __all__ = ("transformation", "current", "runner", "data_quality")
except ImportError as import_exc:
    __exception__ = import_exc


def __getattr__(name: str) -> Any:
    """Provide useful info on missing attributes"""

    # hub was found this is just regular missing attribute
    if __found__:
        raise AttributeError(f"module 'dlt.hub' has no attribute '{name}'")

    from dlt.common.exceptions import MissingDependencyException

    if isinstance(__exception__, MissingDependencyException):
        # plugins will MissingDependencyException if they are not installed with a right version
        # in that case just re-raise original message
        raise __exception__

    raise MissingDependencyException(
        "dlt.hub",
        ["dlt[hub]"],
        "This will install `dlthub` plugin package in with a matching "
        f"version.\nfrom:\n({str(__exception__)})",
    )
