import typing as t
import warnings

from dlt.common.warnings import Dlt04DeprecationWarning
from dlt.common.destination import Destination, TDestinationReferenceArg


def credentials_argument_deprecated(
    caller_name: str, credentials: t.Optional[t.Any], destination: TDestinationReferenceArg = None
) -> None:
    if credentials is None:
        return

    dest_name = Destination.to_name(destination) if destination else "postgres"

    warnings.warn(
        f"The `credentials argument` to {caller_name} is deprecated and will be removed in a future"
        " version. Pass the same credentials to the `destination` instance instead, e.g."
        f" {caller_name}(destination=dlt.destinations.{dest_name}(credentials=...))",
        Dlt04DeprecationWarning,
        stacklevel=2,
    )
