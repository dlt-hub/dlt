from typing import Any, Callable, Dict, Optional

from dlt import version
from dlt.common.exceptions import MissingDependencyException

try:
    from pyiceberg.transforms import (
        Transform,
        IdentityTransform,
        YearTransform,
        MonthTransform,
        DayTransform,
        HourTransform,
        BucketTransform,
        TruncateTransform,
        S,
    )
except ImportError:
    raise MissingDependencyException(
        "dlt iceberg partition spec",
        [f"{version.DLT_PKG_NAME}[pyiceberg]"],
        "Install `pyiceberg` for dlt iceberg partition spec utilities to work",
    )

from dlt.destinations.impl.filesystem.iceberg_adapter import PartitionSpec


_TRANSFORM_LOOKUP: Dict[str, Callable[[Optional[int]], Transform[S, Any]]] = {
    "identity": lambda _: IdentityTransform(),
    "year": lambda _: YearTransform(),
    "month": lambda _: MonthTransform(),
    "day": lambda _: DayTransform(),
    "hour": lambda _: HourTransform(),
    "bucket": lambda n: BucketTransform(n),
    "truncate": lambda w: TruncateTransform(w),
}


def get_partition_transform(spec: PartitionSpec) -> Transform[S, Any]:
    """Get the PyIceberg Transform object for a partition spec.

    Args:
        spec: The PartitionSpec to get the transform for

    Returns:
        A PyIceberg Transform object

    Raises:
        ValueError: If the transform is not recognized
    """
    try:
        factory = _TRANSFORM_LOOKUP[spec.transform]
    except KeyError as exc:
        raise ValueError(f"Unknown partition transformation type: {spec.transform}") from exc
    return factory(spec.param_value)
