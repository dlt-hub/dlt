from typing import Any, Optional

from dlt.common.schema.typing import TColumnNames, TTableSchemaColumns
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource

GEOMETRY_HINT = "x-postgres-geometry"
SRID_HINT = "x-postgres-srid"


def postgres_adapter(
    data: Any,
    geometry: TColumnNames = None,
    srid: Optional[int] = 4326,
) -> DltResource:
    """Prepares data for the postgres destination by specifying which columns should
    be cast to PostGIS geometry types.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        geometry (TColumnNames, optional): Specify columns to cast to geometries.
            It can be a single column name as a string, or a list of column names.
        srid (int, optional): The Spatial Reference System Identifier (SRID) to be
            used for the geometry columns. If not provided, SRID 4326 will be used.

    Returns:
        DltResource: A resource with applied postgres-specific hints.

    Raises:
        ValueError: If input for `geometry` is invalid, or if no geometry columns are specified.

    Examples:
        >>> data = [{"town": "Null Island", "loc": "POINT(0 0)"}]
        >>> postgres_adapter(data, geometry="loc", srid=4326)
        [DltResource with hints applied]
    """
    resource = get_resource_for_adapter(data)

    column_hints: TTableSchemaColumns = {}

    if geometry:
        if isinstance(geometry, str):
            geometry = [geometry]
        if not isinstance(geometry, list):
            raise ValueError(
                "`geometry` must be a list of column names or a single column name as a string."
            )

        for column_name in geometry:
            column_hints[column_name] = {
                "name": column_name,
                GEOMETRY_HINT: True,  # type: ignore[misc]
            }
            if srid is not None:
                column_hints[column_name][SRID_HINT] = srid  # type: ignore

    if not column_hints:
        raise ValueError("A value for `geometry` must be specified.")
    else:
        resource.apply_hints(columns=column_hints)
    return resource
