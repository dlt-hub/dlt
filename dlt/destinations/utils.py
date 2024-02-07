from typing import Any

from dlt.extract import DltResource, resource as make_resource


def ensure_resource(data: Any) -> DltResource:
    """Wraps `data` in a DltResource if it's not a DltResource already."""
    resource: DltResource
    if not isinstance(data, DltResource):
        resource_name: str = None
        if not hasattr(data, "__name__"):
            resource_name = "content"
        resource = make_resource(data, name=resource_name)
    else:
        resource = data
    return resource
