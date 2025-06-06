import dlt
import pytest

from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource


def test_get_resource_for_adapter() -> None:
    # test on pure data
    data = [1, 2, 3]
    adapted_resource = get_resource_for_adapter(data)
    assert isinstance(adapted_resource, DltResource)
    assert list(adapted_resource) == [1, 2, 3]
    assert adapted_resource.name == "content"

    # test on resource
    @dlt.resource(table_name="my_table")
    def some_resource():
        yield [1, 2, 3]

    adapted_resource = get_resource_for_adapter(some_resource)
    assert adapted_resource == some_resource
    assert adapted_resource.name == "some_resource"

    # test on source with one resource
    @dlt.source
    def source():
        return [some_resource]

    adapted_resource = get_resource_for_adapter(source())
    assert adapted_resource.table_name == "my_table"

    # test on source with multiple resources
    @dlt.resource(table_name="my_table")
    def other_resource():
        yield [1, 2, 3]

    @dlt.source
    def other_source():
        return [some_resource, other_resource]

    with pytest.raises(ValueError):
        get_resource_for_adapter(other_source())
