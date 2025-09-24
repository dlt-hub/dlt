import pytest

from dlt.sources.rest_api import rest_api
from dlt.common.configuration.exceptions import ConfigFieldMissingException


def test_config_sections_resolution():
    with pytest.raises(ConfigFieldMissingException) as exc_info:
        list(rest_api())

    # NOTE: we check that the first trace related to rest_api has the correct
    # sections set
    assert list(exc_info.value.traces.values())[1][0].key.startswith(  # type: ignore[union-attr]
        "SOURCES__REST_API__REST_API__"
    )
