import pytest

from dlt.sources.sql_database import sql_database
from dlt.common.configuration.exceptions import ConfigFieldMissingException


def test_config_sections_resolution():
    with pytest.raises(ConfigFieldMissingException) as exc_info:
        list(sql_database())

    # NOTE: we check that the first trace related to rest_api has the correct
    # sections set
    assert list(exc_info.value.traces.values())[0][0].key.startswith(
        "SOURCES__SQL_DATABASE__SQL_DATABASE__"
    )
