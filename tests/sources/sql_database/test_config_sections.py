import pytest

from dlt.sources.sql_database import sql_database, sql_table
from dlt.common.configuration.exceptions import ConfigFieldMissingException


def test_config_sections_resolution():
    with pytest.raises(ConfigFieldMissingException) as exc_info:
        list(sql_database())

    # NOTE: we check that the first trace related to sql_database has the correct
    # sections set
    flat_trace = exc_info.value.attrs()["traces"]["credentials"]
    assert flat_trace[0].key.startswith("SOURCES__SQL_DATABASE__SQL_DATABASE__")

    with pytest.raises(ConfigFieldMissingException) as exc_info:
        list(sql_table(table="my_table"))

    flat_trace = exc_info.value.attrs()["traces"]["credentials"]
    assert flat_trace[0].key.startswith("SOURCES__SQL_DATABASE__MY_TABLE__")
