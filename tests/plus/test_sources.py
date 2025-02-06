import pytest

@pytest.mark.mssql
def test_mssql_source() -> None:
    # we just test wether the mssql source may be imported
    from dlt_plus.sources import mssql
