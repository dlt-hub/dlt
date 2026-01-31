import pytest

# pyodbc not available on mac - skip tests
pytest.importorskip("pyodbc")


def test_mssql_source() -> None:
    # we just test wether the mssql source may be imported
    from dlthub.sources import mssql
