from dlt.common.configuration import resolve_configuration

from dlt.destinations.impl.mssql.configuration import MsSqlCredentials



def test_to_odbc_dsn() -> None:
    creds = resolve_configuration(
        MsSqlCredentials("mssql://test_user:test_password@sql.example.com:12345/test_db?FOO=a&BAR=b")
    )

    dsn = creds.to_odbc_dsn()

    result = {k: v for k, v in (param.split('=') for param in dsn.split(";"))}

    assert result == {
        'DRIVER': 'ODBC Driver 18 for SQL Server',
        'SERVER': 'sql.example.com,12345',
        'DATABASE': 'test_db',
        'UID': 'test_user',
        'PWD': 'test_password',
        'FOO': 'a',
        'BAR': 'b'
    }
