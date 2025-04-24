from dlt.common.destination.exceptions import DestinationTerminalException


class InvalidInMemoryDuckdbCredentials(DestinationTerminalException):
    def __init__(self) -> None:
        super().__init__(
            "To use in-memory instance of duckdb, "
            "please instantiate it first and then pass to destination factory\n"
            '\nconn = duckdb.connect(":memory:")\n'
            'dlt.pipeline(pipeline_name="...", destination=dlt.destinations.duckdb(conn)'
        )


class IcebergViewException(DestinationTerminalException):
    def __init__(self, dbapi_exception: Exception, note: str) -> None:
        self.note = note
        super().__init__(dbapi_exception)

    def __str__(self) -> str:
        msg = super().__str__()
        return f"{self.note}\n{msg}"
