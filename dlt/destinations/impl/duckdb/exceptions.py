from dlt.common.destination.exceptions import DestinationTerminalException


class InvalidInMemoryDuckdbCredentials(DestinationTerminalException):
    def __init__(self) -> None:
        super().__init__(
            "To use in-memory instance of duckdb, "
            "please instantiate it first and then pass to destination factory\n"
            '\nconn = duckdb.connect(":memory:")\n'
            'dlt.pipeline(pipeline_name="...", destination=dlt.destinations.duckdb(conn)'
        )
