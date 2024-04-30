from dlt.common.destination.exceptions import DestinationTerminalException


class InvalidInMemoryDuckDbUsage(DestinationTerminalException):
    def __init__(self) -> None:
        super().__init__(
            'You have specicied credentials=":memory:" this is incorrect.\n'
            "Please pass in memory instance of duckdb connection\n"
            'credentials=dlt.destinations.duckdb(":memory:")`'
        )
