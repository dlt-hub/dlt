from dlt.common.destination.exceptions import DestinationTerminalException


class InvalidInMemoryDuckDbUsage(DestinationTerminalException):
    def __init__(self) -> None:
        super().__init__(
            'You have specicied credentials=":memory:" this is incorrect.\n'
            'Please create in memory instance of duckdb `conn = duckdb.connect(":memory:")`\n'
            "then pass it as via parameter via destination factory"
            'dlt.pipeline(pipeline_name="...", destination=dlt.destinations.duckdb(conn)'
        )
