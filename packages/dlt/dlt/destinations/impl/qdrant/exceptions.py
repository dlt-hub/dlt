from dlt.common.destination.exceptions import DestinationTerminalException


class InvalidInMemoryQdrantCredentials(DestinationTerminalException):
    def __init__(self) -> None:
        super().__init__(
            "To use in-memory instance of qdrant, "
            "please instantiate it first and then pass to destination factory\n"
            '\nclient = QdrantClient(":memory:")\n'
            'dlt.pipeline(pipeline_name="...", destination=dlt.destinations.qdrant(client)'
        )
