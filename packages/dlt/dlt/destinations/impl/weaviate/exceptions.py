from dlt.common.destination.exceptions import DestinationException, DestinationTerminalException


class WeaviateGrpcError(DestinationException):
    pass


class PropertyNameConflict(DestinationTerminalException):
    def __init__(self, error: str) -> None:
        super().__init__(
            "Your data contains items with identical property names when compared case insensitive."
            " Weaviate cannot handle such data. Please clean up your data before loading or change"
            " to case insensitive naming convention. See"
            " https://dlthub.com/docs/dlt-ecosystem/destinations/weaviate#names-normalization for"
            f" details. [{error}]"
        )
