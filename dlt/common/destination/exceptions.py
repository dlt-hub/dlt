from typing import Any, Iterable, List

from dlt.common.exceptions import DltException, TerminalException, TransientException


class DestinationException(DltException):
    pass


class UnknownDestinationModule(DestinationException):
    def __init__(self, destination_module: str) -> None:
        self.destination_module = destination_module
        if "." in destination_module:
            msg = f"Destination module {destination_module} could not be found and imported"
        else:
            msg = f"Destination {destination_module} is not one of the standard dlt destinations"
        super().__init__(msg)


class InvalidDestinationReference(DestinationException):
    def __init__(self, destination_module: Any) -> None:
        self.destination_module = destination_module
        msg = f"Destination {destination_module} is not a valid destination module."
        super().__init__(msg)


class DestinationTerminalException(DestinationException, TerminalException):
    pass


class DestinationUndefinedEntity(DestinationTerminalException):
    pass


class DestinationTransientException(DestinationException, TransientException):
    pass


class DestinationLoadingViaStagingNotSupported(DestinationTerminalException):
    def __init__(self, destination: str) -> None:
        self.destination = destination
        super().__init__(f"Destination {destination} does not support loading via staging.")


class DestinationLoadingWithoutStagingNotSupported(DestinationTerminalException):
    def __init__(self, destination: str) -> None:
        self.destination = destination
        super().__init__(f"Destination {destination} does not support loading without staging.")


class DestinationNoStagingMode(DestinationTerminalException):
    def __init__(self, destination: str) -> None:
        self.destination = destination
        super().__init__(f"Destination {destination} cannot be used as a staging")


class DestinationIncompatibleLoaderFileFormatException(DestinationTerminalException):
    def __init__(
        self, destination: str, staging: str, file_format: str, supported_formats: Iterable[str]
    ) -> None:
        self.destination = destination
        self.staging = staging
        self.file_format = file_format
        self.supported_formats = supported_formats
        supported_formats_str = ", ".join(supported_formats)
        if self.staging:
            if not supported_formats:
                msg = (
                    f"Staging {staging} cannot be used with destination {destination} because they"
                    " have no file formats in common."
                )
            else:
                msg = (
                    f"Unsupported file format {file_format} for destination {destination} in"
                    f" combination with staging destination {staging}. Supported formats:"
                    f" {supported_formats_str}"
                )
        else:
            msg = (
                f"Unsupported file format {file_format} in destination {destination}. Supported"
                f" formats: {supported_formats_str}. If {destination} supports loading data via"
                " staging bucket, more formats may be available."
            )
        super().__init__(msg)


class IdentifierTooLongException(DestinationTerminalException):
    def __init__(
        self,
        destination_name: str,
        identifier_type: str,
        identifier_name: str,
        max_identifier_length: int,
    ) -> None:
        self.destination_name = destination_name
        self.identifier_type = identifier_type
        self.identifier_name = identifier_name
        self.max_identifier_length = max_identifier_length
        super().__init__(
            f"The length of {identifier_type} {identifier_name} exceeds"
            f" {max_identifier_length} allowed for {destination_name}"
        )


class DestinationHasFailedJobs(DestinationTerminalException):
    def __init__(self, destination_name: str, load_id: str, failed_jobs: List[Any]) -> None:
        self.destination_name = destination_name
        self.load_id = load_id
        self.failed_jobs = failed_jobs
        super().__init__(
            f"Destination {destination_name} has failed jobs in load package {load_id}"
        )


class DestinationSchemaTampered(DestinationTerminalException):
    def __init__(self, schema_name: str, version_hash: str, stored_version_hash: str) -> None:
        self.version_hash = version_hash
        self.stored_version_hash = stored_version_hash
        super().__init__(
            f"Schema {schema_name} content was changed - by a loader or by destination code - from"
            " the moment it was retrieved by load package. Such schema cannot reliably be updated"
            f" nor saved. Current version hash: {version_hash} != stored version hash"
            f" {stored_version_hash}. If you are using destination client directly, without storing"
            " schema in load package, you should first save it into schema storage. You can also"
            " use schema._bump_version() in test code to remove modified flag."
        )
