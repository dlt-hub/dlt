from typing import Any, Iterable, List, Sequence

from dlt.common.exceptions import DltException, TerminalException, TransientException
from dlt.common.reflection.exceptions import ReferenceImportError
from dlt.common.reflection.ref import ImportTrace


class DestinationException(DltException):
    pass


class UnknownDestinationModule(ReferenceImportError, DestinationException, KeyError):
    def __init__(
        self, ref: str, qualified_refs: Sequence[str], traces: Sequence[ImportTrace]
    ) -> None:
        self.ref = ref
        self.qualified_refs = qualified_refs
        super().__init__(traces=traces)

    def __str__(self) -> str:
        if "." in self.ref:
            msg = f"Destination module {self.ref} is not registered."
        else:
            msg = f"Destination {self.ref} is not one of the standard dlt destinations."

        if len(self.qualified_refs) == 1 and self.qualified_refs[0] == self.ref:
            pass
        else:
            msg += (
                " Following fully qualified refs were tried in the registry:\n\t%s\n"
                % "\n\t".join(self.qualified_refs)
            )
        if self.traces:
            msg += super().__str__()
        return msg


class InvalidDestinationReference(DestinationException):
    def __init__(self, refs: Any) -> None:
        self.refs = refs
        msg = (
            f"None of supplied destination refs: {refs} can be found in registry or imported as"
            " Python type."
        )
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


class UnsupportedDataType(DestinationTerminalException):
    def __init__(
        self,
        destination_type: str,
        table_name: str,
        column: str,
        data_type: str,
        file_format: str,
        available_in_formats: Sequence[str],
        more_info: str,
    ) -> None:
        self.destination_type = destination_type
        self.table_name = table_name
        self.column = column
        self.data_type = data_type
        self.file_format = file_format
        self.available_in_formats = available_in_formats
        self.more_info = more_info
        msg = (
            f"Destination {destination_type} cannot load data type '{data_type}' from"
            f" '{file_format}' files. The affected table is '{table_name}' column '{column}'."
        )
        if available_in_formats:
            msg += f" Note: '{data_type}' can be loaded from {available_in_formats} formats(s)."
        else:
            msg += f" None of available file formats support '{data_type}' for this destination."
        if more_info:
            msg += " More info: " + more_info
        super().__init__(msg)


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


class DestinationCapabilitiesException(DestinationException):
    pass


class DestinationInvalidFileFormat(DestinationTerminalException):
    def __init__(
        self, destination_type: str, file_format: str, file_name: str, message: str
    ) -> None:
        self.destination_type = destination_type
        self.file_format = file_format
        self.message = message
        super().__init__(
            f"Destination {destination_type} cannot process file {file_name} with format"
            f" {file_format}: {message}"
        )
