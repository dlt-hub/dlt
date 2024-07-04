import semver
from typing import Iterable

from dlt.common.exceptions import DltException, TerminalValueError


class StorageException(DltException):
    def __init__(self, msg: str) -> None:
        super().__init__(msg)


class NoMigrationPathException(StorageException):
    def __init__(
        self,
        storage_path: str,
        initial_version: semver.VersionInfo,
        migrated_version: semver.VersionInfo,
        target_version: semver.VersionInfo,
    ) -> None:
        self.storage_path = storage_path
        self.initial_version = initial_version
        self.migrated_version = migrated_version
        self.target_version = target_version
        super().__init__(
            f"Could not find migration path for {storage_path} from v {initial_version} to"
            f" {target_version}, stopped at {migrated_version}"
        )


class WrongStorageVersionException(StorageException):
    def __init__(
        self,
        storage_path: str,
        initial_version: semver.VersionInfo,
        target_version: semver.VersionInfo,
    ) -> None:
        self.storage_path = storage_path
        self.initial_version = initial_version
        self.target_version = target_version
        super().__init__(
            f"Expected storage {storage_path} with v {target_version} but found {initial_version}"
        )


class StorageMigrationError(StorageException):
    def __init__(
        self,
        storage_path: str,
        from_version: semver.VersionInfo,
        target_version: semver.VersionInfo,
        info: str,
    ) -> None:
        self.storage_path = storage_path
        self.from_version = from_version
        self.target_version = target_version
        super().__init__(
            f"Storage {storage_path} with target v {target_version} at {from_version}: " + info
        )


class LoadStorageException(StorageException):
    pass


class JobFileFormatUnsupported(LoadStorageException, TerminalValueError):
    def __init__(self, load_id: str, supported_formats: Iterable[str], wrong_job: str) -> None:
        self.load_id = load_id
        self.expected_file_formats = supported_formats
        self.wrong_job = wrong_job
        super().__init__(
            f"Job {wrong_job} for load id {load_id} requires job file format that is not one of"
            f" {supported_formats}"
        )


class LoadPackageNotFound(LoadStorageException, FileNotFoundError):
    def __init__(self, load_id: str) -> None:
        self.load_id = load_id
        super().__init__(f"Package with load id {load_id} could not be found")


class LoadPackageAlreadyCompleted(LoadStorageException):
    def __init__(self, load_id: str) -> None:
        self.load_id = load_id
        super().__init__(
            f"Package with load id {load_id} is already completed, but another complete was"
            " requested"
        )


class LoadPackageNotCompleted(LoadStorageException):
    def __init__(self, load_id: str) -> None:
        self.load_id = load_id
        super().__init__(
            f"Package with load id {load_id} is not yet completed, but method required that"
        )


class SchemaStorageException(StorageException):
    pass


class InStorageSchemaModified(SchemaStorageException):
    def __init__(self, schema_name: str, storage_path: str) -> None:
        msg = (
            f"Schema {schema_name} in {storage_path} was externally modified. This is not allowed"
            " as that would prevent correct version tracking. Use import/export capabilities of"
            " dlt to provide external changes."
        )
        super().__init__(msg)


class SchemaNotFoundError(SchemaStorageException, FileNotFoundError, KeyError):
    def __init__(
        self,
        schema_name: str,
        storage_path: str,
        import_path: str = None,
        import_format: str = None,
    ) -> None:
        msg = f"Schema {schema_name} in {storage_path} could not be found."
        if import_path:
            msg += f"Import from {import_path} and format {import_format} failed."
        super().__init__(msg)


class UnexpectedSchemaName(SchemaStorageException, ValueError):
    def __init__(self, schema_name: str, storage_path: str, stored_name: str) -> None:
        super().__init__(
            f"A schema file name '{schema_name}' in {storage_path} does not correspond to the name"
            f" of schema in the file {stored_name}"
        )


class CurrentLoadPackageStateNotAvailable(StorageException):
    def __init__(self) -> None:
        super().__init__(
            "State of the current load package is not available. Current load package state is"
            " only available in a function decorated with @dlt.destination during loading."
        )
