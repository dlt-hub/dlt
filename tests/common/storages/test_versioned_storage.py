import pytest
import semver

from dlt.common.storages.file_storage import FileStorage
from dlt.common.storages.exceptions import NoMigrationPathException, WrongStorageVersionException
from dlt.common.storages.versioned_storage import VersionedStorage

from tests.utils import write_version, test_storage


class MigratedStorage(VersionedStorage):
    def migrate_storage(self, from_version: semver.Version, to_version: semver.Version) -> None:
        # migration example:
        if from_version == "1.0.0" and from_version < to_version:
            from_version = semver.Version.parse("1.1.0")
            self._save_version(from_version)
        if from_version == "1.1.0" and from_version < to_version:
            from_version = semver.Version.parse("1.2.0")
            self._save_version(from_version)


def test_new_versioned_storage(test_storage: FileStorage) -> None:
    v = VersionedStorage(test_storage)
    # versioned storage created by fixture
    assert v.is_storage_ready()

    v.ensure_migration("1.0.1", True)
    assert v.version == "1.0.1"


def test_new_versioned_storage_non_owner(test_storage: FileStorage) -> None:
    v = VersionedStorage(test_storage)
    with pytest.raises(WrongStorageVersionException) as wsve:
        v.ensure_migration("1.0.1", False)
    assert wsve.value.storage_path == test_storage.storage_path
    assert wsve.value.target_version == "1.0.1"
    assert wsve.value.initial_version == "0.0.0"


def test_migration(test_storage: FileStorage) -> None:
    write_version(test_storage, "1.0.0")
    v = MigratedStorage(test_storage)
    v.ensure_migration("1.2.0", True)
    assert v.version == "1.2.0"


def test_unknown_migration_path(test_storage: FileStorage) -> None:
    write_version(test_storage, "1.0.0")
    v = MigratedStorage(test_storage)
    with pytest.raises(NoMigrationPathException) as wmpe:
        v.ensure_migration("1.3.0", True)
    assert wmpe.value.migrated_version == "1.2.0"


def test_only_owner_migrates(test_storage: FileStorage) -> None:
    write_version(test_storage, "1.0.0")
    v = MigratedStorage(test_storage)
    with pytest.raises(WrongStorageVersionException) as wmpe:
        v.ensure_migration("1.2.0", False)
    assert wmpe.value.initial_version == "1.0.0"


def test_downgrade_not_possible(test_storage: FileStorage) -> None:
    write_version(test_storage, "1.2.0")
    v = MigratedStorage(test_storage)
    with pytest.raises(NoMigrationPathException) as wmpe:
        v.ensure_migration("1.1.0", True)
    assert wmpe.value.migrated_version == "1.2.0"


def test_versioned_storage_not_ready(test_storage: FileStorage) -> None:
    storage = FileStorage(test_storage.make_full_path_safe("versioned"))
    v = MigratedStorage(storage)
    assert v.is_storage_ready() is False
