import pytest

from dlt.common.utils import uniq_id
from dlt.common.storages import NormalizeStorage, NormalizeStorageConfiguration
from dlt.common.storages.exceptions import NoMigrationPathException

from tests.utils import write_version, autouse_test_storage


def test_full_migration_path() -> None:
    # create directory structure
    s = NormalizeStorage(True)
    # overwrite known initial version
    write_version(s.storage, "1.0.0")
    # must be able to migrate to current version
    s = NormalizeStorage(True)
    assert s.version == NormalizeStorage.STORAGE_VERSION


def test_unknown_migration_path() -> None:
    # create directory structure
    s = NormalizeStorage(True)
    # overwrite known initial version
    write_version(s.storage, "10.0.0")
    # must be able to migrate to current version
    with pytest.raises(NoMigrationPathException):
        NormalizeStorage(False)


def test_non_owner_migrates_existing_storage() -> None:
    # create directory structure as owner with current version
    NormalizeStorage(True)
    # downgrade version to a migratable older version
    write_version(NormalizeStorage(True).storage, "1.0.0")
    # non-owner opening existing storage migrates it to the current version
    s = NormalizeStorage(False)
    assert s.version == NormalizeStorage.STORAGE_VERSION
