import pytest

from dlt.common.file_storage import FileStorage
from dlt.common.storages.exceptions import NoMigrationPathException
from dlt.common.storages.unpacker_storage import UnpackerStorage
from dlt.common.configuration import UnpackingVolumeConfiguration

from tests.utils import TEST_STORAGE, write_version, autouse_root_storage

@pytest.mark.skip()
def test_load_events_and_group_by_sender() -> None:
    # TODO: create fixture with two sender ids and 3 files and check the result
    pass


@pytest.mark.skip()
def test_chunk_by_events() -> None:
    # TODO: should distribute ~ N events evenly among m cores with fallback for small amounts of events
    pass



def test_full_migration_path() -> None:
    # create directory structure
    s = UnpackerStorage(True, UnpackingVolumeConfiguration)
    # overwrite known initial version
    write_version(s.storage, "1.0.0")
    # must be able to migrate to current version
    s = UnpackerStorage(True, UnpackingVolumeConfiguration)
    assert s.version == UnpackerStorage.STORAGE_VERSION


def test_unknown_migration_path() -> None:
    # create directory structure
    s = UnpackerStorage(True, UnpackingVolumeConfiguration)
    # overwrite known initial version
    write_version(s.storage, "10.0.0")
    # must be able to migrate to current version
    with pytest.raises(NoMigrationPathException):
        UnpackerStorage(False, UnpackingVolumeConfiguration)
