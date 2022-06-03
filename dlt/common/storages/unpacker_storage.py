from typing import List, Sequence, Tuple, Type
from itertools import groupby
from pathlib import Path

from dlt.common.utils import chunks
from dlt.common.file_storage import FileStorage
from dlt.common.configuration import UnpackingVolumeConfiguration
from dlt.common.storages.versioned_storage import VersionedStorage


class UnpackerStorage(VersionedStorage):

    STORAGE_VERSION = "1.0.0"
    UNPACKING_FOLDER: str = "unpacking"  # folder within the volume where files to be unpacked are stored
    UNPACK_FILE_EXTENSION = ".unpack.json"
    UNPACK_FILE_EXTENSION_LEN = len(UNPACK_FILE_EXTENSION)

    def __init__(self, is_owner: bool, C: Type[UnpackingVolumeConfiguration]) -> None:
        super().__init__(UnpackerStorage.STORAGE_VERSION, is_owner, FileStorage(C.UNPACKING_VOLUME_PATH, "t", makedirs=is_owner))

    def initialize_storage(self) -> None:
        self.storage.create_folder(UnpackerStorage.UNPACKING_FOLDER, exists_ok=True)

    def list_files_to_unpack_sorted(self) -> Sequence[str]:
        return sorted(self.storage.list_folder_files(UnpackerStorage.UNPACKING_FOLDER))

    def get_grouped_iterator(self, files: Sequence[str]) -> "groupby[str, str]":
        return groupby(files, lambda f: UnpackerStorage.get_schema_name(f))

    @staticmethod
    def chunk_by_events(files: Sequence[str], max_events: int, processing_cores: int) -> List[Sequence[str]]:
        # should distribute ~ N events evenly among m cores with fallback for small amounts of events

        def count_events(file_name : str) -> int:
            # return event count from file name
            return UnpackerStorage.get_events_count(file_name)

        counts = list(map(count_events, files))
        # make a list of files containing ~max_events
        events_count = 0
        m = 0
        while events_count < max_events and m < len(files):
            events_count += counts[m]
            m += 1
        processing_chunks = round(m / processing_cores)
        if processing_chunks == 0:
            # return one small chunk
            return [files]
        else:
            # should return ~ amount of chunks to fill all the cores
            return list(chunks(files[:m], processing_chunks))

    @staticmethod
    def get_events_count(file_name: str) -> int:
        return UnpackerStorage._parse_unpack_file_name(file_name)[0]

    @staticmethod
    def get_schema_name(file_name: str) -> str:
        return UnpackerStorage._parse_unpack_file_name(file_name)[2]

    @staticmethod
    def build_unpack_file_name(schema_name: str, stem: str, event_count: int, load_id: str) -> str:
        # builds file name of the unpack file for the tracker
        return f"{schema_name}_{stem}_{load_id}_{event_count}{UnpackerStorage.UNPACK_FILE_EXTENSION}"

    @staticmethod
    def _parse_unpack_file_name(file_name: str) -> Tuple[int, str, str]:
        # parser unpack tracker file and returns (events found, load id, schema_name)
        if not file_name.endswith(UnpackerStorage.UNPACK_FILE_EXTENSION):
            raise ValueError(file_name)

        parts = Path(file_name[:-UnpackerStorage.UNPACK_FILE_EXTENSION_LEN]).stem.split("_")
        return (int(parts[-1]), parts[-2], parts[0])