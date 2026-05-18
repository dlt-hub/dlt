from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Iterator, Sequence, Sized, TypeVar

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow

from dlt.common.json import json
from dlt.common.storages import FileStorage
from dlt.common.typing import TDataRecordBatch

TRecordBatch = TypeVar("TRecordBatch", bound=Sized)


class FileBatchIterator(ABC, Generic[TRecordBatch]):
    def __init__(
        self,
        file_path: str,
        batch_size: int,
        record_offset: int,
        columns: Sequence[str] = (),
    ) -> None:
        self._file_path = file_path
        self._batch_size = batch_size
        self._record_offset = record_offset
        self._columns = list(columns)

    @abstractmethod
    def __iter__(self) -> Iterator[TRecordBatch]:
        pass


class ParquetFileBatchIterator(FileBatchIterator["pyarrow.RecordBatch"]):
    def __init__(
        self,
        file_path: str,
        batch_size: int,
        record_offset: int,
        columns: Sequence[str] = (),
    ) -> None:
        super().__init__(file_path, batch_size, record_offset, columns)
        self._batch_offset, remainder = divmod(self._record_offset, self._batch_size)
        assert remainder == 0, "`_record_offset` must be a multiple of `_batch_size`"

    def __iter__(self) -> Iterator[pyarrow.RecordBatch]:
        from dlt.common.libs.pyarrow import pyarrow

        batches_to_skip = self._batch_offset
        with pyarrow.parquet.ParquetFile(self._file_path) as reader:
            for record_batch in reader.iter_batches(
                batch_size=self._batch_size,
                columns=self._columns or None,
            ):
                if batches_to_skip > 0:
                    batches_to_skip -= 1
                    continue
                yield record_batch


class JsonlFileBatchIterator(FileBatchIterator[TDataRecordBatch]):
    def __iter__(self) -> Iterator[TDataRecordBatch]:
        current_batch: TDataRecordBatch = []
        records_to_skip = self._record_offset
        projected_columns = set(self._columns)

        with FileStorage.open_zipsafe_ro(self._file_path) as f:
            for line in f:
                records = json.typed_loads(line)
                if isinstance(records, dict):
                    records = [records]

                for record in records:
                    if records_to_skip > 0:
                        records_to_skip -= 1
                        continue
                    if projected_columns:
                        record = {
                            key: value for key, value in record.items() if key in projected_columns
                        }
                    current_batch.append(record)
                    if len(current_batch) == self._batch_size:
                        yield current_batch
                        current_batch = []

        # yield any remaining records in last partial batch
        if current_batch:
            yield current_batch
