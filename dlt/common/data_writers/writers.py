import abc
import pickle

import sqlglot.expressions as exp

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, IO, Tuple, Type

from dlt.common import json
from dlt.common.typing import StrAny
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.destination import TLoaderFileFormat, DestinationCapabilitiesContext


@dataclass
class TFileFormatSpec:
    file_format: TLoaderFileFormat
    file_extension: str
    is_binary_format: bool
    supports_schema_changes: bool
    requires_destination_capabilities: bool = False
    supports_compression: bool = False


class DataWriter(abc.ABC):
    def __init__(self, f: IO[Any], caps: DestinationCapabilitiesContext = None) -> None:
        self._f = f
        self._caps = caps
        self.items_count = 0

    @abc.abstractmethod
    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        pass

    def write_data(self, rows: Sequence[Any]) -> None:
        self.items_count += len(rows)

    @abc.abstractmethod
    def write_footer(self) -> None:
        pass

    def write_all(self, columns_schema: TTableSchemaColumns, rows: Sequence[Any]) -> None:
        self.write_header(columns_schema)
        self.write_data(rows)
        self.write_footer()


    @classmethod
    @abc.abstractmethod
    def data_format(cls) -> TFileFormatSpec:
        pass

    @classmethod
    def from_file_format(cls, file_format: TLoaderFileFormat, f: IO[Any], caps: DestinationCapabilitiesContext = None) -> "DataWriter":
        return cls.class_factory(file_format)(f, caps)

    @classmethod
    def from_destination_capabilities(cls, caps: DestinationCapabilitiesContext, f: IO[Any]) -> "DataWriter":
        return cls.class_factory(caps.preferred_loader_file_format)(f, caps)

    @classmethod
    def data_format_from_file_format(cls, file_format: TLoaderFileFormat) -> TFileFormatSpec:
        return cls.class_factory(file_format).data_format()

    @staticmethod
    def class_factory(file_format: TLoaderFileFormat) -> Type["DataWriter"]:
        if file_format == "jsonl":
            return JsonlWriter
        elif file_format == "puae-jsonl":
            return JsonlListPUAEncodeWriter
        elif file_format == "insert_values":
            return InsertValuesWriter
        else:
            raise ValueError(file_format)


class JsonlWriter(DataWriter):

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        pass

    def write_data(self, rows: Sequence[Any]) -> None:
        super().write_data(rows)
        for row in rows:
            json.dump(row, self._f)
            self._f.write(b"\n")

    def write_footer(self) -> None:
        pass

    @classmethod
    def data_format(cls) -> TFileFormatSpec:
        return TFileFormatSpec(
            "jsonl",
            file_extension="jsonl",
            is_binary_format=True,
            supports_schema_changes=True,
            supports_compression=True,
        )


class JsonlListPUAEncodeWriter(JsonlWriter):

    def write_data(self, rows: Sequence[Any]) -> None:
        # skip JsonlWriter when calling super
        super(JsonlWriter, self).write_data(rows)
        # write all rows as one list which will require to write just one line
        # encode types with PUA characters
        json.typed_dump(rows, self._f)
        self._f.write(b"\n")

    @classmethod
    def data_format(cls) -> TFileFormatSpec:
        return TFileFormatSpec(
            "puae-jsonl",
            file_extension="jsonl",
            is_binary_format=True,
            supports_schema_changes=True,
            supports_compression=True,
        )


SCT_TO_AST = {
    "text": exp.DataType.build("text"),
    "double": exp.DataType.build("double"),
    "bool": exp.DataType.build("bool"),
    "timestamp": exp.DataType.build("timestamp"),
    "bigint": exp.DataType.build("bigint"),
    "binary": exp.DataType.build("binary"),
    "complex": exp.DataType.build("json"),
    "decimal": exp.DataType.build("decimal"),
    "wei": exp.DataType.build("decimal(38,9)"),
    "date": exp.DataType.build("date"),
}


class InsertValuesWriter(DataWriter):

    def __init__(self, f: IO[Any], caps: DestinationCapabilitiesContext = None) -> None:
        super().__init__(f, caps)
        self._chunks_written = 0
        self._headers_lookup: Dict[str, int] = None
        self._columns_to_types: List[Tuple[str, exp.DataType]] = None
        self._batch: List[Any] = []

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        assert self._chunks_written == 0
        assert columns_schema is not None, "column schema required"
        headers = list(columns_schema.keys())  # List ensures deterministic order
        self._headers_lookup = {v: i for i, v in enumerate(headers)}
        self._columns_to_types = [(column, SCT_TO_AST[meta["data_type"]]) for column, meta in columns_schema.items()]

    def write_data(self, rows: Sequence[Any]) -> None:
        super().write_data(rows)
        row: StrAny
        for row in rows:
            output = [None] * len(self._headers_lookup)
            for n, v in row.items():
                ix = self._headers_lookup[n]
                if self._columns_to_types[ix][1] is SCT_TO_AST["complex"]:
                    v = json.dumps(v)
                output[ix] = v
            self._batch.append(tuple(output))
        self._chunks_written += 1

    def write_footer(self) -> None:
        assert self._chunks_written > 0
        casted_columns = [
            exp.alias_(
                exp.cast(column, to=data_type),
                column,
                copy=False,
            )
            for column, data_type in self._columns_to_types
        ]
        values_exp = exp.values(self._batch, alias="data", columns=dict(self._columns_to_types))
        pickle.dump(
            exp.Insert(
                this=exp.Placeholder(),
                expression=exp.select(*casted_columns).from_(values_exp)
            ),
            self._f,
        )
        self._batch.clear()

    @classmethod
    def data_format(cls) -> TFileFormatSpec:
        return TFileFormatSpec(
            "insert_values",
            file_extension="insert_values",
            is_binary_format=True,
            supports_schema_changes=False,
            supports_compression=True,
            requires_destination_capabilities=True,
        )
