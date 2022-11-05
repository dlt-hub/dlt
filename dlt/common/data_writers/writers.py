import abc
import jsonlines
from dataclasses import dataclass
from typing import Any, Dict, Sequence, IO, Type

from dlt.common import json
from dlt.common.typing import StrAny
from dlt.common.json import json_typed_dumps
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.destination import TLoaderFileFormat, DestinationCapabilitiesContext


@dataclass
class TFileFormatSpec:
    file_format: TLoaderFileFormat
    file_extension: str
    is_binary_format: bool
    supports_schema_changes: bool
    requires_destination_capabilities: bool = False


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
        # use jsonl to write load files https://jsonlines.org/
        with jsonlines.Writer(self._f, dumps=json.dumps) as w:
            w.write_all(rows)

    def write_footer(self) -> None:
        pass

    @classmethod
    def data_format(cls) -> TFileFormatSpec:
        return TFileFormatSpec("jsonl", "jsonl", False, True)


class JsonlListPUAEncodeWriter(JsonlWriter):

    def write_data(self, rows: Sequence[Any]) -> None:
        # skip JsonlWriter when calling super
        super(JsonlWriter, self).write_data(rows)
        # encode types with PUA characters
        with jsonlines.Writer(self._f, dumps=json_typed_dumps) as w:
            # write all rows as one list which will require to write just one line
            w.write_all([rows])

    @classmethod
    def data_format(cls) -> TFileFormatSpec:
        return TFileFormatSpec("puae-jsonl", "jsonl", False, True)


class InsertValuesWriter(DataWriter):

    def __init__(self, f: IO[Any], caps: DestinationCapabilitiesContext = None) -> None:
        super().__init__(f, caps)
        self._chunks_written = 0
        self._headers_lookup: Dict[str, int] = None

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        assert self._chunks_written == 0
        headers = columns_schema.keys()
        # dict lookup is always faster
        self._headers_lookup = {v: i for i, v in enumerate(headers)}
        # do not write INSERT INTO command, this must be added together with table name by the loader
        self._f.write("INSERT INTO {}(")
        self._f.write(",".join(map(self._caps.escape_identifier, headers)))
        self._f.write(")\nVALUES\n")

    def write_data(self, rows: Sequence[Any]) -> None:
        super().write_data(rows)

        def write_row(row: StrAny) -> None:
            output = ["NULL"] * len(self._headers_lookup)
            for n,v  in row.items():
                output[self._headers_lookup[n]] = self._caps.escape_literal(v)
            self._f.write("(")
            self._f.write(",".join(output))
            self._f.write(")")

        # if next chunk add separator
        if self._chunks_written > 0:
            self._f.write(",\n")

        # write rows
        for row in rows[:-1]:
            write_row(row)
            self._f.write(",\n")

        # write last row without separator so we can write footer eventually
        write_row(rows[-1])
        self._chunks_written += 1

    def write_footer(self) -> None:
        assert self._chunks_written > 0
        self._f.write(";")

    @classmethod
    def data_format(cls) -> TFileFormatSpec:
        return TFileFormatSpec("insert_values", "insert_values", False, False, requires_destination_capabilities=True)
