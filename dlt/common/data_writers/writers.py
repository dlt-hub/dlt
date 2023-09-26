import abc

from dataclasses import dataclass
from typing import Any, Dict, Sequence, IO, Type, Optional, List, cast

from dlt.common import json
from dlt.common.typing import StrAny
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.destination import TLoaderFileFormat, DestinationCapabilitiesContext
from dlt.common.configuration import with_config, known_sections, configspec
from dlt.common.configuration.specs import BaseConfiguration

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
        elif file_format == "parquet":
            return ParquetDataWriter  # type: ignore
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


class InsertValuesWriter(DataWriter):

    def __init__(self, f: IO[Any], caps: DestinationCapabilitiesContext = None) -> None:
        super().__init__(f, caps)
        self._chunks_written = 0
        self._headers_lookup: Dict[str, int] = None

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        assert self._chunks_written == 0
        assert columns_schema is not None, "column schema required"
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
        if self._chunks_written > 0:
            self._f.write(";")

    @classmethod
    def data_format(cls) -> TFileFormatSpec:
        return TFileFormatSpec(
            "insert_values",
            file_extension="insert_values",
            is_binary_format=False,
            supports_schema_changes=False,
            supports_compression=True,
            requires_destination_capabilities=True,
        )


@configspec
class ParquetDataWriterConfiguration(BaseConfiguration):
    flavor: str = "spark"
    version: str = "2.4"
    data_page_size: int = 1024 * 1024
    timestamp_precision: str = "us"
    timestamp_timezone: str = "UTC"

    __section__: str = known_sections.DATA_WRITER

class ParquetDataWriter(DataWriter):

    @with_config(spec=ParquetDataWriterConfiguration)
    def __init__(self,
                 f: IO[Any],
                 caps: DestinationCapabilitiesContext = None,
                 *,
                 flavor: str = "spark",
                 version: str = "2.4",
                 data_page_size: int = 1024 * 1024,
                 timestamp_timezone: str = "UTC"
                 ) -> None:
        super().__init__(f, caps)
        from dlt.common.libs.pyarrow import pyarrow

        self.writer: Optional[pyarrow.parquet.ParquetWriter] = None
        self.schema: Optional[pyarrow.Schema] = None
        self.complex_indices: List[str] = None
        self.parquet_flavor = flavor
        self.parquet_version = version
        self.parquet_data_page_size = data_page_size
        self.timestamp_timezone = timestamp_timezone

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        from dlt.common.libs.pyarrow import pyarrow, get_py_arrow_datatype

        # build schema
        self.schema = pyarrow.schema(
            [pyarrow.field(
                name,
                get_py_arrow_datatype(schema_item, self._caps, self.timestamp_timezone),
                nullable=schema_item["nullable"]
            ) for name, schema_item in columns_schema.items()]
        )
        # find row items that are of the complex type (could be abstracted out for use in other writers?)
        self.complex_indices = [i for i, field in columns_schema.items() if field["data_type"] == "complex"]
        self.writer = pyarrow.parquet.ParquetWriter(self._f, self.schema, flavor=self.parquet_flavor, version=self.parquet_version, data_page_size=self.parquet_data_page_size)


    def write_data(self, rows: Sequence[Any]) -> None:
        super().write_data(rows)
        from dlt.common.libs.pyarrow import pyarrow

        # replace complex types with json
        for key in self.complex_indices:
            for row in rows:
                if key in row:
                    row[key] = json.dumps(row[key])

        table = pyarrow.Table.from_pylist(rows, schema=self.schema)
        # Write
        self.writer.write_table(table)

    def write_footer(self) -> None:
        self.writer.close()
        self.writer = None


    @classmethod
    def data_format(cls) -> TFileFormatSpec:
        return TFileFormatSpec("parquet", "parquet", True, False, requires_destination_capabilities=True, supports_compression=False)
