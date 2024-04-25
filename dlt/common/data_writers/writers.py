import abc
import csv
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Type,
    NamedTuple,
    TypeVar,
)

from dlt.common.json import json
from dlt.common.configuration import configspec, known_sections, with_config
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.data_writers.exceptions import (
    SpecLookupFailed,
    DataWriterNotFound,
    FileFormatForItemFormatNotFound,
    FileSpecNotFound,
    InvalidDataItem,
)
from dlt.common.destination import DestinationCapabilitiesContext, TLoaderFileFormat
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import StrAny

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow as pa


TDataItemFormat = Literal["arrow", "object"]
TWriter = TypeVar("TWriter", bound="DataWriter")


class FileWriterSpec(NamedTuple):
    file_format: TLoaderFileFormat
    """format of the output file"""
    data_item_format: TDataItemFormat
    """format of the input data"""
    file_extension: str
    is_binary_format: bool
    supports_schema_changes: Literal["True", "Buffer", "False"]
    """File format supports changes of schema: True - at any moment, Buffer - in memory buffer before opening file,  False - not at all"""
    requires_destination_capabilities: bool = False
    supports_compression: bool = False


class DataWriterMetrics(NamedTuple):
    file_path: str
    items_count: int
    file_size: int
    created: float
    last_modified: float

    def __add__(self, other: Tuple[object, ...], /) -> Tuple[object, ...]:
        if isinstance(other, DataWriterMetrics):
            return DataWriterMetrics(
                "",  # path is not known
                self.items_count + other.items_count,
                self.file_size + other.file_size,
                min(self.created, other.created),
                max(self.last_modified, other.last_modified),
            )
        return NotImplemented


EMPTY_DATA_WRITER_METRICS = DataWriterMetrics("", 0, 0, 2**32, 0.0)


class DataWriter(abc.ABC):
    def __init__(self, f: IO[Any], caps: DestinationCapabilitiesContext = None) -> None:
        self._f = f
        self._caps = caps
        self.items_count = 0

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:  # noqa
        pass

    def write_data(self, rows: Sequence[Any]) -> None:
        self.items_count += len(rows)

    def write_footer(self) -> None:  # noqa
        pass

    def close(self) -> None:  # noqa
        pass

    def write_all(self, columns_schema: TTableSchemaColumns, rows: Sequence[Any]) -> None:
        self.write_header(columns_schema)
        self.write_data(rows)
        self.write_footer()

    @classmethod
    @abc.abstractmethod
    def writer_spec(cls) -> FileWriterSpec:
        pass

    @classmethod
    def from_file_format(
        cls,
        file_format: TLoaderFileFormat,
        data_item_format: TDataItemFormat,
        f: IO[Any],
        caps: DestinationCapabilitiesContext = None,
    ) -> "DataWriter":
        return cls.class_factory(file_format, data_item_format, ALL_WRITERS)(f, caps)

    @classmethod
    def writer_spec_from_file_format(
        cls, file_format: TLoaderFileFormat, data_item_format: TDataItemFormat
    ) -> FileWriterSpec:
        return cls.class_factory(file_format, data_item_format, ALL_WRITERS).writer_spec()

    @classmethod
    def item_format_from_file_extension(cls, extension: str) -> TDataItemFormat:
        """Simple heuristic to get data item format from file extension"""
        if extension == "typed-jsonl":
            return "object"
        elif extension == "parquet":
            return "arrow"
        else:
            raise ValueError(f"Cannot figure out data item format for extension {extension}")

    @staticmethod
    def writer_class_from_spec(spec: FileWriterSpec) -> Type["DataWriter"]:
        try:
            return WRITER_SPECS[spec]
        except KeyError:
            raise FileSpecNotFound(spec.file_format, spec.data_item_format, spec)

    @staticmethod
    def class_factory(
        file_format: TLoaderFileFormat,
        data_item_format: TDataItemFormat,
        writers: Sequence[Type["DataWriter"]],
    ) -> Type["DataWriter"]:
        for writer in writers:
            spec = writer.writer_spec()
            if spec.file_format == file_format and spec.data_item_format == data_item_format:
                return writer
        raise FileFormatForItemFormatNotFound(file_format, data_item_format)


class JsonlWriter(DataWriter):
    def write_data(self, rows: Sequence[Any]) -> None:
        super().write_data(rows)
        for row in rows:
            json.dump(row, self._f)
            self._f.write(b"\n")

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return FileWriterSpec(
            "jsonl",
            "object",
            file_extension="jsonl",
            is_binary_format=True,
            supports_schema_changes="True",
            supports_compression=True,
        )


class TypedJsonlListWriter(JsonlWriter):
    def write_data(self, rows: Sequence[Any]) -> None:
        # skip JsonlWriter when calling super
        super(JsonlWriter, self).write_data(rows)
        # write all rows as one list which will require to write just one line
        # encode types with PUA characters
        json.typed_dump(rows, self._f)
        self._f.write(b"\n")

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return FileWriterSpec(
            "typed-jsonl",
            "object",
            file_extension="typed-jsonl",
            is_binary_format=True,
            supports_schema_changes="True",
            supports_compression=True,
        )


class InsertValuesWriter(DataWriter):
    def __init__(self, f: IO[Any], caps: DestinationCapabilitiesContext = None) -> None:
        assert (
            caps is not None
        ), "InsertValuesWriter requires destination capabilities to be present"
        super().__init__(f, caps)
        self._chunks_written = 0
        self._headers_lookup: Dict[str, int] = None
        self.writer_type = caps.insert_values_writer_type
        if self.writer_type == "default":
            self.pre, self.post, self.sep = ("(", ")", ",\n")
        elif self.writer_type == "select_union":
            self.pre, self.post, self.sep = ("SELECT ", "", " UNION ALL\n")

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        assert self._chunks_written == 0
        assert columns_schema is not None, "column schema required"
        headers = columns_schema.keys()
        # dict lookup is always faster
        self._headers_lookup = {v: i for i, v in enumerate(headers)}
        # do not write INSERT INTO command, this must be added together with table name by the loader
        self._f.write("INSERT INTO {}(")
        self._f.write(",".join(map(self._caps.escape_identifier, headers)))
        self._f.write(")\n")
        if self.writer_type == "default":
            self._f.write("VALUES\n")

    def write_data(self, rows: Sequence[Any]) -> None:
        super().write_data(rows)

        # do not write empty rows, such things may be produced by Arrow adapters
        if len(rows) == 0:
            return

        def write_row(row: StrAny, last_row: bool = False) -> None:
            output = ["NULL"] * len(self._headers_lookup)
            for n, v in row.items():
                output[self._headers_lookup[n]] = self._caps.escape_literal(v)
            self._f.write(self.pre)
            self._f.write(",".join(output))
            self._f.write(self.post)
            if not last_row:
                self._f.write(self.sep)

        # if next chunk add separator
        if self._chunks_written > 0:
            self._f.write(self.sep)

        # write rows
        for row in rows[:-1]:
            write_row(row)

        # write last row without separator so we can write footer eventually
        write_row(rows[-1], last_row=True)
        self._chunks_written += 1

    def write_footer(self) -> None:
        if self._chunks_written > 0:
            self._f.write(";")

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return FileWriterSpec(
            "insert_values",
            "object",
            file_extension="insert_values",
            is_binary_format=False,
            supports_schema_changes="Buffer",
            supports_compression=True,
            requires_destination_capabilities=True,
        )


@configspec
class ParquetDataWriterConfiguration(BaseConfiguration):
    flavor: Optional[str] = None  # could be ie. "spark"
    version: Optional[str] = "2.4"
    data_page_size: Optional[int] = None
    timestamp_timezone: str = "UTC"
    row_group_size: Optional[int] = None
    coerce_timestamps: Optional[Literal["s", "ms", "us", "ns"]] = None
    allow_truncated_timestamps: bool = False

    __section__: ClassVar[str] = known_sections.DATA_WRITER


class ParquetDataWriter(DataWriter):
    @with_config(spec=ParquetDataWriterConfiguration)
    def __init__(
        self,
        f: IO[Any],
        caps: DestinationCapabilitiesContext = None,
        *,
        flavor: Optional[str] = None,
        version: Optional[str] = "2.4",
        data_page_size: Optional[int] = None,
        timestamp_timezone: str = "UTC",
        row_group_size: Optional[int] = None,
        coerce_timestamps: Optional[Literal["s", "ms", "us", "ns"]] = None,
        allow_truncated_timestamps: bool = False,
    ) -> None:
        super().__init__(f, caps or DestinationCapabilitiesContext.generic_capabilities("parquet"))
        from dlt.common.libs.pyarrow import pyarrow

        self.writer: Optional[pyarrow.parquet.ParquetWriter] = None
        self.schema: Optional[pyarrow.Schema] = None
        self.complex_indices: List[str] = None
        self.parquet_flavor = flavor
        self.parquet_version = version
        self.parquet_data_page_size = data_page_size
        self.timestamp_timezone = timestamp_timezone
        self.parquet_row_group_size = row_group_size
        self.coerce_timestamps = coerce_timestamps
        self.allow_truncated_timestamps = allow_truncated_timestamps

    def _create_writer(self, schema: "pa.Schema") -> "pa.parquet.ParquetWriter":
        from dlt.common.libs.pyarrow import pyarrow, get_py_arrow_timestamp

        # if timestamps are not explicitly coerced, use destination resolution
        # TODO: introduce maximum timestamp resolution, using timestamp_precision too aggressive
        # if not self.coerce_timestamps:
        #     self.coerce_timestamps = get_py_arrow_timestamp(
        #         self._caps.timestamp_precision, "UTC"
        #     ).unit
        #     self.allow_truncated_timestamps = True

        return pyarrow.parquet.ParquetWriter(
            self._f,
            schema,
            flavor=self.parquet_flavor,
            version=self.parquet_version,
            data_page_size=self.parquet_data_page_size,
            coerce_timestamps=self.coerce_timestamps,
            allow_truncated_timestamps=self.allow_truncated_timestamps,
        )

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        from dlt.common.libs.pyarrow import pyarrow, get_py_arrow_datatype

        # build schema
        self.schema = pyarrow.schema(
            [
                pyarrow.field(
                    name,
                    get_py_arrow_datatype(
                        schema_item,
                        self._caps,
                        self.timestamp_timezone,
                    ),
                    nullable=schema_item.get("nullable", True),
                )
                for name, schema_item in columns_schema.items()
            ]
        )
        # find row items that are of the complex type (could be abstracted out for use in other writers?)
        self.complex_indices = [
            i for i, field in columns_schema.items() if field["data_type"] == "complex"
        ]
        self.writer = self._create_writer(self.schema)

    def write_data(self, rows: Sequence[Any]) -> None:
        super().write_data(rows)
        from dlt.common.libs.pyarrow import pyarrow

        # replace complex types with json
        for key in self.complex_indices:
            for row in rows:
                if (value := row.get(key)) is not None:
                    # TODO: make this configurable
                    if value is not None and not isinstance(value, str):
                        row[key] = json.dumps(value)

        table = pyarrow.Table.from_pylist(rows, schema=self.schema)
        # Write
        self.writer.write_table(table, row_group_size=self.parquet_row_group_size)

    def close(self) -> None:  # noqa
        if self.writer:
            self.writer.close()
            self.writer = None

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return FileWriterSpec(
            "parquet",
            "object",
            "parquet",
            is_binary_format=True,
            supports_schema_changes="Buffer",
            requires_destination_capabilities=True,
            supports_compression=False,
        )


CsvQuoting = Literal["quote_all", "quote_needed"]


@configspec
class CsvDataWriterConfiguration(BaseConfiguration):
    delimiter: str = ","
    include_header: bool = True
    quoting: CsvQuoting = "quote_needed"

    __section__: ClassVar[str] = known_sections.DATA_WRITER


class CsvWriter(DataWriter):
    @with_config(spec=CsvDataWriterConfiguration)
    def __init__(
        self,
        f: IO[Any],
        caps: DestinationCapabilitiesContext = None,
        *,
        delimiter: str = ",",
        include_header: bool = True,
        quoting: CsvQuoting = "quote_needed",
        bytes_encoding: str = "utf-8",
    ) -> None:
        super().__init__(f, caps)
        self.include_header = include_header
        self.delimiter = delimiter
        self.quoting: CsvQuoting = quoting
        self.writer: csv.DictWriter[str] = None
        self.bytes_encoding = bytes_encoding

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        self._columns_schema = columns_schema
        if self.quoting == "quote_needed":
            quoting: Literal[1, 2] = csv.QUOTE_NONNUMERIC
        elif self.quoting == "quote_all":
            quoting = csv.QUOTE_ALL
        else:
            raise ValueError(self.quoting)

        self.writer = csv.DictWriter(
            self._f,
            fieldnames=list(columns_schema.keys()),
            extrasaction="ignore",
            dialect=csv.unix_dialect,
            delimiter=self.delimiter,
            quoting=quoting,
        )
        if self.include_header:
            self.writer.writeheader()
        # find row items that are of the complex type (could be abstracted out for use in other writers?)
        self.complex_indices = [
            i for i, field in columns_schema.items() if field["data_type"] == "complex"
        ]
        # find row items that are of the complex type (could be abstracted out for use in other writers?)
        self.bytes_indices = [
            i for i, field in columns_schema.items() if field["data_type"] == "binary"
        ]

    def write_data(self, rows: Sequence[Any]) -> None:
        # convert bytes and json
        if self.complex_indices or self.bytes_indices:
            for row in rows:
                for key in self.complex_indices:
                    if (value := row.get(key)) is not None:
                        row[key] = json.dumps(value)
                for key in self.bytes_indices:
                    if (value := row.get(key)) is not None:
                        # assumed bytes value
                        try:
                            row[key] = value.decode(self.bytes_encoding)
                        except UnicodeError:
                            raise InvalidDataItem(
                                "csv",
                                "object",
                                f"'{key}' contains bytes that cannot be decoded with"
                                f" {self.bytes_encoding}. Remove binary columns or replace their"
                                " content with a hex representation: \\x... while keeping data"
                                " type as binary.",
                            )

        self.writer.writerows(rows)
        # count rows that got written
        self.items_count += sum(len(row) for row in rows)

    def close(self) -> None:
        self.writer = None
        self._first_schema = None

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return FileWriterSpec(
            "csv",
            "object",
            file_extension="csv",
            is_binary_format=False,
            supports_schema_changes="False",
            requires_destination_capabilities=False,
            supports_compression=True,
        )


class ArrowToParquetWriter(ParquetDataWriter):
    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        # Schema will be written as-is from the arrow table
        self._column_schema = columns_schema

    def write_data(self, rows: Sequence[Any]) -> None:
        from dlt.common.libs.pyarrow import pyarrow

        for row in rows:
            if not self.writer:
                self.writer = self._create_writer(row.schema)
            if isinstance(row, pyarrow.Table):
                self.writer.write_table(row, row_group_size=self.parquet_row_group_size)
            elif isinstance(row, pyarrow.RecordBatch):
                self.writer.write_batch(row, row_group_size=self.parquet_row_group_size)
            else:
                raise ValueError(f"Unsupported type {type(row)}")
            # count rows that got written
            self.items_count += row.num_rows

    def write_footer(self) -> None:
        if not self.writer:
            raise NotImplementedError("Arrow Writer does not support writing empty files")
        return super().write_footer()

    def close(self) -> None:
        return super().close()

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return FileWriterSpec(
            "parquet",
            "arrow",
            file_extension="parquet",
            is_binary_format=True,
            supports_schema_changes="False",
            requires_destination_capabilities=False,
            supports_compression=False,
        )


class ArrowToCsvWriter(DataWriter):
    @with_config(spec=CsvDataWriterConfiguration)
    def __init__(
        self,
        f: IO[Any],
        caps: DestinationCapabilitiesContext = None,
        *,
        delimiter: str = ",",
        include_header: bool = True,
        quoting: CsvQuoting = "quote_needed",
    ) -> None:
        super().__init__(f, caps)
        self.delimiter = delimiter
        self._delimiter_b = delimiter.encode("ascii")
        self.include_header = include_header
        self.quoting: CsvQuoting = quoting
        self.writer: Any = None

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        self._columns_schema = columns_schema

    def write_data(self, rows: Sequence[Any]) -> None:
        from dlt.common.libs.pyarrow import pyarrow
        import pyarrow.csv

        for row in rows:
            if isinstance(row, (pyarrow.Table, pyarrow.RecordBatch)):
                if not self.writer:
                    if self.quoting == "quote_needed":
                        quoting = "needed"
                    elif self.quoting == "quote_all":
                        quoting = "all_valid"
                    else:
                        raise ValueError(self.quoting)
                    try:
                        self.writer = pyarrow.csv.CSVWriter(
                            self._f,
                            row.schema,
                            write_options=pyarrow.csv.WriteOptions(
                                include_header=self.include_header,
                                delimiter=self._delimiter_b,
                                quoting_style=quoting,
                            ),
                        )
                        self._first_schema = row.schema
                    except pyarrow.ArrowInvalid as inv_ex:
                        if "Unsupported Type" in str(inv_ex):
                            raise InvalidDataItem(
                                "csv",
                                "arrow",
                                "Arrow data contains a column that cannot be written to csv file"
                                f" ({inv_ex}). Remove nested columns (struct, map) or convert them"
                                " to json strings.",
                            )
                        raise
                # make sure that Schema stays the same
                if not row.schema.equals(self._first_schema):
                    raise InvalidDataItem(
                        "csv",
                        "arrow",
                        "Arrow schema changed without rotating the file. This may be internal"
                        " error or misuse of the writer.\nFirst"
                        f" schema:\n{self._first_schema}\n\nCurrent schema:\n{row.schema}",
                    )

                # write headers only on the first write
                try:
                    self.writer.write(row)
                except pyarrow.ArrowInvalid as inv_ex:
                    if "Invalid UTF8 payload" in str(inv_ex):
                        raise InvalidDataItem(
                            "csv",
                            "arrow",
                            "Arrow data contains string or binary columns with invalid UTF-8"
                            " characters. Remove binary columns or replace their content with a hex"
                            " representation: \\x... while keeping data type as binary.",
                        )
                    if "Timezone database not found" in str(inv_ex):
                        raise InvalidDataItem(
                            "csv",
                            "arrow",
                            str(inv_ex)
                            + ". Arrow does not ship with tzdata on Windows. You need to install it"
                            " yourself:"
                            " https://arrow.apache.org/docs/cpp/build_system.html#runtime-dependencies",
                        )
                    raise
            else:
                raise ValueError(f"Unsupported type {type(row)}")
            # count rows that got written
            self.items_count += row.num_rows

    def write_footer(self) -> None:
        if self.writer is None and self.include_header:
            # write empty file
            self._f.write(
                self._delimiter_b.join(
                    [
                        b'"' + col["name"].encode("utf-8") + b'"'
                        for col in self._columns_schema.values()
                    ]
                )
            )

    def close(self) -> None:
        if self.writer:
            self.writer.close()
            self.writer = None
            self._first_schema = None

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return FileWriterSpec(
            "csv",
            "arrow",
            file_extension="csv",
            is_binary_format=True,
            supports_schema_changes="False",
            requires_destination_capabilities=False,
            supports_compression=True,
        )


class ArrowToObjectAdapter:
    """A mixin that will convert object writer into arrow writer."""

    def write_data(self, rows: Sequence[Any]) -> None:
        for batch in rows:
            # convert to object data item format
            super().write_data(batch.to_pylist())  # type: ignore[misc]

    @staticmethod
    def convert_spec(base: Type[DataWriter]) -> FileWriterSpec:
        spec = base.writer_spec()
        return spec._replace(data_item_format="arrow")


class ArrowToInsertValuesWriter(ArrowToObjectAdapter, InsertValuesWriter):
    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return cls.convert_spec(InsertValuesWriter)


class ArrowToJsonlWriter(ArrowToObjectAdapter, JsonlWriter):
    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return cls.convert_spec(JsonlWriter)


class ArrowToTypedJsonlListWriter(ArrowToObjectAdapter, TypedJsonlListWriter):
    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return cls.convert_spec(TypedJsonlListWriter)


def is_native_writer(writer_type: Type[DataWriter]) -> bool:
    """Checks if writer has adapter mixin. Writers with adapters are not native and typically
    decrease the performance.
    """
    # we only have arrow adapters now
    return not issubclass(writer_type, ArrowToObjectAdapter)


ALL_WRITERS: List[Type[DataWriter]] = [
    JsonlWriter,
    TypedJsonlListWriter,
    InsertValuesWriter,
    ParquetDataWriter,
    CsvWriter,
    ArrowToParquetWriter,
    ArrowToInsertValuesWriter,
    ArrowToJsonlWriter,
    ArrowToTypedJsonlListWriter,
    ArrowToCsvWriter,
]

WRITER_SPECS: Dict[FileWriterSpec, Type[DataWriter]] = {
    writer.writer_spec(): writer for writer in ALL_WRITERS
}

NATIVE_FORMAT_WRITERS: Dict[TDataItemFormat, Tuple[Type[DataWriter], ...]] = {
    # all "object" writers are native object writers (no adapters yet)
    "object": tuple(
        writer
        for writer in ALL_WRITERS
        if writer.writer_spec().data_item_format == "object" and is_native_writer(writer)
    ),
    # exclude arrow adapters
    "arrow": tuple(
        writer
        for writer in ALL_WRITERS
        if writer.writer_spec().data_item_format == "arrow" and is_native_writer(writer)
    ),
}


def resolve_best_writer_spec(
    item_format: TDataItemFormat,
    possible_file_formats: Sequence[TLoaderFileFormat],
    preferred_format: TLoaderFileFormat = None,
) -> FileWriterSpec:
    """Finds best writer for `item_format` out of `possible_file_formats`. Tries `preferred_format` first.
    Best possible writer is a native writer for `item_format` writing files in `preferred_format`.
    If not found, any native writer for `possible_file_formats` is picked.
    Native writer supports `item_format` directly without a need to convert to other item formats.
    """
    native_writers = NATIVE_FORMAT_WRITERS[item_format]
    # check if preferred format has native item_format writer
    if preferred_format:
        if preferred_format not in possible_file_formats:
            raise ValueError(
                f"Preferred format {preferred_format} not possible in {possible_file_formats}"
            )
        try:
            return DataWriter.class_factory(
                preferred_format, item_format, native_writers
            ).writer_spec()
        except DataWriterNotFound:
            pass
    # if not found, use scan native file formats for item format
    for supported_format in possible_file_formats:
        if supported_format != preferred_format:
            try:
                return DataWriter.class_factory(
                    supported_format, item_format, native_writers
                ).writer_spec()
            except DataWriterNotFound:
                pass

    # search all writers
    if preferred_format:
        try:
            return DataWriter.class_factory(
                preferred_format, item_format, ALL_WRITERS
            ).writer_spec()
        except DataWriterNotFound:
            pass

    for supported_format in possible_file_formats:
        if supported_format != preferred_format:
            try:
                return DataWriter.class_factory(
                    supported_format, item_format, ALL_WRITERS
                ).writer_spec()
            except DataWriterNotFound:
                pass

    raise SpecLookupFailed(item_format, possible_file_formats, preferred_format)


def get_best_writer_spec(
    item_format: TDataItemFormat, file_format: TLoaderFileFormat
) -> FileWriterSpec:
    """Gets writer for `item_format` writing files in {file_format}. Looks for native writer first"""
    native_writers = NATIVE_FORMAT_WRITERS[item_format]
    try:
        return DataWriter.class_factory(file_format, item_format, native_writers).writer_spec()
    except DataWriterNotFound:
        return DataWriter.class_factory(file_format, item_format, ALL_WRITERS).writer_spec()
