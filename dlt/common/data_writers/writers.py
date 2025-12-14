import abc
import csv
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from dlt.common.configuration import with_config
from dlt.common.data_writers.exceptions import (
    DataWriterNotFound,
    FileFormatForItemFormatNotFound,
    FileSpecNotFound,
    InvalidDataItem,
    SpecLookupFailed,
)
from dlt.common.destination import (
    LOADER_FILE_FORMATS,
    DestinationCapabilitiesContext,
    TLoaderFileFormat,
)
from dlt.common.destination.configuration import (
    CsvFormatConfiguration,
    CsvQuoting,
    IPCFormatConfiguration,
    ParquetFormatConfiguration,
)
from dlt.common.exceptions import ValueErrorWithKnownValues
from dlt.common.json import json
from dlt.common.metrics import DataWriterMetrics
from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import StrAny, TDataItem, TDataItems
from packaging.version import Version

if TYPE_CHECKING:
    from dlt.common.libs.pyarrow import pyarrow as pa


TDataItemFormat = Literal["arrow", "object", "file", "model"]
TWriter = TypeVar("TWriter", bound="DataWriter")


class FileWriterSpec(NamedTuple):
    file_format: TLoaderFileFormat
    """format of the output file"""
    data_item_format: TDataItemFormat
    """format of the input data"""
    file_extension: str
    is_binary_format: bool
    supports_schema_changes: Literal["True", "Buffer", "False"]
    """File format supports changes of schema: True - at any moment, Buffer - in memory buffer before opening file, False - not at all"""
    requires_destination_capabilities: bool = False
    supports_compression: bool = False
    file_max_items: Optional[int] = None
    """Set an upper limit on the number of items in one file"""


EMPTY_DATA_WRITER_METRICS = DataWriterMetrics("", 0, 0, 2**32, 0.0)


class DataWriter(abc.ABC):
    def __init__(self, f: IO[Any], caps: DestinationCapabilitiesContext = None) -> None:
        self._f = f
        self._caps = caps
        self.items_count = 0

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:  # noqa
        pass

    def write_data(self, items: Sequence[TDataItem]) -> None:
        self.items_count += len(items)

    def write_footer(self) -> None:  # noqa
        pass

    def close(self) -> None:  # noqa
        pass

    def write_all(self, columns_schema: TTableSchemaColumns, items: Sequence[TDataItem]) -> None:
        self.write_header(columns_schema)
        self.write_data(items)
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
        elif extension == "model":
            return "model"
        # those files may be imported by normalizer as is
        elif extension in LOADER_FILE_FORMATS:
            return "file"
        else:
            raise ValueError(f"No `data_item_format` associated with file extension: `{extension}`")

    @staticmethod
    def writer_class_from_spec(spec: FileWriterSpec) -> Type["DataWriter"]:
        try:
            return WRITER_SPECS[spec]
        except KeyError:
            if spec.data_item_format == "file":
                return ImportFileWriter
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


class ImportFileWriter(DataWriter):
    """May only import files, fails on any open/write operations"""

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        raise NotImplementedError(
            "`ImportFileWriter` cannot write any files. You have a bug in your code."
        )

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        raise NotImplementedError("`ImportFileWriter` has no single spec")


class JsonlWriter(DataWriter):
    def write_data(self, items: Sequence[TDataItem]) -> None:
        super().write_data(items)
        for row in items:
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


class ModelWriter(DataWriter):
    """Writes incoming items row by row into a text file and ensures a trailing ;"""

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        pass

    def write_data(self, items: Sequence[TDataItem]) -> None:
        super().write_data(items)
        for item in items:
            dialect = item.query_dialect
            query = item.to_sql()
            self._f.write("dialect: " + (dialect or "") + "\n" + query + "\n")

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        return FileWriterSpec(
            "model",
            "model",
            file_extension="model",
            is_binary_format=False,
            supports_schema_changes="True",
            supports_compression=False,
            # NOTE: we create a new model file for each sql row
            file_max_items=1,
        )


class TypedJsonlListWriter(JsonlWriter):
    def write_data(self, items: Sequence[TDataItem]) -> None:
        # skip JsonlWriter when calling super
        super(JsonlWriter, self).write_data(items)
        # write all rows as one list which will require to write just one line
        # encode types with PUA characters
        json.typed_dump(items, self._f)
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

    def write_data(self, items: Sequence[TDataItem]) -> None:
        super().write_data(items)

        # do not write empty rows, such things may be produced by Arrow adapters
        if len(items) == 0:
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
        for row in items[:-1]:
            write_row(row)

        # write last row without separator so we can write footer eventually
        write_row(items[-1], last_row=True)
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


class ParquetDataWriter(DataWriter):
    @with_config(spec=ParquetFormatConfiguration)
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
        use_compliant_nested_type: bool = True,
        _format: ParquetFormatConfiguration = None,  # will receive the full config
    ) -> None:
        super().__init__(f, caps or DestinationCapabilitiesContext.generic_capabilities("parquet"))
        from dlt.common.libs.pyarrow import pyarrow

        self.writer: Optional[pyarrow.parquet.ParquetWriter] = None
        self.schema: Optional[pyarrow.Schema] = None
        self.nested_indices: List[str] = None
        # merge parquet format
        if self._caps.parquet_format is not None:
            self.parquet_format = self._caps.parquet_format.copy()
            self.parquet_format.update(_format.as_dict_nondefault())
        else:
            self.parquet_format = _format

    def _create_writer(self, schema: "pa.Schema") -> "pa.parquet.ParquetWriter":
        from dlt.common.libs.pyarrow import pyarrow

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
            flavor=self.parquet_format.flavor,
            version=self.parquet_format.version,
            data_page_size=self.parquet_format.data_page_size,
            coerce_timestamps=self.parquet_format.coerce_timestamps,
            allow_truncated_timestamps=self.parquet_format.allow_truncated_timestamps,
            use_compliant_nested_type=self.parquet_format.use_compliant_nested_type,
        )

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        from dlt.common.libs.pyarrow import columns_to_arrow

        # build schema
        self.schema = columns_to_arrow(
            columns_schema, self._caps, self.parquet_format.timestamp_timezone
        )
        # find row items that are of the json type (could be abstracted out for use in other writers?)
        self.nested_indices = [
            i for i, field in columns_schema.items() if field["data_type"] == "json"
        ]
        self.writer = self._create_writer(self.schema)

    def write_data(self, items: Sequence[TDataItem]) -> None:
        super().write_data(items)
        from dlt.common.libs.pyarrow import pyarrow

        # serialize json types and replace with strings
        for key in self.nested_indices:
            for row in items:
                if (value := row.get(key)) is not None:
                    # TODO: make this configurable
                    if value is not None and not isinstance(value, str):
                        row[key] = json.dumps(value)

        table = pyarrow.Table.from_pylist(items, schema=self.schema)
        # detect non-null columns receiving nulls. above v.19 it is checked in `write_table`
        if Version(pyarrow.__version__).major < 19:
            table = table.cast(self.schema)
        # Write
        self.writer.write_table(table, row_group_size=self.parquet_format.row_group_size)

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


class IPCDataWriter(DataWriter):
    """Apache Arrow IPC Feather v2 format writer for Python object data.

    This writer focuses on the IPC File Format only, not the IPC Stream Format.

    Converts Python dictionaries/objects to Arrow format and writes to IPC files.
    Input: Python objects (dicts)
    Output: Arrow IPC format
    """

    @with_config(spec=IPCFormatConfiguration)
    def __init__(
        self,
        f: IO[Any],
        caps: DestinationCapabilitiesContext = None,
        *,
        allow_64bit: bool = False,
        compression: Optional[Literal["lz4", "zstd"]] = None,
        use_threads: bool = True,
        emit_dictionary_deltas: bool = False,
        unify_dictionaries: bool = False,
        _format: IPCFormatConfiguration = None,  # will receive the full config
    ) -> None:
        """Initialises the IPC data writer with the given configuration"""
        super().__init__(f, caps or DestinationCapabilitiesContext.generic_capabilities("ipc"))
        from dlt.common.libs.pyarrow import pyarrow

        self.writer: Optional[Union[pyarrow.ipc.RecordBatchFileWriter]] = None
        self.schema: Optional[pyarrow.Schema] = None
        self.nested_indices: List[str] = None
        self.file_metadata: Optional[Dict[str, str]] = None
        if self._caps.ipc_format is not None:
            self.ipc_format = self._caps.ipc_format.copy()
            self.ipc_format.update(_format.as_dict_nondefault())
        else:
            self.ipc_format = _format

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        """Writes the IPC file header and prepares the writer for data rows"""
        from dlt.common.libs.pyarrow import columns_to_arrow, pyarrow

        # build schema
        self.schema = columns_to_arrow(columns_schema, self._caps)
        # find row items that are of the json type
        self.nested_indices = [
            i for i, field in columns_schema.items() if field["data_type"] == "json"
        ]

        options = pyarrow.ipc.IpcWriteOptions(
            allow_64bit=self.ipc_format.allow_64bit,
            compression=self.ipc_format.compression,
            use_threads=self.ipc_format.use_threads,
            emit_dictionary_deltas=self.ipc_format.emit_dictionary_deltas,
            unify_dictionaries=self.ipc_format.unify_dictionaries,
        )

        self.writer = pyarrow.ipc.new_file(self._f, self.schema, options=options)

    def write_data(self, items: Sequence[TDataItem]) -> None:
        """Writes data rows into the IPC file"""
        super().write_data(items)
        from dlt.common.libs.pyarrow import pyarrow

        # serialise json types and replace with strings
        for key in self.nested_indices:
            for row in items:
                if (value := row.get(key)) is not None:
                    if value is not None and not isinstance(value, str):
                        row[key] = json.dumps(value)

        table = pyarrow.Table.from_pylist(items, schema=self.schema)
        # detect non-null columns receiving nulls. above v.19 it is checked in `write_table`
        if Version(pyarrow.__version__).major < 19:
            table = table.cast(self.schema)

        self.writer.write_table(table)

    def close(self) -> None:
        """Closes the IPC writer"""
        if self.writer:
            self.writer.close()
            self.writer = None

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        """Returns the writer specification for IPC format"""
        return FileWriterSpec(
            file_format="ipc",
            data_item_format="object",
            file_extension="arrow",
            is_binary_format=True,
            supports_schema_changes="False",  # IPC is fixed once the schema is written
            requires_destination_capabilities=True,
            supports_compression=False,  # IPC compression is handled internally
        )


class CsvWriter(DataWriter):
    @with_config(spec=CsvFormatConfiguration)
    def __init__(
        self,
        f: IO[Any],
        caps: DestinationCapabilitiesContext = None,
        *,
        delimiter: str = ",",
        include_header: bool = True,
        quoting: CsvQuoting = "quote_needed",
        lineterminator: str = "\n",
        bytes_encoding: str = "utf-8",
    ) -> None:
        super().__init__(f, caps)
        self.include_header = include_header
        self.delimiter = delimiter
        self.quoting: CsvQuoting = quoting
        self.lineterminator = lineterminator
        self.writer: csv.DictWriter[str] = None
        self.bytes_encoding = bytes_encoding

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        self._columns_schema = columns_schema
        if self.quoting == "quote_needed":
            quoting: Literal[0, 1, 2, 3] = csv.QUOTE_NONNUMERIC
        elif self.quoting == "quote_all":
            quoting = csv.QUOTE_ALL
        elif self.quoting == "quote_none":
            quoting = csv.QUOTE_NONE
        elif self.quoting == "quote_minimal":
            quoting = csv.QUOTE_MINIMAL
        else:
            raise ValueError(self.quoting)

        self.writer = csv.DictWriter(
            self._f,
            fieldnames=list(columns_schema.keys()),
            extrasaction="ignore",
            dialect=csv.unix_dialect,
            delimiter=self.delimiter,
            quoting=quoting,
            lineterminator=self.lineterminator,
        )
        if self.include_header:
            self.writer.writeheader()
        # find row items that are of the json type
        self.nested_indices = [
            i for i, field in columns_schema.items() if field["data_type"] == "json"
        ]
        # find row items that are of the binary type
        self.bytes_indices = [
            i for i, field in columns_schema.items() if field["data_type"] == "binary"
        ]

    def write_data(self, items: Sequence[TDataItem]) -> None:
        # convert bytes and json
        if self.nested_indices or self.bytes_indices:
            for row in items:
                for key in self.nested_indices:
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
                                f"'{key}' contains bytes that cannot be decoded with encoding"
                                f" `{self.bytes_encoding}`. Remove binary columns or replace their"
                                " content with a hex representation: \\x... while keeping data"
                                " type as binary.",
                            )

        self.writer.writerows(items)
        # count rows that got written
        self.items_count += sum(len(row) for row in items)

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

    def write_data(self, items: Sequence[TDataItem]) -> None:
        from dlt.common.libs.pyarrow import concat_batches_and_tables_in_order

        if not items:
            return
        # concat batches and tables into a single one, preserving order
        # pyarrow writer starts a row group for each item it writes (even with 0 rows)
        # it also converts batches into tables internally. by creating a single table
        # we allow the user rudimentary control over row group size via max buffered items
        table = concat_batches_and_tables_in_order(items)
        self.items_count += table.num_rows
        if not self.writer:
            self.writer = self._create_writer(table.schema)
        # write concatenated tables
        self.writer.write_table(table, row_group_size=self.parquet_format.row_group_size)

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


class ArrowToIPCWriter(IPCDataWriter):
    """Apache Arrow IPC Feather v2 format writer for Arrow data.

    This writer focuses on the IPC File Format only, not the IPC Stream Format.

    Writes Arrow tables/batches directly to IPC format without conversion.
    Input: Arrow tables/batches
    Output: Arrow IPC format
    """

    def write_header(self, columns_schema: TTableSchemaColumns) -> None:
        """Writes the IPC file header and prepares the writer for data rows

        Args:
            columns_schema (TTableSchemaColumns): The schema of the columns to be written.

        Returns:
            None
        """
        # Schema will be written as-is from the arrow table
        self._column_schema = columns_schema

    def write_data(self, items: Sequence[TDataItem]) -> None:
        """Writes data rows into the IPC file

        Args:
            items (Sequence[TDataItem]): The data items to be written.

        Returns:
            None
        """
        from dlt.common.libs.pyarrow import concat_batches_and_tables_in_order, pyarrow

        if not items:
            return

        table = concat_batches_and_tables_in_order(items)
        self.items_count += table.num_rows

        if not self.writer:
            options = pyarrow.ipc.IpcWriteOptions(
                allow_64bit=self.ipc_format.allow_64bit,
                compression=self.ipc_format.compression,
                use_threads=self.ipc_format.use_threads,
                emit_dictionary_deltas=self.ipc_format.emit_dictionary_deltas,
                unify_dictionaries=self.ipc_format.unify_dictionaries,
            )

            self.writer = pyarrow.ipc.new_file(self._f, table.schema, options=options)

        self.writer.write_table(table)

    def write_footer(self) -> None:
        """Finalises the IPC file writing process."""
        if not self.writer:
            raise NotImplementedError("IPC Writer does not support writing empty files")
        return super().write_footer()

    def close(self) -> None:
        return super().close()

    @classmethod
    def writer_spec(cls) -> FileWriterSpec:
        """Returns the writer specification for IPC format"""
        return FileWriterSpec(
            file_format="ipc",
            data_item_format="arrow",
            file_extension="arrow",
            is_binary_format=True,
            supports_schema_changes="False",  # IPC is fixed once the schema is written
            requires_destination_capabilities=True,
            supports_compression=False,  # IPC compression is handled internally
        )


class ArrowToCsvWriter(DataWriter):
    @with_config(spec=CsvFormatConfiguration)
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

    def write_data(self, items: Sequence[TDataItem]) -> None:
        import pyarrow.csv
        from dlt.common.libs.pyarrow import pyarrow

        for item in items:
            if isinstance(item, (pyarrow.Table, pyarrow.RecordBatch)):
                if not self.writer:
                    try:
                        self.writer = pyarrow.csv.CSVWriter(
                            self._f,
                            item.schema,
                            write_options=pyarrow.csv.WriteOptions(
                                # set include_header to False to handle header separately until
                                # https://github.com/apache/arrow/issues/47575 is released
                                # see _make_csv_header() for details
                                include_header=False,
                                delimiter=self._delimiter_b,
                                quoting_style=self._get_arrow_quoting_style(),
                            ),
                        )
                        if self.include_header:
                            self._f.write(self._make_csv_header())
                        self._first_schema = item.schema
                    except pyarrow.ArrowInvalid as inv_ex:
                        if "Unsupported Type" in str(inv_ex):
                            raise InvalidDataItem(
                                "csv",
                                "arrow",
                                "Arrow data contains a column that cannot be written to csv file"
                                f" `{inv_ex}`. Remove nested columns (struct, map) or convert them"
                                " to json strings.",
                            )
                        raise
                # make sure that Schema stays the same
                if not item.schema.equals(self._first_schema):
                    raise InvalidDataItem(
                        "csv",
                        "arrow",
                        "Arrow schema changed without rotating the file. This may be internal"
                        " error or misuse of the writer.\nFirst"
                        f" schema:\n{self._first_schema}\n\nCurrent schema:\n{item.schema}",
                    )

                # write headers only on the first write
                try:
                    self.writer.write(item)
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
                raise ValueError(f"Unsupported type `{type(item)}`")
            # count rows that got written
            self.items_count += item.num_rows

    def write_footer(self) -> None:
        default_arrow_line_terminator = b"\n"
        if self.writer is None and self.include_header:
            # empty file: emit only the header line (no data rows)
            self._f.write(self._make_csv_header().rstrip(default_arrow_line_terminator))

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

    def _get_arrow_quoting_style(self) -> str:
        if self.quoting == "quote_needed":
            return "needed"
        elif self.quoting == "quote_all":
            return "all_valid"
        elif self.quoting == "quote_none":
            return "none"
        else:
            raise ValueError(self.quoting)

    def _make_csv_header(self) -> bytes:
        # In pyarrow 21.0.0, the CSVWriter does not support specifying the header quote style.
        # This is a workaround to create a header which respects the quote style.
        # See https://github.com/apache/arrow/issues/47575 for details.
        # This needs to be removed once https://github.com/apache/arrow/issues/47575 is released.
        import pyarrow.csv
        from dlt.common.libs.pyarrow import pyarrow

        names = [col["name"] for col in self._columns_schema.values()]
        arrays = [pyarrow.array([n]) for n in names]
        schema = pyarrow.schema([pyarrow.field(n, pyarrow.string()) for n in names])
        table = pyarrow.Table.from_arrays(arrays, schema=schema)

        # Write into an in-memory Arrow sink so schema doesn't affect the real writer
        sink = pyarrow.BufferOutputStream()
        header_writer = pyarrow.csv.CSVWriter(
            sink,
            schema,
            write_options=pyarrow.csv.WriteOptions(
                include_header=False,
                delimiter=self._delimiter_b,
                quoting_style=self._get_arrow_quoting_style(),
            ),
        )
        header_writer.write(table)
        header_writer.close()
        return cast(bytes, sink.getvalue().to_pybytes())


class ArrowToObjectAdapter:
    """A mixin that will convert object writer into arrow writer."""

    def write_data(self, items: Sequence[TDataItem]) -> None:
        for batch in items:
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
    IPCDataWriter,
    CsvWriter,
    ArrowToParquetWriter,
    ArrowToIPCWriter,
    ArrowToInsertValuesWriter,
    ArrowToJsonlWriter,
    ArrowToTypedJsonlListWriter,
    ArrowToCsvWriter,
    ModelWriter,
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
    "model": tuple(
        writer
        for writer in ALL_WRITERS
        if writer.writer_spec().data_item_format == "model" and is_native_writer(writer)
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
            raise ValueErrorWithKnownValues(
                "preferred_format", preferred_format, possible_file_formats
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


def create_import_spec(
    item_file_format: TLoaderFileFormat,
    possible_file_formats: Sequence[TLoaderFileFormat],
) -> FileWriterSpec:
    """Creates writer spec that may be used only to import files"""
    # can the item file be directly imported?
    if item_file_format not in possible_file_formats:
        raise SpecLookupFailed("file", possible_file_formats, item_file_format)

    spec = DataWriter.class_factory(item_file_format, "object", ALL_WRITERS).writer_spec()
    return spec._replace(data_item_format="file")


def count_rows_in_items(item: TDataItems) -> int:
    """Count total number of rows of `items` which may be
    * single item
    * list of single items
    * list of tables like data frames or arrow
    """

    if isinstance(item, list):
        # if item supports "shape" it will be used to count items
        if len(item) > 0 and hasattr(item[0], "shape"):
            return sum(len(tbl) for tbl in item)
        else:
            return len(item)
    else:
        # update row count, if item supports "num_rows" it will be used to count items
        if hasattr(item, "shape"):
            return len(item)
        else:
            return 1
