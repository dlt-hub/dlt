"""A PEP 249 interface over Arrow Flight SQL, implemented with `pyarrow` alone.

Flight SQL is a protobuf protocol layered on Arrow Flight. `pyarrow` ships the Flight transport but
none of the Flight SQL messages, and running queries needs exactly one of them:
`CommandStatementQuery` wrapped in `google.protobuf.Any`. Both are single length-delimited fields,
encoded here directly so that neither a protobuf runtime nor a native driver becomes a dependency.

Servers that implement only the query surface of Flight SQL are supported: prepared statements are
not used (parameters are escaped into the query) and transactions are no-ops.
"""

from typing import Any, Iterator, List, Optional, Sequence, Tuple

import pyarrow
from pyarrow import flight

from dlt.common.data_writers.escape import escape_lancedb_literal

apilevel = "2.0"
threadsafety = 1
paramstyle = "format"

STATEMENT_QUERY_TYPE_URL = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"


def _encode_varint(value: int) -> bytes:
    encoded = bytearray()
    while True:
        lower_bits = value & 0x7F
        value >>= 7
        encoded.append(lower_bits | (0x80 if value else 0))
        if not value:
            return bytes(encoded)


def _encode_bytes_field(field_number: int, payload: bytes) -> bytes:
    return bytes([(field_number << 3) | 2]) + _encode_varint(len(payload)) + payload


def statement_query_command(query: str) -> bytes:
    """Encodes `query` as a Flight SQL `CommandStatementQuery` inside a `google.protobuf.Any`."""
    command = _encode_bytes_field(1, query.encode("utf-8"))
    return _encode_bytes_field(1, STATEMENT_QUERY_TYPE_URL) + _encode_bytes_field(2, command)


def interpolate_parameters(query: str, parameters: Optional[Sequence[Any]]) -> str:
    """Escapes `parameters` into `query`, used because Flight SQL prepared statements are optional
    and commonly unimplemented by servers."""
    if not parameters:
        return query
    return query % tuple(escape_lancedb_literal(parameter) for parameter in parameters)


def connect(
    uri: str,
    headers: Optional[Sequence[Tuple[bytes, bytes]]] = None,
    timeout: Optional[float] = None,
    tls_root_certs: Optional[bytes] = None,
) -> "FlightSqlConnection":
    client = flight.FlightClient(uri, tls_root_certs=tls_root_certs)
    options = flight.FlightCallOptions(headers=list(headers or []), timeout=timeout)
    return FlightSqlConnection(client, options)


class FlightSqlCursor:
    arraysize: int = 1000

    def __init__(self, connection: "FlightSqlConnection") -> None:
        self.connection = connection
        self._schema: Optional[pyarrow.Schema] = None
        self._batches: Optional[Iterator[pyarrow.RecordBatch]] = None
        self._rows: List[Tuple[Any, ...]] = []
        self._row_index = 0

    @property
    def description(self) -> Optional[List[Tuple[str, Any, Any, Any, Any, Any, Any]]]:
        if self._schema is None:
            return None
        return [(field.name, field.type, None, None, None, None, None) for field in self._schema]

    @property
    def rowcount(self) -> int:
        return -1

    def execute(
        self, query: str, parameters: Optional[Sequence[Any]] = None, *args: Any, **kwargs: Any
    ) -> None:
        if isinstance(query, bytes):
            query = query.decode("utf-8")
        descriptor = flight.FlightDescriptor.for_command(
            statement_query_command(interpolate_parameters(query, parameters))
        )
        flight_info = self.connection.client.get_flight_info(descriptor, self.connection.options)
        self._schema = flight_info.schema
        self._batches = self._read_endpoints(flight_info)
        self._rows = []
        self._row_index = 0

    def _read_endpoints(self, flight_info: flight.FlightInfo) -> Iterator[pyarrow.RecordBatch]:
        for endpoint in flight_info.endpoints:
            reader = self.connection.client.do_get(endpoint.ticket, self.connection.options)
            for chunk in reader:
                if chunk.data is not None:
                    yield chunk.data

    def _next_batch(self) -> Optional[pyarrow.RecordBatch]:
        if self._batches is None:
            return None
        return next(self._batches, None)

    def fetch_arrow_table(self) -> pyarrow.Table:
        batches = []
        while (batch := self._next_batch()) is not None:
            batches.append(batch)
        return pyarrow.Table.from_batches(batches, self._schema)

    def iter_arrow_tables(self, chunk_size: Optional[int] = None) -> Iterator[pyarrow.Table]:
        if not chunk_size:
            yield self.fetch_arrow_table()
            return
        pending: List[pyarrow.RecordBatch] = []
        pending_rows = 0
        while (batch := self._next_batch()) is not None:
            pending.append(batch)
            pending_rows += batch.num_rows
            # server batches may be larger than `chunk_size`, so split rather than overshoot
            while pending_rows >= chunk_size:
                table = pyarrow.Table.from_batches(pending, self._schema)
                yield table.slice(0, chunk_size)
                remainder = table.slice(chunk_size)
                pending = remainder.to_batches()
                pending_rows = remainder.num_rows
        if pending_rows:
            yield pyarrow.Table.from_batches(pending, self._schema)

    def _buffer_rows(self, size: Optional[int]) -> None:
        while size is None or len(self._rows) - self._row_index < size:
            batch = self._next_batch()
            if batch is None:
                return
            self._rows.extend(zip(*(column.to_pylist() for column in batch.columns)))

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        rows = self.fetchmany(1)
        return rows[0] if rows else None

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        size = self.arraysize if size is None else size
        self._buffer_rows(size)
        rows = self._rows[self._row_index : self._row_index + size]
        self._row_index += len(rows)
        return rows

    def fetchall(self) -> List[Tuple[Any, ...]]:
        self._buffer_rows(None)
        rows = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return rows

    def close(self) -> None:
        self._batches = None
        self._rows = []
        self._row_index = 0

    def __enter__(self) -> "FlightSqlCursor":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class FlightSqlConnection:
    def __init__(self, client: flight.FlightClient, options: flight.FlightCallOptions) -> None:
        self.client = client
        self.options = options

    def cursor(self) -> FlightSqlCursor:
        return FlightSqlCursor(self)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> "FlightSqlConnection":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
