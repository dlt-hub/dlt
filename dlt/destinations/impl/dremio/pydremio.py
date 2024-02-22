"""
Provides a PEP 249 compatible wrapper around the Arrow Flight client.
This implementation will always eagerly gather the full result set after every query.
This is not ideal, however ultimately this entire module should be replaced with ADBC Flight SQL client.
See: https://github.com/apache/arrow-adbc/issues/1559
"""

from dataclasses import dataclass, field
from http.cookies import SimpleCookie
from typing import Any, List, Tuple, Optional, AnyStr, Mapping

import pyarrow
from pyarrow import flight

apilevel = "2.0"
threadsafety = 2
paramstyle = "format"


def connect(
    uri: str,
    db_kwargs: Optional[Mapping[str, Any]] = None,
    conn_kwargs: Optional[Mapping[str, Any]] = None,
) -> "DremioConnection":
    username = db_kwargs["username"]
    password = db_kwargs["password"]
    tls_root_certs = db_kwargs.get("tls_root_certs")
    client = create_flight_client(location=uri, tls_root_certs=tls_root_certs)
    options = create_flight_call_options(
        username=username,
        password=password,
        client=client,
    )
    return DremioConnection(
        client=client,
        options=options,
    )


def quote_string(string: str) -> str:
    return "'" + string.strip("'") + "'"


def parameterize_query(query: str, parameters: Optional[Tuple[Any, ...]]) -> str:
    parameters = parameters or ()
    parameters = tuple(quote_string(p) if isinstance(p, str) else p for p in parameters)
    return query % parameters


def execute_query(connection: "DremioConnection", query: str) -> pyarrow.Table:
    flight_desc = flight.FlightDescriptor.for_command(query)
    flight_info = connection.client.get_flight_info(flight_desc, connection.options)
    return connection.client.do_get(flight_info.endpoints[0].ticket, connection.options).read_all()


@dataclass
class DremioCursor:
    connection: "DremioConnection"
    table: pyarrow.Table = field(init=False, default_factory=lambda: pyarrow.table([]))

    @property
    def description(self) -> List[Tuple[str, pyarrow.DataType, Any, Any, Any, Any, Any]]:
        return [(fld.name, fld.type, None, None, None, None, None) for fld in self.table.schema]

    @property
    def rowcount(self) -> int:
        return len(self.table)

    def execute(self, query: AnyStr, parameters: Optional[Mapping[str, Any]] = None) -> None:
        parameterized_query = parameterize_query(query, parameters)
        self.table = execute_query(self.connection, parameterized_query)

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return self.fetchmany()

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple[Any, ...]]:
        if size is None:
            size = len(self.table)
        pylist = self.table.to_pylist()
        self.table = pyarrow.Table.from_pylist(pylist[size:])
        return [tuple(d.values()) for d in pylist[:size]]

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        result = self.fetchmany(1)
        return result if result else None

    def fetch_arrow_table(self) -> pyarrow.Table:
        table = self.table
        self.table = pyarrow.table([], schema=table.schema)
        return table

    def close(self) -> None:
        pass

    def __enter__(self) -> "DremioCursor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


@dataclass(frozen=True)
class DremioConnection:
    client: flight.FlightClient
    options: flight.FlightCallOptions

    def close(self) -> None:
        self.client.close()

    def cursor(self) -> DremioCursor:
        return DremioCursor(self)

    def __enter__(self) -> "DremioConnection":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class DremioAuthError(Exception):
    pass


class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates DremioClientAuthMiddleware(s)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.call_credential = None

    def start_call(self, info: flight.CallInfo) -> flight.ClientMiddleware:
        return DremioClientAuthMiddleware(self)

    def set_call_credential(self, call_credential: Tuple[bytes, bytes]) -> None:
        self.call_credential = call_credential


class DremioClientAuthMiddleware(flight.ClientMiddleware):
    """
    A ClientMiddleware that extracts the bearer token from
    the authorization header returned by the Dremio
    Flight Server Endpoint.

    Parameters
    ----------
    factory : ClientHeaderAuthMiddlewareFactory
        The factory to set call credentials if an
        authorization header with bearer token is
        returned by the Dremio server.
    """

    def __init__(self, factory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.factory = factory

    def received_headers(self, headers):
        auth_header_key = "authorization"
        authorization_header = []
        for key in headers:
            if key.lower() == auth_header_key:
                authorization_header = headers.get(auth_header_key)
        if not authorization_header:
            raise DremioAuthError("Did not receive authorization header back from server.")
        self.factory.set_call_credential(
            (b"authorization", authorization_header[0].encode("utf-8"))
        )


class CookieMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates CookieMiddleware(s)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cookies = {}

    def start_call(self, info: flight.CallInfo) -> flight.ClientMiddleware:
        return CookieMiddleware(self)


class CookieMiddleware(flight.ClientMiddleware):
    """
    A ClientMiddleware that receives and retransmits cookies.
    For simplicity, this does not auto-expire cookies.

    Parameters
    ----------
    factory : CookieMiddlewareFactory
        The factory containing the currently cached cookies.
    """

    def __init__(self, factory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.factory = factory

    def received_headers(self, headers):
        for key in headers:
            if key.lower() == "set-cookie":
                cookie = SimpleCookie()
                for item in headers.get(key):
                    cookie.load(item)

                self.factory.cookies.update(cookie.items())

    def sending_headers(self):
        if self.factory.cookies:
            cookie_string = "; ".join(
                "{!s}={!s}".format(key, val.value) for (key, val) in self.factory.cookies.items()
            )
            return {b"cookie": cookie_string.encode("utf-8")}
        return {}


# def tls_root_certs() -> bytes:
#     with open("certs/ca-certificates.crt", "rb") as f:
#         return f.read()


def create_flight_client(
    location: str, tls_root_certs: Optional[bytes] = None, **kwargs
) -> flight.FlightClient:
    return flight.FlightClient(
        location=location,
        tls_root_certs=tls_root_certs,
        middleware=[DremioClientAuthMiddlewareFactory(), CookieMiddlewareFactory()],
        **kwargs,
    )


def create_flight_call_options(
    username: str, password: str, client: flight.FlightClient
) -> flight.FlightCallOptions:
    headers: List[Any] = []
    # Retrieve bearer token and append to the header for future calls.
    bearer_token = client.authenticate_basic_token(
        username,
        password,
        flight.FlightCallOptions(headers=headers),
    )
    headers.append(bearer_token)
    return flight.FlightCallOptions(headers=headers)
