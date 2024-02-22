import abc
from dataclasses import dataclass, field
from http.cookies import SimpleCookie
from typing import Any, List, Tuple, Optional, AnyStr, Mapping

import pyarrow
from pyarrow import flight

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


class _Closeable(abc.ABC):
    """Base class providing context manager interface."""

    def __enter__(self) -> "Self":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @abc.abstractmethod
    def close(self) -> None:
        ...


def connect(location: str, username: str, password: str, tls_root_certs: Optional[bytes] = None) -> "DremioConnection":
    client = create_flight_client(location=location, tls_root_certs=tls_root_certs)
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


def quote_string_parameters(parameters: Mapping[str, Any]) -> Mapping[str, Any]:
    return {k: quote_string(v) if isinstance(v, str) else v for k, v in parameters.items()}


def parameterize_query(query: str, parameters: Optional[Mapping[str, Any]]) -> str:
    parameters = parameters or {}
    parameters = quote_string_parameters(parameters)
    return query % parameters


def execute_query(connection: "DremioConnection", query: str) -> pyarrow.Table:
    flight_desc = flight.FlightDescriptor.for_command(query)
    flight_info = connection.client.get_flight_info(flight_desc, connection.options)
    return connection.client.do_get(flight_info.endpoints[0].ticket, connection.options).read_all()


@dataclass
class DremioCursor(_Closeable):
    connection: "DremioConnection"
    table: pyarrow.Table = field(init=False, default_factory=lambda: pyarrow.table([]))

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

    def close(self) -> None:
        pass


@dataclass(frozen=True)
class DremioConnection(_Closeable):
    client: flight.FlightClient
    options: flight.FlightCallOptions

    def close(self) -> None:
        self.client.close()

    def cursor(self) -> DremioCursor:
        return DremioCursor(self)


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
            raise DremioAuthError(
                "Did not receive authorization header back from server."
            )
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
                "{!s}={!s}".format(key, val.value)
                for (key, val) in self.factory.cookies.items()
            )
            return {b"cookie": cookie_string.encode("utf-8")}
        return {}


# def tls_root_certs() -> bytes:
#     with open("certs/ca-certificates.crt", "rb") as f:
#         return f.read()


def create_flight_client(location: str, tls_root_certs: Optional[bytes] = None, **kwargs) -> flight.FlightClient:
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
