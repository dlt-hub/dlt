"""
  Copyright (C) 2017-2021 Dremio Corporation

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

The code in this module was original from https://github.com/dremio-hub/arrow-flight-client-examples/tree/main/python.
The code has been modified and extended to provide a PEP 249 compatible interface.

This implementation will eagerly gather the full result set after every query.
Eventually, this module should be replaced with ADBC Flight SQL client.
See: https://github.com/apache/arrow-adbc/issues/1559
"""

from dataclasses import dataclass, field
from datetime import datetime  # noqa: I251
from http.cookies import SimpleCookie
from typing import Any, List, Tuple, Optional, Mapping, Dict, AnyStr

import pyarrow
import pytz
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


def format_datetime(d: datetime) -> str:
    return d.astimezone(pytz.UTC).replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")


def format_parameter(param: Any) -> str:
    if isinstance(param, str):
        return quote_string(param)
    elif isinstance(param, datetime):
        return quote_string(format_datetime(param))
    else:
        return str(param)


class MalformedQueryError(Exception):
    pass


def parameterize_query(query: str, parameters: Optional[Tuple[Any, ...]]) -> str:
    parameters = parameters or ()
    parameters = tuple(format_parameter(p) for p in parameters)
    try:
        return query % parameters
    except TypeError as ex:
        raise MalformedQueryError(*ex.args)


def execute_query(connection: "DremioConnection", query: str) -> pyarrow.Table:
    flight_desc = flight.FlightDescriptor.for_command(query)
    flight_info = connection.client.get_flight_info(flight_desc, connection.options)
    return connection.client.do_get(flight_info.endpoints[0].ticket, connection.options).read_all()


def _any_str_to_str(string: AnyStr) -> str:
    if isinstance(string, bytes):
        return string.decode()
    else:
        return string


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

    def execute(
        self, query: AnyStr, parameters: Optional[Tuple[Any, ...]] = None, *args: Any, **kwargs: Any
    ) -> None:
        query_str = _any_str_to_str(query)
        parameterized_query = parameterize_query(query_str, parameters)
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
        return result[0] if result else None

    def fetch_arrow_table(self) -> pyarrow.Table:
        table = self.table
        self.table = pyarrow.table({col.name: [] for col in table.schema}, schema=table.schema)
        return table

    def close(self) -> None:
        pass

    def __enter__(self) -> "DremioCursor":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
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

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        self.close()


class DremioAuthError(Exception):
    pass


class DremioClientAuthMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates DremioClientAuthMiddleware(s)."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.call_credential: Optional[Tuple[bytes, bytes]] = None

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

    def __init__(self, factory: flight.ClientMiddlewareFactory, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.factory = factory

    def received_headers(self, headers: Mapping[str, str]) -> None:
        auth_header_key = "authorization"
        authorization_header = None
        for key in headers:
            if key.lower() == auth_header_key:
                authorization_header = headers.get(auth_header_key)
        if authorization_header:
            self.factory.set_call_credential(
                (b"authorization", authorization_header[0].encode("utf-8"))
            )


class CookieMiddlewareFactory(flight.ClientMiddlewareFactory):
    """A factory that creates CookieMiddleware(s)."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.cookies: Dict[str, Any] = {}

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

    def __init__(self, factory: CookieMiddlewareFactory, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.factory = factory

    def received_headers(self, headers: Mapping[str, str]) -> None:
        for key in headers:
            if key.lower() == "set-cookie":
                cookie = SimpleCookie()
                for item in headers.get(key):
                    cookie.load(item)

                self.factory.cookies.update(cookie.items())

    def sending_headers(self) -> Dict[bytes, bytes]:
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
    location: str, tls_root_certs: Optional[bytes] = None, **kwargs: Any
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
