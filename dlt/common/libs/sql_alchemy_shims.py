"""
Ports fragments of URL class from Sql Alchemy to use them when dependency is not available.
"""

from typing import cast

# port basic functionality without the whole Sql Alchemy

import re
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    overload,
)
import collections.abc as collections_abc
from urllib.parse import (
    quote_plus,
    parse_qsl,
    quote,
    unquote,
)
from dlt.common.exceptions import TypeErrorWithKnownTypes

_KT = TypeVar("_KT", bound=Any)
_VT = TypeVar("_VT", bound=Any)


class ImmutableDict(Dict[_KT, _VT]):
    """Not a real immutable dict"""

    def __setitem__(self, __key: _KT, __value: _VT) -> None:
        raise NotImplementedError("Cannot modify immutable dict")

    def __delitem__(self, _KT: Any) -> None:
        raise NotImplementedError("Cannot modify immutable dict")

    def update(self, *arg: Any, **kw: Any) -> None:
        raise NotImplementedError("Cannot modify immutable dict")


EMPTY_DICT: ImmutableDict[Any, Any] = ImmutableDict()


def to_list(value: Any, default: Optional[List[Any]] = None) -> List[Any]:
    if value is None:
        return default
    if not isinstance(value, collections_abc.Iterable) or isinstance(value, str):
        return [value]
    elif isinstance(value, list):
        return value
    else:
        return list(value)


class URL(NamedTuple):
    """
    Represent the components of a URL used to connect to a database.

    Based on SqlAlchemy URL class with copyright as below:

    # engine/url.py
    # Copyright (C) 2005-2023 the SQLAlchemy authors and contributors
    #
    # This module is part of SQLAlchemy and is released under
    # the MIT License: https://www.opensource.org/licenses/mit-license.php
    """

    drivername: str
    """database backend and driver name, such as `postgresql+psycopg2`"""
    username: Optional[str]
    "username string"
    password: Optional[str]
    """password, which is normally a string but may also be any object that has a `__str__()` method."""
    host: Optional[str]
    """hostname or IP number.  May also be a data source name for some drivers."""
    port: Optional[int]
    """integer port number"""
    database: Optional[str]
    """database name"""
    query: ImmutableDict[str, Union[Tuple[str, ...], str]]
    """an immutable mapping representing the query string.  contains strings
    for keys and either strings or tuples of strings for values"""

    @classmethod
    def create(
        cls,
        drivername: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        query: Mapping[str, Union[Sequence[str], str]] = None,
    ) -> "URL":
        """Create a new `URL` object."""
        return cls(
            cls._assert_str(drivername, "drivername"),
            cls._assert_none_str(username, "username"),
            password,
            cls._assert_none_str(host, "host"),
            cls._assert_port(port),
            cls._assert_none_str(database, "database"),
            cls._str_dict(query or EMPTY_DICT),
        )

    @classmethod
    def _assert_port(cls, port: Optional[int]) -> Optional[int]:
        if port is None:
            return None
        try:
            return int(port)
        except TypeError:
            raise TypeErrorWithKnownTypes("port", port, ["int", None])

    @classmethod
    def _assert_str(cls, v: str, paramname: str) -> str:
        if not isinstance(v, str):
            raise TypeErrorWithKnownTypes("paramname", paramname, ["str"])
        return v

    @classmethod
    def _assert_none_str(cls, v: Optional[str], paramname: str) -> Optional[str]:
        if v is None:
            return v

        return cls._assert_str(v, paramname)

    @classmethod
    def _str_dict(
        cls,
        dict_: Optional[
            Union[
                Sequence[Tuple[str, Union[Sequence[str], str]]],
                Mapping[str, Union[Sequence[str], str]],
            ]
        ],
    ) -> ImmutableDict[str, Union[Tuple[str, ...], str]]:
        if dict_ is None:
            return EMPTY_DICT

        @overload
        def _assert_value(
            val: str,
        ) -> str: ...

        @overload
        def _assert_value(
            val: Sequence[str],
        ) -> Union[str, Tuple[str, ...]]: ...

        def _assert_value(
            val: Union[str, Sequence[str]],
        ) -> Union[str, Tuple[str, ...]]:
            if isinstance(val, str):
                return val
            elif isinstance(val, collections_abc.Sequence):
                return tuple(_assert_value(elem) for elem in val)
            else:
                raise TypeErrorWithKnownTypes("value", val, ["str", "Sequence[str]"])

        def _assert_str(v: str) -> str:
            if not isinstance(v, str):
                raise TypeErrorWithKnownTypes("key", v, ["str"])
            return v

        dict_items: Iterable[Tuple[str, Union[Sequence[str], str]]]
        if isinstance(dict_, collections_abc.Sequence):
            dict_items = dict_
        else:
            dict_items = dict_.items()

        return ImmutableDict(
            {
                _assert_str(key): _assert_value(
                    value,
                )
                for key, value in dict_items
            }
        )

    def set(  # noqa
        self,
        drivername: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        query: Optional[Mapping[str, Union[Sequence[str], str]]] = None,
    ) -> "URL":
        """return a new `URL` object with modifications."""

        kw: Dict[str, Any] = {}
        if drivername is not None:
            kw["drivername"] = drivername
        if username is not None:
            kw["username"] = username
        if password is not None:
            kw["password"] = password
        if host is not None:
            kw["host"] = host
        if port is not None:
            kw["port"] = port
        if database is not None:
            kw["database"] = database
        if query is not None:
            kw["query"] = query

        return self._assert_replace(**kw)

    def _assert_replace(self, **kw: Any) -> "URL":
        """argument checks before calling _replace()"""

        if "drivername" in kw:
            self._assert_str(kw["drivername"], "drivername")
        for name in "username", "host", "database":
            if name in kw:
                self._assert_none_str(kw[name], name)
        if "port" in kw:
            self._assert_port(kw["port"])
        if "query" in kw:
            kw["query"] = self._str_dict(kw["query"])

        return self._replace(**kw)

    def update_query_string(self, query_string: str, append: bool = False) -> "URL":
        return self.update_query_pairs(parse_qsl(query_string), append=append)

    def update_query_pairs(
        self,
        key_value_pairs: Iterable[Tuple[str, Union[str, List[str]]]],
        append: bool = False,
    ) -> "URL":
        """Return a new `URL` object with the `query` parameter dictionary updated by the given sequence of key/value pairs"""
        existing_query = self.query
        new_keys: Dict[str, Union[str, List[str]]] = {}

        for key, value in key_value_pairs:
            if key in new_keys:
                new_keys[key] = to_list(new_keys[key])
                cast("List[str]", new_keys[key]).append(cast(str, value))
            else:
                new_keys[key] = to_list(value) if isinstance(value, (list, tuple)) else value

        new_query: Mapping[str, Union[str, Sequence[str]]]
        if append:
            new_query = {}

            for k in new_keys:
                if k in existing_query:
                    new_query[k] = tuple(to_list(existing_query[k]) + to_list(new_keys[k]))
                else:
                    new_query[k] = new_keys[k]

            new_query.update(
                {k: existing_query[k] for k in set(existing_query).difference(new_keys)}
            )
        else:
            new_query = ImmutableDict(
                {
                    **self.query,
                    **{k: tuple(v) if isinstance(v, list) else v for k, v in new_keys.items()},
                }
            )
        return self.set(query=new_query)

    def update_query_dict(
        self,
        query_parameters: Mapping[str, Union[str, List[str]]],
        append: bool = False,
    ) -> "URL":
        return self.update_query_pairs(query_parameters.items(), append=append)

    def render_as_string(self, hide_password: bool = True) -> str:
        """Render this `URL` object as a string."""
        s = self.drivername + "://"
        if self.username is not None:
            s += quote(self.username, safe=" +")
            if self.password is not None:
                s += ":" + ("***" if hide_password else quote(str(self.password), safe=" +"))
            s += "@"
        if self.host is not None:
            if ":" in self.host:
                s += f"[{self.host}]"
            else:
                s += self.host
        if self.port is not None:
            s += ":" + str(self.port)
        if self.database is not None:
            s += "/" + self.database
        if self.query:
            keys = to_list(self.query)
            keys.sort()
            s += "?" + "&".join(
                f"{quote_plus(k)}={quote_plus(element)}"
                for k in keys
                for element in to_list(self.query[k])
            )
        return s

    def __repr__(self) -> str:
        return self.render_as_string()

    def __copy__(self) -> "URL":
        return self.__class__.create(
            self.drivername,
            self.username,
            self.password,
            self.host,
            self.port,
            self.database,
            self.query.copy(),
        )

    def __deepcopy__(self, memo: Any) -> "URL":
        return self.__copy__()

    def __hash__(self) -> int:
        return hash(str(self))

    def __eq__(self, other: Any) -> bool:
        return (
            isinstance(other, URL)
            and self.drivername == other.drivername
            and self.username == other.username
            and self.password == other.password
            and self.host == other.host
            and self.database == other.database
            and self.query == other.query
            and self.port == other.port
        )

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def get_backend_name(self) -> str:
        """Return the backend name.

        This is the name that corresponds to the database backend in
        use, and is the portion of the `drivername`
        that is to the left of the plus sign.

        """
        if "+" not in self.drivername:
            return self.drivername
        else:
            return self.drivername.split("+")[0]

    def get_driver_name(self) -> str:
        """Return the backend name.

        This is the name that corresponds to the DBAPI driver in
        use, and is the portion of the `drivername`
        that is to the right of the plus sign.
        """

        if "+" not in self.drivername:
            return self.drivername
        else:
            return self.drivername.split("+")[1]


def make_url(name_or_url: Union[str, URL]) -> URL:
    """Given a string, produce a new URL instance.

    The format of the URL generally follows `RFC-1738`, with some exceptions, including
    that underscores, and not dashes or periods, are accepted within the
    "scheme" portion.

    If a `URL` object is passed, it is returned as is."""

    if isinstance(name_or_url, str):
        return _parse_url(name_or_url)
    elif not isinstance(name_or_url, URL):
        raise ValueError(f"Expected string or URL object, got `{name_or_url!r}`")
    else:
        return name_or_url


def _parse_url(name: str) -> URL:
    pattern = re.compile(
        r"""
            (?P<name>[\w\+]+)://
            (?:
                (?P<username>[^:/]*)
                (?::(?P<password>[^@]*))?
            @)?
            (?:
                (?:
                    \[(?P<ipv6host>[^/\?]+)\] |
                    (?P<ipv4host>[^/:\?]+)
                )?
                (?::(?P<port>[^/\?]*))?
            )?
            (?:/(?P<database>[^\?]*))?
            (?:\?(?P<query>.*))?
            """,
        re.X,
    )

    m = pattern.match(name)
    if m is not None:
        components = m.groupdict()
        query: Optional[Dict[str, Union[str, List[str]]]]
        if components["query"] is not None:
            query = {}

            for key, value in parse_qsl(components["query"]):
                if key in query:
                    query[key] = to_list(query[key])
                    cast("List[str]", query[key]).append(value)
                else:
                    query[key] = value
        else:
            query = None

        components["query"] = query
        if components["username"] is not None:
            components["username"] = unquote(components["username"])

        if components["password"] is not None:
            components["password"] = unquote(components["password"])

        ipv4host = components.pop("ipv4host")
        ipv6host = components.pop("ipv6host")
        components["host"] = ipv4host or ipv6host
        name = components.pop("name")

        if components["port"]:
            components["port"] = int(components["port"])

        return URL.create(name, **components)  # type: ignore

    else:
        raise ValueError(f"Could not parse SQLAlchemy URL from string '{name}'")
