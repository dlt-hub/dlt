import dataclasses
from typing import Any, ClassVar, Dict, List, Optional, Union

# avoid importing sqlalchemy
from dlt.common.libs.sql_alchemy_shims import URL
from dlt.common.configuration.specs.exceptions import InvalidConnectionString
from dlt.common.typing import TSecretStrValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec


@configspec
class ConnectionStringCredentials(CredentialsConfiguration):
    drivername: str = dataclasses.field(default=None, init=False, repr=False, compare=False)
    database: Optional[str] = None
    password: Optional[TSecretStrValue] = None
    username: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    query: Optional[Dict[str, Any]] = None

    __config_gen_annotations__: ClassVar[List[str]] = [
        "database",
        "port",
        "username",
        "password",
        "host",
    ]

    def __init__(self, connection_string: Union[str, Dict[str, Any]] = None) -> None:
        """Initializes the credentials from SQLAlchemy like connection string or from dict holding connection string elements"""
        super().__init__()
        self._apply_init_value(connection_string)

    def parse_native_representation(self, native_value: Any) -> None:
        if not isinstance(native_value, str):
            raise InvalidConnectionString(self.__class__, native_value, self.drivername)
        try:
            from dlt.common.libs.sql_alchemy_compat import make_url

            url = make_url(native_value)
            # update only values that are not None
            self.update({k: v for k, v in url._asdict().items() if v is not None})
            if self.query is not None:
                # query may be immutable so make it mutable
                self.query = dict(self.query)
        except Exception:
            raise InvalidConnectionString(self.__class__, native_value, self.drivername)

    def on_resolved(self) -> None:
        if self.password:
            self.password = self.password.strip()

    def to_native_representation(self) -> str:
        return self.to_url().render_as_string(hide_password=False)

    def get_query(self) -> Dict[str, Any]:
        """Gets query preserving parameter types. Mostly used internally to export connection params"""
        return {} if self.query is None else self.query

    def to_url(self) -> URL:
        """Creates SQLAlchemy compatible URL object, computes current query via `get_query` and serializes its values to str"""
        # circular dependencies here
        from dlt.common.configuration.utils import serialize_value

        def _serialize_value(v_: Any) -> str:
            if v_ is None:
                return None
            return serialize_value(v_)

        # query must be str -> str
        query = {k: _serialize_value(v) for k, v in self.get_query().items()}

        # import "real" URL
        from dlt.common.libs.sql_alchemy_compat import URL

        return URL.create(
            self.drivername,
            self.username,
            self.password,
            self.host,
            self.port,
            self.database,
            query,
        )

    def __str__(self) -> str:
        url = self.to_url()
        # do not display query. it often contains secret values
        url = url._replace(query=None)
        # we only have control over netloc/path
        return url.render_as_string(hide_password=True)
