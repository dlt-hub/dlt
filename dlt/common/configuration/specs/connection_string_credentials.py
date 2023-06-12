from typing import Any, ClassVar, Dict, List, Optional
from sqlalchemy.engine import URL, make_url
from dlt.common.configuration.specs.exceptions import InvalidConnectionString

from dlt.common.typing import TSecretValue
from dlt.common.configuration.specs.base_configuration import CredentialsConfiguration, configspec


@configspec
class ConnectionStringCredentials(CredentialsConfiguration):
    drivername: str = None
    database: str = None
    password: Optional[TSecretValue] = None
    username: str = None
    host: Optional[str] = None
    port: Optional[int] = None
    query: Optional[Dict[str, str]] = None

    __config_gen_annotations__: ClassVar[List[str]] = ["port", "password", "host"]

    def parse_native_representation(self, native_value: Any) -> None:
        if not isinstance(native_value, str):
            raise InvalidConnectionString(self.__class__, native_value, self.drivername)
        try:
            url = make_url(native_value)
            # update only values that are not None
            self.update(
                {k: v for k,v in url._asdict().items() if v is not None}
            )
            if self.query is not None:
                self.query = dict(self.query)
        except Exception:
            raise InvalidConnectionString(self.__class__, native_value, self.drivername)

    def on_resolved(self) -> None:
        if self.password:
            self.password = TSecretValue(self.password.strip())

    def to_native_representation(self) -> str:
        return self.to_url().render_as_string(hide_password=False)

    def to_url(self) -> URL:
        return URL.create(self.drivername, self.username, self.password, self.host, self.port, self.database, self.query)

    def __str__(self) -> str:
        return self.to_url().render_as_string(hide_password=True)
