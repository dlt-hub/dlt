from dlt.common.configuration import configspec

from dlt.destinations.impl.sqlalchemy.configuration import (
    SqlalchemyCredentials,
    SqlalchemyClientConfiguration
)

@configspec(init = False)
class StarrocksCredentials(SqlalchemyCredentials):
    drivername: str = "starrocks"
    http_port: int = 8040
    http_host: str = None

@configspec
class StarrocksClientConfiguration(SqlalchemyClientConfiguration):
    credentials: StarrocksCredentials = None
