from typing import Tuple, Type

from dlt.common.typing import StrAny
from dlt.common.configuration import make_configuration, PostgresCredentials

from dlt.load.configuration import LoaderClientDwhConfiguration


class RedshiftClientConfiguration(LoaderClientDwhConfiguration):
    CLIENT_TYPE: str = "redshift"


def configuration(initial_values: StrAny = None) -> Tuple[Type[RedshiftClientConfiguration], Type[PostgresCredentials]]:
    return (
        make_configuration(RedshiftClientConfiguration, RedshiftClientConfiguration, initial_values=initial_values),
        make_configuration(PostgresCredentials, PostgresCredentials, initial_values=initial_values)
    )
