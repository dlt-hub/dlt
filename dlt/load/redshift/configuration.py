from typing import Tuple

from dlt.common.typing import StrAny
from dlt.common.configuration import configspec, make_configuration, PostgresCredentials

from dlt.load.configuration import LoaderClientDwhConfiguration


@configspec
class RedshiftClientConfiguration(LoaderClientDwhConfiguration):
    client_type: str = "redshift"


def configuration(initial_values: StrAny = None) -> Tuple[RedshiftClientConfiguration, PostgresCredentials]:
    return (
        make_configuration(RedshiftClientConfiguration(), initial_value=initial_values),
        make_configuration(PostgresCredentials(), initial_value=initial_values)
    )
