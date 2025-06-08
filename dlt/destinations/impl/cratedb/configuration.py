import dataclasses
from typing import Final
from dlt.common.configuration import configspec
from dlt.destinations.impl.postgres.configuration import (
    PostgresCredentials,
    PostgresClientConfiguration,
)


@configspec(init=False)
class CrateDbCredentials(PostgresCredentials):
    drivername: Final[str] = dataclasses.field(
        default="postgres", init=False, repr=False, compare=False
    )  # type: ignore


# CrateDB does not support databases, just schemas.
# In dlt, schemas are conveyed by `dataset_name`?
del CrateDbCredentials.__dataclass_fields__["database"]


@configspec
class CrateDbClientConfiguration(PostgresClientConfiguration):
    destination_type: Final[str] = dataclasses.field(
        default="cratedb", init=False, repr=False, compare=False
    )  # type: ignore
    credentials: CrateDbCredentials = None
