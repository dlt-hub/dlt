import contextlib
from typing import Iterator, NamedTuple

from dlt.common.configuration.utils import add_config_to_env
from dlt.common.destination.reference import DestinationClientDwhConfiguration
from dlt.common.runners import Venv
from dlt.common.typing import StrAny

from dlt.helpers.dbt.configuration import DBTRunnerConfiguration
from dlt.helpers.dbt.runner import DBTPackageRunner, create_runner

from tests.load.utils import cm_yield_client, delete_dataset
from tests.utils import TEST_STORAGE_ROOT, init_test_logging


FIXTURES_DATASET_NAME = "test_fixture_carbon_bot_session_cases"


class DBTDestinationInfo(NamedTuple):
    destination_name: str
    replace_strategy: str
    incremental_strategy: str


def setup_rasa_runner(
    profile_name: str, dataset_name: str = None, override_values: StrAny = None
) -> DBTPackageRunner:
    C = DBTRunnerConfiguration()
    C.package_location = (  # "/home/rudolfix/src/dbt/rasa_semantic_schema"
        "https://github.com/scale-vector/rasa_semantic_schema.git"
    )
    C.package_repository_branch = "dlt-dbt-runner-ci-do-not-delete"

    # override values including the defaults above
    if override_values:
        C.update(override_values)
        # for k,v in override_values.items():
        #     setattr(C, k, v)

    runner = create_runner(
        Venv.restore_current(),
        # credentials are exported to env in setup_rasa_runner_client
        DestinationClientDwhConfiguration(dataset_name=dataset_name or FIXTURES_DATASET_NAME),
        TEST_STORAGE_ROOT,
        package_profile_name=profile_name,
        config=C,
    )
    # now C is resolved
    init_test_logging(C.runtime)
    return runner


@contextlib.contextmanager
def setup_rasa_runner_client(
    destination_name: str, destination_dataset_name: str
) -> Iterator[None]:
    with cm_yield_client(destination_name, FIXTURES_DATASET_NAME) as client:
        # emit environ so credentials are passed to dbt profile
        add_config_to_env(client.config, ("DLT",))
        yield
        # delete temp schemas
        dataset_name = f"{destination_dataset_name}_views"
        delete_dataset(client.sql_client, dataset_name)
        dataset_name = f"{destination_dataset_name}_staging"
        delete_dataset(client.sql_client, dataset_name)
        dataset_name = f"{destination_dataset_name}_event"
        delete_dataset(client.sql_client, dataset_name)
