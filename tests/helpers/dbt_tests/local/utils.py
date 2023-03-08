
import contextlib
from typing import Iterator, NamedTuple
from dlt.common import logger
from dlt.common.configuration.utils import add_config_to_env
from dlt.common.runners import Venv
from dlt.common.typing import StrAny
from dlt.helpers.dbt.configuration import DBTRunnerConfiguration
from dlt.helpers.dbt.runner import DBTPackageRunner, create_runner

from tests.load.utils import cm_yield_client
from tests.utils import TEST_STORAGE_ROOT


FIXTURES_DATASET_NAME = "test_fixture_carbon_bot_session_cases"


class DBTDestinationInfo(NamedTuple):
    destination_name: str
    replace_strategy: str
    incremental_strategy: str


def setup_rasa_runner(profile_name: str, dataset_name: str = None, override_values: StrAny = None) -> DBTPackageRunner:

    C = DBTRunnerConfiguration()
    C.package_location = "https://github.com/scale-vector/rasa_semantic_schema.git"  # "/home/rudolfix/src/dbt/rasa_semantic_schema"
    C.package_repository_branch = "dlt-dbt-runner-ci-do-not-delete"

    # override values including the defaults above
    if override_values:
        C.update(override_values)
        # for k,v in override_values.items():
        #     setattr(C, k, v)

    runner = create_runner(
        Venv.restore_current(),
        None,   # credentials are exported to env in setup_rasa_runner_client
        TEST_STORAGE_ROOT,
        dataset_name or FIXTURES_DATASET_NAME,
        package_profile_name=profile_name,
        config=C
    )
    # now C is resolved
    logger.init_logging_from_config(C.runtime)
    return runner


@contextlib.contextmanager
def setup_rasa_runner_client(destination_name: str, destination_dataset_name: str) -> Iterator[None]:
    with cm_yield_client(destination_name, FIXTURES_DATASET_NAME) as client:
        # emit environ so credentials are passed to dbt profile
        add_config_to_env(client.config.credentials)
        yield
        # delete temp schemas
        dataset_name = f"{destination_dataset_name}_views"
        try:
            with client.sql_client.with_alternative_dataset_name(dataset_name):
                client.sql_client.drop_dataset()
        except Exception as ex1:
            logger.error(f"Error when deleting temp dataset {dataset_name}: {str(ex1)}")

        dataset_name = f"{destination_dataset_name}_staging"
        try:
            with client.sql_client.with_alternative_dataset_name(dataset_name):
                client.sql_client.drop_dataset()
        except Exception as ex2:
            logger.error(f"Error when deleting temp dataset {dataset_name}: {str(ex2)}")

        dataset_name = f"{destination_dataset_name}_event"
        try:
            with client.sql_client.with_alternative_dataset_name(dataset_name):
                client.sql_client.drop_dataset()
        except Exception as ex2:
            logger.error(f"Error when deleting temp dataset {dataset_name}: {str(ex2)}")