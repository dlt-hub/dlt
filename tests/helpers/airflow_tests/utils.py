from configparser import DuplicateSectionError
import os
import argparse
import pytest
from airflow.cli.commands.db_command import resetdb
from airflow.configuration import conf
from airflow.models.variable import Variable

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs import PluggableRunContext
from dlt.common.configuration.providers.vault import SECRETS_TOML_KEY

# Test data
SECRETS_TOML_CONTENT = """
[sources]
api_key = "test_value"
"""


@pytest.fixture(scope="function", autouse=True)
def initialize_airflow_db():
    setup_airflow()
    # backup context providers
    providers = Container()[PluggableRunContext].providers
    # allow airflow provider
    os.environ["PROVIDERS__ENABLE_AIRFLOW_SECRETS"] = "true"
    Variable.set(SECRETS_TOML_KEY, SECRETS_TOML_CONTENT)
    # re-create providers
    Container()[PluggableRunContext].reload_providers()
    yield
    # restore providers
    Container()[PluggableRunContext].providers = providers
    # Make sure the variable is not set
    Variable.delete(SECRETS_TOML_KEY)


def setup_airflow() -> None:
    # Disable loading examples
    try:
        conf.add_section("core")
    except DuplicateSectionError:
        pass
    conf.set("core", "load_examples", "False")
    # Prepare the arguments for the initdb function
    args = argparse.Namespace()
    # becomes database/sql_alchemy_conn in apache 2.7.0
    args.backend = conf.get(section="core", key="sql_alchemy_conn")

    # Run Airflow resetdb before running any tests
    args.yes = True
    args.skip_init = False
    resetdb(args)
