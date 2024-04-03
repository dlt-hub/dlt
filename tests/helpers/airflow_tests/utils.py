from configparser import DuplicateSectionError
import os
import argparse
import pytest
from airflow.cli.commands.db_command import resetdb
from airflow.configuration import conf
from airflow.models.variable import Variable

from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_providers_context import (
    ConfigProvidersContext,
)
from dlt.common.configuration.providers.toml import SECRETS_TOML_KEY


@pytest.fixture(scope="function", autouse=True)
def initialize_airflow_db():
    setup_airflow()
    # backup context providers
    providers = Container()[ConfigProvidersContext]
    # allow airflow provider
    os.environ["PROVIDERS__ENABLE_AIRFLOW_SECRETS"] = "true"
    # re-create providers
    del Container()[ConfigProvidersContext]
    yield
    # restore providers
    Container()[ConfigProvidersContext] = providers
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
