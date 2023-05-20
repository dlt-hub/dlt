from typing import Iterator
from contextlib import contextmanager

from dlt.load import Load
from dlt.common.schema import Schema
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.destination.reference import DestinationReference
from dlt.destinations import filesystem
from dlt.destinations.filesystem.configuration import FilesystemClientConfiguration
from dlt.destinations.filesystem.filesystem import FilesystemClient

from tests.utils import clean_test_storage, init_test_logging, TEST_DICT_CONFIG_PROVIDER


def get_client(schema: Schema, dataset_name: str) -> FilesystemClient:
    config = filesystem.spec()(dataset_name=dataset_name)
    with Container().injectable_context(ConfigSectionContext(sections=('filesystem',))):
        return filesystem.client(schema, config)  # type: ignore[return-value]


def setup_loader(dataset_name: str) -> Load:
    destination: DestinationReference = filesystem  # type: ignore[assignment]
    config = filesystem.spec()(dataset_name=dataset_name)
    # setup loader
    with Container().injectable_context(ConfigSectionContext(sections=('filesystem',))):
        return Load(
            destination,
            initial_client_config=config
        )
