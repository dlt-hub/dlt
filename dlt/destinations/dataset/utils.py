from typing import Tuple

from dlt import version

from dlt.common.exceptions import MissingDependencyException

from dlt.common.destination import AnyDestination
from dlt.common.destination.reference import (
    Destination,
    JobClientBase,
    DestinationClientDwhConfiguration,
    DestinationClientStagingConfiguration,
    DestinationClientConfiguration,
    DestinationClientDwhWithStagingConfiguration,
)

from dlt.common.schema import Schema


# helpers
def get_destination_client_initial_config(
    destination: AnyDestination,
    default_schema_name: str,
    dataset_name: str,
    as_staging: bool = False,
) -> DestinationClientConfiguration:
    client_spec = destination.spec

    # this client supports many schemas and datasets
    if issubclass(client_spec, DestinationClientDwhConfiguration):
        if issubclass(client_spec, DestinationClientStagingConfiguration):
            spec: DestinationClientDwhConfiguration = client_spec(as_staging_destination=as_staging)
        else:
            spec = client_spec()

        spec._bind_dataset_name(dataset_name, default_schema_name)
        return spec

    return client_spec()


def get_destination_clients(
    schema: Schema,
    destination: AnyDestination = None,
    destination_dataset_name: str = None,
    destination_initial_config: DestinationClientConfiguration = None,
    staging: AnyDestination = None,
    staging_dataset_name: str = None,
    staging_initial_config: DestinationClientConfiguration = None,
    # pipeline specific settings
    default_schema_name: str = None,
) -> Tuple[JobClientBase, JobClientBase]:
    destination = Destination.from_reference(destination) if destination else None
    staging = Destination.from_reference(staging) if staging else None

    try:
        # resolve staging config in order to pass it to destination client config
        staging_client = None
        if staging:
            if not staging_initial_config:
                # this is just initial config - without user configuration injected
                staging_initial_config = get_destination_client_initial_config(
                    staging,
                    dataset_name=staging_dataset_name,
                    default_schema_name=default_schema_name,
                    as_staging=True,
                )
            # create the client - that will also resolve the config
            staging_client = staging.client(schema, staging_initial_config)

        if not destination_initial_config:
            # config is not provided then get it with injected credentials
            initial_config = get_destination_client_initial_config(
                destination,
                dataset_name=destination_dataset_name,
                default_schema_name=default_schema_name,
            )

        # attach the staging client config to destination client config - if its type supports it
        if (
            staging_client
            and isinstance(initial_config, DestinationClientDwhWithStagingConfiguration)
            and isinstance(staging_client.config, DestinationClientStagingConfiguration)
        ):
            initial_config.staging_config = staging_client.config
        # create instance with initial_config properly set
        client = destination.client(schema, initial_config)
        return client, staging_client
    except ModuleNotFoundError:
        client_spec = destination.spec()
        raise MissingDependencyException(
            f"{client_spec.destination_type} destination",
            [f"{version.DLT_PKG_NAME}[{client_spec.destination_type}]"],
            "Dependencies for specific destinations are available as extras of dlt",
        )
