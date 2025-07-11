from typing import Tuple
from dlt.common.exceptions import MissingDependencyException
from dlt.common.destination import AnyDestination, Destination
from dlt.common.destination.client import (
    JobClientBase,
    DestinationClientDwhConfiguration,
    DestinationClientStagingConfiguration,
    DestinationClientConfiguration,
    DestinationClientDwhWithStagingConfiguration,
)
from dlt.common.schema import Schema


def get_destination_client_initial_config(
    destination: AnyDestination,
    dataset_name: str,
    multi_dataset_default_schema_name: str = None,
    as_staging: bool = False,
) -> DestinationClientConfiguration:
    client_spec = destination.spec

    # this client supports many schemas and datasets
    if issubclass(client_spec, DestinationClientDwhConfiguration):
        if issubclass(client_spec, DestinationClientStagingConfiguration):
            config: DestinationClientDwhConfiguration = client_spec(
                as_staging_destination=as_staging
            )
        else:
            config = client_spec()
        config._bind_dataset_name(
            dataset_name, default_schema_name=multi_dataset_default_schema_name
        )
    else:
        config = client_spec()  # type: ignore[assignment]

    return config


def get_destination_clients(
    schema: Schema,
    destination: AnyDestination = None,
    destination_dataset_name: str = None,
    multi_dataset_default_schema_name: str = None,
    staging: AnyDestination = None,
    staging_dataset_name: str = None,
) -> Tuple[JobClientBase, JobClientBase]:
    """Creates destination and/or staging job clients and binds them to `schema` and dataset names.
    Configuration in destination SPEC will be resolved.

    if `multi_dataset_default_schema_name` is set, each `schema` gets own dataset name
    as `dataset_name__{schema.name}`, except the default schema that gets `dataset_name`

    """

    destination = Destination.from_reference(destination) if destination else None
    staging = Destination.from_reference(staging) if staging else None

    try:
        # resolve staging config in order to pass it to destination client config
        staging_client = None
        if staging:
            staging_initial_config = get_destination_client_initial_config(
                staging,
                dataset_name=staging_dataset_name,
                multi_dataset_default_schema_name=multi_dataset_default_schema_name,
                as_staging=True,
            )
            # create the client - that will also resolve the config
            staging_client = staging.client(schema, staging_initial_config)

        initial_config = get_destination_client_initial_config(
            destination,
            dataset_name=destination_dataset_name,
            multi_dataset_default_schema_name=multi_dataset_default_schema_name,
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
        # TODO: support destinations defined in other packages ie. by importing
        # top level module of destination type and getting PKG_NAME from version module of it
        from dlt import version

        client_spec = destination.spec()
        raise MissingDependencyException(
            f"{client_spec.destination_type} destination",
            [f"{version.DLT_PKG_NAME}[{client_spec.destination_type}]"],
            "Dependencies for specific destinations are available as extras of dlt",
        )
