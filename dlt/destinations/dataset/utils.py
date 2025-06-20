from typing import Tuple, Dict, Any

import sqlglot
import sqlglot.expressions as sge
from sqlglot.schema import Schema as SQLGlotSchema

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
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.destinations.sql_client import SqlClientBase


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


def normalize_query(
    sqlglot_schema: SQLGlotSchema,
    qualified_query: sge.Query,
    sql_client: SqlClientBase[Any],
) -> sge.Query:
    """Normalizes a qualified query compliant with the dlt schema into the namespace of the source dataset"""

    # this function modifies the incoming query
    qualified_query = qualified_query.copy()

    # do we need to case-fold identifiers?
    caps = sql_client.capabilities
    is_casefolding = caps.casefold_identifier is not str

    # preserve "column" names in original selects which are done in dlt schema namespace
    orig_selects: Dict[int, str] = None
    if is_casefolding:
        orig_selects = {}
        for i, proj in enumerate(qualified_query.selects):
            orig_selects[i] = proj.name or proj.args["alias"].name

    # case fold all identifiers and quote
    for node in qualified_query.walk():
        if isinstance(node, sge.Table):
            # expand named of known tables. this is currently clickhouse things where
            # we use dataset.table in queries but render those as dataset___table
            if sqlglot_schema.column_names(node):
                expanded_path = sql_client.make_qualified_table_name_path(
                    node.name, quote=False, casefold=False
                )
                # set the table name
                if node.name != expanded_path[-1]:
                    node.this.set("this", expanded_path[-1])
                # set the dataset/schema name
                if node.db != expanded_path[-2]:
                    node.set("db", sqlglot.to_identifier(expanded_path[-2], quoted=False))
                # set the catalog name
                if len(expanded_path) == 3:
                    if node.db != expanded_path[0]:
                        node.set("catalog", sqlglot.to_identifier(expanded_path[0], quoted=False))
        # quote and case-fold identifiers, TODO: maybe we could be more intelligent, but then we need to unquote ibis
        if isinstance(node, sge.Identifier):
            if is_casefolding:
                node.set("this", caps.casefold_identifier(node.this))
            node.set("quoted", True)

    # add aliases to output selects to stay compatible with dlt schema after the query
    if orig_selects:
        for i, orig in orig_selects.items():
            case_folded_orig = caps.casefold_identifier(orig)
            if case_folded_orig != orig:
                # somehow we need to alias just top select in UNION (tested on Snowflake)
                sel_expr = qualified_query.selects[i]
                qualified_query.selects[i] = sge.alias_(sel_expr, orig, quoted=True)

    return qualified_query
