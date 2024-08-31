from typing import List, Optional, Sequence

from dlt.common import logger
from dlt.common.configuration.inject import with_config
from dlt.common.destination.exceptions import (
    DestinationCapabilitiesException,
    IdentifierTooLongException,
)
from dlt.common.schema import Schema
from dlt.common.schema.exceptions import (
    SchemaIdentifierNormalizationCollision,
)
from dlt.common.schema.typing import TLoaderMergeStrategy, TSchemaTables, TTableSchema
from dlt.common.schema.utils import get_merge_strategy
from dlt.common.typing import ConfigValue, DictStrStr

from .capabilities import DestinationCapabilitiesContext


def verify_schema_capabilities(
    schema: Schema,
    load_tables: Sequence[TTableSchema],
    capabilities: DestinationCapabilitiesContext,
    destination_type: str,
    warnings: bool = True,
) -> List[Exception]:
    """Verifies `load_tables` that have all hints filled by job client before loading against capabilities.
    Returns a list of exceptions representing critical problems with the schema.
    It will log warnings by default. It is up to the caller to eventually raise exception

    * Checks all table and column name lengths against destination capabilities and raises on too long identifiers
    * Checks if schema has collisions due to case sensitivity of the identifiers
    """

    # collect all exceptions to show all problems in the schema
    exception_log: List[Exception] = []
    # combined casing function
    case_identifier = lambda ident: capabilities.casefold_identifier(
        (str if capabilities.has_case_sensitive_identifiers else str.casefold)(ident)  # type: ignore
    )
    table_name_lookup: DictStrStr = {}
    # name collision explanation
    collision_msg = "Destination is case " + (
        "sensitive" if capabilities.has_case_sensitive_identifiers else "insensitive"
    )
    if capabilities.casefold_identifier is not str:
        collision_msg += (
            f" but it uses {capabilities.casefold_identifier} to generate case insensitive"
            " identifiers. You may try to change the destination capabilities by changing the"
            " `casefold_identifier` to `str`"
        )
    collision_msg += (
        ". Please clean up your data before loading so the entities have different name. You can"
        " also change to case insensitive naming convention. Note that in that case data from both"
        " columns will be merged into one."
    )

    # check for any table clashes
    for table in load_tables:
        table_name = table["name"]
        # detect table name conflict
        cased_table_name = case_identifier(table_name)
        if cased_table_name in table_name_lookup:
            conflict_table_name = table_name_lookup[cased_table_name]
            exception_log.append(
                SchemaIdentifierNormalizationCollision(
                    schema.name,
                    table_name,
                    "table",
                    table_name,
                    conflict_table_name,
                    schema.naming.name(),
                    collision_msg,
                )
            )
        table_name_lookup[cased_table_name] = table_name
        if len(table_name) > capabilities.max_identifier_length:
            exception_log.append(
                IdentifierTooLongException(
                    destination_type,
                    "table",
                    table_name,
                    capabilities.max_identifier_length,
                )
            )

        column_name_lookup: DictStrStr = {}
        for column_name in dict(table["columns"]):
            # detect table name conflict
            cased_column_name = case_identifier(column_name)
            if cased_column_name in column_name_lookup:
                conflict_column_name = column_name_lookup[cased_column_name]
                exception_log.append(
                    SchemaIdentifierNormalizationCollision(
                        schema.name,
                        table_name,
                        "column",
                        column_name,
                        conflict_column_name,
                        schema.naming.name(),
                        collision_msg,
                    )
                )
            column_name_lookup[cased_column_name] = column_name
            if len(column_name) > capabilities.max_column_identifier_length:
                exception_log.append(
                    IdentifierTooLongException(
                        destination_type,
                        "column",
                        f"{table_name}.{column_name}",
                        capabilities.max_column_identifier_length,
                    )
                )
    return exception_log


@with_config
def resolve_merge_strategy(
    tables: TSchemaTables,
    table: TTableSchema,
    destination_capabilities: Optional[DestinationCapabilitiesContext] = ConfigValue,
) -> Optional[TLoaderMergeStrategy]:
    """Resolve merge strategy for a table, possibly resolving the 'x-merge-strategy from a table chain. strategies selector in `destination_capabilities`
    is used if present. If `table` does not contain strategy hint, a default value will be used which is the first.

    `destination_capabilities` are injected from context if not explicitly passed.

    Returns None if table write disposition is not merge
    """
    if table.get("write_disposition") == "merge":
        destination_capabilities = (
            destination_capabilities or DestinationCapabilitiesContext.generic_capabilities()
        )
        supported_strategies = destination_capabilities.supported_merge_strategies
        table_name = table["name"]
        if destination_capabilities.merge_strategies_selector:
            supported_strategies = destination_capabilities.merge_strategies_selector(
                supported_strategies, table_schema=table
            )
        if not supported_strategies:
            table_format_info = ""
            if destination_capabilities.supported_table_formats:
                table_format_info = (
                    " or try different table format which may offer `merge`:"
                    f" {destination_capabilities.supported_table_formats}"
                )
            logger.warning(
                "Destination does not support any merge strategies and `merge` write disposition "
                f" for table `{table_name}` cannot be met and will fall back to `append`. Change"
                f" write disposition{table_format_info}."
            )
            return None
        merge_strategy = get_merge_strategy(tables, table_name)
        # use first merge strategy as default
        if merge_strategy is None and supported_strategies:
            merge_strategy = supported_strategies[0]
        if merge_strategy not in supported_strategies:
            raise DestinationCapabilitiesException(
                f"`{merge_strategy}` merge strategy not supported"
                f" for table `{table_name}`. Available strategies: {supported_strategies}"
            )
        return merge_strategy
    return None
