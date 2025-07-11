import contextlib
from typing import Dict, Iterable, List, Optional, Set

from dlt.common import logger
from dlt.common.configuration.inject import with_config
from dlt.common.destination.exceptions import (
    DestinationCapabilitiesException,
    IdentifierTooLongException,
)
from dlt.common.destination.typing import PreparedTableSchema
from dlt.common.destination.exceptions import UnsupportedDataType
from dlt.common.destination.capabilities import DestinationCapabilitiesContext, LOADER_FILE_FORMATS
from dlt.common.schema import Schema
from dlt.common.schema.exceptions import (
    SchemaIdentifierNormalizationCollision,
    TableNotFound,
)
from dlt.common.schema.typing import (
    TColumnType,
    TLoaderMergeStrategy,
    TLoaderReplaceStrategy,
    TSchemaTables,
    TTableSchema,
)
from dlt.common.schema.utils import (
    fill_hints_from_parent_and_clone_table,
    get_merge_strategy,
    is_complete_column,
    find_incomplete_columns,
)
from dlt.common.storages import ParsedLoadJobFileName
from dlt.common.typing import ConfigValue, DictStrStr, TLoaderFileFormat


def verify_schema_capabilities(
    schema: Schema,
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
        (str if capabilities.has_case_sensitive_identifiers else str.casefold)(ident)
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
    for table in schema.data_tables():
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


def column_type_to_str(column: TColumnType) -> str:
    """Converts column type to db-like type string"""
    data_type: str = column["data_type"]
    precision = column.get("precision")
    scale = column.get("scale")
    if precision is not None and scale is not None:
        data_type += f"({precision},{scale})"
    elif precision is not None:
        data_type += f"({precision})"
    return data_type


def verify_supported_data_types(
    prepared_tables: Iterable[PreparedTableSchema],
    new_jobs: Iterable[ParsedLoadJobFileName],
    capabilities: DestinationCapabilitiesContext,
    destination_type: str,
    warnings: bool = True,
) -> List[Exception]:
    exception_log: List[Exception] = []
    # can't check types without type mapper
    if capabilities.type_mapper is None or not new_jobs:
        return exception_log

    type_mapper = capabilities.get_type_mapper()

    # index available file formats
    table_file_formats: Dict[str, Set[TLoaderFileFormat]] = {}
    for parsed_file in new_jobs:
        formats = table_file_formats.setdefault(parsed_file.table_name, set())
        if parsed_file.file_format in LOADER_FILE_FORMATS:
            formats.add(parsed_file.file_format)  # type: ignore[arg-type]
    # all file formats
    all_file_formats = set(capabilities.supported_loader_file_formats or []) | set(
        capabilities.supported_staging_file_formats or []
    )

    for table in prepared_tables:
        # map types
        for column in table["columns"].values():
            # do not verify incomplete columns, those won't be created
            if not is_complete_column(column):
                continue
            try:
                type_mapper.to_destination_type(column, table)
            except Exception as ex:
                # collect mapping exceptions
                exception_log.append(ex)
            # ensure if types can be loaded from file formats present in jobs
            for format_ in table_file_formats.get(table["name"], []):
                try:
                    type_mapper.ensure_supported_type(column, table, format_)
                except ValueError as err:
                    # figure out where data type is supported
                    available_in_formats: List[TLoaderFileFormat] = []
                    for candidate_format in all_file_formats - set([format_]):
                        with contextlib.suppress(Exception):
                            type_mapper.ensure_supported_type(column, table, candidate_format)
                            available_in_formats.append(candidate_format)
                    exception_log.append(
                        UnsupportedDataType(
                            destination_type,
                            table["name"],
                            column["name"],
                            column_type_to_str(column),
                            format_,
                            available_in_formats,
                            err.args[0],
                        )
                    )

    return exception_log


@with_config
def prepare_load_table(
    tables: TSchemaTables,
    table: TTableSchema,
    destination_capabilities: DestinationCapabilitiesContext = ConfigValue,
) -> PreparedTableSchema:
    """Prepares a table schema to be loaded by filling missing hints and doing other modifications required by given destination.

    `destination_capabilities` are injected from context if not explicitly passed.

    Returns: prepared table, note: `table` is cloned
    """
    table_name = table["name"]
    try:
        prep_table = fill_hints_from_parent_and_clone_table(tables, table)
        table_format = prep_table.get("table_format")
        # set table format if preferred
        if destination_capabilities.preferred_table_format and not table_format:
            prep_table["table_format"] = destination_capabilities.preferred_table_format
        # if table_format == "preferred":
        #     # NOTE: can also set None as table format
        #     prep_table["table_format"] = destination_capabilities.preferred_table_format
        if table_format == "native":
            # no special table format, use whatever is supported
            prep_table["table_format"] = None
        # remove incomplete columns
        for column, _ in find_incomplete_columns(table):
            prep_table["columns"].pop(column["name"])
        return prep_table  # type: ignore[return-value]
    except KeyError:
        raise TableNotFound("<>", table_name)


def resolve_replace_strategy(
    table: PreparedTableSchema,
    required_strategy: Optional[TLoaderReplaceStrategy],
    destination_capabilities: Optional[DestinationCapabilitiesContext],
) -> Optional[TLoaderReplaceStrategy]:
    """Returns replace strategy for the supplied prepared table and destination capabilities.
    If `required_strategy` cannot be fulfilled or capabilities do not support replace, None is returned.
    """
    supported_replace_strategies = (
        destination_capabilities.replace_strategies_selector(
            destination_capabilities.supported_replace_strategies, table_schema=table
        )
        if destination_capabilities.replace_strategies_selector
        else destination_capabilities.supported_replace_strategies
    )
    if not supported_replace_strategies:
        return None

    if required_strategy and required_strategy not in supported_replace_strategies:
        return None

    return required_strategy or supported_replace_strategies[0]


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
        # prepare table
        table = prepare_load_table(tables, table, destination_capabilities)

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
                f"`{merge_strategy}` merge strategy not supported for table `{table_name}`. "
                f"Available strategies: {supported_strategies}"
            )
        return merge_strategy
    return None
