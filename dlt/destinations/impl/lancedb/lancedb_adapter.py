from typing import Any, Dict

from dlt.common.schema.typing import TTableSchemaColumns
from dlt.common.typing import TColumnNames
from dlt.destinations.utils import get_resource_for_adapter
from dlt.extract import DltResource
from dlt.extract.items import TTableHintTemplate


VECTORIZE_HINT = "x-lancedb-embed"
NO_REMOVE_ORPHANS_HINT = "x-lancedb-remove-orphans"


def lancedb_adapter(
    data: Any,
    embed: TColumnNames = None,
    merge_key: TColumnNames = None,
    no_remove_orphans: bool = False,
) -> DltResource:
    """Prepares data for the LanceDB destination by specifying which columns should be embedded.

    Args:
        data (Any): The data to be transformed. It can be raw data or an instance
            of DltResource. If raw data, the function wraps it into a DltResource
            object.
        embed (TColumnNames, optional): Specify columns to generate embeddings for.
            It can be a single column name as a string, or a list of column names.
        merge_key (TColumnNames, optional): Specify columns to merge on.
            It can be a single column name as a string, or a list of column names.
        no_remove_orphans (bool): Specify whether to remove orphaned records in child
            tables with no parent records after merges to maintain referential integrity.

    Returns:
        DltResource: A resource with applied LanceDB-specific hints.

    Raises:
        ValueError: If input for `embed` invalid or empty.

    Examples:
        >>> data = [{"name": "Marcel", "description": "Moonbase Engineer"}]
        >>> lancedb_adapter(data, embed="description")
        [DltResource with hints applied]
    """
    resource = get_resource_for_adapter(data)

    additional_table_hints: Dict[str, TTableHintTemplate[Any]] = {}
    column_hints: TTableSchemaColumns = None

    if embed:
        if isinstance(embed, str):
            embed = [embed]
        if not isinstance(embed, list):
            raise ValueError(
                "`embed` must be a list of column names or a single column name as a string."
            )
        column_hints = {}

        # TODO: warn if hint exists and we override nullable
        for column_name in embed:
            column_hints[column_name] = {
                "name": column_name,
                VECTORIZE_HINT: True,  # type: ignore[misc]
                "nullable": True,  # must be nullable because lance will override it anyway
            }

    additional_table_hints[NO_REMOVE_ORPHANS_HINT] = no_remove_orphans

    if column_hints or additional_table_hints or merge_key:
        resource.apply_hints(
            merge_key=merge_key, columns=column_hints, additional_table_hints=additional_table_hints
        )
    else:
        raise ValueError(
            "You must must provide at least either the `embed` or `merge_key` or `remove_orphans`"
            " argument if using the adapter."
        )

    return resource
