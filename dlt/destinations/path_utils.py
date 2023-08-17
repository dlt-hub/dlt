# this can probably go some other place, but it is shared by destinations, so for now it is here
from typing import List, Sequence, Tuple

import pendulum
import re

from dlt.destinations.exceptions import InvalidFilesystemLayout, CantExtractTablePrefix

# TODO: ensure layout only has supported placeholders
SUPPORTED_PLACEHOLDERS = {
    "schema_name",
    "table_name",
    "load_id",
    "file_id",
    "ext",
    "curr_date"
}

SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS = (
    "schema_name",
)


def check_layout(layout: str) -> List[str]:
    placeholders = get_placeholders(layout)
    invalid_placeholders = [p for p in placeholders if p not in SUPPORTED_PLACEHOLDERS]
    if invalid_placeholders:
        raise InvalidFilesystemLayout(invalid_placeholders)
    return placeholders

def get_placeholders(layout: str) -> List[str]:
    return re.findall(r'\{(.*?)\}', layout)


def create_path(layout: str, schema_name: str, table_name: str, load_id: str, file_id: str, ext: str) -> str:
    """create a filepath from the layout and our default params"""
    placeholders = check_layout(layout)
    path = layout.format(
        schema_name=schema_name,
        table_name=table_name,
        load_id=load_id,
        file_id=file_id,
        ext=ext,
        curr_date=str(pendulum.today())
    )
    # if extension is not defined, we append it at the end
    if "ext" not in placeholders:
        path += f".{ext}"
    return path


def get_table_prefix_layout(
    layout: str,
    supported_prefix_placeholders: Sequence[str] = SUPPORTED_TABLE_NAME_PREFIX_PLACEHOLDERS
) -> str:
    """get layout fragment that defines positions of the table, cutting other placeholders

       allowed `supported_prefix_placeholders` that may appear before table.
    """
    placeholders = get_placeholders(layout)

    # fail if table name is not defined
    if "table_name" not in placeholders:
        raise CantExtractTablePrefix(layout, "{table_name} placeholder not found. ")
    table_name_index = placeholders.index("table_name")

    # fail if any other prefix is defined before table_name
    if [p for p in placeholders[:table_name_index] if p not in supported_prefix_placeholders]:
        if len(supported_prefix_placeholders) == 0:
            details = "No other placeholders are allowed before {table_name} but you have %s present. " % placeholders[:table_name_index]
        else:
            details = "Only %s are allowed before {table_name} but you have %s present. " % (supported_prefix_placeholders, placeholders[:table_name_index])
        raise CantExtractTablePrefix(layout, details)

    # we include the char after the table_name here, this should be a separator not a new placeholder
    # this is to prevent selecting tables that have the same starting name
    prefix = layout[:layout.index("{table_name}") + 13]
    if prefix[-1] == "{":
        raise CantExtractTablePrefix(layout, "A separator is required after a {table_name}. ")

    return prefix
