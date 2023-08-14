# this can probably go some other place, but it is shared by destinations, so for now it is here
from typing import List, Tuple

import pendulum
import re

from dlt.destinations.exceptions import InvalidFilesystemLayout, CantExtractTablePrefix

# TODO: ensure lauyout only has supported placeholders
supported_placeholders = {
    "schema_name",
    "table_name",
    "load_id",
    "file_id",
    "ext",
    "curr_date"
}

supported_table_name_prefix_placeholders = {
    "schema_name",
    "table_name"
}

def check_layout(layout: str) -> List[str]:
    placeholders = get_placeholders(layout)
    invalid_placeholders = [p for p in placeholders if p not in supported_placeholders]
    if invalid_placeholders:
        raise InvalidFilesystemLayout(invalid_placeholders)
    return placeholders

def get_placeholders(layout: str) -> List[str]:
    return re.findall(r'\{(.*?)\}', layout)

def create_path(layout: str, schema_name: str, table_name: str, load_id: str, file_id: str, ext: str) -> str:
    """creata a filepath from the layout and our default params"""
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

def get_table_prefix(layout: str, schema_name: str, table_name: str) -> str:
    """extract the prefix for all the files of a table"""
    placeholders = get_placeholders(layout)

    # fail if table name is not defined
    if "table_name" not in placeholders:
        raise CantExtractTablePrefix(table_name)
    table_name_index = placeholders.index("table_name")

    # fail if any other prefix is defined before table_name
    if [p for p in placeholders[0:table_name_index] if p not in supported_table_name_prefix_placeholders]:
        raise CantExtractTablePrefix(table_name)

    # we include the char after the table_name here, this should be a separator not a new placeholder
    # this is to prevent selecting tables that have the same starting name
    prefix = layout[0:layout.index("{table_name}") + 13]
    if prefix[-1] == "{":
        raise CantExtractTablePrefix(table_name)
    return prefix.format(schema_name=schema_name, table_name=table_name)
