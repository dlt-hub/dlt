import re
from typing import Any, Sequence
from functools import lru_cache

from dlt.common.schema.exceptions import InvalidDatasetName


RE_UNDERSCORES = re.compile("_+")
RE_DOUBLE_UNDERSCORES = re.compile("__+")
RE_LEADING_DIGITS = re.compile(r"^\d+")
RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d_]+")
SNAKE_CASE_BREAK_1 = re.compile("([^_])([A-Z][a-z]+)")
SNAKE_CASE_BREAK_2 = re.compile("([a-z0-9])([A-Z])")


# subsequent nested fields will be separated with the string below, applies both to field and table names
PATH_SEPARATOR = "__"


# fix a name so it's acceptable as database table name
@lru_cache(maxsize=None)
def normalize_table_name(name: str) -> str:
    if not name:
        raise ValueError(name)

    def camel_to_snake(name: str) -> str:
        name = SNAKE_CASE_BREAK_1.sub(r'\1_\2', name)
        return SNAKE_CASE_BREAK_2.sub(r'\1_\2', name).lower()

    # all characters that are not letters digits or a few special chars are replaced with underscore
    # then convert to snake case
    name = camel_to_snake(RE_NON_ALPHANUMERIC.sub("_", name))
    # leading digits will be prefixed
    if RE_LEADING_DIGITS.match(name):
        name = "_" + name
    # max 2 consecutive underscores are allowed
    return RE_DOUBLE_UNDERSCORES.sub("__", name)


# fix a name so it's an acceptable name for a database column
@lru_cache(maxsize=None)
def normalize_column_name(name: str) -> str:
    # replace consecutive underscores with single one to prevent name clashes with PATH_SEPARATOR
    return RE_UNDERSCORES.sub("_", normalize_table_name(name))


# fix a name so it is acceptable as schema name
def normalize_schema_name(name: str) -> str:
    return normalize_column_name(name)


# build full db dataset (dataset) name out of (normalized) default dataset and schema name
def normalize_make_dataset_name(dataset_name: str, default_schema_name: str, schema_name: str) -> str:
    if not schema_name:
        raise ValueError("schema_name is None")
    norm_name = normalize_schema_name(dataset_name)
    if norm_name != dataset_name:
        raise InvalidDatasetName(dataset_name, norm_name)
    # if default schema is None then suffix is not added
    if default_schema_name is not None and schema_name != default_schema_name:
        norm_name += "_" + schema_name

    return norm_name


# this function builds path out of path elements using PATH_SEPARATOR
def normalize_make_path(*elems: Any) -> str:
    return PATH_SEPARATOR.join(elems)


# this function break path into elements
def normalize_break_path(path: str) -> Sequence[str]:
    return path.split(PATH_SEPARATOR)
