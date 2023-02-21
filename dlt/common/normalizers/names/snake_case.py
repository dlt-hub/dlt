import re
from typing import Any, Sequence
from functools import lru_cache

from dlt.common.schema.exceptions import InvalidDatasetName
from dlt.common.utils import digest128


RE_UNDERSCORES = re.compile("_+")
RE_DOUBLE_UNDERSCORES = re.compile("__+")
RE_LEADING_DIGITS = re.compile(r"^\d+")
RE_NON_ALPHANUMERIC = re.compile(r"[^a-zA-Z\d_]+")
SNAKE_CASE_BREAK_1 = re.compile("([^_])([A-Z][a-z]+)")
SNAKE_CASE_BREAK_2 = re.compile("([a-z0-9])([A-Z])")


# subsequent nested fields will be separated with the string below, applies both to field and table names
PATH_SEPARATOR = "__"


def camel_to_snake(name: str) -> str:
    name = SNAKE_CASE_BREAK_1.sub(r'\1_\2', name)
    return SNAKE_CASE_BREAK_2.sub(r'\1_\2', name).lower()


@lru_cache(maxsize=None)
def normalize_path(path: str) -> str:
    """Breaks path into identifiers using PATH_SEPARATOR, normalizes components and reconstitutes the path"""
    return normalize_make_path(*map(normalize_identifier, normalize_break_path(path)))


# fix a name so it's an acceptable name for a database column
@lru_cache(maxsize=None)
def normalize_identifier(name: str) -> str:
    """Normalizes the identifier according to naming convention represented by this function"""
    if not name:
        raise ValueError(name)
    # all characters that are not letters digits or a few special chars are replaced with underscore
    # then convert to snake case
    name = camel_to_snake(RE_NON_ALPHANUMERIC.sub("_", name))
    # leading digits will be prefixed
    if RE_LEADING_DIGITS.match(name):
        name = "_" + name

    # replace consecutive underscores with single one to prevent name clashes with PATH_SEPARATOR
    return RE_UNDERSCORES.sub("_", name)


def normalize_make_dataset_name(dataset_name: str, default_schema_name: str, schema_name: str) -> str:
    """Builds full db dataset (dataset) name out of (normalized) default dataset and schema name"""
    if not schema_name:
        raise ValueError("schema_name is None or empty")
    if not dataset_name:
        raise ValueError("dataset_name is None or empty")
    norm_name = normalize_identifier(dataset_name)
    if norm_name != dataset_name:
        raise InvalidDatasetName(dataset_name, norm_name)
    # if default schema is None then suffix is not added
    if default_schema_name is not None and schema_name != default_schema_name:
        norm_name += "_" + schema_name

    return norm_name


def normalize_make_path(*identifiers: Any) -> str:
    """Builds path out of path identifiers using PATH_SEPARATOR. Identifiers are not normalized"""
    return PATH_SEPARATOR.join(identifiers)


def normalize_break_path(path: str) -> Sequence[str]:
    """Breaks path into sequence of identifiers"""
    return path.split(PATH_SEPARATOR)


def shorten_name(name: str, max_length: int, tag_length: int = 10) -> str:
    """Shortens the `name` to `max_length` and adds a tag to it to make it unique"""
    if len(name) > max_length:
        digest = digest128(name)
        return name[:max_length - tag_length] + digest[:tag_length].lower()
    else:
        return name
