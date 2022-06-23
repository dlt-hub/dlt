import re

RE_UNDERSCORES = re.compile("_+")
RE_DOUBLE_UNDERSCORES = re.compile("__+")
RE_LEADING_DIGITS = re.compile(r"^\d+")
INVALID_SQL_IDENT_CHARS = "- *!:,.'\\\"`"
INVALID_SQL_TX = str.maketrans(INVALID_SQL_IDENT_CHARS, "_" * len(INVALID_SQL_IDENT_CHARS))
SNAKE_CASE_BREAK_1 = re.compile("(.)([A-Z][a-z]+)")
SNAKE_CASE_BREAK_2 = re.compile("([a-z0-9])([A-Z])")


# fix a name so it's acceptable as database table name
def normalize_table_name(name: str) -> str:

    def camel_to_snake(name: str) -> str:
        name = SNAKE_CASE_BREAK_1.sub(r'\1_\2', name)
        return SNAKE_CASE_BREAK_2.sub(r'\1_\2', name).lower()

    # all characters that are not letters digits or a few special chars are replaced with underscore
    # then convert to snake case
    name = camel_to_snake(name.translate(INVALID_SQL_TX))
    # leading digits will be removed
    name = RE_LEADING_DIGITS.sub("_", name)
    # max 2 consecutive underscores are allowed
    return RE_DOUBLE_UNDERSCORES.sub("__", name)

# fix a name so it's an acceptable name for a database column name
def normalize_db_name(name: str) -> str:
    # replace consecutive underscores with single one to prevent name clashes with parent child
    return RE_UNDERSCORES.sub("_", normalize_table_name(name))


def normalize_schema_name(name: str) -> str:
    # remove underscores
    return normalize_db_name(name).replace("_", "")
