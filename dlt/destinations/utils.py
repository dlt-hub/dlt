import re
from typing import Any, cast, Tuple, Dict, Type

from dlt.destinations.exceptions import DatabaseTransientException
from dlt.extract import DltResource, resource as make_resource


def ensure_resource(data: Any) -> DltResource:
    """Wraps `data` in a DltResource if it's not a DltResource already."""
    if isinstance(data, DltResource):
        return data
    resource_name = None if hasattr(data, "__name__") else "content"
    return cast(DltResource, make_resource(data, name=resource_name))


def _convert_to_old_pyformat(
    new_style_string: str, args: Tuple[Any, ...], operational_error_cls: Type[Exception]
) -> Tuple[str, Dict[str, Any]]:
    """Converts a query string from the new pyformat style to the old pyformat style.

    The new pyformat style uses placeholders like %s, while the old pyformat style
    uses placeholders like %(arg0)s, where the number corresponds to the index of
    the argument in the args tuple.

    Args:
        new_style_string (str): The query string in the new pyformat style.
        args (Tuple[Any, ...]): The arguments to be inserted into the query string.
        operational_error_cls (Type[Exception]): The specific OperationalError class to be raised
            in case of a mismatch between placeholders and arguments. This should be the
            OperationalError class provided by the DBAPI2-compliant driver being used.

    Returns:
        Tuple[str, Dict[str, Any]]: A tuple containing the converted query string
            in the old pyformat style, and a dictionary mapping argument keys to values.

    Raises:
        DatabaseTransientException: If there is a mismatch between the number of
            placeholders in the query string, and the number of arguments provided.
    """
    if len(args) == 1 and isinstance(args[0], tuple):
        args = args[0]

    keys = [f"arg{str(i)}" for i, _ in enumerate(args)]
    old_style_string, count = re.subn(r"%s", lambda _: f"%({keys.pop(0)})s", new_style_string)
    mapping = dict(zip([f"arg{str(i)}" for i, _ in enumerate(args)], args))
    if count != len(args):
        raise DatabaseTransientException(operational_error_cls())
    return old_style_string, mapping
