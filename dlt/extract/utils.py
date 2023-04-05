from typing import Union, List, Any, Sequence

from dlt.extract.typing import TTableHintTemplate, TDataItem
from dlt.common.schema.typing import TColumnKey


def resolve_column_value(column_hint: TTableHintTemplate[TColumnKey], item: TDataItem) -> Union[Any, List[Any]]:
    """Extract values from the data item given a column hint.
    Returns either a single value or list of values when hint is a composite.
    """
    columns = column_hint(item) if callable(column_hint) else column_hint
    if isinstance(columns, str):
        return item[columns]
    return [item[k] for k in columns]
