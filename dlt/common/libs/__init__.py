"""Detect optional dataframe libs without forcing their import."""
import sys
from types import ModuleType
from typing import Any, Optional


__all__ = [
    "get_pyarrow_module",
    "get_pandas_module",
    "get_polars_module",
    "get_pydantic_module",
    "is_arrow_object",
    "is_pandas_frame",
    "is_polars_frame",
    "is_pydantic_model",
    "is_instance_lib",
]


def get_pyarrow_module() -> Optional[Any]:
    return sys.modules.get("pyarrow")


def get_pandas_module() -> Optional[Any]:
    return sys.modules.get("pandas")


def get_polars_module() -> Optional[Any]:
    return sys.modules.get("polars")


def get_pydantic_module() -> Optional[Any]:
    return sys.modules.get("pydantic")


def is_arrow_object(obj: Any) -> bool:
    m = get_pyarrow_module()
    return m is not None and isinstance(obj, (m.Table, m.RecordBatch))


def is_pandas_frame(obj: Any) -> bool:
    m = get_pandas_module()
    return m is not None and isinstance(obj, m.DataFrame)


def is_polars_frame(obj: Any) -> bool:
    m = get_polars_module()
    return m is not None and isinstance(obj, (m.DataFrame, m.LazyFrame))


def is_pydantic_model(obj: Any) -> bool:
    m = get_pydantic_module()
    return m is not None and isinstance(obj, m.BaseModel)


def is_instance_lib(obj: Any, *, class_ref: str) -> bool:
    """Allows `isinstance()` checks without directly importing 3rd party libraries

    Example:
        ```python
        df = pd.DataFrame(...)
        is_instance_lib(df, class_ref="pandas.DataFrame")
        ```
    """
    import_parts = class_ref.split(".")
    module_name = import_parts[0]

    if module_name not in sys.modules:
        return False

    module: ModuleType = sys.modules[module_name]
    target_class: Any = module
    for part in import_parts[1:]:
        target_class = getattr(target_class, part)

    return isinstance(obj, target_class)
