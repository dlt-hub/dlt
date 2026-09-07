"""Detect optional dataframe libs without forcing their import."""
import importlib
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


def is_arrow_schema(obj: Any) -> bool:
    m = get_pyarrow_module()
    return m is not None and isinstance(obj, m.Schema)


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

    target_class: Any = sys.modules[module_name]
    for idx, part in enumerate(import_parts[1:], start=1):
        # packages do not necessarily re-export their submodules: import them on demand
        if isinstance(target_class, ModuleType) and not hasattr(target_class, part):
            try:
                importlib.import_module(".".join(import_parts[: idx + 1]))
            except ImportError:
                return False
        target_class = getattr(target_class, part)

    return isinstance(obj, target_class)
