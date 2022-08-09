from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Dict, Type, TypeVar, TYPE_CHECKING, Union, Generic
from multiprocessing.pool import Pool
from weakref import WeakValueDictionary

from dlt.common.telemetry import TRunMetrics
from dlt.common.typing import TFun

TPool = TypeVar("TPool", bound=Pool)


class Runnable(ABC, Generic[TPool]):
    if TYPE_CHECKING:
        TWeakValueDictionary = WeakValueDictionary[int, "Runnable[Any]"]
    else:
        TWeakValueDictionary = Dict[int, "Runnable"]

    # use weak reference container, once other references are dropped the referenced object is garbage collected
    RUNNING: TWeakValueDictionary = WeakValueDictionary({})

    def __new__(cls: Type["Runnable[TPool]"], *args: Any, **kwargs: Any) -> "Runnable[TPool]":
        """Registers Runnable instance as running for a time when context is active.
        Used with `~workermethod` decorator to pass a class instance to decorator function that must be static thus avoiding pickling such instance.

        Args:
            cls (Type[&quot;Runnable&quot;]): type of class to be instantiated

        Returns:
            Runnable: new class instance
        """
        i = super().__new__(cls)
        Runnable.RUNNING[id(i)] = i
        return i

    @abstractmethod
    def run(self, pool: TPool) -> TRunMetrics:
        pass


def workermethod(f: TFun) -> TFun:
    """Decorator to be used on static method of Runnable to make it behave like instance method.
    Expects that first parameter to decorated function is an instance `id` of Runnable that gets translated into Runnable instance.
    Such instance is then passed as `self` to decorated function.

    Args:
        f (TFun): worker function to be decorated

    Returns:
        TFun: wrapped worker function
    """
    @wraps(f)
    def _wrap(rid: Union[int, Runnable[TPool]], *args: Any, **kwargs: Any) -> Any:
        if isinstance(rid, int):
            rid = Runnable.RUNNING[rid]
        return f(rid, *args, **kwargs)

    return _wrap  # type: ignore
