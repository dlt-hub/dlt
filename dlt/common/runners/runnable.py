from abc import ABC, abstractmethod
from functools import wraps
from typing import Any, Dict, Type, TypeVar, TYPE_CHECKING, Union, Generic
from multiprocessing.pool import Pool
from weakref import WeakValueDictionary
from concurrent.futures import Executor

from dlt.common.typing import TFun
from dlt.common.runners.typing import TRunMetrics

TExecutor = TypeVar("TExecutor", bound=Executor)


class Runnable(ABC, Generic[TExecutor]):
    if TYPE_CHECKING:
        TWeakValueDictionary = WeakValueDictionary[int, "Runnable[Any]"]
    else:
        TWeakValueDictionary = Dict[int, "Runnable"]

    # use weak reference container, once other references are dropped the referenced object is garbage collected
    RUNNING: TWeakValueDictionary = WeakValueDictionary({})

    def __new__(
        cls: Type["Runnable[TExecutor]"], *args: Any, **kwargs: Any
    ) -> "Runnable[TExecutor]":
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
    def run(self, pool: TExecutor) -> TRunMetrics:
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
    def _wrap(rid: Union[int, Runnable[TExecutor]], *args: Any, **kwargs: Any) -> Any:
        if isinstance(rid, int):
            rid = Runnable.RUNNING[rid]
        return f(rid, *args, **kwargs)

    return _wrap  # type: ignore


# def configuredworker(f: TFun) -> TFun:
#     """Decorator for a process/thread pool worker function facilitates passing bound configuration type across the process boundary. It requires the first method
#     of the worker function to be annotated with type derived from Type[BaseConfiguration] and the worker function to be called (typically by the Pool class) with a
#     configuration values serialized to dict (via `as_dict` method). The decorator will synthesize a new derived type and apply the serialized value, mimicking the
#     original type to be transferred across the process boundary.

#     Args:
#         f (TFun): worker function to be decorated

#     Raises:
#         ValueError: raised when worker function signature does not contain required parameters or/and annotations


#     Returns:
#         TFun: wrapped worker function
#     """
#     @wraps(f)
#     def _wrap(config: Union[StrAny, Type[BaseConfiguration]], *args: Any, **kwargs: Any) -> Any:
#         if isinstance(config, Mapping):
#             # worker process may run in separate process started with spawn and should not share any state with the parent process ie. global variables like config
#             # first function parameter should be of Type[BaseConfiguration]
#             sig = inspect.signature(f)
#             try:
#                 first_param: inspect.Parameter = next(iter(sig.parameters.values()))
#                 T = get_args(first_param.annotation)[0]
#                 if not issubclass(T, BaseConfiguration):
#                     raise ValueError(T)
#             except Exception:
#                 raise ValueError(f"First parameter of wrapped worker method {f.__name__} must by annotated as Type[BaseConfiguration]")
#             CONFIG = type(f.__name__ + uniq_id(), (T, ), {})
#             CONFIG.apply_dict(config)  # type: ignore
#             config = CONFIG

#         return f(config, *args, **kwargs)

#     return _wrap  # type: ignore
