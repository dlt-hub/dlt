from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, ClassVar, ContextManager, Dict, List, Type, TYPE_CHECKING, DefaultDict

from dlt.common.typing import DictStrAny
if TYPE_CHECKING:
    from tqdm import tqdm
    import enlighten
    from enlighten import Counter as EnlCounter, StatusBar as EnlStatusBar, Manager as EnlManager
    from alive_progress import alive_bar
else:
    tqdm = EnlCounter = EnlStatusBar = EnlManager = Any

import io
import logging

from dlt.common.exceptions import MissingDependencyException


class Collector(ABC):

    step: str

    @abstractmethod
    def update(self, name: str, inc: int = 1, total: int = None, message: str = None, label: str = None) -> None:
        """Creates or updates a counter

        This function updates a counter `name` with a value `inc`. If counter does not exist, it is created with optional total value of `total`.
        Depending on implementation `label` may be used to create nested counters and message to display additional information associated with a counter.

        Args:
            name (str): An unique name of a counter, displayable.
            inc (int, optional): Increase amount. Defaults to 1.
            total (int, optional): Maximum value of a counter. Defaults to None which means unbound counter.
            message (str, optional): Additional message attached to a counter. Defaults to None.
            label (str, optional): Creates nested counter for counter `name`. Defaults to None.
        """
        pass

    @abstractmethod
    def _start(self, step: str) -> None:
        """Starts counting for a processing step with name `step`"""
        pass

    @abstractmethod
    def _stop(self) -> None:
        """Stops counting. Should close all counters and release resources ie. screen or push the results to a server."""
        pass

    def __call__(self, step: str) -> Any:
        """Syntactic sugar for nicer context managers"""
        self.step = step
        return self

    def __enter__(self) -> "Collector":
        self._start(self.step)
        return self

    def __exit__(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: Any) -> None:
        self._stop()


class NullCollector(Collector):
    """A default counter that does not count anything."""

    def update(self, name: str, inc: int = 1, total: int = None, message: str = None, label: str = None) -> None:
        pass

    def _start(self, step: str) -> None:
        pass

    def _stop(self) -> None:
        pass


class DictCollector(Collector):
    """A collector that just counts"""

    def __init__(self) -> None:
        self.counters: DefaultDict[str, int] = None

    def update(self, name: str, inc: int = 1, total: int = None, message: str = None, label: str = None) -> None:
        assert not label, "labels not supported in dict collector"
        self.counters[name] += inc

    def _start(self, step: str) -> None:
        self.counters = defaultdict(int)

    def _stop(self) -> None:
        self.counters = None


class TqdmCollector(Collector):

    def __init__(self, single_bar: bool = False, **tqdm_kwargs: Any) -> None:
        """Collector that uses tqdm to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to tqdm in kwargs"""
        try:
            global tqdm
            from tqdm import tqdm
        except ImportError:
            raise MissingDependencyException("TqdmCollector", ["tqdm"], "We need tqdm to display progress bars.")
        self.single_bar = single_bar
        self._bars: Dict[str, tqdm] = {}
        self.tqdm_kwargs = tqdm_kwargs or {}

    def update(self, name: str, inc: int = 1, total: int = None, message: str = None, label: str = "") -> None:
        key = f"{name}_{label}"
        bar = self._bars.get(key)
        if bar is None:
            if label:
                name = f"{name}[{label}]"
            if len(self._bars) == 0:
                desc = self.step + ": " + name
            else:
                # do not add any more counters
                if self.single_bar:
                    return
                desc = name
            bar = tqdm(desc=desc, total=total, leave=False, **self.tqdm_kwargs)
            bar.refresh()
            self._bars[key] = bar
        if message:
            bar.set_postfix_str(message)
        bar.update(inc)

    def _start(self, step: str) -> None:
        self._bars = {}

    def _stop(self) -> None:
        for bar in self._bars.values():
            bar.refresh()
            bar.close()
        self._bars.clear()


class AliveCollector(Collector):

    def __init__(self, single_bar: bool = False, **alive_kwargs: Any) -> None:
        """Collector that uses alive_progress to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to alive_progress in kwargs"""
        try:
            global alive_bar
            from alive_progress import alive_bar

        except ImportError:
            raise MissingDependencyException("AliveCollector", ["alive-progress"], "We need alive-progress to display progress bars.")
        self.single_bar = single_bar
        self._bars: Dict[str, Any] = {}
        self._bars_contexts: Dict[str, ContextManager[Any]] = {}
        self.alive_kwargs = alive_kwargs or {}

    def update(self, name: str, inc: int = 1, total: int = None, message: str = None, label: str = "") -> None:
        key = f"{name}_{label}"
        bar = self._bars.get(key)
        if bar is None:
            if label:
                name = f"{name}[{label}]"
            if len(self._bars) == 0:
                desc = self.step + ": " + name
            else:
                # do not add any more counters
                if self.single_bar:
                    return
                desc = name
            bar = alive_bar(total=total, title=desc, **self.alive_kwargs)
            self._bars_contexts[key] = bar
            bar = self._bars[key] = bar.__enter__()
        # if message:
        #     bar.set_postfix_str(message)
        bar(inc)

    def _start(self, step: str) -> None:
        self._bars = {}
        self._bars_contexts = {}

    def _stop(self) -> None:
        for bar in self._bars_contexts.values():
            bar.__exit__(None, None, None)
        self._bars.clear()
        self._bars_contexts.clear()


class EnlightenCollector(Collector):

    _bars: Dict[str, EnlCounter]
    _manager: EnlManager
    _status: EnlStatusBar

    def __init__(self, single_bar: bool = False, **enlighten_kwargs: Any) -> None:
        """Collector that uses Enlighten to display counters as progress bars. Set `single_bar` to True to show just the main progress bar. Pass any config to Enlighten in kwargs"""
        print("enlighten")
        try:
            global enlighten

            import enlighten
            from enlighten import Counter as EnlCounter, StatusBar as EnlStatusBar, Manager as EnlManager
        except ImportError:
            raise MissingDependencyException("EnlightenCollector", ["enlighten"], "We need enlighten to display progress bars with a space for log messages.")
        self.single_bar = single_bar
        self.enlighten_kwargs = enlighten_kwargs

    def update(self,  name: str, inc: int = 1, total: int = None, message: str = None, label: str = "") -> None:
        key = f"{name}_{label}"
        bar = self._bars.get(key)
        if bar is None:
            if label:
                name = f"{name}[{label}]"
            if len(self._bars) > 0 and self.single_bar:
                # do not add any more counters
                return
            bar = self._manager.counter(desc=name, total=total, leave=True, force=True, **self.enlighten_kwargs)
            bar.refresh()
            self._bars[key] = bar
        bar.update(inc)

    def _start(self, step: str) -> None:
        self._bars = {}
        self._manager = enlighten.get_manager(enabled=True)
        self._status = self._manager.status_bar(leave=True, justify=enlighten.Justify.CENTER, fill="=")
        self._status.update(step)

    def _stop(self) -> None:
        if self._status:
            self._status.close()
        for bar in self._bars.values():
            bar.refresh()
            bar.close()
        self._bars.clear()
        self._manager.stop()
        self._manager = None
        self._bars = None
        self._status = None


# NULL_COLLECTOR =  AliveCollector(single_bar=True, theme="smooth")
NULL_COLLECTOR =  NullCollector()  #  EnlightenCollector(single_bar=False)