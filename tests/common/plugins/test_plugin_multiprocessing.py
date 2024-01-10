from typing import Set

import pickle
import threading
from multiprocessing import get_context
from concurrent.futures import ProcessPoolExecutor

from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.plugins import CallbackPlugin, PluginsContext, on_main_process
from concurrent.futures import ThreadPoolExecutor


class CustomPluginsContext(PluginsContext):
    def on_subprocess_call(self, value: int, some_string: str) -> None:
        for p in self._plugins:
            p.on_subprocess_call(value, some_string)  # type: ignore

    @on_main_process
    def on_mainprocess_call(self, value: int, some_string: str) -> None:
        for p in self._plugins:
            p.on_mainprocess_call(value, some_string)  # type: ignore


class MultiprocessingPlugin(CallbackPlugin[BaseConfiguration]):
    def __init__(self) -> None:
        super().__init__()
        self.some_value = "initial"
        self.main_process_calls = 0
        self.sub_process_calls = 0
        self.main_process_calls_thread_ids: Set[int] = set()
        self.sub_process_calls_thread_ids: Set[int] = set()

    def on_subprocess_call(self, value: int, some_string: str) -> None:
        self.sub_process_calls += value
        assert some_string == "hello"
        self.sub_process_calls_thread_ids.add(threading.get_ident())

    def on_mainprocess_call(self, value: int, some_string: str) -> None:
        self.main_process_calls += value
        assert some_string == "hello"
        self.main_process_calls_thread_ids.add(threading.get_ident())


def test_pickle_and_queue_same_process() -> None:
    context1 = CustomPluginsContext()
    context1.setup_plugins([MultiprocessingPlugin])
    assert len(context1._plugins) == 1
    assert context1._plugins[0].some_value == "initial"  # type: ignore
    context1._plugins[0].some_value = "changed"  # type: ignore
    assert context1._main is True

    # after pickle, plugins are there again but reset
    context2 = pickle.loads(pickle.dumps(context1))
    assert len(context2._plugins) == 1
    assert context2._plugins[0].some_value == "initial"
    assert context2._main is False

    # check calls are being routed to the right processes
    context1.on_subprocess_call(5, some_string="hello")
    context1.on_mainprocess_call(10, some_string="hello")
    context2.on_subprocess_call(5, some_string="hello")
    context2.on_mainprocess_call(10, some_string="hello")

    context1.process_queue()

    # plugin on main context
    assert context1._plugins[0].sub_process_calls == 5  # type: ignore
    assert context1._plugins[0].main_process_calls == 20  # type: ignore

    # plugin on subprocess context
    assert context2._plugins[0].sub_process_calls == 5
    assert context2._plugins[0].main_process_calls == 0


def child_process(context: CustomPluginsContext):
    context.on_subprocess_call(5, some_string="hello")
    context.on_mainprocess_call(10, some_string="hello")


def test_multiprocessing_multiple_processes() -> None:
    context1 = CustomPluginsContext()
    context1.setup_plugins([MultiprocessingPlugin])

    context1.on_subprocess_call(5, some_string="hello")
    context1.on_mainprocess_call(10, some_string="hello")

    # spawn 4 processes
    pool = ProcessPoolExecutor(max_workers=4, mp_context=get_context())
    for _i in range(4):
        pool.submit(child_process, context1)

    pool.shutdown(wait=True)

    # collect messages
    context1.process_queue()
    assert context1._plugins[0].sub_process_calls == 5  # type: ignore
    assert context1._plugins[0].main_process_calls == 50  # type: ignore
    # all calls on main process calls should be done on the main thread
    assert context1._plugins[0].main_process_calls_thread_ids == {threading.get_ident()}  # type: ignore
    assert context1._plugins[0].sub_process_calls_thread_ids == {threading.get_ident()}  # type: ignore


def test_multiprocessing_multiple_threads() -> None:
    context1 = CustomPluginsContext()
    context1.setup_plugins([MultiprocessingPlugin])

    context1.on_subprocess_call(5, some_string="hello")
    context1.on_mainprocess_call(10, some_string="hello")

    # spawn 4 processes
    pool = ThreadPoolExecutor(max_workers=4)  # , mp_context=get_context())
    for _i in range(4):
        pool.submit(child_process, context1)

    pool.shutdown(wait=True)

    # collect messages
    context1.process_queue()
    assert context1._plugins[0].sub_process_calls == 25  # type: ignore
    assert context1._plugins[0].main_process_calls == 50  # type: ignore

    # all calls on main process calls should be done on the main thread
    assert context1._plugins[0].main_process_calls_thread_ids == {threading.get_ident()}  # type: ignore
    assert len(context1._plugins[0].sub_process_calls_thread_ids) == 5  # type: ignore
