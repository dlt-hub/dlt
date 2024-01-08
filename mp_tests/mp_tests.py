import pickle
import time
from multiprocessing import get_context
from concurrent.futures import ProcessPoolExecutor

from dlt.common.configuration.specs.base_configuration import BaseConfiguration
from dlt.common.plugins import CallbackPlugin, PluginsContext, on_main_process


class CustomPluginsContext(PluginsContext):
    def on_subprocess_call(self, value: int, some_string: str) -> None:
        print(self._queue)
        for p in self._plugins:
            p.on_subprocess_call(value, some_string)

    @on_main_process
    def on_mainprocess_call(self, value: int, some_string: str) -> None:
        for p in self._plugins:
            p.on_mainprocess_call(value, some_string)


class MultiprocessingPlugin(CallbackPlugin[BaseConfiguration]):
    def __init__(self) -> None:
        super().__init__()
        self.some_value = "initial"
        self.main_process_calls = 0
        self.sub_process_calls = 0

    def on_subprocess_call(self, value: int, some_string: str) -> None:
        self.sub_process_calls += value
        assert some_string == "hello"

    def on_mainprocess_call(self, value: int, some_string: str) -> None:
        self.main_process_calls += value
        assert some_string == "hello"


def child_process(context: CustomPluginsContext):
    print("child start")
    context.on_subprocess_call(5, some_string="hello")
    context.on_mainprocess_call(10, some_string="hello")
    print("child done")


if __name__ == '__main__':
    context1 = CustomPluginsContext()
    context1.setup_plugins([MultiprocessingPlugin])

    context1.on_subprocess_call(5, some_string="hello") 
    context1.on_mainprocess_call(10, some_string="hello")

    # spawn 4 processes
    pool = ProcessPoolExecutor(
        max_workers=4, mp_context=get_context()
    )
    for i in range(4):
        pool.submit(child_process, context1)

    time.sleep(2)

    # collect messages
    context1.process_queue()
    assert context1._plugins[0].sub_process_calls == 5
    print(context1._plugins[0].main_process_calls)
    assert context1._plugins[0].main_process_calls == 50


