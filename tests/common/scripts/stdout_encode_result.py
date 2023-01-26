from functools import partial

from dlt.common.runners.stdout import exec_to_stdout
from dlt.common.telemetry import TRunMetrics


def worker(data1, data2):
    print("in func")
    return data1, data2

f = partial(worker, "this is string", TRunMetrics(True, True, 300))
with exec_to_stdout(f) as rv:
    print(rv)
