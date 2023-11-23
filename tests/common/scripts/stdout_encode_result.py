from functools import partial

from dlt.common.runners import TRunMetrics
from dlt.common.runners.stdout import exec_to_stdout


def worker(data1, data2):
    print("in func")
    return data1, data2


f = partial(worker, "this is string", TRunMetrics(True, 300))
with exec_to_stdout(f) as rv:
    print(rv)
