from dlt.common.runners.stdout import exec_to_stdout


def worker():
    return [open("tests/common/scripts/counter.py", "r", encoding="utf-8")]


with exec_to_stdout(worker) as rv:
    print(rv)
