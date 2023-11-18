import gc
import pytest
import multiprocessing
# from multiprocessing.pool import Pool
# from multiprocessing.dummy import Pool as ThreadPool
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from typing import Any

from dlt.normalize.configuration import SchemaStorageConfiguration
from dlt.common.runners import Runnable

from tests.common.runners.utils import _TestRunnableWorkerMethod, _TestRunnableWorker, ALL_METHODS, mp_method_auto


@pytest.mark.parametrize('method', ALL_METHODS)
def test_runnable_process_pool(method: str) -> None:
    # 4 tasks
    r = _TestRunnableWorker(4)
    # create 4 workers
    with ProcessPoolExecutor(4, mp_context=multiprocessing.get_context(method)) as p:
        rv = r._run(p)
        p.shutdown()
    assert len(rv) == 4
    assert [v[0] for v in rv] == list(range(4))
    # must contain 4 different pids (coming from 4 worker processes)
    assert len(set(v[1] for v in rv)) == 4


def test_runnable_thread_pool() -> None:
    r = _TestRunnableWorkerMethod(4)
    with ThreadPoolExecutor(4) as p:
        rv = r._run(p)
        p.shutdown()
        assert len(rv) == 4
        assert [v[0] for v in rv] == list(range(4))
        # must contain 1 pid (all in single process)
        assert len(set(v[1] for v in rv)) == 1
        # must contain one uniq_id coming from forked instance
        assert len(set(v[1] for v in rv)) == 1


def test_runnable_direct_worker_call() -> None:
    r = _TestRunnableWorkerMethod(4)
    rv = _TestRunnableWorkerMethod.worker(r, 199)
    assert rv[0] == 199


@pytest.mark.parametrize('method', ALL_METHODS)
def test_process_worker_started_early(method: str) -> None:
    with ProcessPoolExecutor(4, mp_context=multiprocessing.get_context(method)) as p:
        r = _TestRunnableWorkerMethod(4)
        if method == "spawn":
            # spawn processes are started upfront, so process pool cannot be started before class instance is created: mapping not exist in worker
            with pytest.raises(KeyError):
                r._run(p)
        else:  # With fork method processes are spawned lazily so this order is fine
            r._run(p)
        p.shutdown(wait=True)


@pytest.mark.skip("Hangs on gc.collect")
def test_weak_pool_ref() -> None:
    r: Runnable[Any] = _TestRunnableWorkerMethod(4)
    rid = id(r)
    wref = r.RUNNING
    assert wref[rid] is not None
    r = None
    gc.collect()
    # weak reference will be removed from container
    with pytest.raises(KeyError):
        r = wref[rid]


@pytest.mark.parametrize('method', ALL_METHODS)
def test_configuredworker(method: str) -> None:
    # call worker method with CONFIG values that should be restored into CONFIG type
    config = SchemaStorageConfiguration()
    config["import_schema_path"] = "test_schema_path"
    _worker_1(config, "PX1", par2="PX2")

    # must also work across process boundary
    with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context(method)) as p:
        p.map(_worker_1, *zip(*[(config, "PX1", "PX2")]))


def _worker_1(CONFIG: SchemaStorageConfiguration, par1: str, par2: str = "DEFAULT") -> None:
    # a correct type was passed
    assert type(CONFIG) is SchemaStorageConfiguration
    # check if config values are restored
    assert CONFIG.import_schema_path == "test_schema_path"
    # check if other parameters are correctly
    assert par1 == "PX1"
    assert par2 == "PX2"
