import gc
import pytest
import multiprocessing
from multiprocessing.pool import Pool
from multiprocessing.dummy import Pool as ThreadPool

from dlt.normalize.configuration import SchemaVolumeConfiguration

from tests.common.runners.utils import _TestRunnableWorkerMethod, _TestRunnableWorker, ALL_METHODS, mp_method_auto


@pytest.mark.parametrize('method', ALL_METHODS)
def test_runnable_process_pool(method: str) -> None:
    multiprocessing.set_start_method(method, force=True)
    # 4 tasks
    r = _TestRunnableWorker(4)
    # create 4 workers
    p = Pool(4)
    rv = r._run(p)
    p.close()
    assert len(rv) == 4
    assert [v[0] for v in rv] == list(range(4))
    # must contain 4 different pids (coming from 4 worker processes)
    assert len(set(v[1] for v in rv)) == 4


def test_runnable_thread_pool() -> None:
    r = _TestRunnableWorkerMethod(4)
    p = ThreadPool(4)
    rv = r._run(p)
    p.close()
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


def test_fail_on_process_worker_started_early() -> None:
    # process pool cannot be started before class instance is created: mapping not exist in worker
    p = Pool(4)
    r = _TestRunnableWorkerMethod(4)
    with pytest.raises(KeyError):
        r._run(p)
    p.close()


@pytest.mark.skip("Hangs on gc.collect")
def test_weak_pool_ref() -> None:
    r = _TestRunnableWorkerMethod(4)
    rid = id(r)
    wref = r.RUNNING
    assert wref[rid] is not None
    r = None
    gc.collect()
    # weak reference will be removed from container
    with pytest.raises(KeyError):
        r = wref[rid]


def test_configuredworker() -> None:
    # call worker method with CONFIG values that should be restored into CONFIG type
    config = SchemaVolumeConfiguration()
    config["import_schema_path"] = "test_schema_path"
    _worker_1(config, "PX1", par2="PX2")

    # must also work across process boundary
    with Pool(1) as p:
        p.starmap(_worker_1, [(config, "PX1", "PX2")])


def _worker_1(CONFIG: SchemaVolumeConfiguration, par1: str, par2: str = "DEFAULT") -> None:
    # a correct type was passed
    assert type(CONFIG) is SchemaVolumeConfiguration
    # check if config values are restored
    assert CONFIG.import_schema_path == "test_schema_path"
    # check if other parameters are correctly
    assert par1 == "PX1"
    assert par2 == "PX2"
