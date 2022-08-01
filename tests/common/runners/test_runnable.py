import gc
from multiprocessing.pool import Pool
from multiprocessing.dummy import Pool as ThreadPool
import pytest

from tests.common.runners.utils import _TestRunnable
from tests.utils import skipifspawn


@skipifspawn
def test_runnable_process_pool() -> None:
    # 4 tasks
    r = _TestRunnable(4)
    # create 4 workers
    p = Pool(4)
    rv = r._run(p)
    p.close()
    assert len(rv) == 4
    assert [v[0] for v in rv] == list(range(4))
    # must contain 4 different pids (coming from 4 worker processes)
    assert len(set(v[2] for v in rv)) == 4
    # must contain one uniq_id coming from forked instance
    assert len(set(v[1] for v in rv)) == 1


def test_runnable_thread_pool() -> None:
    r = _TestRunnable(4)
    p = ThreadPool(4)
    rv = r._run(p)
    p.close()
    assert len(rv) == 4
    assert [v[0] for v in rv] == list(range(4))
    # must contain 1 pid (all in single process)
    assert len(set(v[2] for v in rv)) == 1
    # must contain one uniq_id coming from forked instance
    assert len(set(v[1] for v in rv)) == 1



@skipifspawn
def test_fail_on_process_worker_started_early() -> None:
    # process pool cannot be started before class instance is created: mapping not exist in worker
    p = Pool(4)
    r = _TestRunnable(4)
    with pytest.raises(KeyError):
        r._run(p)
    p.close()


def test_weak_pool_ref() -> None:
    r = _TestRunnable(4)
    rid = id(r)
    wref = r.RUNNING
    assert wref[rid] is not None
    r = None
    gc.collect()
    # weak reference will be removed from container
    with pytest.raises(KeyError):
        r = wref[rid]
