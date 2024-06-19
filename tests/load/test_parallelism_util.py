"""
Tests to test the parallelism settings on the loader
NOTE: there are tests in custom destination to check parallelism settings are applied
"""

from typing import Tuple

from dlt.load.utils import filter_new_jobs
from dlt.load.configuration import LoaderConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import ParsedLoadJobFileName


def create_job_name(table: str, index: int) -> str:
    uid = uniq_id()
    return f"{table}.{uid}.{index}.jsonl"


def get_caps_conf() -> Tuple[DestinationCapabilitiesContext, LoaderConfiguration]:
    return DestinationCapabilitiesContext(), LoaderConfiguration()


def test_max_workers() -> None:
    job_names = [create_job_name("t1", i) for i in range(100)]
    caps, conf = get_caps_conf()

    # default is 20
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 20

    # we can change it
    conf.workers = 35
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 35

    # destination may override this
    caps.max_parallel_load_jobs = 15
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 15

    # lowest value will prevail
    conf.workers = 5
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 5


def test_table_sequential_parallelism_strategy() -> None:
    # we create 10 jobs for 8 different tables
    job_names = []
    for y in range(8):
        job_names += [create_job_name(f"t{y}", i) for i in range(10)]
    assert len(job_names) == 80
    assert len({ParsedLoadJobFileName.parse(j).table_name for j in job_names}) == 8
    caps, conf = get_caps_conf()

    # default is 20
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 20

    # table sequential will give us 8, one for each table
    conf.parallelism_strategy = "table-sequential"
    filtered = filter_new_jobs(job_names, caps, conf, 0)
    assert len(filtered) == 8
    assert len({ParsedLoadJobFileName.parse(j).table_name for j in job_names}) == 8

    # max workers also are still applied
    conf.workers = 3
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 3


def test_strategy_preference() -> None:
    # we create 10 jobs for 8 different tables
    job_names = []
    for y in range(8):
        job_names += [create_job_name(f"t{y}", i) for i in range(10)]
    caps, conf = get_caps_conf()

    # nothing set will default to parallel
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 20

    caps.loader_parallelism_strategy = "table-sequential"
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 8

    caps.loader_parallelism_strategy = "sequential"
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 1

    # config may override (will go back to default 20)
    conf.parallelism_strategy = "parallel"
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 20

    conf.parallelism_strategy = "table-sequential"
    assert len(filter_new_jobs(job_names, caps, conf, 0)) == 8


def test_no_input() -> None:
    caps, conf = get_caps_conf()
    assert filter_new_jobs([], caps, conf, 0) == []


def test_existing_jobs_count() -> None:
    jobs = [f"job{i}" for i in range(50)]
    caps, conf = get_caps_conf()

    # default is 20 jobs
    assert len(filter_new_jobs(jobs, caps, conf, 0)) == 20

    # if 5 are already running, just return 15
    assert len(filter_new_jobs(jobs, caps, conf, 5)) == 15

    # ...etc
    assert len(filter_new_jobs(jobs, caps, conf, 16)) == 4

    assert len(filter_new_jobs(jobs, caps, conf, 300)) == 0
    assert len(filter_new_jobs(jobs, caps, conf, 20)) == 0
    assert len(filter_new_jobs(jobs, caps, conf, 19)) == 1
