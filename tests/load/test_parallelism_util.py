"""
Tests to test the parallelism settings on the loader
NOTE: there are tests in custom destination to check parallelism settings are applied
"""

from typing import Tuple, Any, cast

from dlt.load.utils import filter_new_jobs, get_available_worker_slots
from dlt.load.configuration import LoaderConfiguration
from dlt.common.destination import DestinationCapabilitiesContext
from dlt.common.utils import uniq_id
from dlt.common.storages.load_storage import ParsedLoadJobFileName


def create_job_name(table: str, index: int) -> str:
    uid = uniq_id()
    return f"{table}.{uid}.{index}.jsonl"


def get_caps_conf() -> Tuple[DestinationCapabilitiesContext, LoaderConfiguration]:
    return DestinationCapabilitiesContext(), LoaderConfiguration()


def test_get_available_worker_slots() -> None:
    caps, conf = get_caps_conf()

    conf.workers = 20
    assert get_available_worker_slots(conf, caps, []) == 20

    # change workers
    conf.workers = 30
    assert get_available_worker_slots(conf, caps, []) == 30

    # check with existing jobs
    assert get_available_worker_slots(conf, caps, cast(Any, range(3))) == 27
    assert get_available_worker_slots(conf, caps, cast(Any, range(50))) == 0

    # table-sequential will not change anything
    caps.loader_parallelism_strategy = "table-sequential"
    assert get_available_worker_slots(conf, caps, []) == 30

    # caps with lower value will override
    caps.max_parallel_load_jobs = 10
    assert get_available_worker_slots(conf, caps, []) == 10

    # lower conf workers will override aing
    conf.workers = 3
    assert get_available_worker_slots(conf, caps, []) == 3

    # sequential strategy only allows one
    caps.loader_parallelism_strategy = "sequential"
    assert get_available_worker_slots(conf, caps, []) == 1


def test_table_sequential_parallelism_strategy() -> None:
    # we create 10 jobs for 8 different tables
    job_names = []
    for y in range(8):
        job_names += [create_job_name(f"t{y}", i) for i in range(10)]
    assert len(job_names) == 80
    assert len({ParsedLoadJobFileName.parse(j).table_name for j in job_names}) == 8
    caps, conf = get_caps_conf()

    # default is 20
    assert len(filter_new_jobs(job_names, caps, conf, [], 20)) == 20

    # table sequential will give us 8, one for each table
    conf.parallelism_strategy = "table-sequential"
    filtered = filter_new_jobs(job_names, caps, conf, [], 20)
    assert len(filtered) == 8
    assert len({ParsedLoadJobFileName.parse(j).table_name for j in job_names}) == 8

    # only free available slots are also applied
    assert len(filter_new_jobs(job_names, caps, conf, [], 3)) == 3


def test_strategy_preference() -> None:
    # we create 10 jobs for 8 different tables
    job_names = []
    for y in range(8):
        job_names += [create_job_name(f"t{y}", i) for i in range(10)]
    caps, conf = get_caps_conf()

    # nothing set will default to parallel
    assert (
        len(filter_new_jobs(job_names, caps, conf, [], get_available_worker_slots(conf, caps, [])))
        == 20
    )

    caps.loader_parallelism_strategy = "table-sequential"
    assert (
        len(filter_new_jobs(job_names, caps, conf, [], get_available_worker_slots(conf, caps, [])))
        == 8
    )

    caps.loader_parallelism_strategy = "sequential"
    assert (
        len(filter_new_jobs(job_names, caps, conf, [], get_available_worker_slots(conf, caps, [])))
        == 1
    )

    # config may override (will go back to default 20)
    conf.parallelism_strategy = "parallel"
    assert (
        len(filter_new_jobs(job_names, caps, conf, [], get_available_worker_slots(conf, caps, [])))
        == 20
    )

    conf.parallelism_strategy = "table-sequential"
    assert (
        len(filter_new_jobs(job_names, caps, conf, [], get_available_worker_slots(conf, caps, [])))
        == 8
    )


def test_no_input() -> None:
    caps, conf = get_caps_conf()
    assert filter_new_jobs([], caps, conf, [], 50) == []
