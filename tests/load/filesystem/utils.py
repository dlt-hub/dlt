import posixpath
from typing import Iterator, List, Sequence, Tuple
from contextlib import contextmanager

from dlt.load import Load
from dlt.common.configuration.container import Container
from dlt.common.configuration.specs.config_section_context import ConfigSectionContext
from dlt.common.destination.reference import Destination, LoadJob, TDestination
from dlt.destinations import filesystem
from dlt.destinations.impl.filesystem.filesystem import FilesystemClient
from dlt.destinations.job_impl import EmptyLoadJob
from tests.load.utils import prepare_load_package


def setup_loader(dataset_name: str) -> Load:
    destination: TDestination = filesystem()  # type: ignore[assignment]
    config = filesystem.spec(dataset_name=dataset_name)
    # setup loader
    with Container().injectable_context(ConfigSectionContext(sections=("filesystem",))):
        return Load(destination, initial_client_config=config)


@contextmanager
def perform_load(
    dataset_name: str, cases: Sequence[str], write_disposition: str = "append"
) -> Iterator[Tuple[FilesystemClient, List[LoadJob], str, str]]:
    load = setup_loader(dataset_name)
    load_id, schema = prepare_load_package(load.load_storage, cases, write_disposition)
    client: FilesystemClient = load.get_destination_client(schema)  # type: ignore[assignment]

    # for the replace disposition in the loader we truncate the tables, so do this here
    truncate_tables = []
    if write_disposition == "replace":
        for item in cases:
            parts = item.split(".")
            truncate_tables.append(parts[0])

    client.initialize_storage(truncate_tables=truncate_tables)
    client.update_stored_schema()
    root_path = posixpath.join(client.fs_path, client.config.dataset_name)

    files = load.load_storage.list_new_jobs(load_id)
    try:
        jobs = []
        for f in files:
            job = Load.w_spool_job(load, f, load_id, schema)
            # job execution failed
            if isinstance(job, EmptyLoadJob):
                raise RuntimeError(job.exception())
            jobs.append(job)

        yield client, jobs, root_path, load_id
    finally:
        try:
            client.drop_storage()
        except Exception:
            print(f"Failed to delete FILESYSTEM dataset: {client.dataset_path}")
