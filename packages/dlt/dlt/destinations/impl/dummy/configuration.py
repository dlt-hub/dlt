import dataclasses
from typing import Final

from dlt.common.configuration import configspec
from dlt.common.destination import TLoaderFileFormat
from dlt.common.destination.client import (
    DestinationClientConfiguration,
    CredentialsConfiguration,
)


@configspec
class DummyClientCredentials(CredentialsConfiguration):
    def __str__(self) -> str:
        return "/dev/null"


@configspec
class DummyClientConfiguration(DestinationClientConfiguration):
    destination_type: Final[str] = dataclasses.field(default="dummy", init=False, repr=False, compare=False)  # type: ignore
    loader_file_format: TLoaderFileFormat = "jsonl"
    fail_schema_update: bool = False
    fail_prob: float = 0.0
    """probability of terminal fail"""
    retry_prob: float = 0.0
    """probability of job retry"""
    completed_prob: float = 0.0
    """probability of successful job completion"""
    exception_prob: float = 0.0
    """probability of exception transient exception when running job"""
    timeout: float = 10.0
    """timeout time"""
    fail_terminally_in_init: bool = False
    """raise terminal exception in job init"""
    fail_transiently_in_init: bool = False
    """raise transient exception in job init"""
    truncate_tables_on_staging_destination_before_load: bool = True
    """truncate tables on staging destination"""

    # new jobs workflows
    create_followup_jobs: bool = False
    """create followup job for individual jobs"""
    fail_followup_job_creation: bool = False
    """Raise generic exception during followupjob creation"""
    fail_table_chain_followup_job_creation: bool = False
    """Raise generic exception during tablechain followupjob creation"""
    create_followup_table_chain_sql_jobs: bool = False
    """create a table chain merge job which is guaranteed to fail"""
    create_followup_table_chain_reference_jobs: bool = False
    """create table chain jobs which succeed """
    credentials: DummyClientCredentials = None
