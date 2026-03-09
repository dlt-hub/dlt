# job_runs.py

from typing import TypedDict

import dlt
from dlt.common.pipeline import TRefreshMode
from dlt.sources.sql_database import sql_table
from dlt.hub.data_quality import delivery
from dlt.hub import runtime, runner

from manifest_spec import TTrigger


# run context injected by Runtime
class TJobRunContext(TypedDict):
    run_id: str
    trigger: TTrigger


# create the incremental table resource with row_order to ensure we don't miss rows
chat_messages = sql_table(
    table="chat_message",
    chunk_size=1000,
    incremental=dlt.sources.incremental(
        "id",
        row_order="asc",
        range_start="open",
    ),
)


def _load_chat_message(refresh: TRefreshMode = None, pages: int = 2):
    pipeline = dlt.pipeline("ingest", destination="warehouse", dataset_name="raw_data")
    # retry is defined in code and is stateful. no retries on control plane
    while (
        not runner(pipeline, retry_policy="backoff")
        .run(chat_messages.add_limit(pages), refresh=refresh)
        .is_empty
    ):
        refresh = None


# no triggers — manual only, killed after 24h
@runtime.job(timeout="24h")
def backfil_chats():
    """Backfill chat messages from source database"""
    _load_chat_message(refresh="drop_sources")


# cron schedule (shorthand auto-normalized to "schedule:0 8 * * *")
@runtime.job(trigger="0 8 * * *", timeout="4h")
def get_daily_chats():
    _load_chat_message()


# tag trigger — run_context lets us branch on which trigger fired
@runtime.job(trigger="tag:backfill", timeout="24h")
def load_chats(run_context: TJobRunContext):
    if run_context["trigger"] == TTrigger("tag:backfill"):
        _load_chat_message(refresh="drop_sources")
    else:
        _load_chat_message()


@runtime.job(
    trigger=[backfil_chats.success, get_daily_chats.success],  # deliver=quality_source
)
def chat_quality():
    # compute and store data quality
    pass


@delivery.sla(deadline="8am on Mondays")
@dlt.source
def chat_analytics(chat_dataset: dlt.Dataset):
    @dlt.hub.transformation
    def chat_sentiments(_ds: dlt.Dataset):
        pass

    return chat_sentiments(chat_dataset)


# job.success Python sugar: backfil_chats.success resolves to
# "job.success:jobs.job_runs.backfil_chats" in the manifest
@runtime.job(
    trigger=[backfil_chats.success, get_daily_chats.success],
    deliver=chat_analytics,
)
def analyze_chats(run_context: TJobRunContext):
    pipeline = dlt.pipeline(
        "analytics_pipeline", destination="warehouse", dataset_name="analytics_data"
    )
    runner(pipeline, retry="backoff").run(
        chat_analytics(dlt.dataset("warehouse", "raw_data")),
        refresh="drop_sources" if run_context["trigger"] == backfil_chats.success else None,
    )


# config injection: dlt.config.value keys are discovered and stored in manifest
# config layout follows the ref: [jobs.job_runs.job_picker]
@runtime.job(starred=True)
def job_picker(job_name=dlt.config.value):
    """This magic job will run code for any other job"""
    pass


if __name__ == "__main__":
    runtime.tests.run_context()
