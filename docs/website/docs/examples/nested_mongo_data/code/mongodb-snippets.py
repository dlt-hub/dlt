from tests.utils import skipifgithubfork


@skipifgithubfork
def incremental_snippet() -> None:

    # @@@DLT_SNIPPET_START example
    # @@@DLT_SNIPPET_START markdown_source
    from typing import Iterator, Optional, Dict, Any, Tuple

    import dlt
    from dlt.common import pendulum
    from dlt.common.time import ensure_pendulum_datetime
    from dlt.common.typing import TDataItem, TDataItems, TAnyDateTime
    from dlt.extract.source import DltResource
    from dlt.sources.helpers.requests import client

    @dlt.source
    def mongodb_source(
        credentials: Dict[str, str]=dlt.secrets.value,
        start_date: Optional[TAnyDateTime] = pendulum.datetime(year=2000, month=1, day=1),  # noqa: B008
        end_date: Optional[TAnyDateTime] = None,
    ):
        pass

    # @@@DLT_SNIPPET_END markdown_source

    # @@@DLT_SNIPPET_START markdown_pipeline
    __name__ = "__main__" # @@@DLT_REMOVE
    if __name__ == "__main__":
        # create dlt pipeline
        pipeline = dlt.pipeline(
            pipeline_name="pipeline_name", destination="duckdb", dataset_name="dataset_name"
        )

        load_info = pipeline.run(mongodb_source())
        print(load_info)
    # @@@DLT_SNIPPET_END markdown_pipeline
    # @@@DLT_SNIPPET_END example


