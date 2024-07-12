from tests.pipeline.utils import assert_load_info


def start_snippet() -> None:
    # @@@DLT_SNIPPET_START start
    import dlt

    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    pipeline = dlt.pipeline(
        pipeline_name="quick_start", destination="duckdb", dataset_name="mydata"
    )
    load_info = pipeline.run(data, table_name="users")

    print(load_info)
    # @@@DLT_SNIPPET_END start

    assert_load_info(load_info)


def json_snippet() -> None:
    # @@@DLT_SNIPPET_START json
    import dlt

    from dlt.common import json

    with open("./assets/json_file.json", "rb") as file:
        data = json.load(file)

    pipeline = dlt.pipeline(
        pipeline_name="from_json",
        destination="duckdb",
        dataset_name="mydata",
    )

    # NOTE: test data that we load is just a dictionary so we enclose it in a list
    # if your JSON contains a list of objects you do not need to do that
    load_info = pipeline.run([data], table_name="json_data")

    print(load_info)
    # @@@DLT_SNIPPET_END json

    assert_load_info(load_info)


def csv_snippet() -> None:
    # @@@DLT_SNIPPET_START csv
    import dlt
    import pandas as pd

    owid_disasters_csv = "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020)/Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020).csv"
    df = pd.read_csv(owid_disasters_csv)
    data = df.to_dict(orient="records")

    pipeline = dlt.pipeline(
        pipeline_name="from_csv",
        destination="duckdb",
        dataset_name="mydata",
    )
    load_info = pipeline.run(data, table_name="natural_disasters")

    print(load_info)
    # @@@DLT_SNIPPET_END csv

    assert_load_info(load_info)


def api_snippet() -> None:
    # @@@DLT_SNIPPET_START api
    import dlt
    from dlt.sources.helpers import requests

    # url to request dlt-hub/dlt issues
    url = "https://api.github.com/repos/dlt-hub/dlt/issues"
    # make the request and check if succeeded
    response = requests.get(url)
    response.raise_for_status()

    pipeline = dlt.pipeline(
        pipeline_name="from_api",
        destination="duckdb",
        dataset_name="github_data",
    )
    # the response contains a list of issues
    load_info = pipeline.run(response.json(), table_name="issues")

    print(load_info)
    # @@@DLT_SNIPPET_END api

    assert_load_info(load_info)


def db_snippet() -> None:
    # @@@DLT_SNIPPET_START db
    import dlt
    from sqlalchemy import create_engine

    # use any sql database supported by SQLAlchemy, below we use a public mysql instance to get data
    # NOTE: you'll need to install pymysql with "pip install pymysql"
    # NOTE: loading data from public mysql instance may take several seconds
    engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")
    with engine.connect() as conn:
        # select genome table, stream data in batches of 100 elements
        rows = conn.execution_options(yield_per=100).exec_driver_sql(
            "SELECT * FROM genome LIMIT 1000"
        )

        pipeline = dlt.pipeline(
            pipeline_name="from_database",
            destination="duckdb",
            dataset_name="genome_data",
        )

        # here we convert the rows into dictionaries on the fly with a map function
        load_info = pipeline.run(map(lambda row: dict(row._mapping), rows), table_name="genome")

    print(load_info)
    # @@@DLT_SNIPPET_END db

    assert_load_info(load_info)


def replace_snippet() -> None:
    # @@@DLT_SNIPPET_START replace
    import dlt

    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    pipeline = dlt.pipeline(
        pipeline_name="replace_data",
        destination="duckdb",
        dataset_name="mydata",
    )
    load_info = pipeline.run(data, table_name="users", write_disposition="replace")

    print(load_info)
    # @@@DLT_SNIPPET_END replace

    assert_load_info(load_info)


def incremental_snippet() -> None:
    # @@@DLT_SNIPPET_START incremental
    import dlt
    from dlt.sources.helpers import requests

    @dlt.resource(table_name="issues", write_disposition="append")
    def get_issues(
        created_at=dlt.sources.incremental("created_at", initial_value="1970-01-01T00:00:00Z")
    ):
        # NOTE: we read only open issues to minimize number of calls to the API. There's a limit of ~50 calls for not authenticated Github users
        url = "https://api.github.com/repos/dlt-hub/dlt/issues?per_page=100&sort=created&directions=desc&state=open"

        while True:
            response = requests.get(url)
            response.raise_for_status()
            yield response.json()

            # stop requesting pages if the last element was already older than initial value
            # note: incremental will skip those items anyway, we just do not want to use the api limits
            if created_at.is_below_initial_value:
                break

            # get next page
            if "next" not in response.links:
                break
            url = response.links["next"]["url"]

    pipeline = dlt.pipeline(
        pipeline_name="github_issues_incremental",
        destination="duckdb",
        dataset_name="github_data_append",
    )
    load_info = pipeline.run(get_issues)
    row_counts = pipeline.last_trace.last_normalize_info

    print(row_counts)
    print("------")
    print(load_info)
    # @@@DLT_SNIPPET_END incremental

    assert_load_info(load_info)


def incremental_merge_snippet() -> None:
    # @@@DLT_SNIPPET_START incremental_merge
    import dlt
    from dlt.sources.helpers import requests

    @dlt.resource(
        table_name="issues",
        write_disposition="merge",
        primary_key="id",
    )
    def get_issues(
        updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z")
    ):
        # NOTE: we read only open issues to minimize number of calls to the API. There's a limit of ~50 calls for not authenticated Github users
        url = f"https://api.github.com/repos/dlt-hub/dlt/issues?since={updated_at.last_value}&per_page=100&sort=updated&directions=desc&state=open"

        while True:
            response = requests.get(url)
            response.raise_for_status()
            yield response.json()

            # get next page
            if "next" not in response.links:
                break
            url = response.links["next"]["url"]

    pipeline = dlt.pipeline(
        pipeline_name="github_issues_merge",
        destination="duckdb",
        dataset_name="github_data_merge",
    )
    load_info = pipeline.run(get_issues)
    row_counts = pipeline.last_trace.last_normalize_info

    print(row_counts)
    print("------")
    print(load_info)
    # @@@DLT_SNIPPET_END incremental_merge

    assert_load_info(load_info)


def table_dispatch_snippet() -> None:
    # @@@DLT_SNIPPET_START table_dispatch
    import dlt
    from dlt.sources.helpers import requests

    @dlt.resource(primary_key="id", table_name=lambda i: i["type"], write_disposition="append")
    def repo_events(last_created_at=dlt.sources.incremental("created_at")):
        url = "https://api.github.com/repos/dlt-hub/dlt/events?per_page=100"

        while True:
            response = requests.get(url)
            response.raise_for_status()
            yield response.json()

            # stop requesting pages if the last element was already older than initial value
            # note: incremental will skip those items anyway, we just do not want to use the api limits
            if last_created_at.is_below_initial_value:
                break

            # get next page
            if "next" not in response.links:
                break
            url = response.links["next"]["url"]

    pipeline = dlt.pipeline(
        pipeline_name="github_events",
        destination="duckdb",
        dataset_name="github_events_data",
    )
    load_info = pipeline.run(repo_events)
    row_counts = pipeline.last_trace.last_normalize_info

    print(row_counts)
    print("------")
    print(load_info)
    # @@@DLT_SNIPPET_END table_dispatch

    assert_load_info(load_info)
