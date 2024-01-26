from tests.pipeline.utils import assert_load_info


def intro_snippet() -> None:
    # @@@DLT_SNIPPET_START api
    import dlt
    from dlt.sources.helpers import requests

    # Create a dlt pipeline that will load
    # chess player data to the DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline", destination="duckdb", dataset_name="player_data"
    )
    # Grab some player data from Chess.com API
    data = []
    for player in ["magnuscarlsen", "rpragchess"]:
        response = requests.get(f"https://api.chess.com/pub/player/{player}")
        response.raise_for_status()
        data.append(response.json())
    # Extract, normalize, and load the data
    load_info = pipeline.run(data, table_name='player')
    # @@@DLT_SNIPPET_END api

    assert_load_info(load_info)


def csv_snippet() -> None:

    # @@@DLT_SNIPPET_START csv
    import dlt
    import pandas as pd

    owid_disasters_csv = (
        "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/"
        "Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020)/"
        "Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020).csv"
    )
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

def db_snippet() -> None:

    # @@@DLT_SNIPPET_START db
    import dlt
    from sqlalchemy import create_engine

    # Use any SQL database supported by SQLAlchemy, below we use a public
    # MySQL instance to get data.
    # NOTE: you'll need to install pymysql with `pip install pymysql`
    # NOTE: loading data from public mysql instance may take several seconds
    engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")

    with engine.connect() as conn:
        # Select genome table, stream data in batches of 100 elements
        query = "SELECT * FROM genome LIMIT 1000"
        rows = conn.execution_options(yield_per=100).exec_driver_sql(query)

        pipeline = dlt.pipeline(
            pipeline_name="from_database",
            destination="duckdb",
            dataset_name="genome_data",
        )

        # Convert the rows into dictionaries on the fly with a map function
        load_info = pipeline.run(
            map(lambda row: dict(row._mapping), rows),
            table_name="genome"
        )

    print(load_info)
    # @@@DLT_SNIPPET_END db

    assert_load_info(load_info)

