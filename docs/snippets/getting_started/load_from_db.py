# @@@SNIPSTART getting_started_index_snippet_db
import dlt
from sqlalchemy import create_engine

# use any sql database supported by SQLAlchemy, below we use a public mysql instance to get data
# NOTE: you'll need to install pymysql with "pip install pymysql"
# NOTE: loading data from public mysql instance may take several seconds
engine = create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")
with engine.connect() as conn:
    # select genome table, stream data in batches of 100 elements
    rows = conn.execution_options(yield_per=100).exec_driver_sql("SELECT * FROM genome LIMIT 1000")

    pipeline = dlt.pipeline(
        pipeline_name='from_database',
        destination='duckdb',
        dataset_name='genome_data',
    )

    # here we convert the rows into dictionaries on the fly with a map function
    load_info = pipeline.run(map(dict, rows), table_name="genome")
print(load_info)
# @@@SNIPEND