"""dlt

How to create a data loading pipeline with dlt in 3 seconds:

    1. Write a pipeline script
    >>> import dlt
    >>> dlt.run(source=my_complicated_json, destination="duckdb")

    2. Run your pipeline script
    $ python my_pipeline.py

    3. See and use your data
    $ dlt pipeline show my_pipeline.py

    This will auto-generate and run a Streamlit app where you can see the data and the schema


Or start with our pipeline template with sample chess.com data to bigquery

    $ dlt init chess.com bigquery

For more detailed info, see https://dlthub.com/docs
"""

from dlt.pipeline import pipeline, run, restore
from dlt.extract.decorators import source, resource
from dlt.__version__ import __version__

