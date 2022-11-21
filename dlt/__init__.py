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

    $ dlt init chess bigquery

For more detailed info, see https://dlthub.com/docs
"""

from dlt.pipeline import pipeline as _pipeline, run, attach, Pipeline
from dlt.pipeline.state import state
from dlt.extract.decorators import source, resource, transformer, defer
from dlt.extract.source import with_table_name
from dlt.common.schema import Schema
from dlt.common.configuration.accessors import config, secrets
from dlt.common.typing import TSecretValue as _TSecretValue
from dlt.common.configuration.specs import CredentialsConfiguration as _CredentialsConfiguration

pipeline = _pipeline

TSecretValue = _TSecretValue
"When typing source/resource function arguments indicates that given argument is a secret and should be taken from dlt.secrets. The value itself is a string"

TCredentials = _CredentialsConfiguration
"When typing source/resource function arguments indicates that given argument represents credentials and should be taken from dlt.secrets. Credentials may be string, dictionaries or any other types."

from dlt.__version__ import __version__


