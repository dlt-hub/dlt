Pipeline class
==============

.. module:: dlt.pipeline.pipeline
   :synopsis: Pipeline class for controlling data extraction, normalization, and loading.

.. autoclass:: dlt.pipeline.Pipeline
   :members:
   :undoc-members:
   :special-members: __init__
   :show-inheritance:

   The Pipeline class is the central component of dlt that orchestrates the entire data pipeline process: extracting data from sources, normalizing it, and loading it into destinations.

   .. rubric:: Example

   .. code-block:: python

      import dlt
      from dlt.sources.helpers import requests

      # Create a pipeline
      pipeline = dlt.pipeline(
          pipeline_name='chess_pipeline',
          destination='duckdb',
          dataset_name='player_data'
      )

      # Extract, normalize, and load data
      data = []
      for player in ['magnuscarlsen', 'rpragchess']:
          response = requests.get(f'https://api.chess.com/pub/player/{player}')
          response.raise_for_status()
          data.append(response.json())

      # Run the pipeline
      pipeline.run(data, table_name='player')

      # Access the loaded data
      df = pipeline.dataset().player.df()