Configuration
=============

.. module:: dlt.common.configuration.accessors
   :synopsis: Configuration utilities

.. autodata:: dlt.config
   :annotation: = Configuration accessor

Configuration Usage
-------------------

dlt's configuration system allows you to set and retrieve configuration values from various sources.

Examples
^^^^^^^^

.. code-block:: python

    import dlt

    # Access configuration values
    project_id = dlt.config["destinations.bigquery.project_id"]

    # Set configuration values
    dlt.config["destinations.bigquery.location"] = "US"

    # Configuration can also be provided at pipeline creation
    pipeline = dlt.pipeline(
        pipeline_name="example",
        destination="bigquery",
        dataset_name="example_data"
    )
