SOURCES = "sources"
"""a top section holding source and resource configs often within their own sections named after modules they are in"""

DESTINATION = "destination"
"""a top section holding sections named after particular destinations with configurations and credentials."""

LOAD = "load"
"""load and load storage configuration"""

NORMALIZE = "normalize"
"""normalize and normalize storage configuration"""

EXTRACT = "extract"
"""extract stage of the pipeline"""

PROVIDERS = "providers"
"""secrets and config providers"""

DATA_WRITER = "data_writer"
"""default section holding BufferedDataWriter settings"""

DBT_PACKAGE_RUNNER =  "dbt_package_runner"
"""dbt package runner configuration (DBTRunnerConfiguration)"""
