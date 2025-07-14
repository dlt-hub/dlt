# TODO: register layout types and specs for each top level sections so we can generate full config typing

SOURCES = "sources"
"""a top section holding source and resource configs often within their own sections named after modules they are in"""

DESTINATION = "destination"
"""a top section holding sections named after particular destinations with configurations and credentials. NOTE: will be deprecated"""

DESTINATIONS = "destinations"
"""a top section holding sections named after particular destinations with configurations and credentials. NOTE: not yet supported"""

PIPELINES = "pipelines"
"""a top section holding pipeline configurations"""

DATASETS = "datasets"
"""a top section holding dataset configurations"""

LOAD = "load"
"""load and load storage configuration"""

NORMALIZE = "normalize"
"""normalize and normalize storage configuration"""

EXTRACT = "extract"
"""extract stage of the pipeline"""

SCHEMA = "schema"
"""schema configuration, ie. normalizers"""

PROVIDERS = "providers"
"""secrets and config providers"""

DATA_WRITER = "data_writer"
"""default section holding BufferedDataWriter settings"""

DBT_PACKAGE_RUNNER = "dbt_package_runner"
"""dbt package runner configuration (DBTRunnerConfiguration)"""

DBT_CLOUD = "dbt_cloud"
"""dbt cloud helpers configuration (DBTCloudConfiguration)"""

DASHBOARD = "dashboard"
"""dashboard configuration (DashboardConfiguration)"""
