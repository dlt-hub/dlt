from dlt.pipeline import Pipeline, PostgresPipelineCredentials

credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "mainnet_3", "loader", "3.73.90.3")
# credentials = Pipeline.load_gcp_credentials("_secrets/project1234_service.json", "mainnet_4")
pipeline = Pipeline("ethereum")
pipeline.create_pipeline(credentials)
schema = Pipeline.load_schema_from_file("examples/schemas/ethereum_schema.yml")
# set the loaded schema for the whole pipeline
pipeline.set_default_schema(schema)
# will sync schema with the target
pipeline.sync_schema()
