from dlt.pipeline import Pipeline, PostgresPipelineCredentials

credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "gamma_guild_5", "loader", "3.73.90.3")
pipeline = Pipeline("ethereum")
pipeline.create_pipeline(credentials)
schema = Pipeline.load_schema_from_file("examples/schemas/ethereum_schema.yml")
# set the loaded schema for the whole pipeline
pipeline.set_current_schema(schema)
# will sync schema with the target
pipeline.sync_schema()
