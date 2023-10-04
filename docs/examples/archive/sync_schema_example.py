# from dlt.pipeline import Pipeline,  GCPPipelineCredentials

# # credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "mainnet_6", "loader", "3.73.90.3")
# credentials = GCPPipelineCredentials.from_services_file("_secrets/project1234_service.json", "mainnet_4")
# pipeline = Pipeline("ethereum")
# pipeline.create_pipeline(credentials)
# schema = Pipeline.load_schema_from_file("examples/schemas/ethereum_schema.yml")
# # set the loaded schema for the whole pipeline
# pipeline.set_default_schema(schema)
# # will sync schema with the target
# # pipeline.sync_schema()
# # with pipeline.sql_client() as c:
# #     c.execute_sql("BEGIN TRANSACTION;\n INSERT INTO `mainnet_6_ethereum._dlt_loads` VALUES('2022-06-09 22:09:21.254700 UTC', '1654812536.548904', 1); COMMIT TRANSACTION;")
