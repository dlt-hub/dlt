from dlt.pipeline import Pipeline

if __name__ == '__main__':
    credentials = Pipeline.load_gcp_credentials("_secrets/project1234_service.json", "mainnet_3")
    # credentials = PostgresPipelineCredentials("redshift", "chat_analytics_rasa", "mainnet_2", "loader", "3.73.90.3")

    pipeline = Pipeline("ethereum")
    # restore pipeline from the working directory
    # working directory contains pipeline full state after it was created
    # it is possible to restore pipeline in many concurrently running components ie. several extractors, separate unpacker and loader
    # pipeline.restore_pipeline(credentials, "/tmp/tmp724aveoc/")
    pipeline.restore_pipeline(credentials, "/tmp/tmpswbi3v0o/")

    # the code below will unpack and load anything that was extracted by extractor process(es)

    # do we have anything to unpack
    print(pipeline.list_extracted_loads())

    # just finalize
    pipeline.flush()
