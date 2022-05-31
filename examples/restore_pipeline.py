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

    # unpack
    m = pipeline.unpack()
    if m.has_failed:
        print("Unpacking failed")
        print(pipeline.last_run_exception)
        exit(0)

    # show loads to be loaded to target
    print(pipeline.list_unpacked_loads())

    # and finish up loading
    m = pipeline.load()
    if m.has_failed:
        print("Loading failed, fix the problem, restore the pipeline and run loading packages again")
        print(pipeline.last_run_exception)
    else:
        # should be empty
        new_loads = pipeline.list_unpacked_loads()
        print(new_loads)

        # now enumerate all complete loads if we have any failed packages
        # complete but failed job will not raise any exceptions
        completed_loads = pipeline.list_completed_loads()
        # print(completed_loads)
        for load_id in completed_loads:
            print(f"Checking failed jobs in {load_id}")
            for job, failed_message in pipeline.list_failed_jobs(load_id):
                print(f"JOB: {job}\nMSG: {failed_message}")
