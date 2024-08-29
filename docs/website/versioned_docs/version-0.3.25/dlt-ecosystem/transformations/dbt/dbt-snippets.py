def run_dbt_standalone_snippet() -> None:
    # @@@DLT_SNIPPET_START run_dbt_standalone
    import os

    from dlt.helpers.dbt import create_runner

    runner = create_runner(
        None,  # use current virtual env to run dlt
        None,  # we do not need dataset name and we do not pass any credentials in environment to dlt
        working_dir=".",  # the package below will be cloned to current dir
        package_location="https://github.com/dbt-labs/jaffle_shop.git",
        package_profiles_dir=os.path.abspath("."),  # profiles.yml must be placed in this dir
        package_profile_name="duckdb_dlt_dbt_test"  # name of the profile
    )

    models = runner.run_all()
    # @@@DLT_SNIPPET_END run_dbt_standalone

    for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")
