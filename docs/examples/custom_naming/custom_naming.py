"""
---
title: Create and use own naming convention
description: We demonstrate how to create naming conventions that allow UNICODE letters and never generate collisions
keywords: [example]
---

This example shows how to add and use custom naming convention. Naming conventions translate identifiers found in source data into identifiers in
destination, where rules for a valid identifier are constrained.

Custom naming conventions are classes that derive from `NamingConvention` that you can import from `dlt.common.normalizers.naming`. We recommend the following module layout:
1. Each naming convention resides in a separate Python module (file)
2. The class is always named `NamingConvention`

There are two naming conventions in this example:
1. A variant of `sql_ci` that generates identifier collisions with a low (user defined) probability by appending a deterministic tag to each name.
2. A variant of `sql_cs` that allows for LATIN (ie. umlaut) characters

With this example you will learn to:
* Create a naming convention module with a recommended layout
* Use naming convention by explicitly passing it to `duckdb` destination factory
* Use naming convention by configuring it config.toml
* Changing the declared case sensitivity by overriding `is_case_sensitive` property
* Providing custom normalization logic by overriding `normalize_identifier` method

"""

import dlt

if __name__ == "__main__":
    # sql_cs_latin2 module
    import sql_cs_latin2  # type: ignore[import-not-found]

    # create postgres destination with a custom naming convention. pass sql_cs_latin2 as module
    # NOTE: ql_cs_latin2 is case sensitive and postgres accepts UNICODE letters in identifiers
    dest_ = dlt.destinations.postgres(
        "postgresql://loader:loader@localhost:5432/dlt_data", naming_convention=sql_cs_latin2
    )
    # run a pipeline
    pipeline = dlt.pipeline(
        pipeline_name="sql_cs_latin2_pipeline",
        destination=dest_,
        dataset_name="example_data",
        dev_mode=True,
    )
    # Extract, normalize, and load the data
    load_info = pipeline.run([{"StückId": 1}], table_name="Ausrüstung")
    print(load_info)
    with pipeline.sql_client() as client:
        # NOTE: we quote case sensitive identifers
        with client.execute_query('SELECT "StückId" FROM "Ausrüstung"') as cur:
            print(cur.description)
            print(cur.fetchone())

    # sql_ci_no_collision (configured in config toml)
    # NOTE: pipeline with name `sql_ci_no_collision` will create default schema with the same name
    # so we are free to use it in config.toml to just affect this pipeline and leave the postgres pipeline as it is
    pipeline = dlt.pipeline(
        pipeline_name="sql_ci_no_collision",
        destination="duckdb",
        dataset_name="example_data",
        dev_mode=True,
    )
    # duckdb is case insensitive so tables and columns below would clash but sql_ci_no_collision prevents that
    data_1 = {"ItemID": 1, "itemid": "collides"}
    load_info = pipeline.run([data_1], table_name="BigData")

    data_2 = {"1Data": 1, "_1data": "collides"}
    # use colliding table
    load_info = pipeline.run([data_2], table_name="bigdata")

    with pipeline.sql_client() as client:
        from duckdb import DuckDBPyConnection

        conn: DuckDBPyConnection = client.native_connection
        # tags are deterministic so we can just use the naming convention to get table names to select
        first_table = pipeline.default_schema.naming.normalize_table_identifier("BigData")
        sql = f"DESCRIBE TABLE {first_table}"
        print(sql)
        print(conn.sql(sql))
        second_table = pipeline.default_schema.naming.normalize_table_identifier("bigdata")
        sql = f"DESCRIBE TABLE {second_table}"
        print(sql)
        print(conn.sql(sql))

    # print(pipeline.default_schema.to_pretty_yaml())
