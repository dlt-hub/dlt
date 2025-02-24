---
title: Troubleshooting
description: Doc explaining the common failure scenarios in extract, transform and load stage and their mitigation measures
keywords: [faq, usage information, technical help]
---

# Pipeline common failure scenarios and mitigation measures

This guide outlines common failure scenarios during the Extract, Normalize, and Load stages of a data pipeline.

## Extract stage

Failures during the **Extract** stage often stem from source errors, memory limitations, or timestamp-related issues. Below are common scenarios and possible solutions.

### Source errors

Source errors typically result from rate limits, invalid credentials, or misconfigured settings.

### Common scenarios and possible solutions

1. **Rate limits (Error 429):**

    - **Scenario:**
        - Exceeding API rate limits triggers `Error 429`.

    - **Possible solution:**
        - Verify that authentication is functioning as expected.
        - Increase the API rate limit if permissible.
        - Review the API documentation to understand rate limits and examine headers such as `Retry-After`.
        - Implement request delays using functions like `time.sleep()` or libraries such as `ratelimiter` to ensure compliance with rate limits.
        - Handle "Too Many Requests" (`429`) responses by implementing retry logic with exponential backoff strategies.
        - Optimize API usage by batching requests when possible and caching results to reduce the number of calls.

2. **Invalid credentials (Error 401, 403, or `ConfigFieldMissingException`):**

    - **Scenario:**
        - Missing or invalid credentials cause these errors.

    - **Possible solution:**
        - Verify credentials and ensure proper scopes/permissions are enabled. For more on how to set up credentials:  [Read our docs](../general-usage/credentials/setup).
        - If dlt expects a configuration of secrets value but cannot find it, it will output the `ConfigFieldMissingException`. [Read more about the exceptions here.](../general-usage/credentials/setup#understanding-the-exceptions)

3. **Source configuration errors (`DictValidationException`):**

    - **Scenario:**
        - Incorrect field placement (e.g., `params` outside the `endpoint` field).
        - Unexpected fields in the configuration.
        - For example, this is the incorrect configuration:
      ```py
      # ERROR 2: Method outside endpoint
      source = rest_api_source(
          config={
              "client": {
                  "base_url": "https://jsonplaceholder.typicode.com/"
              },
              "resources": [
                  {
                      "name": "posts",
                      # Wrong: method should be inside endpoint
                      "method": "GET",  
                      "endpoint": {
                          "path": "posts",
                          "params": {
                              "_limit": 5
                          }
                      }
                  }
              ]
          }
      )
      ```
    - Correct configuration:
    
    ```py
    # Create the source first
    source = rest_api_source(
        config={
            "client": {
                "base_url": "https://jsonplaceholder.typicode.com/"
            },
            "resources": [  # Add this line
                {
                    "name": "posts",
                    "endpoint": {
                        "path": "posts",
                        "method": "GET",
                        "params": {
                            "_limit": 5
                        }
                    }
                }
            ]
        }
    )
    ```
    - **Possible solution:**
    - Review and validate the code configuration structure against the source documentation.
        
    Read [REST API’s source here.](../dlt-ecosystem/verified-sources/rest_api/)

### Memory errors

Memory issues can disrupt extraction processes.

### Common scenarios and possible solutions

   1. **RAM exhaustion:**

       - **Scenario:**
           - Available RAM is insufficient for in-memory operations.

       - **Possible solution:**

           1. **Buffer Size Management:**
               - Adjust `max_buffer_items` to limit buffer size. [Learn about buffer configuration.](./performance#controlling-in-memory-buffers)
           2. Streaming Processing
               - Big data should be processed in chunks for efficient handling.

   2. **Storage memory shortages:**

       - **Scenario:**

           - Intermediate files exceed available storage space.

       - **Possible solution:**

           - If your storage reaches its limit, you can mount an external cloud storage location and set the `DLT_DATA_DIR` environment variable to point to it. This ensures that dlt uses the mounted storage as its data directory instead of local disk space. [Read more here.](./performance)
   
### Unsupported timestamps

Timestamp issues occur when formats are incompatible with the destination or inconsistent across pipeline runs.

### Common scenarios and possible solutions

1. **Unsupported formats or features:**

    - **Scenario:**

        - Combining `precision` and `timezone` in timestamps causes errors in specific destinations (e.g., DuckDB).

    - **Possible solution:**

        - Simplify the timestamp format to exclude unsupported features. Example:

        ```py
        import dlt

        @dlt.resource(
            columns={"event_tstamp": {"data_type": "timestamp", "precision": 3, "timezone": False}},
            primary_key="event_id",
        )
        def events():
            yield [{"event_id": 1, "event_tstamp": "2024-07-30T10:00:00.123+00:00"}]
        
        pipeline = dlt.pipeline(destination="duckdb")
        pipeline.run(events())
        ```

2. **Inconsistent formats across runs:**

    - **Scenario:**

        - Different pipeline runs use varying timestamp formats, affecting column datatype inference at the destination.
        
    - **Impact:**

        - For instance:
            - **1st pipeline run:** `{"id": 1, "end_date": "2024-02-28 00:00:00"}`
            - **2nd pipeline run:** `{"id": 2, "end_date": "2024/02/28"}`
            - **3rd pipeline run:** `{"id": 3, "end_date": "2024-07-30T10:00:00.123456789"}`
       
        - If the first run uses a timestamp-compatible format (e.g., `YYYY-MM-DD HH:MM:SS`), the destination (BigQuery) infers the column as a `TIMESTAMP`. Subsequent runs using compatible formats are automatically converted to this type.
        
        - However, introducing incompatible formats later, such as:
            - **4th pipeline run:** `{"id": 4, "end_date": "20-08-2024"}` (DD-MM-YYYY)
            - **5th pipeline run:** `{"id": 5, "end_date": "04th of January 2024"}`
        
        - BigQuery will interpret these as text and create a new variant column (`end_date__v_text`) to store the incompatible values. This preserves the schema consistency while accommodating all data.

    - **Possible solution:**

        - Standardize timestamp formats across all runs to maintain consistent schema inference and avoid the creation of variant columns.
        
3. **Inconsistent formats for incremental loading**

    - **Scenario:**

        - Data source returns string timestamps but incremental loading is configured with an integer timestamp value.
        - Example:
        ```py
        # API response
        data = [
            {"id": 1, "name": "Item 1", "created_at": "2024-01-01 00:00:00"},
        ]
        
        # Incorrect configuration (type mismatch)
        @dlt.resource(primary_key="id")
        def my_data(
            created_at=dlt.sources.incremental(
                "created_at",
                initial_value= 9999
            )
        ):
            yield data
        ```

    - **Impact:**

       - Pipeline fails with `IncrementalCursorInvalidCoercion` error
       - Error message indicates comparison failure between integer and string types
       - Unable to perform incremental loading until type mismatch is resolved

    - **Possible Solutions:**

       - Use string timestamp for incremental loading.
       - Convert source data using “add_map”.
       - If you need to use timestamps for comparison but want to preserve the original format, create a separate column.

## Normalize stage

Failures during the **Normalize** stage commonly arise from memory limitations, parallelization issues, or schema inference errors.

### Memory errors

Memory-intensive operations may fail during normalization.

### Common scenarios and possible solutions

1. **Large dataset in one resource:**

    - **Scenario:**

        - Large datasets exhaust memory during processing.

    - **Possible solution:**

        - Enable file rotation using `file_max_items` or `file_max_bytes`.
        - Increase parallel workers for better processing. [Read more about parallel processing.](./performance#parallelism-within-a-pipeline)

2. **Storage memory shortages**

    - **Scenario:**

        - When lots of files are being processed, the available storage space might be insufficient.

    - **Possible solution:**

        - If your storage reaches its limit, you can mount an external cloud storage location and set the `DLT_DATA_DIR` environment variable to point to it. This ensures that dlt uses the mounted storage as its data directory instead of local disk space. [Read more here.](./performance#keep-pipeline-working-folder-in-a-bucket-on-constrained-environments)

### Parallelization issues

Improper configuration of workers may lead to inefficiencies or failures.

### Common scenarios and possible solutions

1. **Resource exhaustion or underutilization:**

    - **Scenario:**

        - Too many workers may exhaust resources; too few may underutilize capacity.

    - **Possible solution:**

        - Adjust worker settings in the `config.toml` file. [Read more about parallel processing.](./performance#parallelism-within-a-pipeline)

2. **Threading conflicts:**

    - **Scenario:**

        - The `fork` process spawning method (default on Linux) conflicts with threaded libraries.

    - **Possible solution:**

        - Switch to the `spawn` method for process pool creation. [Learn more about process spawning.](./performance#normalize)

### Schema inference errors

Complex or inconsistent data structures can cause schema inference failures.

### Common scenarios and possible solutions

1. **Inconsistent data types:**

    - **Scenario:**
      ```py
      # First pipeline run
      data_run_1 = [
          {"id": 1, "value": 42},              # value is integer
          {"id": 2, "value": 123}
      ]
      
      # Second pipeline run
      data_run_2 = [
          {"id": 3, "value": "high"},          # value changes to text
          {"id": 4, "value": "low"}
      ]
      
      # Third pipeline run
      data_run_3 = [
          {"id": 5, "value": 789},             # back to integer
          {"id": 6, "value": "medium"}         # mixed types
      ]
      ```

    - **Impact:**

        - Original column remains as is.
        - New variant column `value__v_text` created for text values.
        - May require additional data handling in downstream processes.

    - **Possible solutions:**

        - Enforce Type Consistency
            - You can enforce type consistency using the `apply_hints` method. This ensures all values in a column follow the specified data type.

                ```py
                # Assuming 'resource' is your data resource
                resource.apply_hints(columns={
                    "value": {"data_type": "text"},  # Enforce 'value' to be of type 'text'
                })
                ```

            - In this example, the `value` column is always treated as text, even if the original data contains integers or mixed types.

        - Handle multiple types with separate columns.
            - The `dlt` library automatically handles mixed data types by creating variant columns. If a column contains different data types, `dlt` generates a separate column for each type.
            - For example, if a column named `value` contains both integers and strings, `dlt` creates a new column called `value__v_text` for the string values.
            - After processing multiple runs, the schema will be:

                ```text
                | name          | data_type     | nullable |
                |---------------|---------------|----------|
                | id            | bigint        | true     |
                | value         | bigint        | true     |
                | value__v_text | text          | true     |
                ```

        - Use Type validation **to Ensure Consistency**
            - When processing pipeline runs with mixed data types, type validation can be applied to enforce strict type rules.

            - **Example:**
            ```py
            def validate_value(value):
                if not isinstance(value, (int, str)):  # Allow only integers and strings
                    raise TypeError(f"Invalid type: {type(value)}. Expected int or str.")
                return str(value)  # Convert all values to a consistent type (e.g., text)

            # First pipeline run
            data_run_1 = [{"id": 1, "value": validate_value(42)},
                          {"id": 2, "value": validate_value(123)}]
            
            # Second pipeline run
            data_run_2 = [{"id": 3, "value": validate_value("high")},
                          {"id": 4, "value": validate_value("low")}]
            
            # Third pipeline run
            data_run_3 = [{"id": 7, "value": validate_value([1, 2, 3])}]
            ```

            In this example, data_run_4 contains an invalid value (a list) instead of an integer or string. When the pipeline runs with data_run_4, the validate_value function raises a TypeError.

2. **Nested data challenges:**

    - **Scenario:**

        - Issues arise due to deep nesting, inconsistent nesting, or unsupported types.

    - **Possible solution:**

        - Simplify nested structures or preprocess data. [Read about nested tables.](../general-usage/destination-tables#nested-tables)
        - You can limit unnesting level with `max_table_nesting`.

## Load stage

Failures in the **Load** stage often relate to authentication issues, schema changes, datatype mismatches or memory problems. 

### Authentication and connection failures

### Common scenarios and possible solutions

- **Scenario:**

    - Incorrect credentials.
    - Data loading is interrupted due to connection issues or database downtime. This may leave some tables partially loaded or completely empty, halting the pipeline process.

- **Possible solution:**

    - Verify credentials and follow proper setup instructions. [Credential setup guide.](../general-usage/credentials/setup)
    - If the connection is restored, you can resume the load process using the `pipeline.load()` method. This ensures the pipeline picks up from where it stopped, reloading any remaining data packages.
    - If data was **partially loaded**, check the `dlt_loads` table. If a `load_id` is missing from this table, it means the corresponding load **failed**. You can then remove partially loaded data by deleting any records associated with `load_id` values that do not exist in `dlt_loads`. [More details here](../general-usage/destination-tables#load-packages-and-load-ids).

### Schema changes (e.g., column renaming, Datatype mismatches)

### Common scenarios and possible solutions

- **Scenario:**

    - Renamed columns create variant columns in the destination schema.
    - Incoming datatypes that the destination doesn’t support result in variant columns.

- **Possible solution:**

    - Use schema evolution to handle column renaming. [Read more about schema evolution.](../general-usage/schema-evolution#evolving-the-schema)

### `FileNotFoundError` for 'schema_updates.json' in parallel runs

- **Scenario**
  When running the same pipeline name multiple times in parallel (e.g., via Airflow), `dlt` may fail at the load stage with an error like:
  
  > `FileNotFoundError: schema_updates.json not found`
  
  This happens because `schema_updates.json` is generated during normalization. Concurrent runs using the same pipeline name may overwrite or lock access to this file, causing failures.
  
- **Possible Solutions**
  
  1. **Use unique pipeline names for each parallel run** 
  
     If calling `pipeline.run()` multiple times within the same workflow (e.g., once per resource), assign a unique `pipeline_name` for each run. This ensures separate working directories, preventing file conflicts.
  
  2. **Leverage dlt’s concurrency management or Airflow helpers**  
  
     dlt’s Airflow integration “serializes” resources into separate tasks while safely handling concurrency. To parallelize resource extraction without file conflicts, use:  
     ```py
     decompose="serialize"
     ```
     More details are available in the [Airflow documentation](../walkthroughs/deploy-a-pipeline/deploy-with-airflow-composer#2-valueerror-can-only-decompose-dlt-source).
  
  3. **Disable dev mode to prevent multiple destination datasets**  
  
     When `dev_mode=True`, dlt generates unique dataset names (`<dataset_name>_<timestamp>`) for each run. To maintain a consistent dataset, set:  
     ```py
     dev_mode=False
     ```
     Read more about this in the [dev mode documentation](../general-usage/pipeline#do-experiments-with-dev-mode).

### Memory management issues

- **Scenario:**

    - Loading large datasets without file rotation enabled. This would make dlt try to upload a huge data set into destination at once. *(Note: Rotation is disabled by default.)*

- **Impact:**

    - Pipeline failures due to out-of-memory errors.

- **Possible Solution:**

    - Enable file rotation. [Read more about it here.](./performance#controlling-intermediary-file-size-and-rotation)

By identifying potential failure scenarios and applying the suggested mitigation strategies, you can ensure reliable and efficient pipeline performance.