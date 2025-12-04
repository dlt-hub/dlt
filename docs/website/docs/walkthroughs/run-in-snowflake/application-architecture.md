---
title: Application Architecture Documentation
description: Architecture and security overview of the dlt ingest app
keywords: [snowflake, native app, dlt]
---


# Snowflake Native App Architecture

The dlt ingest app is a Snowflake Native App that uses Snowpark Container Servcies and dlt to extract, normalize, and load data from user-configured source systems into a Snowflake destination.

## Application Architecture 

### Containers in the Application
The application runs a Snowpark Container Services job container responsible for executing the data pipeline.

```yaml
containers:
  - name: jobcontainer
    image: /tutorial_image_database/tutorial_image_schema/tutorial_image_repo/my_job_service_image:dlt_ingest_app
    env:
      SNOWFLAKE_WAREHOUSE: tutorial_warehouse
```

### External Integrarions

External Access Integrations allow the application to connect to external systems such as databases or S3 buckets. 

```yaml
 EXTERNAL_SOURCE_DATABASE_ACCESS:
      label: "Source database"
      description: "This External Access Integration is required to access the source database"
      privileges:
        - USAGE
      object_type: EXTERNAL_ACCESS_INTEGRATION
      register_callback: app_core.register_reference
      configuration_callback: app_core.get_external_access_config
 EXTERNAL_STAGE_ACCESS:
      label: "External stage"
      description: "This External Access Integration is required to access the external stage (S3)"
      privileges:
        - USAGE
      object_type: EXTERNAL_ACCESS_INTEGRATION
      register_callback: app_core.register_reference
      configuration_callback: app_core.get_external_access_config
```

### Egress Network Rules

The app only connects to S3 endpoints defined by the user; no wildcard 0.0.0.0 egress.

Egress URLs:
`{bucket_name}.s3.{bucket_region_name}.amazonaws.com:443`, where `bucket_name`and `bucket_region_name` are configured by the app user.

### UDFs and Stored Procedures 

Functions and procedures provide configuration lookup, ID generation, secret handling, job execution, compute management, and task scheduling.

#### Configuration UDFs and Helper Function

`get_pipeline_job_config`: Looks up pipeline-specific job configuration stored in `config.jobs`.

```sql
CREATE OR REPLACE FUNCTION code.get_pipeline_job_config(pipeline_name STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    SELECT value FROM config.jobs WHERE key = pipeline_name
$$;
```

`get_pipeline_integrations_config`: Looks up integration configuration (e.g. references, staging type) in `config.integrations`.

```sql
CREATE OR REPLACE FUNCTION code.get_pipeline_integrations_config(pipeline_name STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    SELECT value FROM config.integrations WHERE key = pipeline_name
$$;
```

`get_task_id`: Helper function that namespaces task names in `task`.

```sql
CREATE OR REPLACE FUNCTION code.get_task_id(task_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
    'tasks.' || task_name
$$;
```
#### Secret Helper

`create_get_sercet_proc`: Dynamically generates a Python UDF and a corresponding SQL wrapper that securely retrieve a user-provided secret through its Snowflake secret reference. Because secret reference names are only known at installation time, this dynamic creation ensures the application can access each secret without hard-coding object names or embedding any sensitive information.


```sql
CREATE OR REPLACE PROCEDURE code.create_get_secret_proc(ref_name STRING, secret_ref_name STRING)
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    func_id STRING;
    proc_id STRING;
BEGIN
    func_id := 'unversioned_code.get_' || secret_ref_name || '_func';
    proc_id := 'unversioned_code.get_' || secret_ref_name;

    EXECUTE IMMEDIATE '
        CREATE OR REPLACE FUNCTION ' || func_id || '()
            RETURNS VARIANT
            LANGUAGE PYTHON
            RUNTIME_VERSION = 3.12
            HANDLER = ''get_secret_username_password''
            EXTERNAL_ACCESS_INTEGRATIONS = (reference(''' || ref_name || '''))
            SECRETS = (''secret'' = reference(''' || secret_ref_name || '''))
            AS
''
import _snowflake

def get_secret_username_password() -> dict:
    username_password_object = _snowflake.get_username_password("secret");

    return {
        "username": username_password_object.username,
        "password": username_password_object.password
    }
'';

    EXECUTE IMMEDIATE '
        CREATE OR REPLACE PROCEDURE ' || proc_id || '()
        RETURNS VARIANT
        LANGUAGE SQL
        AS
        BEGIN
            RETURN ' || func_id || '();
        END;
    ';

    RETURN 'OK';
END;
```

#### Reference Registration 


```sql
CREATE OR REPLACE PROCEDURE code.register_reference(ref_name STRING, operation STRING, ref_or_alias STRING)
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    integration_id INT;
    secret_ref_name STRING;
BEGIN
    CASE (operation)
        WHEN 'ADD' THEN
            SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
            SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
        WHEN 'CLEAR' THEN
            SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
        ELSE RETURN 'unknown operation: ' || operation;
    END CASE; 

    IF (ref_name LIKE 'EXTERNAL_SOURCE_DATABASE_ACCESS_%') THEN
        integration_id := RIGHT(ref_name, 1)::INT;
        secret_ref_name := 'SOURCE_DATABASE_SECRET_' || integration_id;
        CALL code.create_get_secret_proc(:ref_name, :secret_ref_name);
    END IF;

    IF (ref_name LIKE 'EXTERNAL_STAGE_ACCESS_%') THEN
        integration_id := RIGHT(ref_name, 1)::INT;
        secret_ref_name := 'AWS_SECRET_ACCESS_KEY_' || integration_id;
        CALL code.create_get_secret_proc(:ref_name, :secret_ref_name);
    END IF;

    RETURN 'OK';
END;
```

#### External Access Configuration Procedures

`configure_external_access`: Writes/stores config for a ref (pipeline, host/port, secret reference).

`get_external_access_config`: Returns configuration for a given reference to Snowflake.


```sql
CREATE OR REPLACE PROCEDURE code.configure_external_access(
    ref_name STRING,
    pipeline_name STRING,
    host_port STRING,
    secret_ref_name STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = (
    '/scripts/imports/constants.py',
    '/scripts/imports/external_access.py',
    '/scripts/imports/kv_table.py',
    '/scripts/imports/types_.py'
)
HANDLER = 'external_access.configure_external_access';

CREATE OR REPLACE PROCEDURE code.get_external_access_config(ref_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = (
    '/scripts/imports/constants.py',
    '/scripts/imports/external_access.py',
    '/scripts/imports/kv_table.py',
    '/scripts/imports/types_.py'
)
HANDLER = 'external_access.get_external_access_config';
```

#### Job Run Initialization & Compute Setup:


`init_job_run`:
- Generates `job_id` and records a row in `jobs.run`.
- Captures who triggered the job and when.


```sql
CREATE OR REPLACE PROCEDURE code.init_job_run(pipeline_name STRING, triggered_by STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    job_id STRING;
BEGIN
    job_id := UUID_STRING();

    INSERT INTO jobs.runs (
        job_id,
        pipeline_name,
        triggered_by,
        triggered_at,
        status
    )
    VALUES (
        :job_id,
        :pipeline_name,
        UPPER(:triggered_by),
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()),
        'STARTING'
    );

    RETURN :job_id;
END;
$$;
```

`create_or_alter_compute`: Loads pipeline configuration (pool instance family, auto-suspend, warehouse size). Creates and update compute pool and warehouse.

```sql
CREATE OR REPLACE PROCEDURE code.create_or_alter_compute(pipeline_name STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    pipeline_job_config VARIANT;
    compute_pool_instance_family STRING;
    compute_pool_auto_suspend INT;
    warehouse_size STRING;
    warehouse_auto_suspend INT;
    pool_name VARCHAR;
    warehouse_name VARCHAR;
BEGIN
   pipeline_job_config := code.get_pipeline_job_config(:pipeline_name);

   compute_pool_instance_family := pipeline_job_config:compute_pool_instance_family;
   compute_pool_auto_suspend := pipeline_job_config:compute_pool_auto_suspend;
   warehouse_size := pipeline_job_config:warehouse_size;
   warehouse_auto_suspend := pipeline_job_config:warehouse_auto_suspend;

   pool_name := (SELECT CURRENT_DATABASE()) || '_CP_' || :compute_pool_instance_family;
   warehouse_name := (SELECT CURRENT_DATABASE()) || '_JOB_WH';

   CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER(:pool_name)
      MIN_NODES = 1
      MAX_NODES = 1
      INSTANCE_FAMILY = :compute_pool_instance_family
      AUTO_RESUME = true
      AUTO_SUSPEND_SECS = :compute_pool_auto_suspend;

   ALTER COMPUTE POOL IF EXISTS IDENTIFIER(:pool_name) SET AUTO_SUSPEND_SECS = :compute_pool_auto_suspend;

   CREATE OR ALTER WAREHOUSE IDENTIFIER(:warehouse_name)
      WAREHOUSE_SIZE = :warehouse_size
      AUTO_SUSPEND = :warehouse_auto_suspend;

   RETURN OBJECT_CONSTRUCT('pool_name', :pool_name, 'warehouse_name', :warehouse_name);
END;
$$;
```

#### Executing the Job Service

`execute_job_services`:
- Drops any existing `jobs.dlt_pipeline_run` service to ensure a clean start.
- Determines whether to use an external stage based on staging.
- Builds a list of `EXTERNAL_ACCESS_INTEGRATIONS` references.
- Executes `EXECUTE JOB SERVICE` using `job_service/job_spec.yaml`, passing `job_id` and `pipeline_name`.
- Grants `MONITOR` privilege on the service to the app role.

```sql
CREATE OR REPLACE PROCEDURE code.execute_job_service(
    job_id STRING,
    pipeline_name STRING,
    pool_name STRING
)
    RETURNS STRING
    LANGUAGE SQL
AS
DECLARE
    staging STRING;
    pipeline_job_config VARIANT;
    pipeline_integrations_config VARIANT;
    source_database_ref_name STRING;
    external_stage_ref_name STRING;
    use_external_stage BOOLEAN;
    eai_refs STRING;
BEGIN
    DROP SERVICE IF EXISTS jobs.dlt_pipeline_run;

    pipeline_job_config := code.get_pipeline_job_config(:pipeline_name);
    staging := pipeline_job_config:staging;
    use_external_stage := (staging <> 'Internal');

    pipeline_integrations_config := code.get_pipeline_integrations_config(:pipeline_name);
    source_database_ref_name := pipeline_integrations_config:source_database:ref_name;

    IF (:use_external_stage) THEN
        external_stage_ref_name := pipeline_integrations_config:external_stage:ref_name;
        eai_refs := 'reference(''' || source_database_ref_name || '''), reference(''' || external_stage_ref_name || ''')';
    ELSE
        eai_refs := 'reference(''' || source_database_ref_name || ''')';
    END IF;

    EXECUTE IMMEDIATE '
        EXECUTE JOB SERVICE
            IN COMPUTE POOL identifier(''' || :pool_name || ''')
            FROM SPECIFICATION_TEMPLATE_FILE = ''job_service/job_spec.yaml''
            USING (job_id => ''"' || :job_id || '"'', pipeline_name => ''"' || :pipeline_name || '"'' ) 
            NAME = jobs.dlt_pipeline_run 
            ASYNC = TRUE 
            EXTERNAL_ACCESS_INTEGRATIONS = (' || :eai_refs || ')
    ';

    GRANT MONITOR ON SERVICE jobs.dlt_pipeline_run TO APPLICATION ROLE app_user;

    RETURN 'OK';
END;
```

####  Job Orchestration

`run_job`: Entry point for running a pipeline, that initializes the job run, ensures compute pool and warehouse are configured and executes the job service in the compute pool.

```sql
CREATE OR REPLACE PROCEDURE code.run_job(pipeline_name STRING, triggered_by STRING DEFAULT NULL)
    RETURNS STRING
    LANGUAGE SQL
AS
DECLARE
    job_id STRING;
    compute_result VARIANT;
    pool_name VARCHAR;
BEGIN
   CALL code.init_job_run(:pipeline_name, :triggered_by) INTO job_id;
   CALL code.create_or_alter_compute(:pipeline_name) INTO compute_result;
   pool_name := compute_result:pool_name;
   CALL code.execute_job_service(:job_id, :pipeline_name, :pool_name);

   RETURN 'OK';
END;
```

#### Task Management

`create_or_alter_task`: Creates or alters Snowflake task, which calls `code.run_job`to execute pipeline. 

```sql
CREATE OR REPLACE PROCEDURE code.create_or_alter_task(
    task_name STRING,
    schedule STRING,
    pipeline_name STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    task_id STRING;
BEGIN
    task_id := code.get_task_id(task_name);
    EXECUTE IMMEDIATE 'CREATE OR ALTER TASK IDENTIFIER(''' || :task_id || ''') ' ||
        'SCHEDULE = ''' || :schedule || ''' ' ||
        'COMMENT = ''{"pipeline_name": "' || :pipeline_name || '"}'' ' ||
        'AS CALL code.run_job('''|| :pipeline_name || ''', ''' || :task_id || ''');';

    GRANT MONITOR ON TASK IDENTIFIER(:task_id) TO APPLICATION ROLE app_user;
    RETURN 'OK';
END;
$$;
```
Other task procedures (`resume_task`, `suspend_task`, `drop_task`) compute the `task_id` and run the corresponding `ALTER` or `DROP` statements.

### Security Controls
User-provided credentials are stored in the consumer account using Snowflake secrets.

### Architecture Diagram

![Run dlt in Snowflake architecture](https://storage.googleapis.com/dlt-blog-images/snowflake-app-architecture.svg)

### Components

**Streamlit UI**  
User-facing application hosted in Snowflake. It opens a Snowpark session and interacts only with Snowflake objects (tables, procedures, tasks) to manage pipelines and runs.

**Snowflake Objects**  
Schemas, tables, functions, procedures, and tasks created in the consumer account. They store pipeline configuration, orchestrate execution, and bridge the UI with Snowflake compute (compute pools and warehouses) and job services.

**Job Service (SPCS)**  
Containerized runtime executed by Snowflake. It connects to external sources via External Access Integrations and Secrets, merges UI configuration with integration metadata and secrets, and runs dlt to produce destination-ready loads.

**Consumer Snowflake DB (Destination)**  
The target database in the consumer account where normalized dlt output tables are created and maintained.

**External Source System**  
User-configured source database reachable through an External Access Integration and corresponding Secret.


### End-to-end flow

1. **User configures a pipeline in the UI** (e.g., pipeline name, source settings, scheduling options).
2. **Streamlit UI opens a Snowpark session** and routes user actions (create/edit pipeline, run, schedule, delete) to stored procedures.
3. **Snowflake objects persist configuration** by writing UI values into `config.*` tables.
4. **Orchestration procedures prepare execution** by:
   1. inserting a run record into `jobs.runs`,  
   2. ensuring the required compute pool and warehouse exist or are updated,  
   3. creating or updating a Snowflake task when scheduling is enabled.
5. **Job service starts in Snowpark Container Services** and retrieves the pipeline configuration plus required integration references and secrets.
6. **dlt extracts data from the external source** using credentials stored in Snowflake secrets.
7. **dlt normalizes and loads data into the destination database** using the configured warehouse/compute pool.
8. **Job service records pipeline state, logs, and status** by writing into `jobs.logs` and updating `jobs.runs`.
9. **Streamlit “Runs” tab queries Snowflake tables** to display run history, details, and logs back to the user.





### Data Accessed / Porcessed by 

The app extracts data from source databases (configured by the user), and normalizes this data, before loading it into a Snowflake “destination” database. The extraxt-normalize-load is done by dlt, which runs in a Snowpark Container services container.

### Consumer Data Accessed

The app does not access consumer data in Snowflake beyond what it itself writes into the destination database. The app does access external consumer data that lives in the source database(s) the user configures in the app.

### Consumer Data Stored Outside Consumer Account

Error and warning logs are shared. Debug logs are shared optionally.

```yaml
configuration:
  log_level: INFO
  trace_level: ALWAYS
  metric_level: ALL
  telemetry_event_definitions:
    - type: ERRORS_AND_WARNINGS
      sharing: MANDATORY
    - type: DEBUG_LOGS
      sharing: OPTIONAL
```

## Security Assurance

SDLC security activities include:
- Authenticated access to source code
- Peer reviews


### Container Images

Base Image: `FROM python:3.13-slim`

Dockerfile:

```sh
FROM python:3.13-slim

ARG USERNAME=jobuser

# Install system dependencies as root
RUN apt-get update && \
    apt-get install -y git curl && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user and job directory
RUN useradd -m $USERNAME && \
    mkdir /job && \
    chown $USERNAME:$USERNAME /job

# Switch to non-root user for security
USER $USERNAME

# Set working directory and copy application files
WORKDIR /job
COPY . ./
COPY --from=common . ./common/

# Install uv and extend PATH to include its binary
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/home/$USERNAME/.local/bin:$PATH"

# Create virtual environment and install Python dependencies
RUN uv venv && uv pip install -r requirements.txt

ENTRYPOINT ["uv", "run", "main.py"]
```


Security measures applied to the container image:
- CVE scanning: Yes (Trivy), no critical/high issues.
- Malware scanning: Yes (ClamAV).
- Non-root user: Yes, container runs as jobuser.
- Image layers & history: Yes, available for audit/debugging.

### Application Objects in Consumer Account

#### `setup.sql`:

```sql
EXECUTE IMMEDIATE FROM 'create_schemas.sql';

CREATE OR REPLACE STREAMLIT code.app_ui
    FROM '/streamlit'
    MAIN_FILE = '/streamlit_app.py';

EXECUTE IMMEDIATE FROM 'create_tables.sql';
EXECUTE IMMEDIATE FROM 'create_funcs.sql';
EXECUTE IMMEDIATE FROM 'create_procs.sql';
EXECUTE IMMEDIATE FROM 'grant_privs.sql';
```

#### `create_schemas.sql`:
```sql
CREATE SCHEMA IF NOT EXISTS config;
CREATE SCHEMA IF NOT EXISTS jobs;
CREATE SCHEMA IF NOT EXISTS tasks;

CREATE SCHEMA IF NOT EXISTS unversioned_code;
CREATE OR ALTER VERSIONED SCHEMA code;
```

#### `create_tables.sql`:

```sql
CREATE TABLE IF NOT EXISTS config.jobs (
    key STRING,
    value VARIANT
);

CREATE TABLE IF NOT EXISTS config.integrations (
    key STRING,
    value VARIANT
);

CREATE TABLE IF NOT EXISTS jobs.runs (
    job_id STRING,
    pipeline_name STRING,
    triggered_by STRING,
    triggered_at TIMESTAMP_TZ,
    started_at TIMESTAMP_TZ,
    ended_at TIMESTAMP_TZ,
    status STRING
);

CREATE TABLE IF NOT EXISTS jobs.logs (
    job_id STRING,
    timestamp TIMESTAMP_TZ,
    level STRING,
    message STRING,
    exc_text STRING
);
``` 

#### `create_funcs.sql`:

```sql 
CREATE OR REPLACE FUNCTION code.get_pipeline_job_config(pipeline_name STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    SELECT value FROM config.jobs WHERE key = pipeline_name
$$;

CREATE OR REPLACE FUNCTION code.get_pipeline_integrations_config(pipeline_name STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    SELECT value FROM config.integrations WHERE key = pipeline_name
$$;
``` 

#### `create_procs.sql`:

Contains all the procedures described in section [UDFs and Stored Procedures](#udfs-and-stored-procedures), including `init_job_run`, `create_or_alter_compute`, `execute_job_service`, `run_job`, `register_reference`, `create_get_secret_proc`, `configure_external_access`, `get_external_access_config`, `get_secret_config`, and `task-management` procedures.

Implements the full lifecycle:
- Storing and retrieving secrets.
- Configuring external access.
- Running pipeline jobs via job services.
- Managing scheduled tasks.


#### `grant_privs.sql`:

```sql
CREATE APPLICATION ROLE IF NOT EXISTS app_user;

-- Schema grants
GRANT USAGE ON SCHEMA jobs TO APPLICATION ROLE app_user;
GRANT USAGE ON SCHEMA tasks TO APPLICATION ROLE app_user;
GRANT USAGE ON SCHEMA code TO APPLICATION ROLE app_user;

-- Table grants
GRANT SELECT ON TABLE jobs.runs TO APPLICATION ROLE app_user;
GRANT SELECT ON TABLE jobs.logs TO APPLICATION ROLE app_user;

-- Streamlit grants
GRANT USAGE ON STREAMLIT code.app_ui TO APPLICATION ROLE app_user;

-- Procedure grants
GRANT USAGE ON PROCEDURE code.register_reference(STRING, STRING, STRING) TO APPLICATION ROLE app_user;
GRANT USAGE ON PROCEDURE code.get_external_access_config(STRING) TO APPLICATION ROLE app_user;
GRANT USAGE ON PROCEDURE code.get_secret_config(STRING) TO APPLICATION ROLE app_user;
GRANT USAGE ON PROCEDURE code.run_job(STRING, STRING) TO APPLICATION ROLE app_user;
``` 

#### priviliges in `manifest.yml`:

```yaml
privileges:
- EXECUTE TASK:
    description: "Create and run tasks"
- EXECUTE MANAGED TASK:
    description: "Create and run serverless tasks"
- CREATE COMPUTE POOL:
    description: "Permission to create compute pools for running services"
- CREATE WAREHOUSE:
    description: "Allows app to create warehouses in consumer account"
```

### Authentication Requirements

All app functionality is accessed via authenticated Snowflake sessions. No functionality can be accessed without authenticating through Snowflake first.



### Incidents and support
:::tip Support & security contact
For Snowflake Natiev App support and security-related incidents reports, contact **support@dlthub.com**.  


We’ll get back to you within **three working days**.
:::
