# Run a pipeline

Follow the steps below to run your pipeline script, see your loaded data and tables, inspect pipeline state, trace and handle the most common problems.

## 1. Write and execute pipeline script

Once you [created a new pipeline](create-a-pipeline) or [added an existing one](add-a-pipeline) you want to use it to load data. You need to write (or [customize](add-a-pipeline#3-customize-or-write-a-pipeline-script)) a pipeline script, like the one below that loads the data from chess.com API.

```python
import dlt
from chess import chess

if __name__ == "__main__" :
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
    # get data for a few famous players
    data = chess(['magnuscarlsen', 'rpragchess'], start_month="2022/11", end_month="2022/12")
    load_info = pipeline.run(data)
```

The `run` method will [extract](../how-dlt-works.md#extract) data from the chess API, [normalize](../how-dlt-works.md#normalize) it into tables and then [load](../how-dlt-works.md#load) into `duckdb` in form of one or many load packages. The `run` method returns a `load_info` object that, when printed, displays information with pipeline and dataset names, ids of the load packages, optionally with the information on failed jobs. Add the following line to your script:

```python
  print(load_info)
```

To get this printed:
```
Pipeline chess_pipeline completed in 1.80 seconds
1 load package(s) were loaded to destination duckdb and into dataset games_data
The duckdb destination used duckdb:////home/rudolfix/src/dlt_tests/dlt-cmd-test-3/chess_pipeline.duckdb location to store data
Load package 1679931001.985323 is COMPLETED and contains no failed jobs
```

## 2. See your data and tables

You can quickly inspect the generated tables, the data, see how many rows were loaded to which table, do SQL queries etc. by executing the following command from the same folder as your script.

```sh
$ dlt pipeline chess_pipeline show
```

This will launch a Streamlit app, that you can open in your browser:

```
Found pipeline chess_pipeline in /home/rudolfix/.dlt/pipelines

Collecting usage statistics. To deactivate, set browser.gatherUsageStats to False.


  You can now view your Streamlit app in your browser.

  Network URL: http://192.168.131.137:8501
  External URL: http://46.142.217.118:8501
```

## 3. Inspect a load process

`dlt` loads data in form of **load packages**. Each package contains several jobs with data for particular tables. The packages are identified by **load_id**, that you can see in the printout above or get by running the following command:

```sh
$ echo "Get information on the pipeline state and a list of load package ids
$ dlt pipeline chess_pipeline info
```

You can inspect the package, get list of jobs and in case of failed ones, get the associated error messages.

```sh
$ echo "See the most recent load package info"
$ dlt pipeline chess_pipeline load-package
$ echo "See package info with given load i"
$ dlt pipeline chess_pipeline load-package 1679931001.985323
$ echo "Also see the schema changes introduced in the package"
$ dlt pipeline -v chess_pipeline load-package
```

`dlt` stores the trace of the most recent data load. The trace contains information on the pipeline processing steps: `extract`, `normalize` and `load`. It also shows the last `load_info`:

```sh
$ dlt pipeline chess_pipeline trace
```

You can access all this information in your pipeline script, save `load_info` and trace to the destination etc. Please refer to [Running in production](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace) for more details.

## 4. Detect and handle problems

What happens if something goes wrong? In most cases `dlt` `run` command raises exceptions. We put a lot of effort into making the exception messages easy to understand. Reading them is the first step to solving your problem. Let us know
if you come across one that is not clear to you [here](https://github.com/dlt-hub/dlt/issues/new).

### Missing secret or configuration values

The most common exception that you will encounter looks like this. Here we modify our `chess_pipeline.py` script to load data into postgres, but we are not providing the password.

```
$ CREDENTIALS="postgres://loader@localhost:5432/dlt_data" python chess_pipeline.py
...
dlt.common.configuration.exceptions.ConfigFieldMissingException: Following fields are missing: ['password'] in configuration with spec PostgresCredentials
        for field "password" config providers and keys were tried in following order:
                In Environment Variables key CHESS_PIPELINE__DESTINATION__POSTGRES__CREDENTIALS__PASSWORD was not found.
                In Environment Variables key CHESS_PIPELINE__DESTINATION__CREDENTIALS__PASSWORD was not found.
                In Environment Variables key CHESS_PIPELINE__CREDENTIALS__PASSWORD was not found.
                In secrets.toml key chess_games.destination.postgres.credentials.password was not found.
                In secrets.toml key chess_games.destination.credentials.password was not found.
                In secrets.toml key chess_games.credentials.password was not found.
                In Environment Variables key DESTINATION__POSTGRES__CREDENTIALS__PASSWORD was not found.
                In Environment Variables key DESTINATION__CREDENTIALS__PASSWORD was not found.
                In Environment Variables key CREDENTIALS__PASSWORD was not found.
                In secrets.toml key destination.postgres.credentials.password was not found.
                In secrets.toml key destination.credentials.password was not found.
                In secrets.toml key credentials.password was not found.
Please refer to https://dlthub.com/docs/customization/credentials for more information

```
What this exception tells you?
1. You are missing a `password` field ("Following fields are missing: ['password']")
2. `dlt` tried to look for the password in `secrets.toml` and environment variables.
3. `dlt` tried several different locations or keys in which password could be stored, starting from more precise to more general.

How to fix that?

The easiest way is to look at the last line of the exception message: `In secrets.toml key credentials.password was not found.` and just add the `password` to your `secrets.toml` using the suggested key:

```toml
credentials.password="loader"
```

### Failed API or database connections and other exceptions

`dlt` will raise `PipelineStepFailed` exception to inform you of a problem encountered during execution of particular step. You can catch those in code:

```python
from dlt.pipeline.exceptions import PipelineStepFailed

try:
    return pipeline.run(data)
except PipelineStepFailed as step_failed:
    print(f"We failed at step: {step_failed.step} with step info {step_failed.step_info}")
    raise
```

or use `trace` command to review the last exception. Here we provided a wrong postgres password:

```sh
$ dlt pipeline chess_pipeline trace
```

```
Found pipeline chess_pipeline in /home/rudolfix/.dlt/pipelines
Run started at 2023-03-28T09:13:56.277016+00:00 and FAILED in 0.01 seconds with 1 steps.
Step run FAILED in 0.01 seconds.
Failed due to: connection to server at "localhost" (127.0.0.1), port 5432 failed: FATAL:  password authentication failed for user "loader"
```

### Failed jobs in load package

In rare cases some of the jobs in a load package will fail in such a way that `dlt` will not be able to load it, even if it retries the process. In that case the job is marked as failed and additional information is available. Please note that (if not otherwise configured), `dlt` **will not raise exception on failed jobs**.

```
Step run COMPLETED in 14.21 seconds.
Pipeline chess_pipeline completed in 35.21 seconds
1 load package(s) were loaded to destination dummy and into dataset None
The dummy destination used /dev/null location to store data
Load package 1679996953.776288 is COMPLETED and contains 4 FAILED job(s)!
```

What now?

Investigate further with following command:

```sh
$ dlt pipeline chess_pipeline failed-jobs
```

to get following output:

```
Found pipeline chess_pipeline in /home/rudolfix/.dlt/pipelines
Checking failed jobs in load id '1679996953.776288'
JOB: players_games.80eb41650c.0.jsonl(players_games)
JOB file type: jsonl
JOB file path: /home/rudolfix/.dlt/pipelines/chess_pipeline/load/loaded/1679996953.776288/failed_jobs/players_games.80eb41650c.0.jsonl
a random fail occured

```

The `a random fail occurred` (on console in red) is the error message from the destination. It should tell you what went wrong.

The most probable cause of the failed job is **the data in the job file**. You can inspect the file using **JOB file path** provided.

## Further readings
- [Beef up your script for production](../running-in-production/running.md), easily add alerting, retries and logging so you are well informed when something goes wrong
- [Deploy this pipeline](./walkthroughs/deploy-a-pipeline), so that your pipeline script is automatically executed on a schedule