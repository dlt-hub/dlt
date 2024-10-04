---
title: Run a pipeline
description: How to run a pipeline
keywords: [how to, run a pipeline]
---

# Run a pipeline

Follow the steps below to run your pipeline script, see your loaded data and tables, inspect
pipeline state, trace and handle the most common problems.

## 1. Write and execute pipeline script

Once you have [created a new pipeline](create-a-pipeline) or
[added and verified a source](add-a-verified-source), you will want to use it to load data. You need to write
(or [customize](add-a-verified-source#3-customize-or-write-a-pipeline-script)) a pipeline script,
like the one below that loads data from the [chess.com](https://www.chess.com) API:

```py
import dlt
from chess import chess

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
    # get data for a few famous players
    data = chess(['magnuscarlsen', 'rpragchess'], start_month="2022/11", end_month="2022/12")
    load_info = pipeline.run(data)
```

The `run` method will [extract](../reference/explainers/how-dlt-works.md#extract) data from the
chess API, [normalize](../reference/explainers/how-dlt-works.md#normalize) it into tables, and then
[load](../reference/explainers/how-dlt-works.md#load) it into `duckdb` in the form of one or many load
packages. The `run` method returns a `load_info` object that, when printed, displays information
with pipeline and dataset names, ids of the load packages, and optionally, information on failed
jobs. Add the following line to your script:

```py
print(load_info)
```

To get this printed:

```text
Pipeline chess_pipeline completed in 1.80 seconds
1 load package(s) were loaded to destination duckdb and into dataset games_data
The duckdb destination used duckdb:////home/user-name/src/dlt_tests/dlt-cmd-test-3/chess_pipeline.duckdb location to store data
Load package 1679931001.985323 is COMPLETED and contains no failed jobs
```

## 2. See the progress during loading

Suppose you want to load a whole year of chess games and that it takes some time. You can enable
progress bars or console logging to observe what the pipeline is doing. We support most of the Python
progress bar libraries, Python loggers, or just a text console. To demonstrate, let's modify the
script to get a year of chess games data:

```py
data = chess(['magnuscarlsen', 'rpragchess'], start_month="2021/11", end_month="2022/12")
```

Install [enlighten](https://github.com/Rockhopper-Technologies/enlighten). Enlighten displays
progress bars that can be mixed with log messages:

```sh
pip install enlighten
```

Run your script setting the `PROGRESS` environment variable to the library name:

```sh
PROGRESS=enlighten python chess_pipeline.py
```

Other libraries that you can use are [tqdm](https://github.com/tqdm/tqdm),
[alive_progress](https://github.com/rsalmei/alive-progress). Set the name to `log` to dump progress
to the console periodically:

```sh
PROGRESS=log python chess_pipeline.py
```

[You can configure the progress bars however you want in code](../general-usage/pipeline.md#display-loading-progress).

## 3. See your data and tables

You can quickly inspect the generated tables, the data, see how many rows were loaded to which
table, do SQL queries, etc., by executing the following command from the same folder as your script:

```sh
dlt pipeline chess_pipeline show
```

This will launch a Streamlit app, which you can open in your browser:

```text
Found pipeline chess_pipeline in /home/user-name/.dlt/pipelines

Collecting usage statistics. To deactivate, set browser.gatherUsageStats to False.


  You can now view your Streamlit app in your browser.

  Network URL: http://192.168.131.137:8501
  External URL: http://46.142.217.118:8501
```

## 4. Inspect a load process

`dlt` loads data in the form of **load packages**. Each package contains several jobs with data for
particular tables. The packages are identified by **load_id**, which you can see in the printout
above or obtain by running the following command:

```sh
dlt pipeline chess_pipeline info
```

You can inspect the package, get a list of jobs, and in the case of failed ones, get the associated error
messages.
- See the most recent load package info:
  ```sh
  dlt pipeline chess_pipeline load-package
  ```
- See package info with a given load id:
  ```sh
  dlt pipeline chess_pipeline load-package 1679931001.985323
  ```
- Also, see the schema changes introduced in the package:
  ```sh
  dlt pipeline -v chess_pipeline load-package
  ```

`dlt` stores the trace of the most recent data load. The trace contains information on the pipeline
processing steps: `extract`, `normalize`, and `load`. It also shows the last `load_info`:

```sh
dlt pipeline chess_pipeline trace
```

You can access all this information in your pipeline script, save `load_info` and trace to the
destination, etc. Please refer to
[Running in production](../running-in-production/running.md#inspect-and-save-the-load-info-and-trace)
for more details.

## 5. Detect and handle problems

What happens if something goes wrong? In most cases, the `dlt` `run` command raises exceptions. We put a
lot of effort into making the exception messages easy to understand. Reading them is the first step
to solving your problem. Let us know if you come across one that is not clear to you
[here](https://github.com/dlt-hub/dlt/issues/new).

### Missing secret or configuration values

The most common exception that you will encounter looks like this. Here we modify our
`chess_pipeline.py` script to load data into PostgreSQL, but we are not providing the password.

```sh
CREDENTIALS="postgres://loader@localhost:5432/dlt_data" python chess_pipeline.py
...
dlt.common.configuration.exceptions.ConfigFieldMissingException: Following fields are missing: ['password'] in configuration with spec PostgresCredentials
    for field "password" config providers and keys were tried in the following order:
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
Please refer to https://dlthub.com/docs/general-usage/credentials for more information
```

What does this exception tell you?

1. You are missing a `password` field ("Following fields are missing: \['password'\]").
1. `dlt` tried to look for the password in `secrets.toml` and environment variables.
1. `dlt` tried several locations or keys in which the password could be stored, starting from
   more precise to more general.

How to fix that?

The easiest way is to look at the last line of the exception message:

`In secrets.toml key credentials.password was not found.`

and just add the `password` to your
`secrets.toml` using the suggested key:

```toml
credentials.password="loader"
```

> 💡 Make sure you run the script from the same folder in which it is saved. For example,
> `python chess_demo/chess.py` will run the script from the `chess_demo` folder, but the current working
> directory is the folder above. This prevents `dlt` from finding `chess_demo/.dlt/secrets.toml` and
> filling in credentials.

### Failed API or database connections and other exceptions

`dlt` will raise a `PipelineStepFailed` exception to inform you of a problem encountered during
the execution of a particular step. You can catch those in code:

```py
from dlt.pipeline.exceptions import PipelineStepFailed

try:
    pipeline.run(data)
except PipelineStepFailed as step_failed:
    print(f"We failed at step: {step_failed.step} with step info {step_failed.step_info}")
    raise
```

Or use the `trace` command to review the last exception. Here we provided a wrong PostgreSQL password:

```sh
dlt pipeline chess_pipeline trace
```

```text
Found pipeline chess_pipeline in /home/user-name/.dlt/pipelines
Run started at 2023-03-28T09:13:56.277016+00:00 and FAILED in 0.01 seconds with 1 steps.
Step run FAILED in 0.01 seconds.
Failed due to: connection to server at "localhost" (127.0.0.1), port 5432 failed: FATAL:  password authentication failed for user "loader"
```

### Failed jobs in load package

In rare cases, some jobs in a load package will fail in such a way that `dlt` will not be able
to load it, even if it retries the process. In that case, the job is marked as failed, and additional
information is available. Please note that ([if not otherwise configured](../running-in-production//running.md#failed-jobs)), `dlt` **will raise
an exception on failed jobs and abort the package**. Aborted packages cannot be retried.

```text
Step run COMPLETED in 14.21 seconds.
Pipeline chess_pipeline completed in 35.21 seconds
1 load package(s) were loaded to destination dummy and into dataset None
The dummy destination used /dev/null location to store data
Load package 1679996953.776288 is COMPLETED and contains 4 FAILED job(s)!
```

What now?

Investigate further with the following command:

```sh
dlt pipeline chess_pipeline failed-jobs
```

To get the following output:

```text
Found pipeline chess_pipeline in /home/user-name/.dlt/pipelines
Checking failed jobs in load id '1679996953.776288'
JOB: players_games.80eb41650c.0.jsonl(players_games)
JOB file type: jsonl
JOB file path: /home/user-name/.dlt/pipelines/chess_pipeline/load/loaded/1679996953.776288/failed_jobs/players_games.80eb41650c.0.jsonl
a random fail occurred
```

The `a random fail occurred` (on console in red) is the error message from the destination. It
should tell you what went wrong.

The most probable cause of the failed job is **the data in the job file**. You can inspect the file
using the **JOB file path** provided.

## Further readings

- [Beef up your script for production](../running-in-production/running.md), easily add alerting,
  retries, and logging, so you are well-informed when something goes wrong.
- [Deploy this pipeline with GitHub Actions](deploy-a-pipeline/deploy-with-github-actions), so that
  your pipeline script is automatically executed on a schedule.

