# Command-Line Tool Documentation

This document lists all the commands and subcommands available for this tool.

## `telemetry`

**Description**: No description available.

### Arguments:


---

## `schema`

**Description**: No description available.

### Arguments:

- **`file`**: Schema file name, in yaml or json format, will autodetect based on extension
- **`format`**: Display schema in this format
- **`remove_defaults`**: Does not show default hint values

---

## `pipeline`

**Description**: No description available.

### Arguments:

- **`list_pipelines`**: List local pipelines
- **`hot_reload`**: Reload streamlit app (for core development)
- **`pipeline_name`**: Pipeline name
- **`pipelines_dir`**: Pipelines working directory
- **`verbosity`**: Provides more information for certain commands.
- **`operation`**: None
  ## `info`

  **Description**: No description available.

  ### Arguments:


---

  ## `show`

  **Description**: No description available.

  ### Arguments:


---

  ## `failed-jobs`

  **Description**: No description available.

  ### Arguments:


---

  ## `drop-pending-packages`

  **Description**: No description available.

  ### Arguments:


---

  ## `sync`

  **Description**: No description available.

  ### Arguments:

  - **`destination`**: Sync from this destination when local pipeline state is missing.
  - **`dataset_name`**: Dataset name to sync from when local pipeline state is missing.

---

  ## `trace`

  **Description**: No description available.

  ### Arguments:


---

  ## `schema`

  **Description**: No description available.

  ### Arguments:

  - **`format`**: Display schema in this format
  - **`remove_defaults`**: Does not show default hint values

---

  ## `drop`

  **Description**: No description available.

  ### Arguments:

  - **`destination`**: Sync from this destination when local pipeline state is missing.
  - **`dataset_name`**: Dataset name to sync from when local pipeline state is missing.
  - **`resources`**: One or more resources to drop. Can be exact resource name(s) or regex pattern(s). Regex patterns must start with re:
  - **`drop_all`**: Drop all resources found in schema. Supersedes [resources] argument.
  - **`state_paths`**: State keys or json paths to drop
  - **`schema_name`**: Schema name to drop from (if other than default schema).
  - **`state_only`**: Only wipe state for matching resources without dropping tables.

---

  ## `load-package`

  **Description**: No description available.

  ### Arguments:

  - **`load_id`**: Load id of completed or normalized package. Defaults to the most recent package.

---


---

## `init`

**Description**: No description available.

### Arguments:

- **`list_sources`**: List available sources
- **`source`**: Name of data source for which to create a pipeline. Adds existing verified source or creates a new pipeline template if verified source for your data source is not yet implemented.
- **`destination`**: Name of a destination ie. bigquery or redshift
- **`location`**: Advanced. Uses a specific url or local path to verified sources repository.
- **`branch`**: Advanced. Uses specific branch of the init repository to fetch the template.
- **`eject`**: Ejects the source code of the core source like sql_database

---

## `deploy`

**Description**: No description available.

### Arguments:

- **`pipeline_script_path`**: Path to a pipeline script
- **`deployment_method`**: None
  ## `github-action`

  **Description**: No description available.

  ### Arguments:

  - **`location`**: Advanced. Uses a specific url or local path to pipelines repository.
  - **`branch`**: Advanced. Uses specific branch of the deploy repository to fetch the template.
  - **`schedule`**: A schedule with which to run the pipeline, in cron format. Example: '*/30 * * * *' will run the pipeline every 30 minutes. Remember to enclose the scheduler expression in quotation marks!
  - **`run_manually`**: Allows the pipeline to be run manually form Github Actions UI.
  - **`run_on_push`**: Runs the pipeline with every push to the repository.

---

  ## `airflow-composer`

  **Description**: No description available.

  ### Arguments:

  - **`location`**: Advanced. Uses a specific url or local path to pipelines repository.
  - **`branch`**: Advanced. Uses specific branch of the deploy repository to fetch the template.
  - **`secrets_format`**: Format of the secrets

---


---

