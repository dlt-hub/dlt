---
title: Configure a pipeline
description: How to configure a pipeline
keywords: [how to, configure a pipeline]
---

# Configure a pipeline

To configure your pipeline in `dlt`, you can use providers to set up your configurations. These configurations can be specified using one or a combination of the following methods:

1. **Environment Variables:** Set configuration parameters directly in your environment, which `dlt` can read at runtime.
2. **Vaults:** Use secure vault services such as Airflow, Google Cloud, AWS, or Azure to manage and retrieve configuration parameters.
3. **Configuration Files:** Utilize `secrets.toml` and `config.toml` files to define your pipeline settings.
4. **Default Argument Values:** Specify default values directly within your code when initializing the pipeline.

## Example Configuration

To configure a `dataset_name_layout` which will prefix the names of all datasets created by your pipeline, follow these steps using one of the following methods:


1. **Environment Variables:**
   ```bash
   export DATASET_NAME_LAYOUT="myprefix_%s"
   ```

2. **Configuration Files:**
   - Create and define `config.toml`:
     ```toml
     dataset_name_layout = "myprefix_%s"
     ```

> Note: the value of `dataset_name_layout` must contain a '%s' placeholder for dataset_name. For example: 'olivia_%s'.

Using one of these methods, you can set the `dataset_name_layout` to ensure that all datasets created by your pipeline have the desired prefix. This helps your team better organize and manage data while working together.


## Further readings

- [Beef up your script for production](../running-in-production/running.md), easily add alerting,
  retries and logging, so you are well-informed when something goes wrong.
- [Deploy this pipeline with GitHub Actions](deploy-a-pipeline/deploy-with-github-actions), so that
  your pipeline script is automatically executed on a schedule.
