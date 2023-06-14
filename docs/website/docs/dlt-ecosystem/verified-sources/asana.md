---
title: Asana
description: dlt verified source for Asana API
keywords: [asana api, asana verified source, asana]
---

# Asana

Asana is a widely used web-based project management and collaboration tool that helps teams stay organized, focused, and productive. With Asana, team members can easily create, assign, and track tasks, set deadlines, and communicate with each other in real time.

When you use Asana, you can create various resources like "projects", "tasks", "users", "workspaces" and others mentioned below to help you manage your work effectively. You'll be happy to know that you can easily load these resources via the dlt pipeline.

Resources that can be loaded using this verified source are:

| S.No. | Name | Description |
| --- | --- | --- |
| 1 | workspaces | people, materials, or assets required to complete a task or project successfully |
| 2 | projects | collections of tasks and related information |
| 3 | sections | used to organize tasks within a project into smaller groups or categories |
| 4 | tags | labels that can be attached to tasks, projects, or conversations to help categorize and organize them. |
| 5 | stories | updates or comments that team members can add to a task or project |
| 6 | teams | groups of individuals who work together to complete projects and tasks |
| 7 | users | individuals who have access to the Asana platform |

## Grab Asana credentials

1. To get started, head over to the Asana developer console by clicking on this link: **[https://app.asana.com/-/developer_console](https://app.asana.com/-/developer_console)**.
2. Next,  click on the "Create new token" button located in the "Personal Access Token" section.
3. Give your access token a name that is meaningful to you and take a moment to read and agree to the API terms and conditions.
4. After that, simply click on "Create token" and you're all set!
5. Now, be sure to copy your Access token safely as it is only displayed once.
6. This token will be used to configure secrets.toml, so keep it secure and don't share it with anyone.

## Initialize the pipeline with Asana source

To get started with your data pipeline, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:

```bash

dlt init asana_dlt bigquery
```

This command will initialize your pipeline with Asana as the source and BigQuery as the destination. If you'd like to use a different destination, simply replace **`bigquery`** with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](../destinations/)

3. After running this command, a new directory will be created with the necessary files and configuration settings to get started. From here, you can begin configuring your pipeline to suit your specific needs.

## **Add credential**

1. Inside the **`.dlt`** folder, you'll find a file called **`secrets.toml`**, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.

Here's what the file looks like:

```

[sources.asana_dlt]
access_token = "access_token" # please set me up!

[destination.bigquery.credentials]
project_id = "project_id" # GCP project ID
private_key = "private_key" # Unique private key (including `BEGIN and END PRIVATE KEY`)
client_email = "client_email" # Service account email
location = "US" # Project location (e.g. “US”)
```

2. Replace the value of **`access_token`** with the one that [you copied above](asana.md#grab-asana-credentials). This will ensure that your data pipeline can access your Asana resources securely.
3. Finally, follow the instructions in **[Destinations](../destinations/)** to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

## Customize the pipeline

1. Within the file **`asana_dlt_pipeline.py`**, you will find a **`load`** function that is defined to load data from Asana to your chosen destination.
2. The **`load`** function defines the pipeline name, destination, and dataset name that will be loaded to the destination. You may keep the defaults or modify them as needed.
3. Below the **`load`** function is the main method, which is written as follows:

```python
if __name__ == "__main__":
    # Add your desired resources to the list...
    resources = ["projects", "tasks", "users", "workspaces", "tags", "stories", "sections", "teams"]
    load(resources)
```

You can customize the resources you want to load exclusively by modifying the **`resources`** list. For example, if you only want to load “tasks”, “users”, and “workspaces”, you can modify the method as follows:

```python

if __name__ == "__main__":
    # Add your desired resources to the list...
    resources = ["tasks", "users", "workspaces"]
    load(resources)
```

Please remember to save the file once you have made the necessary changes.

## Run the pipeline[](https://dlthub.com/docs/dlt-ecosystem/verified-sources/strapi#run-the-pipeline)

1. Before running the pipeline, ensure that you have installed all the necessary dependencies by running the command:

    **`pip install -r requirements.txt`**

2. You're now ready to run the pipeline! To get started, run the following command:

    **`python3 asana_dlt_pipeline.py`**

3. Once the pipeline has finished running, you can verify that everything loaded correctly by using the following command:

    **`dlt pipeline <pipeline_name> show`**


Note that in the above command, replace **`<pipeline_name>`** with the name of your pipeline. For example, if you named your pipeline "asana," you would run:

**`dlt pipeline asana show`**


That's it! Enjoy running your Asana DLT pipeline!
