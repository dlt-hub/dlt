# Secure Data Access and Sharing

dlt+ makes it easy for end-users (like data scientists) to access high quality production data in a secure and Python-friendly way. dlt+ Projects expose a standard Python API which connects to the production data using the profile "access". How the end-users interact with the data can be restricted by setting data and schema contracts for the datasets in the manifest file (`dlt.yml`) under the profile "access".  
  
## Pythonic data access and sharing

As dlt+ packages are distributed over PyPI or git, data end-users are easily able to interact with them within their own Pythonic workflows. An example of what this can look like:  
  
1. They pip install the project created by the data engineer into their local python environment (for example, notebooks).

    ```sh
    pip install -U --index-url https://pypi.dlthub.com dlt_example_project
    ```
2. They import the project like any Python module and access the datasets. The datasets declared in the project create a data catalog which can be used to discover the schema, and read and write data.

    ```py
    import dlt_example_project as dlt_project
 
    catalog = dlt_project.catalog() # Inspect datasets and available tables
    print(catalog)
    print(catalog.github_events_dataset) # Access the dataset github_events_dataset from the catalog
    ```
3. They do their analytics on the data and are able to share back their results.
  
    ```py
    # load data as dataframe data frame (use .arrow() if you prefer arrow. you can also do sql)
    df = catalog.github_events_dataset.issues_event.df()

    # create reports
    reports_df = aggregate_issues(df)

    # share back the reports
    print(catalog.reports_dataset.save(reports_df, table_name="aggregated_issues"))
    ```

## Security and contracts

When the end-users interact with the data using the Python API, they are doing it through a profile called "access". This access can be managed simply by setting configurations and credentials for this profile in the manifest (`dlt.yml`) or in the toml files. Read more about setting secrets and configurations for different profile [here](../core-concepts/profiles.md). 

It's also possible to set granular limits on how users can write the data through schema and data contracts. These can be set individually per profile per dataset.
  
```yaml
profiles:

    access:
        datasets:

            github_events_dataset:
                # no new tables, no column changes
                contract: freeze 

            reports_dataset:
                # allow new tables but no column changes
                contract:
                    tables: evolve
                    columns: freeze
                    data_type: freeze
```

In the example above, users with profile "access" are restricted from writing any tables or modifying the schema of existing tables in the dataset `github_events_dataset`. So if the user tried to write back their tables to this dataset instead of the `reports_dataset`:  
  
```py
print(catalog.github_events_dataset.save(reports_df, table_name="aggregated_issues"))
```

then they would get the following error:  
  
```bash
PipelineStepFailed: Pipeline execution failed at stage extract when processing package 1730314603.1941314 with exception:

<class 'dlt.common.schema.exceptions.DataValidationError'>
In schema: events: In Schema: events Table: aggregated_issues  . Contract on tables with mode freeze is violated. Trying to add table aggregated_issues but new tables are frozen.
```

There are also contracts set on the `reports_dataset` that allow users to write tables but restrict them from modifying existing schema. For example, if the user wanted to add a new column `id` to the existing table `aggregated_issues` inside the `reports_dataset`:  
  
```py
# Access the aggregated_issues table from the reports_dataset in the catalog
reports_df = catalog.reports_dataset.aggregated_issues.df()

# Create a new column "id"
reports_df["id"] = 1

# Push back the modified table
print(catalog.reports_dataset.save(reports_df, table_name="aggregated_issues"))
```

then they would get the following error:  
  
```bash

PipelineStepFailed: Pipeline execution failed at stage extract when processing package 1730314610.4309433 with exception:

<class 'dlt.common.schema.exceptions.DataValidationError'>
In schema: out_source: In Schema: out_source Table: aggregated_issues Column: id . Contract on columns with mode freeze is violated. Trying to add column id to table aggregated_issues but columns are frozen.
```
