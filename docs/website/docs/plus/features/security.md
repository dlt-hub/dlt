# Secure Data Access and Sharing

dlt+ makes it easy for end-users (like data scientists) to access high quality production data in a secure and Python-friendly way. dlt+ Projects expose a standard Python API which connects to the production data using the profile "access". How the end-users interact with the data can be restricted by setting data and schema contracts for the datasets in the manifest file (`dlt.yml`) under the profile "access".  
  
## Pythonic data access and sharing

As dlt+ packages are distributed over PyPI or git, data end-users are easily able to interact with them within their own Pythonic workflows. An example of what this can look like:  
  
1. They pip install the project created by the data engineer into their local python environment (for example, notebooks).

    ```sh
    pip install -U --index-url https://pypi.dlthub.com dlt_example_project
    ```
2. They import the project like any Python module and are able access the data catalog, datasets, and tables from production.

    ```py
    import dlt_example_project as dlt_project
 
    catalog = dlt_project.catalog() # Inspect datasets and available tables
    print(catalog)
    print(catalog.github_events_dataset)
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

### Security and contracts

