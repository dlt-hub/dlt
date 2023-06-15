# Salesforce

Salesforce is a cloud-based platform that helps businesses manage customer relationships and optimize various operational aspects. It empowers you to effectively handle sales, marketing, customer service, and more.

This Salesforce `dlt` verified source and pipeline example offers the capability to load Salesforce endpoints such as "User" and "Opportunity" to a destination of your choosing. It enables you to conveniently load the following endpoints:

### Single loading endpoints (replace mode)

| Endpoint | Mode | Description |
| --- | --- | --- |
| User | replace | refers to an individual who has access to a Salesforce org or instance |
| UserRole | replace | a standard object that represents a role within the organization's hierarchy |
| Lead | replace | prospective customer/individual/org. that has shown interest in a company's products/services |
| Contact | replace | an individual person associated with an account or organization |
| Campaign | replace | marketing initiative or project designed to achieve specific goals, such as generating leads etc. |
| Product2 | replace | for managing and organizing your product-related data within the Salesforce ecosystem. |
| Pricebook2 | replace | used to manage product pricing and create price books. |
| PricebookEntry | replace | an object that represents a specific price for a product in a price book |

### Incremental endpoints (merge mode)

| Endpoint | Mode | Description |
| --- | --- | --- |
| Opportunity | merge | represents a sales opportunity for a specific account or contact |
| OpportunityLineItem | merge | represents individual line items or products associated with an Opportunity |
| OpportunityContactRole | merge | represents the association between an Opportunity and a Contact |
| Account | merge | individual or organization that interacts with your business |
| CampaignMember | merge | association between a Contact or Lead and a Campaign |
| Task | merge | used to track and manage various activities and tasks within the Salesforce platform |
| Event | merge | used to track and manage calendar-based events, such as meetings, appointments calls, or any other time-specific activities |

## Grab credentials

To set up your pipeline, you will require the following credentials: “*user_name”*, “*password”*, and “*security_token”*. The “*user_name”* and “*password”* are the ones that you used to log into your Salesforce account, to grab the “*security token”* please follow these steps:

### Grab `security_token`

1. Log in to your Salesforce account using your username and password.
2. Click on your profile picture or avatar at the top-right corner of the screen.
3. From the drop-down menu, select "Settings."
4. In the left-hand sidebar, under "Personal Setup," click on "My Personal Information," then select "Reset My Security Token."
5. On the Reset Security Token page, click the "Reset Security Token" button.
6. Salesforce will send an email to the email address associated with your account, note it.

## Initialize the Salesforce verified source and pipeline example

To get started with your verified source, follow these steps:

1. Open up your terminal or command prompt and navigate to the directory where you'd like to create your project.
2. Enter the following command:
    
    ```bash
    dlt init salesforce bigquery
    ```
    
    This command will initialize your verified source with Salesforce and creates a pipeline example with BigQuery as the destination. If you'd like to use a different destination, simply replace `bigquery` with the name of your preferred destination. You can find supported destinations and their configuration options in our [documentation](https://dlthub.com/docs/destinations/duckdb).
    
3. After running this command, a new directory will be created with the necessary files and configuration settings to get started.
    
    ```toml
    salesforce_source
    ├── .dlt
    │   ├── config.toml
    │   └── secrets.toml
    ├── salesforce
    │   └── __init__.py
    │   └── helpers.py
    │   └── settings.py
    ├── .gitignore
    ├── requirements.txt
    └── salesforce_pipeline.py
    ```
    

## **Add credential**

1. Inside the `.dlt` folder, you'll find a file called “*secrets.toml*”, which is where you can securely store your access tokens and other sensitive information. It's important to handle this file with care and keep it safe.
    
    Here's what the file looks like:
    
    ```toml
    # put your secret values and credentials here. do not share this file and do not push it to github
    [sources.salesforce]
    username = "please set me up!" # salesforce user name
    password = "please set me up!" # salesforce password
    security_token = "please set me up!" # salesforce security token generated
    
    [destination.bigquery.credentials]
    project_id = "project_id" # GCP project ID
    private_key = "private_key" # Unique private key (including `BEGIN and END PRIVATE KEY`)
    client_email = "client_email" # Service account email
    location = "US" # Project location (e.g. “US”)
    
    ```
    
2. Please replace the values of “*username”* and “*password”* in the “*secrets.toml”* with your actual Salesforce account login credentials.
3. Next, replace the value of “*security_token”* with the one that [you copied above](salesforce.md#grab-credentials). This will ensure that your verified source can access your Salesforce resources securely.
4. Next, follow the instructions in **[Destinations](https://dlthub.com/docs/destinations/duckdb)** to add credentials for your chosen destination. This will ensure that your data is properly routed to its final destination.

## Run the pipeline example

1. Install the necessary dependencies by running the following command:
    
    ```bash
    pip install -r requirements.txt
    ```
    
2. Now the pipeline can be run by using the command:
    
    ```bash
    python3 salesforce_pipeline.py
    ```
    
3. To make sure that everything is loaded as expected, use the command:
    
    ```bash
    dlt pipeline <pipeline_name> show
    ```
    
    For example, the pipeline_name for the above pipeline is `salesforce`, you may also use any custom name instead.
    

## Customizations

To load data to the destination using this verified source, you have the option to write your own methods.

### Source and resource methods

`dlt` works on the principle of [sources](https://dlthub.com/docs/general-usage/source) and [resources](https://dlthub.com/docs/general-usage/resource) that for this verified source are found in the `__init__.py` file within the salesforce directory. The `salesforce_pipeline.py` has two default methods:

**Source** **<u>salesforce_source:</u>**

```python
@dlt.source(name="salesforce")
def salesforce_source(
    user_name: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    security_token: str = dlt.secrets.value,
) ->Iterable[DltResource]:

    client = Salesforce(user_name, password, security_token)
```

- **`user_name`**: the name used to sign in to your sales force account.
- **`password`**: the password used to login to the salesforce account
- **`security_token`**:  A token required to authenticate the Salesforce API. This token is defined in the “*dlt.secret.toml”* file.

Inside the “*salesforce_source”* function, a connection to the Salesforce API is established using the provided “*user_name”*, “*password”*, and “*security_token”* parameters. This connection is used to initialize a “*client”* object using the “*Salesforce”* class, which handles the authentication with the Salesforce API.

The “*client”* object is then utilized within the resource methods to interact with the Salesforce API and retrieve data from specific Salesforce endpoints. Each resource method corresponds to a different Salesforce object and is responsible for fetching data from that object.

- **Resource** **<u>sf_user</u>: (single load)**

```python
@dlt.resource(write_disposition="replace")
def sf_user() -> Iterator[Dict[str, Any]]:
    yield from get_records(client, "User")
```

**`sf_user`**: This resource retrieves records from the Salesforce "User" endpoint. It is configured with a write disposition of "replace," which means the existing data in the destination will be replaced with the new data.

In addition to the **resource "sf_user"** there are several other resources defined in the file `__init.py__`that utilizes the replace mode when writing data to the destination.

| user_role() | contact() | lead() | campaign() | product_2() | pricebook_2() | pricebook_entry() |
| --- | --- | --- | --- | --- | --- | --- |

The functions mentioned above retrieve records from specific endpoints based on their names. For instance, the function `user_role()` retrieves data from the “user_role” endpoint.

Each resource returns an iterator that yields dictionaries representing the records fetched from the Salesforce API using the `get_records()` function.

- **Resource <u>opportunity:</u> (incremental loading)**

```python
@dlt.resource(write_disposition="merge")
def opportunity(
    last_timestamp: Incremental[str] = dlt.sources.incremental(
        "SystemModstamp", initial_value=None
		)) -> Iterator[Dict[str, Any]]:
        
    yield from get_records(
        client, "Opportunity", last_timestamp.last_value, "SystemModstamp"
		)
```

The above code defines a function called "opportunity" that writes data to a destination using a “*merge”* mode. This means that existing data in the destination will be preserved, and new data will be added or updated as needed. It works on the principle of *[incremental loading](https://dlthub.com/docs/general-usage/incremental-loading).*

- The function takes an optional parameter called "last_timestamp," which is used to track the timestamp of the last processed "Opportunity" record during the previous pipeline run. This parameter is obtained from a source called “*dlt.sources.incremental”* and is stored in the pipeline's *[state](https://dlthub.com/docs/general-usage/state)*.
- Within the function, there is a call to another function called "*get_records*." This function is provided with several arguments: the API client, "*Opportunity*" as the source of data, the initial value of the last processed timestamp from the previous pipeline run, and "*SystemModstamp*" as the timestamp until which the pipeline should run.
- The "*get_records*" function is expected to return an iterator containing dictionaries, with each dictionary representing a single record.
- The function uses a generator with the "yield from" statement to yield each record from the iterator obtained from "*get_records*". This allows the records to be consumed one by one.

In addition to the **resource "opportunity"** there are several other resources defined in the file `__init.py__`that utilizes the merge mode and incremental loading when writing data to the destination.

| opportunity_line_item() | opportunity_contact_role() | account() | campaign_member() | task() | event() |
| --- | --- | --- | --- | --- | --- |

The functions mentioned above retrieve records from specific endpoints based on their names. For instance, the function `opportunity_line_item()` retrieves data from the OpportunityLineItem endpoint.

Each resource returns an iterator that yields dictionaries representing the records fetched from the Salesforce API using the `get_records()` function.

### **Create Your Data Loading Pipeline**

If you wish to create your own pipelines you can leverage source and resource methods as discussed above.

To create your data pipeline using single loading and [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading), follow these steps:

1. Configure the pipeline by specifying the pipeline name, destination, and dataset. To read more about pipeline configuration, please refer to our documentation [here](https://dlthub.com/docs/general-usage/pipeline).
    
    ```python
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_pipeline",  # Use a custom name if desired
        destination="bigquery",  # Choose the appropriate destination (e.g., duckdb, redshift, post)
        dataset_name="salesforce_data"  # Use a custom name if desired
    )
    ```
    
2. To load data from all the endpoints, use the `salesforce_source` method as follows:
    
    ```python
    load_data = salesforce_source()
    ```
    
3. Use the method **`pipeline.run()`** to execute the pipeline:
    
    ```python
    load_info = pipeline.run(load_data)
    # pretty print the information on data that was loaded
    print(load_info)
    ```
    
4. To use the method `*pipeline.run()**` to load custom endpoints “*candidates*” and “*members*”, the above script may be modified as:
    
    ```python
    load_info = pipeline.run(load_data.with_resources("opportunity", "contact")
    # pretty print the information on data that was loaded
    print(load_info)
    ```
    
    > Note: that in the above run, the "opportunity" and “contact” endpoints are loaded incrementally using the 'merge' mode, utilizing the 'last_timestamp' key. During the initial run, the 'last_timestamp' value will be set to “None” which means it’ll load all the data. However, for subsequent runs, the pipeline will only merge data for the "opportunity" and “contact” endpoint starting from the 'last_timestamp.last_value', which represents the last 'last_timestamp' value from the previous pipeline run. It’s important to note that incremental loading is only possible for endpoints that have resources defined in merge mode and use “*dlt.sources.incremental*” parameter.
    > 

5. For using incremental loading for endpoints, it's important to keep the pipeline name and destination dataset name unchanged. The pipeline name is crucial for retrieving the [state](https://dlthub.com/docs/general-usage/state) of the last pipeline run, which includes the end date needed for loading data incrementally. Modifying these names can lead to [“full_refresh”](https://dlthub.com/docs/general-usage/pipeline#do-experiments-with-full-refresh) which will disrupt the tracking of relevant metadata(state) for [incremental data loading](https://dlthub.com/docs/general-usage/incremental-loading).
6. If you want to load data from the “contact” and “task” endpoints. You can do it as follows:
    
    ```python
    load_info = pipeline.run(load_data.with_resources("contact", "task")
    # pretty print the information on data that was loaded
    print(load_info)
    ```
    
    > Note: that in the mentioned pipeline, the "contact" parameter is loaded in "replace" mode for every run, meaning that it overwrites the existing data completely. On the other hand, the "task" endpoint can be loaded incrementally in "merge" mode, where new data is added or existing data is updated as necessary while preserving the already loaded data using the ‘**last_timestamp'**  value.
    > 

That’s it! Enjoy running your Salesforce DLT pipeline!