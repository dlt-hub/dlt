# API

### dlt.pipeline.attach(pipeline_name: str = None, pipelines_dir: str = None, pipeline_salt: ~dlt.common.typing.TSecretValue = None, full_refresh: bool = False, credentials: ~typing.Any = None, progress: ~dlt.common.runtime.collector.Collector | ~typing.Literal['tqdm', 'enlighten', 'log', 'alive_progress'] = <dlt.common.runtime.collector.NullCollector object>, \*\*kwargs: ~typing.Any)

Attaches to the working folder of pipeline_name in pipelines_dir or in default directory. Requires that valid pipeline state exists in working folder.

### dlt.pipeline.pipeline(pipeline_name: str = None, pipelines_dir: str = None, pipeline_salt: TSecretValue = None, destination: TDestinationReferenceArg = None, staging: TDestinationReferenceArg = None, dataset_name: str = None, import_schema_path: str = None, export_schema_path: str = None, full_refresh: bool = False, credentials: Any = None, progress: TCollectorArg = \_NULL_COLLECTOR)

### dlt.pipeline.pipeline()

Creates a new instance of dlt pipeline, which moves the data from the source ie. a REST API to a destination ie. database or a data lake.

Summary
: The pipeline functions allows you to pass the destination name to which the data should be loaded, the name of the dataset and several other options that govern loading of the data.
  The created Pipeline object lets you load the data from any source with run method or to have more granular control over the loading process with extract, normalize and load methods.

Please refer to the following doc pages
: - Write your first pipeline walkthrough: [https://dlthub.com/docs/walkthroughs/create-a-pipeline](https://dlthub.com/docs/walkthroughs/create-a-pipeline)
  - Pipeline architecture and data loading steps: [https://dlthub.com/docs/reference](https://dlthub.com/docs/reference)
  - List of supported destinations: [https://dlthub.com/docs/dlt-ecosystem/destinations](https://dlthub.com/docs/dlt-ecosystem/destinations)

* **Parameters:**
  * **pipeline_name** (*May also be provided later to the run* *or* *load methods* *of* *the Pipeline. If not provided at all then defaults to the*) – A name of the pipeline that will be used to identify it in monitoring events and to restore its state and data schemas on subsequent runs.
  * **added.** (*Defaults to the file name* *of* *pipeline script with dlt_ prefix*) – 
  * **pipelines_dir** (*str**,* *optional*) – A working directory in which pipeline state and temporary files will be stored. Defaults to user home directory: ~/dlt/pipelines/.
  * **pipeline_salt** (*TSecretValue**,* *optional*) – A random value used for deterministic hashing during data anonymization. Defaults to a value derived from the pipeline name.
  * **purposes.** (*Default value should not be used for any cryptographic*) – 
  * **destination** (*str* *|* *DestinationReference**,* *optional*) – A name of the destination to which dlt will load the data, or a destination module imported from dlt.destination.
  * **pipeline.** (*May also be provided to run method* *of* *the*) – 
  * **staging** (*str* *|* *DestinationReference**,* *optional*) – A name of the destination where dlt will stage the data before final loading, or a destination module imported from dlt.destination.
  * **pipeline.** – 
  * **dataset_name** (*str**,* *optional*) – A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. schema in relational databases or folder grouping many files.
  * **pipeline_name** – 
  * **import_schema_path** (*str**,* *optional*) – A path from which the schema yaml file will be imported on each pipeline run. Defaults to None which disables importing.
  * **export_schema_path** (*str**,* *optional*) – A path where the schema yaml file will be exported after every schema change. Defaults to None which disables exporting.
  * **full_refresh** (*bool**,* *optional*) – When set to True, each instance of the pipeline with the pipeline_name starts from scratch when run and loads the data to a separate dataset.
  * **False.** (*The datasets are identified by dataset_name_ + datetime suffix. Use this setting whenever you experiment with your data to be sure you start fresh on each run. Defaults to*) – 
  * **credentials** (*Any**,* *optional*) – Credentials for the destination ie. database connection string or a dictionary with google cloud credentials.
  * **None** (*In most cases should be set to*) – 
  * **values.** (*which lets dlt to use secrets.toml* *or* *environment variables to infer right credentials*) – 
  * **progress** (*str**,* *Collector*) – A progress monitor that shows progress bars, console or log messages with current information on sources, resources, data items etc. processed in
  * **extract** – 
  * **module.** (*normalize and load stage. Pass a string with a collector name* *or* *configure your own by choosing from dlt.progress*) – 
  * **libraries** (*We support most* *of* *the progress*) – try passing tqdm, enlighten or alive_progress or log to write to console/log.
* **Returns:**
  An instance of Pipeline class with. Please check the documentation of run method for information on what to do with it.
* **Return type:**
  Pipeline

### dlt.pipeline.run(data: Any, \*, destination: DestinationReference | module | None | str = None, staging: DestinationReference | module | None | str = None, dataset_name: str | None = None, credentials: Any | None = None, table_name: str | None = None, write_disposition: Literal['skip', 'append', 'replace', 'merge'] | None = None, columns: Sequence[TColumnSchema] | None = None, schema: Schema | None = None)

Loads the data in data argument into the destination specified in destination and dataset specified in dataset_name.

Summary
: This method will extract the data from the data argument, infer the schema, normalize the data into a load package (ie. jsonl or PARQUET files representing tables) and then load such packages into the destination.

The data may be supplied in several forms:
: * a list or Iterable of any JSON-serializable objects ie. dlt.run([1, 2, 3], table_name=”numbers”)
  * any Iterator or a function that yield (Generator) ie. dlt.run(range(1, 10), table_name=”range”)
  * a function or a list of functions decorated with @dlt.resource ie. dlt.run([chess_players(title=”GM”), chess_games()])
  * a function or a list of functions decorated with @dlt.source.

Please note that dlt deals with bytes, datetime, decimal and uuid objects so you are free to load binary data or documents containing dates.

Execution
: The run method will first use sync_destination method to synchronize pipeline state and schemas with the destination. You can disable this behavior with restore_from_destination configuration option.
  Next it will make sure that data from the previous is fully processed. If not, run method normalizes and loads pending data items.
  Only then the new data from data argument is extracted, normalized and loaded.

* **Parameters:**
  * **data** (*The behavior* *of* *this argument depends on the type* *of* *the*) – Data to be loaded to destination
  * **destination** (*str* *|* *DestinationReference**,* *optional*) – A name of the destination to which dlt will load the data, or a destination module imported from dlt.destination.
  * **provided** (*If not*) – 
  * **used.** (*the value passed to dlt.pipeline will be*) – 
  * **dataset_name** (*str**,* *optional*) – A name of the dataset to which the data will be loaded. A dataset is a logical group of tables ie. schema in relational databases or folder grouping many files.
  * **provided** – 
  * **pipeline_name** (*the value passed to dlt.pipeline will be used. If not provided at all then defaults to the*) – 
  * **credentials** (*Any**,* *optional*) – Credentials for the destination ie. database connection string or a dictionary with google cloud credentials.
  * **None** (*In most cases should be set to*) – 
  * **values.** (*which lets dlt to use secrets.toml* *or* *environment variables to infer right credentials*) – 
  * **table_name** (*str**,* *optional*) – The name of the table to which the data should be loaded within the dataset. This argument is required for a data that is a list/Iterable or Iterator without \_\_name_\_ attribute.
  * **data** – 
  * **functions** (*\* generator*) – the function name is used as table name, table_name overrides this default
  * **@dlt.resource** (*\**) – resource contains the full table schema and that includes the table name. table_name will override this property. Use with care!
  * **@dlt.source** (*\**) – source contains several resources each with a table schema. table_name will override all table names within the source and load the data into single table.
  * **write_disposition** (*Literal**[**"skip"**,* *"append"**,* *"replace"**,* *"merge"**]**,* *optional*) – Controls how to write data to a table. append will always add new data at the end of the table. replace will replace existing data with new data. skip will prevent data from loading. “merge” will deduplicate and merge data based on “primary_key” and “merge_key” hints. Defaults to “append”.
  * **dlt.source** (*Please note that in case* *of* *dlt.resource the table schema value will be overwritten and in case of*) – 
  * **overwritten.** (*the values in all resources will be*) – 
  * **columns** (*Sequence**[**TColumnSchema**]**,* *optional*) – A list of column schemas. Typed dictionary describing column names, data types, write disposition and performance hints that gives you full control over the created table schema.
  * **schema** (*Schema**,* *optional*) – An explicit Schema object in which all table schemas will be grouped. By default dlt takes the schema from the source (if passed in data argument) or creates a default one itself.
* **Raises:**
  **PipelineStepFailed when a problem happened during extract****,** **normalize** **or** **load steps.** – 
* **Returns:**
  Information on loaded data including the list of package ids and failed job statuses. Please not that dlt will not raise if a single job terminally fails. Such information is provided via LoadInfo.
* **Return type:**
  LoadInfo
