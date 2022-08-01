## **pipeline is a singleton**.

only one active pipeline at a time (but you can switch). default pipeline is activated when imported (exactly how stramlit works)

## **we optionally support repository structure**

`.dlt/`
- `secrets` (one of credential providers)
- `preferences` - here you can configure pipelines that are being created instead of providing parameters in the code. the intention is that the difference between production and dev pipline is the config - when deleted we are in production mode
- `deployments`: here the auto created deployments go ie. `dlt deploy github-action` will create github workflow here. `dlt deploy helm` will create helm deployment here

example `preferences`
```
schema_sync_folder = "./schemas"
should_sync_schemas = true
should_full_refresh = true
```

`schemas/`
this folder will automatically sync the schemas with the pipeline
- after every run (or schema modifications) the changes are written here
- on every run the schemas are imported from there
- we backup all the schema versions

the behavior is optional

## **sources**

- 3.a `ad hoc sources` that are a part of pipeline and you do not intent to share. may be a snippet though
- 3.b `importable sources` that are meant to be reused

the API for both should be different

### **3.a**

here the API will be similar to your pseudo-code

### **3.b. what is the importable source?**

1. importable source with single table (jsonl source, database table/query source)
2. importable source with many tables as separate iterators (metabase, ethereum)

3. imortable source with many tables in single iterator (rasa case and any other stream of events ie. discord gateway)

4. 1 - 3 but with the table configuration (schema) that cannot be declared or hardcoded but must be inferred (ie. google sheets, data base source etc.)

all sources return iterators (or list of iterators?)
