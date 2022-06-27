from oauth2client.service_account import ServiceAccountCredentials

from dlt.pipeline import Pipeline, GCPPipelineCredentials

from examples.sources.google_sheets import get_source


p = Pipeline("annotations")
credentials = GCPPipelineCredentials.from_services_file('_secrets/project1234_service.json', "load_3")
p.create_pipeline(credentials)

# create google credentials to get data from source
sheets_credentials = ServiceAccountCredentials.from_json_keyfile_name('_secrets/project1234_service.json')

p.extract(
    get_source(sheets_credentials, "11G95oVZjieRhyGqtQMQqlqpxyvWkRXowKE8CtdLtFaU", "2022-05"),
    table_name="model_2022-05"
)
p.flush()
# display inferred schema
print(p.get_default_schema().as_yaml(remove_defaults=True))
