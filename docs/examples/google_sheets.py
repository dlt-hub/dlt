import dlt

from examples.sources.google_sheets import google_spreadsheet

dlt.pipeline(destination="bigquery", full_refresh=False)
# see example.secrets.toml to where to put credentials

info = google_spreadsheet("11G95oVZjieRhyGqtQMQqlqpxyvWkRXowKE8CtdLtFaU", ["2022-05", "model_metadata"]).run()
print(info)
