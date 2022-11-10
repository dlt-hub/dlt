from examples.sources.google_sheets import google_spreadsheet

# see example.secrets.toml to where to put credentials

info = google_spreadsheet("11G95oVZjieRhyGqtQMQqlqpxyvWkRXowKE8CtdLtFaU", ["2022-05", "model_metadata"]).run(destination="postgres")
print(info)
