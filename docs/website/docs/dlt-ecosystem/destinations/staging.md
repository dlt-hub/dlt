---
title: Staging
description: Configure an s3 or gcs bucket for staging before copying into the destination
keywords: [staging, destination]
---

# Staging

dlt supports a staging location for some destinations. Currently it is possible to copy files from a s3 bucket into redshift and from a gcs bucket into bigquery. dlt will automatically select an appropriate loader file format for the staging files. For this to work you have to set the staging argument of the pipeline to filesystem and provide both the credentials for the staging and the destination.

## Redshift and s3

```python
import dlt
import requests
# Create a dlt pipeline that will load
# chess player data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='redshift',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
# Grab some player data from Chess.com API
data = []
for player in ['magnuscarlsen', 'rpragchess']:
  response = requests.get(f'https://api.chess.com/pub/player/{player}')
  response.raise_for_status()
  data.append(response.json())
# Extract, normalize, and load the data
pipeline.run(data, table_name='player')
```

For this to work, the credentials for redshift and the s3 filesystem destinations must be set up as described in the respective sections in your `secrets.toml`.

```toml
[destination]
redshift.credentials="postgresql://..."
bucket_url = "s3://my_bucket"

[destination.filesystem.credentials]
aws_access_key_id="access key"
aws_secret_access_key="secret access key"
```

## GCS and BigQuery

```python
import dlt
import requests
# Create a dlt pipeline that will load
# chess player data to the DuckDB destination
pipeline = dlt.pipeline(
    pipeline_name='chess_pipeline',
    destination='biquery',
    staging='filesystem', # add this to activate the staging location
    dataset_name='player_data'
)
# Grab some player data from Chess.com API
data = []
for player in ['magnuscarlsen', 'rpragchess']:
  response = requests.get(f'https://api.chess.com/pub/player/{player}')
  response.raise_for_status()
  data.append(response.json())
# Extract, normalize, and load the data
pipeline.run(data, table_name='player')
```

For this to work, the credentials for redshift and the s3 filesystem destinations must be set up as described in the respective sections in your `secrets.toml`.

```toml
[destination.credentials]
client_email=
private_key=
project_id=
```

Todo: give access to bigquery somewhere in the google console?