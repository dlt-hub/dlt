import os
from typing import Iterator
import dlt
from dlt.destinations import postgres


@dlt.resource
def simple_data(
    api_url: str = dlt.config.value, api_secret: dlt.TSecretValue = dlt.secrets.value
) -> Iterator[str]:
    # just yield api_url and api_secret to show what was configured in the example
    yield api_url
    yield api_secret


# you do not need to follow the recommended way to keep your config and secrets
# here we pass the secrets explicitly to the `simple_data`
data = simple_data(dlt.config["custom.simple_data.api_url"], dlt.secrets["simple_data_secret_7"])
# you should see the values from config.toml or secrets.toml coming from custom locations
# config.toml:
# [custom]
# simple_data.api_url="api_url from custom location"
#
# secrets.toml
# simple_data_secret_7="secret from custom location"
print(list(data))

# all the config providers are being searched, let's use the environment variables which have precedence over the toml files
os.environ["CUSTOM__SIMPLE_DATA__API_URL"] = "api url from env"
os.environ["SIMPLE_DATA_SECRET_7"] = "api secret from env"
data = simple_data(dlt.config["custom.simple_data.api_url"], dlt.secrets["simple_data_secret_7"])
print(list(data))

# you are free to pass credentials from custom location to destination
pipeline = dlt.pipeline(
    destination=postgres(credentials=dlt.secrets["custom.destination.credentials"])
)
# see nice credentials object
print(pipeline.credentials)

# you can also pass credentials partially, only the password comes from the secrets or environment
pipeline = dlt.pipeline(
    destination=postgres(credentials="postgres://loader@localhost:5432/dlt_data")
)

# now lets compare it with default location for config and credentials
data = simple_data()
print(list(data))
