import os
import toml 
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal

secrets = toml.load("../tests/.dlt/secrets.toml")
dd = secrets["destination"]["databricks"]["credentials"]

def credential_provider():
    config = Config(
        host          = f"https://{dd['server_hostname']}",
        client_id     = dd["client_id"],
        client_secret = dd["client_secret"]
    )
    return oauth_service_principal(config)        

with sql.connect(server_hostname      = dd["server_hostname"],
                 http_path            = dd["http_path"],
                 credentials_provider = credential_provider,
                 auth_type = "databricks-oauth") as conn_oauth:
    cursor = conn_oauth.cursor()
    cursor.execute('select * from samples.nyctaxi.trips limit 5')
    result = cursor.fetchall()
    for row in result:
        print(row)
