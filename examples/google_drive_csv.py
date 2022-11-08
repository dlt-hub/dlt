# from apiclient.discovery import build
# from oauth2client.service_account import ServiceAccountCredentials
# from googleapiclient.http import MediaIoBaseDownload
# import io
# from typing import Any, Iterator
# import csv

# from dlt.common.typing import DictStrAny, StrAny
# from dlt.common.schema import Schema

# from dlt.pipeline import Pipeline, GCPPipelineCredentials

# SCOPES = ['https://www.googleapis.com/auth/drive']
# # KEY_FILE_LOCATION = '/Users/adrian/PycharmProjects/sv/dlt/temp/scalevector-1235ac340b0b.json'
# KEY_FILE_LOCATION = '_secrets/scalevector-1235ac340b0b.json'


# def _initialize_drive() -> Any:
#     """Initializes an drive service object.

#     Returns:
#     An authorized drive service object.
#     """
#     credentials = ServiceAccountCredentials.from_json_keyfile_name(
#         KEY_FILE_LOCATION, SCOPES)

#     # Build the service object.
#     service = build('drive', 'v3', credentials=credentials)

#     return service


# # TODO: consider using https://pythonhosted.org/PyDrive/index.html or https://github.com/wkentaro/gdown
# def download_csv_as_json(file_id: str, csv_options: StrAny = None) -> Iterator[DictStrAny]:
#     if csv_options is None:
#         csv_options = {}

#     drive_service = _initialize_drive()
#     request = drive_service.files().get_media(fileId=file_id)
#     fh = io.BytesIO()
#     downloader = MediaIoBaseDownload(fh, request)
#     done = False
#     while done is False:
#         status, done = downloader.next_chunk()
#         print("Download %d%%." % int(status.progress() * 100))
#     rows = fh.getvalue().decode("utf-8")
#     return csv.DictReader(io.StringIO(rows), **csv_options)


# if __name__ == "__main__":
#     file_id = '1dkJYb-WVVseZIQLAY0jh_o0s7Bas4Gp5'
#     options = {'delimiter': ';'}
#     iter_d = download_csv_as_json(file_id, csv_options=options)

#     # LOADING DETAILS
#     table_n = 'people'
#     schema_prefix = 'drive'
#     schema_source_suffix = 'csv'

#     # you authenticate by passing a credential, such as RDBMS host/user/port/pass or gcp service account json.
#     gcp_credential_json_file_path = KEY_FILE_LOCATION

#     # SCHEMA CREATION
#     data_schema = None
#     data_schema_file_path = f"examples/schemas/inferred_drive_csv_{file_id}.schema.yml"

#     credentials = GCPPipelineCredentials.from_services_file(gcp_credential_json_file_path, schema_prefix)

#     pipeline = Pipeline(schema_source_suffix)

#     schema: Schema = None
#     # uncomment to use already modified schema, but the auto-inferred schema will also work nicely
#     # TODO: schema below needs is not yet finished, more exploration needed
#     # schema = Pipeline.load_schema_from_file("examples/schemas/discord_schema.yml")
#     pipeline.create_pipeline(credentials, schema=schema)
#     # the pipeline created a working directory. you can always attach the pipeline back by just providing the dir to
#     # Pipeline::restore_pipeline
#     print(pipeline.root_path)
#     # /private/var/folders/4q/gzmrslrs03j587990jcqt23h0000gn/T/tmp5e17osrg/

#     # and extract it
#     pipeline.extract(iter_d, table_name=table_n)
#     pipeline.flush()

#     # save schema
#     schema = pipeline.get_default_schema()
#     schema_yaml = schema.to_pretty_yaml()
#     f = open(data_schema_file_path, "a", encoding="utf-8")
#     f.write(schema_yaml)
#     f.close()
