from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaIoBaseDownload
import io
from typing import Any, Iterator
import csv

from autopoiesis.common.typing import StrAny
from autopoiesis.common.schema import Schema
from dlt.pipeline import Pipeline

SCOPES = ['https://www.googleapis.com/auth/drive']
# KEY_FILE_LOCATION = '/Users/adrian/PycharmProjects/sv/dlt/temp/scalevector-1235ac340b0b.json'
# KEY_FILE_LOCATION = '_secrets/scalevector-1235ac340b0b.json'
KEY_FILE_LOCATION = '_secrets/project1234_service.json'


def _initialize_drive() -> Any:
    """Initializes an drive service object.

    Returns:
    An authorized drive service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    service = build('drive', 'v3', credentials=credentials)

    return service


def _initialize_sheets() -> Any:

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    service = build('sheets', 'v4', credentials=credentials)

    return service


def download_csv_as_json(file_id: str, csv_options: StrAny = None) -> Iterator[StrAny]:
    if csv_options is None:
        csv_options = {}

    drive_service = _initialize_drive()
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))
    rows = fh.getvalue().decode("utf-8")
    return csv.DictReader(io.StringIO(rows), **csv_options)


def download_sheet_to_csv(spreadsheet_id: str, sheet_name: str) -> Iterator[StrAny]:
    sheets = _initialize_sheets()

    # get list of list of typed values
    result = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        # unformatted returns typed values
        valueRenderOption="UNFORMATTED_VALUE",
        # will return formatted dates
        dateTimeRenderOption="FORMATTED_STRING"
    ).execute()

    values = result.get('values')

    # yield dicts assuming row 0 contains headers and following rows values and all rows have identical length
    for v in values[1:]:
        yield {h: v for h, v in zip(values[0], v)}


if __name__ == "__main__":
    file_id = '11G95oVZjieRhyGqtQMQqlqpxyvWkRXowKE8CtdLtFaU'
    sheet_name = "2022-05"
    options = {'delimiter': ';'}
    iter_d = download_sheet_to_csv(file_id, sheet_name)

    # LOADING DETAILS
    table_n = 'model_annotations'
    schema_prefix = 'annotations'
    schema_source_suffix = 'csv'

    # you authenticate by passing a credential, such as RDBMS host/user/port/pass or gcp service account json.
    gcp_credential_json_file_path = KEY_FILE_LOCATION

    # SCHEMA CREATION
    data_schema = None
    # data_schema_file_path = f"/Users/adrian/PycharmProjects/sv/dlt/examples/schemas/inferred_drive_csv_{file_id}_schema.yml"
    data_schema_file_path = f"examples/schemas/inferred_drive_csv_{file_id}_schema.yml"

    credentials = Pipeline.load_gcp_credentials(gcp_credential_json_file_path, schema_prefix)

    pipeline = Pipeline(schema_source_suffix)

    schema: Schema = None
    # uncomment to use already modified schema, but the auto-inferred schema will also work nicely
    # TODO: schema below needs is not yet finished, more exploration needed
    # schema = Pipeline.load_schema_from_file("examples/schemas/discord_schema.yml")
    pipeline.create_pipeline(credentials, schema=schema)
    # the pipeline created a working directory. you can always attach the pipeline back by just providing the dir to
    # Pipeline::restore_pipeline
    print(pipeline.root_path)
    # /private/var/folders/4q/gzmrslrs03j587990jcqt23h0000gn/T/tmp5e17osrg/

    # and extract it
    m = pipeline.extract_iterator(table_n, iter_d)

    # please note that all pipeline methods that return TRunMetrics are atomic so
    # - if m.has_failed is False the operation worked fully
    # - if m.has_failed is False the operation failed fully and can be retried
    if m.has_failed:
        print("Extracting failed")
        print(pipeline.last_run_exception)
        exit(0)

    # now create loading packages and infer the schema
    m = pipeline.unpack()
    if m.has_failed:
        print("Unpacking failed")
        print(pipeline.last_run_exception)
        exit(0)

    # save schema
    schema = pipeline.get_current_schema()
    schema_yaml = schema.as_yaml()
    f = open(data_schema_file_path, "a")
    f.write(schema_yaml)
    f.close()
    # pipeline.save_schema_to_file(data_schema_file_path, schema)

    # show loads, each load contains a copy of the schema that corresponds to the data inside
    # and a set of directories for job states (new -> in progress -> failed|completed)
    new_loads = pipeline.list_unpacked_loads()
    print(new_loads)

    # load packages
    m = pipeline.load()
    if m.has_failed:
        print("Loading failed, fix the problem, restore the pipeline and run loading packages again")
        print(pipeline.last_run_exception)
    else:
        # should be empty
        new_loads = pipeline.list_unpacked_loads()
        print(new_loads)

        # now enumerate all complete loads if we have any failed packages
        # complete but failed job will not raise any exceptions
        completed_loads = pipeline.list_completed_loads()
        # print(completed_loads)
        for load_id in completed_loads:
            print(f"Checking failed jobs in {load_id}")
            for job, failed_message in pipeline.list_failed_jobs(load_id):
                print(f"JOB: {job}\nMSG: {failed_message}")
