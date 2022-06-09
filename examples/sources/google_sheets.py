from typing import Any, Iterator

from dlt.common.typing import DictStrAny
from dlt.pipeline.exceptions import MissingDependencyException

try:
    from oauth2client.service_account import ServiceAccountCredentials
    from apiclient.discovery import build
except ImportError:
    raise MissingDependencyException("Google Sheets Source", ["google1", "google2"])


# gets spreadsheet values from a single sheet preserving original typing for schema generation
# TODO: write schema discovery to better handle dates


def get_source(credentials: ServiceAccountCredentials,  spreadsheet_id: str, sheet_name: str) -> Iterator[DictStrAny]:
    sheets = _initialize_sheets(credentials)

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


def _initialize_sheets(credentials: ServiceAccountCredentials) -> Any:

    # Build the service object.
    service = build('sheets', 'v4', credentials=credentials)

    return service
