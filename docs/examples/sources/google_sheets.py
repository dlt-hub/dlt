from typing import Any, Iterator, Sequence, Union, cast

import dlt
from dlt.common.configuration.specs import GcpClientCredentialsWithDefault
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.exceptions import MissingDependencyException

try:
    from apiclient.discovery import build
except ImportError:
    raise MissingDependencyException("Google Sheets Source", ["google1", "google2"])


# gets sheet values from a single spreadsheet preserving original typing for schema generation
# TODO: write schema discovery to better handle dates
# TODO: consider using https://github.com/burnash/gspread for spreadsheet discovery


def _initialize_sheets(credentials: GcpClientCredentialsWithDefault) -> Any:
    # Build the service object.
    service = build('sheets', 'v4', credentials=credentials.to_service_account_credentials())

    return service


@dlt.source
def google_spreadsheet(spreadsheet_id: str, sheet_names: Sequence[str], credentials: Union[GcpClientCredentialsWithDefault, str, StrAny] = dlt.secrets.value) -> Any:

    sheets = _initialize_sheets(cast(GcpClientCredentialsWithDefault, credentials))

    def get_sheet(sheet_name: str) -> Iterator[DictStrAny]:

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

    # create resources from supplied sheet names
    return [dlt.resource(get_sheet(name), name=name, write_disposition="replace") for name in sheet_names]
