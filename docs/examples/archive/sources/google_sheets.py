from typing import Any, Iterator, Sequence, Union, cast

import dlt
from dlt.common.configuration.specs import (
    GcpServiceAccountCredentials,
    GcpOAuthCredentials,
)
from dlt.common.typing import DictStrAny, StrAny
from dlt.common.exceptions import MissingDependencyException

try:
    from apiclient.discovery import build
except ModuleNotFoundError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


# gets sheet values from a single spreadsheet preserving original typing for schema generation
# TODO: write schema discovery to better handle dates
# TODO: consider using https://github.com/burnash/gspread for spreadsheet discovery


def _initialize_sheets(
    credentials: Union[GcpOAuthCredentials, GcpServiceAccountCredentials]
) -> Any:
    # Build the service object.
    service = build("sheets", "v4", credentials=credentials.to_native_credentials())
    return service


@dlt.source
def google_spreadsheet(
    spreadsheet_id: str,
    sheet_names: Sequence[str],
    credentials: Union[
        GcpServiceAccountCredentials, GcpOAuthCredentials, str, StrAny
    ] = dlt.secrets.value,
) -> Any:
    sheets = _initialize_sheets(cast(GcpServiceAccountCredentials, credentials))

    # import pprint
    # meta = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id, ranges=sheet_names, includeGridData=True).execute()
    # pprint.pprint(meta)
    def get_sheet(sheet_name: str) -> Iterator[DictStrAny]:
        # get list of list of typed values
        result = (
            sheets.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                # unformatted returns typed values
                valueRenderOption="UNFORMATTED_VALUE",
                # will return formatted dates
                dateTimeRenderOption="FORMATTED_STRING",
            )
            .execute()
        )

        # pprint.pprint(result)
        values = result.get("values")

        # yield dicts assuming row 0 contains headers and following rows values and all rows have identical length
        for v in values[1:]:
            yield {h: v for h, v in zip(values[0], v)}

    # create resources from supplied sheet names
    return [
        dlt.resource(get_sheet(name), name=name, write_disposition="replace")
        for name in sheet_names
    ]
