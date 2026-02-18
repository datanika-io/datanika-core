"""Custom dlt source for Google Sheets using gspread."""

import json

import dlt
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _make_sheet_resource(spreadsheet, sheet_name):
    """Create a named dlt resource for a single worksheet."""

    @dlt.resource(name=sheet_name, write_disposition="replace")
    def _resource():
        worksheet = spreadsheet.worksheet(sheet_name)
        records = worksheet.get_all_records()
        yield from records

    return _resource


@dlt.source
def google_sheets_source(spreadsheet_url, credentials_json, sheet_names=None):
    """Extract data from a Google Spreadsheet.

    Args:
        spreadsheet_url: Full URL or spreadsheet ID.
        credentials_json: JSON string of service account credentials.
        sheet_names: Optional list of sheet names to extract. If None, all sheets.
    """
    creds = Credentials.from_service_account_info(
        json.loads(credentials_json), scopes=SCOPES
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_url(spreadsheet_url)
    sheets = sheet_names or [ws.title for ws in spreadsheet.worksheets()]
    for sheet_name in sheets:
        yield _make_sheet_resource(spreadsheet, sheet_name)
