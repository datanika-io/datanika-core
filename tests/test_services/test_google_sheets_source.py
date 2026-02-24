"""Tests for Google Sheets dlt source — mocks gspread to avoid real API calls."""

from unittest.mock import MagicMock, patch


class TestGoogleSheetsSource:
    @patch("datanika.services.google_sheets_source.gspread")
    @patch("datanika.services.google_sheets_source.Credentials")
    def test_yields_records_from_all_sheets(self, mock_creds_cls, mock_gspread):
        from datanika.services.google_sheets_source import google_sheets_source

        # Setup mocks
        mock_creds = MagicMock()
        mock_creds_cls.from_service_account_info.return_value = mock_creds

        mock_ws1 = MagicMock()
        mock_ws1.title = "Sheet1"
        mock_ws1.get_all_records.return_value = [{"id": 1, "name": "Alice"}]

        mock_ws2 = MagicMock()
        mock_ws2.title = "Sheet2"
        mock_ws2.get_all_records.return_value = [{"id": 2, "name": "Bob"}]

        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheets.return_value = [mock_ws1, mock_ws2]
        mock_spreadsheet.worksheet.side_effect = lambda name: {
            "Sheet1": mock_ws1,
            "Sheet2": mock_ws2,
        }[name]

        mock_gc = MagicMock()
        mock_gc.open_by_url.return_value = mock_spreadsheet
        mock_gspread.authorize.return_value = mock_gc

        creds_json = '{"type": "service_account", "project_id": "test"}'
        source = google_sheets_source(
            "https://docs.google.com/spreadsheets/d/abc123",
            creds_json,
        )

        # Source should yield resources for both sheets
        resources = list(source.resources.keys())
        assert len(resources) == 2

    @patch("datanika.services.google_sheets_source.gspread")
    @patch("datanika.services.google_sheets_source.Credentials")
    def test_filters_by_sheet_names(self, mock_creds_cls, mock_gspread):
        from datanika.services.google_sheets_source import google_sheets_source

        mock_creds = MagicMock()
        mock_creds_cls.from_service_account_info.return_value = mock_creds

        mock_ws1 = MagicMock()
        mock_ws1.title = "Sheet1"
        mock_ws1.get_all_records.return_value = [{"id": 1}]

        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheet.return_value = mock_ws1

        mock_gc = MagicMock()
        mock_gc.open_by_url.return_value = mock_spreadsheet
        mock_gspread.authorize.return_value = mock_gc

        creds_json = '{"type": "service_account", "project_id": "test"}'
        source = google_sheets_source(
            "https://docs.google.com/spreadsheets/d/abc123",
            creds_json,
            sheet_names=["Sheet1"],
        )

        resources = list(source.resources.keys())
        assert len(resources) == 1
        # worksheets() should not be called when sheet_names is provided
        mock_spreadsheet.worksheets.assert_not_called()

    @patch("datanika.services.google_sheets_source.gspread")
    @patch("datanika.services.google_sheets_source.Credentials")
    def test_credentials_use_readonly_scope(self, mock_creds_cls, mock_gspread):
        from datanika.services.google_sheets_source import SCOPES, google_sheets_source

        mock_creds = MagicMock()
        mock_creds_cls.from_service_account_info.return_value = mock_creds

        mock_spreadsheet = MagicMock()
        mock_spreadsheet.worksheets.return_value = []
        mock_gc = MagicMock()
        mock_gc.open_by_url.return_value = mock_spreadsheet
        mock_gspread.authorize.return_value = mock_gc

        creds_json = '{"type": "service_account", "project_id": "test"}'
        google_sheets_source(
            "https://docs.google.com/spreadsheets/d/abc123",
            creds_json,
        )

        mock_creds_cls.from_service_account_info.assert_called_once()
        call_kwargs = mock_creds_cls.from_service_account_info.call_args
        assert call_kwargs[1]["scopes"] == SCOPES
        assert "spreadsheets.readonly" in SCOPES[0]
