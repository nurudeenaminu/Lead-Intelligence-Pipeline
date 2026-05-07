"""
sheets_exporter.py — Stage 5: Google Sheets Export

Pushes final scored leads to a Google Sheet using a Service Account.
Creates the sheet if it doesn't exist, clears and rewrites if it does.

Formatting applied:
    - Row 1: bold, white text, dark green background (#1a4a1a)
    - High Priority rows: light green background (#e6f4e6)
    - Row 1 frozen as header
    - All columns auto-resized

The shareable Sheet URL is logged and saved to data/sheet_url.txt
for use by the Streamlit dashboard.

Usage:
    python sheets_exporter.py     # standalone run
    export_to_sheets()            # called by main.py orchestrator
"""

import logging
import os
from pathlib import Path

import gspread
import pandas as pd
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from gspread_formatting import (
    BooleanCondition,
    BooleanRule,
    CellFormat,
    Color,
    ConditionalFormatRule,
    GridRange,
    TextFormat,
    get_conditional_format_rules,
    format_cell_range,
    set_frozen,
)

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────
Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("sheets_exporter")

# ── Config ────────────────────────────────────────────────────────────────────
SCORED_PATH       = Path("data/scored/scored_leads.csv")
SHEET_URL_PATH    = Path("data/sheet_url.txt")
CREDENTIALS_PATH  = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
SHEET_NAME        = os.getenv("GOOGLE_SHEET_NAME", "Lead Intelligence Pipeline")
SERVICE_ACCOUNT   = "lead-intel@lead-intelligence-scraper.iam.gserviceaccount.com"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Columns to export — controls order in the sheet
EXPORT_COLUMNS = [
    "name", "city", "score", "score_label", "rating",
    "review_count", "website", "phone", "category",
    "meta_description", "services_snippet", "data_quality", "maps_url",
]


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_client() -> gspread.Client:
    """
    Authenticate with Google using the Service Account credentials file.

    Args:
        None — reads GOOGLE_CREDENTIALS_PATH from environment.

    Returns:
        Authenticated gspread Client.

    Raises:
        FileNotFoundError: If credentials.json does not exist.
    """
    creds_path = Path(CREDENTIALS_PATH)
    if not creds_path.exists():
        raise FileNotFoundError(
            f"credentials.json not found at '{creds_path}'. "
            "Download it from Google Cloud Console → Service Accounts → Keys."
        )

    creds = Credentials.from_service_account_file(str(creds_path), scopes=SCOPES)
    client = gspread.authorize(creds)
    logger.info("Authenticated with Google Sheets API")
    return client


# ── Sheet Management ──────────────────────────────────────────────────────────

def _get_or_create_sheet(client: gspread.Client) -> gspread.Spreadsheet:
    """
    Open the target spreadsheet by name from the authenticated user's Drive.

    The sheet must already exist and be shared with the service account
    as Editor. This avoids Drive quota issues from programmatic creation.

    Args:
        client: Authenticated gspread Client.

    Returns:
        gspread Spreadsheet object.

    Raises:
        gspread.SpreadsheetNotFound: If the sheet name doesn't match or
        the service account hasn't been granted Editor access.
    """
    try:
        spreadsheet = client.open(SHEET_NAME)
        logger.info("Opened sheet: '%s'", SHEET_NAME)
        return spreadsheet
    except gspread.SpreadsheetNotFound:
        raise gspread.SpreadsheetNotFound(
            f"Sheet '{SHEET_NAME}' not found. "
            "Create it in Google Drive and share it with "
            f"{SERVICE_ACCOUNT} as Editor."
        )


# ── Formatting ────────────────────────────────────────────────────────────────

def _apply_formatting(sheet: gspread.Worksheet, num_rows: int) -> None:
    """
    Apply header formatting, High Priority row highlighting, and freeze row 1.

    Formatting applied:
        - Row 1: bold white text on dark green (#1a4a1a) background
        - Rows where score_label = 'High Priority': light green (#e6f4e6)
        - Row 1 frozen
        - All columns auto-resized to fit content

    Args:
        sheet:    The worksheet to format.
        num_rows: Number of data rows (excluding header) for range calculations.
    """
    spreadsheet = sheet.spreadsheet

    # ── Header row formatting ─────────────────────────────────────────────────
    header_format = CellFormat(
        backgroundColor=Color(0.102, 0.290, 0.102),  # #1a4a1a in 0-1 RGB
        textFormat=TextFormat(
            bold=True,
            foregroundColor=Color(1, 1, 1),  # white
            fontSize=10,
        ),
    )
    format_cell_range(sheet, "1:1", header_format)
    logger.info("Applied header formatting")

    # ── Freeze header row ─────────────────────────────────────────────────────
    set_frozen(sheet, rows=1)
    logger.info("Frozen row 1")

    # ── Conditional format: High Priority rows → light green ─────────────────
    # score_label is column D (index 3, 0-based) in EXPORT_COLUMNS
    score_label_col_index = EXPORT_COLUMNS.index("score_label")
    score_label_col_letter = chr(ord("A") + score_label_col_index)

    rule = ConditionalFormatRule(
        ranges=[GridRange.from_a1_range(f"A2:{chr(ord('A') + len(EXPORT_COLUMNS) - 1)}{num_rows + 1}", sheet)],
        booleanRule=BooleanRule(
            condition=BooleanCondition(
                "TEXT_EQ",
                [f"${score_label_col_letter}2"],
            ),
            format=CellFormat(
                backgroundColor=Color(0.902, 0.957, 0.902),  # #e6f4e6
            ),
        ),
    )

    # Fetch existing rules, append ours, then save back
    rules = get_conditional_format_rules(sheet)
    rules.clear()
    rules.append(rule)
    rules.save()
    logger.info("Applied High Priority conditional formatting")

    # ── Auto-resize all columns ───────────────────────────────────────────────
    body = {
        "requests": [{
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet.id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": len(EXPORT_COLUMNS),
                }
            }
        }]
    }
    spreadsheet.batch_update(body)
    logger.info("Auto-resized all columns")


# ── Main Export Function ──────────────────────────────────────────────────────

def export_to_sheets() -> str:
    """
    Load scored leads and push them to Google Sheets.

    Steps:
        1. Authenticate with Service Account
        2. Open or create the target spreadsheet
        3. Clear existing content
        4. Write headers and all rows
        5. Apply formatting
        6. Save and log the shareable URL

    Returns:
        Shareable Google Sheet URL string.

    Raises:
        FileNotFoundError: If scored_leads.csv or credentials.json missing.
    """
    if not SCORED_PATH.exists():
        raise FileNotFoundError(
            f"Scored data not found at {SCORED_PATH}. Run scorer.py first."
        )

    # Load and prepare data
    df = pd.read_csv(SCORED_PATH, dtype={"phone": str, "website": str, "maps_url": str})
    logger.info("Loaded %d scored leads from %s", len(df), SCORED_PATH)

    # Keep only export columns that exist in the dataframe
    export_cols = [c for c in EXPORT_COLUMNS if c in df.columns]
    df = df[export_cols].fillna("")

    # Auth and sheet access
    client      = _get_client()
    spreadsheet = _get_or_create_sheet(client)
    sheet       = spreadsheet.sheet1

    # Clear and rewrite — never append
    sheet.clear()
    logger.info("Cleared existing sheet content")

    # Write header + all rows in one batch call (faster than row-by-row)
    rows = [export_cols] + df.astype(str).values.tolist()
    sheet.update(rows, "A1")
    logger.info("Written %d rows to sheet", len(df))

    # Apply formatting
    _apply_formatting(sheet, len(df))

    # Save URL
    sheet_url = spreadsheet.url
    SHEET_URL_PATH.parent.mkdir(parents=True, exist_ok=True)
    SHEET_URL_PATH.write_text(sheet_url, encoding="utf-8")

    logger.info("Sheet URL: %s", sheet_url)
    logger.info("Sheet URL saved to %s", SHEET_URL_PATH)

    return sheet_url


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    url = export_to_sheets()
    print(f"\nGoogle Sheet: {url}")