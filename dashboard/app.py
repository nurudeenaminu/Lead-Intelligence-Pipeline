"""
dashboard/app.py — Stage 7: Streamlit Lead Intelligence Dashboard

Displays scored leads with interactive filters, metrics, score distribution,
and a download button for filtered results.

Run with:
    streamlit run dashboard/app.py

Reads from:
    data/scored/scored_leads.csv
    data/sheet_url.txt
    logs/pipeline.log  (for last run timestamp)
"""

import re
from pathlib import Path

import pandas as pd
import streamlit as st
import json
import os
from pathlib import Path

# On Streamlit Cloud, credentials.json is injected via secrets
# On local, it's read from disk directly


# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Lead Intelligence",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).parent.parent
SCORED_PATH   = ROOT / "data" / "scored" / "scored_leads.csv"
SHEET_URL_PATH = ROOT / "data" / "sheet_url.txt"
LOG_PATH      = ROOT / "logs" / "pipeline.log"

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    /* Dark sidebar */
    [data-testid="stSidebar"] {
        background: #0f1a0f;
        border-right: 1px solid #1a4a1a;
    }
    [data-testid="stSidebar"] * {
        color: #c8e6c8 !important;
    }
    [data-testid="stSidebar"] .stSlider label,
    [data-testid="stSidebar"] .stMultiSelect label,
    [data-testid="stSidebar"] .stRadio label,
    [data-testid="stSidebar"] .stSelectbox label {
        color: #6dbf6d !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-weight: 500;
    }

    /* Main background */
    .stApp {
        background: #f7faf7;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: white;
        border: 1px solid #d4e8d4;
        border-radius: 8px;
        padding: 1rem 1.25rem;
        box-shadow: 0 1px 3px rgba(26,74,26,0.06);
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.7rem !important;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #5a8a5a !important;
        font-weight: 600;
    }
    [data-testid="stMetricValue"] {
        font-family: 'DM Mono', monospace !important;
        color: #1a4a1a !important;
        font-size: 1.8rem !important;
    }

    /* Header */
    .dash-header {
        background: #1a4a1a;
        padding: 1.5rem 2rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
    }
    .dash-header h1 {
        color: white;
        font-size: 1.6rem;
        font-weight: 600;
        margin: 0;
        letter-spacing: -0.02em;
    }
    .dash-header p {
        color: #8fbc8f;
        font-size: 0.8rem;
        margin: 0.25rem 0 0 0;
        font-family: 'DM Mono', monospace;
    }

    /* Section labels */
    .section-label {
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #5a8a5a;
        font-weight: 600;
        margin: 1.5rem 0 0.5rem 0;
        padding-bottom: 0.4rem;
        border-bottom: 1px solid #d4e8d4;
    }

    /* Score badge styling in dataframe */
    .high { color: #1a4a1a; font-weight: 600; }
    .medium { color: #5a6a1a; }
    .low { color: #8a5a1a; }

    /* Sheet link */
    .sheet-link {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        background: #1a4a1a;
        color: white !important;
        padding: 0.5rem 1rem;
        border-radius: 6px;
        text-decoration: none;
        font-size: 0.85rem;
        font-weight: 500;
    }

    /* Download button */
    .stDownloadButton button {
        background: #1a4a1a !important;
        color: white !important;
        border: none !important;
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 500 !important;
    }
    .stDownloadButton button:hover {
        background: #2d7a2d !important;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] {
        border: 1px solid #d4e8d4;
        border-radius: 8px;
        overflow: hidden;
    }

    /* Bar chart */
    [data-testid="stBarChart"] {
        background: white;
        border: 1px solid #d4e8d4;
        border-radius: 8px;
        padding: 1rem;
    }

    /* Hide streamlit branding */
    #MainMenu, footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)


# ── Data Loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    """
    Load scored leads from Google Sheets.

    Prefers Google Sheets as the source on Streamlit Cloud where the
    local CSV doesn't exist. Falls back to local CSV for development.
    Cached for 5 minutes to avoid hammering the Sheets API.

    Returns:
        DataFrame with all scored lead columns.
    """
    # Try local CSV first (development)
    if SCORED_PATH.exists():
        df = pd.read_csv(
            SCORED_PATH,
            dtype={"phone": str, "website": str, "maps_url": str},
        )
    else:
        # Streamlit Cloud — read from Google Sheets
        try:
            import gspread
            from google.oauth2.service_account import Credentials

            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]

            # Build credentials from Streamlit secrets
            creds_dict = dict(st.secrets["GOOGLE_CREDENTIALS_JSON"])
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            client = gspread.authorize(creds)

            sheet_name = st.secrets.get("GOOGLE_SHEET_NAME", "Lead Intelligence Pipeline")
            spreadsheet = client.open(sheet_name)
            records = spreadsheet.sheet1.get_all_records()

            df = pd.DataFrame(records)
            if df.empty:
                return pd.DataFrame()

        except Exception as exc:
            st.error(f"Could not load data from Google Sheets: {exc}")
            return pd.DataFrame()

    df["rating"]       = pd.to_numeric(df["rating"], errors="coerce")
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0).astype(int)
    df["score"]        = pd.to_numeric(df["score"], errors="coerce").fillna(0).astype(int)
    return df

def get_last_run() -> str:
    """
    Extract the last pipeline completion timestamp from the log file.

    Returns:
        Timestamp string, or 'No runs logged yet' if log is missing.
    """
    if not LOG_PATH.exists():
        return "No runs logged yet"
    lines = LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line in reversed(lines):
        if "Scored output written" in line:
            match = re.match(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
            if match:
                return match.group(1)
    return "Unknown"


def get_sheet_url() -> str:
    """
    Read the Google Sheet URL saved by sheets_exporter.py.

    Returns:
        URL string, or empty string if file not found.
    """
    if SHEET_URL_PATH.exists():
        return SHEET_URL_PATH.read_text(encoding="utf-8").strip()
    return ""


def make_clickable_links(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format the website column as markdown hyperlinks for display.

    Args:
        df: DataFrame with a 'website' column.

    Returns:
        Copy of df with website column replaced by markdown link strings.
    """
    display = df.copy()
    display["website"] = display["website"].apply(
        lambda url: f"[{url}]({url})" if pd.notna(url) and str(url).startswith("http") else url
    )
    return display


# ── Load Data ─────────────────────────────────────────────────────────────────
df = load_data()

if df.empty:
    st.error("No scored data found. Run the pipeline first: `python main.py`")
    st.stop()

# ── Header ────────────────────────────────────────────────────────────────────
last_run  = get_last_run()
sheet_url = get_sheet_url()

st.markdown(f"""
<div class="dash-header">
    <h1>🎯 Lead Intelligence Dashboard</h1>
    <p>Last pipeline run: {last_run} &nbsp;·&nbsp; {len(df)} total leads across {df['city'].nunique()} cities</p>
</div>
""", unsafe_allow_html=True)

# ── Sidebar Filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### Filters")

    cities = sorted(df["city"].dropna().unique().tolist())
    selected_cities = st.multiselect(
        "City",
        options=cities,
        default=cities,
    )

    min_score = st.slider("Minimum Score", 0, 10, 0)
    min_rating = st.slider("Minimum Rating", 0.0, 5.0, 0.0, step=0.1)

    quality_filter = st.radio(
        "Data Quality",
        options=["All", "Complete only", "Partial and above"],
        index=0,
    )

    label_filter = st.selectbox(
        "Score Label",
        options=["All", "High Priority", "Medium Priority", "Low Priority"],
    )

# ── Apply Filters ─────────────────────────────────────────────────────────────
filtered = df.copy()

if selected_cities:
    filtered = filtered[filtered["city"].isin(selected_cities)]

filtered = filtered[filtered["score"] >= min_score]
filtered = filtered[filtered["rating"].fillna(0) >= min_rating]

if quality_filter == "Complete only":
    filtered = filtered[filtered["data_quality"] == "complete"]
elif quality_filter == "Partial and above":
    filtered = filtered[filtered["data_quality"].isin(["complete", "partial"])]

if label_filter != "All":
    filtered = filtered[filtered["score_label"] == label_filter]

# ── Metrics Row ───────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Leads", len(filtered))
with col2:
    high_count = (filtered["score_label"] == "High Priority").sum()
    st.metric("High Priority", high_count)
with col3:
    avg_score = round(filtered["score"].mean(), 1) if len(filtered) else 0
    st.metric("Avg Score", avg_score)
with col4:
    avg_rating = round(filtered["rating"].mean(), 2) if len(filtered) else 0
    st.metric("Avg Rating", avg_rating)

# ── Main Table ────────────────────────────────────────────────────────────────
st.markdown('<p class="section-label">Leads</p>', unsafe_allow_html=True)

display_cols = [
    "name", "city", "score", "score_label",
    "rating", "review_count", "website", "data_quality",
]

table_df = filtered[
    [c for c in display_cols if c in filtered.columns]
].copy()

st.dataframe(
    table_df,
    width="stretch",
    hide_index=True,
    column_config={
        "score": st.column_config.NumberColumn("Score", format="%d"),
        "score_label": st.column_config.TextColumn("Priority"),
        "rating": st.column_config.NumberColumn("Rating", format="%.1f"),
        "review_count": st.column_config.NumberColumn("Reviews"),
        "website": st.column_config.LinkColumn("Website"),
        "data_quality": st.column_config.TextColumn("Quality"),
    },
)

# ── Download Button ───────────────────────────────────────────────────────────
csv_bytes = filtered.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇ Download filtered results as CSV",
    data=csv_bytes,
    file_name="filtered_leads.csv",
    mime="text/csv",
)

# ── Score Distribution Chart ──────────────────────────────────────────────────
st.markdown('<p class="section-label">Score Distribution</p>', unsafe_allow_html=True)

score_counts = (
    filtered["score"]
    .value_counts()
    .reindex(range(11), fill_value=0)
    .reset_index()
)
score_counts.columns = ["Score", "Count"]
score_counts = score_counts.set_index("Score")

st.bar_chart(score_counts, color="#1a4a1a")

# ── Google Sheet Link ─────────────────────────────────────────────────────────
if sheet_url:
    st.markdown('<p class="section-label">Google Sheet</p>', unsafe_allow_html=True)
    st.markdown(
        f'<a class="sheet-link" href="{sheet_url}" target="_blank">'
        f'📊 Open in Google Sheets</a>',
        unsafe_allow_html=True,
    )