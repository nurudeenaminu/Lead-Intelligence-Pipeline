"""
cleaner.py — Stage 3: Data Cleaning and Normalisation

Reads enriched_leads.csv (or raw_leads.csv if enrichment was skipped),
applies a documented cleaning sequence, and writes cleaned_leads.csv.

Cleaning sequence:
    1. Reconstruct real Google Maps URLs from place_id in maps_url field
    2. Deduplicate on name + city combined
    3. Normalise phone numbers to +1XXXXXXXXXX format
    4. Strip HTML entities and extra whitespace from all text fields
    5. Coerce rating to float (errors -> NaN, rows kept)
    6. Coerce review_count to int (errors -> 0)
    7. Assign data_quality flag per row

Usage:
    python cleaner.py          # standalone run
    clean_leads()              # called by main.py orchestrator
"""

import html
import logging
import re
from pathlib import Path

import pandas as pd

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
logger = logging.getLogger("cleaner")

# ── Paths ─────────────────────────────────────────────────────────────────────
# Prefer enriched output; fall back to raw if enrichment stage was skipped
ENRICHED_PATH = Path("data/raw/enriched_leads.csv")
RAW_PATH      = Path("data/raw/raw_leads.csv")
OUTPUT_PATH   = Path("data/cleaned/cleaned_leads.csv")

# Enrichment columns — may not exist if enricher hasn't run yet
ENRICHMENT_COLS = ["meta_description", "page_title", "services_snippet"]

# ── Helpers ───────────────────────────────────────────────────────────────────

def _resolve_input_path() -> Path:
    """
    Return enriched_leads.csv if it exists, otherwise raw_leads.csv.
    Logs which source is being used so pipeline runs are auditable.
    """
    if ENRICHED_PATH.exists():
        logger.info("Using enriched input: %s", ENRICHED_PATH)
        return ENRICHED_PATH
    logger.warning(
        "Enriched file not found — falling back to raw: %s. "
        "Run enricher.py first for full data quality.",
        RAW_PATH,
    )
    return RAW_PATH


def _reconstruct_maps_url(serpapi_url: str) -> str:
    """
    Extract place_id from a SerpAPI internal URL and build a real Google Maps URL.

    SerpAPI returns URLs like:
        https://serpapi.com/search.json?...&place_id=ChIJzbMUeBDNZSUR180NiXJXuX8
    We want:
        https://www.google.com/maps/place/?q=place_id:ChIJzbMUeBDNZSUR180NiXJXuX8

    Args:
        serpapi_url: Raw maps_url string from collector output.

    Returns:
        Real Google Maps URL string, or original value if place_id not found.
    """
    if not isinstance(serpapi_url, str) or "place_id=" not in serpapi_url:
        return serpapi_url

    match = re.search(r"place_id=([^&]+)", serpapi_url)
    if not match:
        return serpapi_url

    place_id = match.group(1)
    return f"https://www.google.com/maps/place/?q=place_id:{place_id}"


def _normalise_phone(raw: str) -> str:
    """
    Strip non-numeric characters and reformat US numbers as +1XXXXXXXXXX.

    Handles formats: (917) 444-3666, 917-444-3666, 9174443666, +19174443666.
    Returns empty string for numbers that aren't 10 or 11 digits after stripping.

    Args:
        raw: Raw phone string from SerpAPI.

    Returns:
        Formatted string like '+19174443666', or '' if unparseable.
    """
    if not isinstance(raw, str) or not raw.strip():
        return ""

    digits = re.sub(r"\D", "", raw)

    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"

    logger.debug("Could not normalise phone '%s' (%d digits) — blanking", raw, len(digits))
    return ""


def _clean_text(value: object) -> str:
    """
    Decode HTML entities and collapse extra whitespace in a text field.

    Args:
        value: Any value — non-strings are returned as-is after str conversion.

    Returns:
        Cleaned string.
    """
    if not isinstance(value, str):
        return value
    decoded = html.unescape(value)
    return re.sub(r"\s+", " ", decoded).strip()


def _assign_data_quality(row: pd.Series) -> str:
    """
    Assign a data quality tier based on which key fields are populated.

    Scoring tier logic:
        complete  — website + rating + meta_description all present
        partial   — website present but at least one of the above missing
        minimal   — no website

    Args:
        row: Single DataFrame row as a pandas Series.

    Returns:
        One of: 'complete', 'partial', 'minimal'
    """
    def is_populated(val) -> bool:
        """Return True only if value is a non-empty, non-NaN string."""
        if val is None:
            return False
        if pd.isna(val):
            return False
        return bool(str(val).strip()) and str(val).strip().lower() != "nan"

    has_website = is_populated(row.get("website"))
    has_rating  = is_populated(row.get("rating"))
    has_meta    = is_populated(row.get("meta_description"))

    if not has_website:
        return "minimal"
    if has_website and has_rating and has_meta:
        return "complete"
    return "partial"


# ── Main Pipeline Function ────────────────────────────────────────────────────

def clean_leads() -> int:
    """
    Execute the full cleaning sequence on the best available input file.

    Steps:
        1. Load input (enriched preferred, raw fallback)
        2. Inject missing enrichment columns as empty strings if not present
        3. Reconstruct real Google Maps URLs
        4. Drop exact duplicates on name + city
        5. Normalise phone numbers
        6. Clean text fields
        7. Coerce rating to float
        8. Coerce review_count to int
        9. Assign data_quality flag
        10. Write cleaned output

    Returns:
        Number of rows in the cleaned output file.

    Raises:
        FileNotFoundError: If neither enriched nor raw input file exists.
    """
    
    input_path = _resolve_input_path()

    if not input_path.exists():
        raise FileNotFoundError(
            f"No input file found. Run collector.py first. Looked for: {input_path}"
        )

    df = pd.read_csv(input_path, dtype=str)  # load everything as str first
    logger.info("Loaded %d rows from %s", len(df), input_path)

    # ── Step 1: Inject missing enrichment columns ──────────────────────────
    for col in ENRICHMENT_COLS:
        if col not in df.columns:
            df[col] = ""
            logger.info("Enrichment column '%s' not found — added as empty", col)

    # ── Step 2: Reconstruct Google Maps URLs ───────────────────────────────
    df["maps_url"] = df["maps_url"].apply(_reconstruct_maps_url)
    logger.info("Reconstructed Google Maps URLs from place_id")

    # ── Step 3: Deduplicate on name + city ─────────────────────────────────
    before = len(df)
    df["_dedup_key"] = df["name"].str.strip().str.lower() + "|" + df["city"].str.strip().str.lower()
    df = df.drop_duplicates(subset="_dedup_key", keep="first")
    df = df.drop(columns=["_dedup_key"])
    dropped = before - len(df)
    logger.info("Deduplication: dropped %d duplicate rows, %d remain", dropped, len(df))

    # ── Step 4: Normalise phone numbers ───────────────────────────────────
    df["phone"] = df["phone"].apply(_normalise_phone)
    df["phone"] = df["phone"].astype(str)  # prevent pandas from reading +1XXXXXXXXXX as float on reload

    # ── Step 5: Clean all text fields ──────────────────────────────────────
    text_cols = ["name", "full_address", "category", "meta_description",
                 "page_title", "services_snippet"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].apply(_clean_text)

    # ── Step 6: Coerce rating to float ─────────────────────────────────────
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    bad_ratings = df["rating"].isna().sum()
    if bad_ratings:
        logger.warning("%d rows have unparseable rating — set to NaN (rows kept)", bad_ratings)

    # ── Step 7: Coerce review_count to int ────────────────────────────────
    df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce").fillna(0).astype(int)

    # ── Step 8: Assign data_quality flag ───────────────────────────────────
    df["data_quality"] = df.apply(_assign_data_quality, axis=1)
    quality_counts = df["data_quality"].value_counts().to_dict()
    logger.info("Data quality breakdown: %s", quality_counts)

    # ── Step 9: Write output ───────────────────────────────────────────────
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    # Re-read and write with phone forced as string to prevent float coercion on reload
    df = pd.read_csv(OUTPUT_PATH, dtype={"phone": str, "website": str, "maps_url": str})
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    logger.info("Cleaned output written to %s (%d rows)", OUTPUT_PATH, len(df))

    return len(df)


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    clean_leads()