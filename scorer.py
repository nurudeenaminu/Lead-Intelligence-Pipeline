"""
scorer.py — Stage 4: Lead Scoring

Scores each lead from 0-10 based on signals that indicate need for
digital marketing services — not just business legitimacy.

Scoring philosophy:
    Models NEED for services, not business quality. Every business in this
    dataset has a website and a high rating — those signals do not differentiate.
    What differentiates is how underserved their online presence is.

Score criteria:
    +2  review_count < 50   — small footprint, high growth need
    +1  review_count < 20   — stacks: very low visibility
    +2  rating >= 4.0       — worth pursuing (good product, weak marketing)
    +1  rating >= 4.7       — stacks: strong reputation, underselling it
    +2  meta_description missing or < 50 chars — clear SEO gap
    +1  services_snippet missing or empty — no structured content
    +1  page_title present and > 30 chars — site is indexable and active

Labels:
    8-10 = High Priority
    5-7  = Medium Priority
    0-4  = Low Priority

Usage:
    python scorer.py          # standalone run
    score_leads()             # called by main.py orchestrator
"""

import logging
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
logger = logging.getLogger("scorer")

# ── Paths ─────────────────────────────────────────────────────────────────────
INPUT_PATH  = Path("data/cleaned/cleaned_leads.csv")
OUTPUT_PATH = Path("data/scored/scored_leads.csv")


# ── Scoring Functions ─────────────────────────────────────────────────────────

def score_lead(row: pd.Series) -> int:
    """
    Score a single lead row from 0 to 10.

    Called by df.apply() in score_leads(). Each criterion is evaluated
    independently. Stacking criteria are only awarded if the base criterion
    is also met.

    Args:
        row: Single DataFrame row as a pandas Series.

    Returns:
        Integer score between 0 and 10 inclusive.
    """
    score = 0

    # ── Review count — low count = high opportunity (+2 base, +1 stacking) ───
    try:
        review_count = int(row.get("review_count", 0) or 0)
    except (ValueError, TypeError):
        review_count = 0

    if review_count < 50:
        score += 2
    if review_count < 20:
        score += 1  # stacks — very low visibility

    # ── Rating — good product, needs better marketing (+2 base, +1 stacking) ─
    try:
        rating = float(row.get("rating", 0) or 0)
    except (ValueError, TypeError):
        rating = 0.0

    if rating >= 4.0:
        score += 2
    if rating >= 4.7:
        score += 1  # stacks — strong reputation but underselling it

    # ── Meta description missing = SEO gap (+2) ───────────────────────────────
    meta = str(row.get("meta_description", "")).strip()
    meta_missing = not meta or meta.lower() == "nan" or len(meta) < 50
    if meta_missing:
        score += 2

    # ── Services snippet missing = no structured content (+1) ────────────────
    snippet = str(row.get("services_snippet", "")).strip()
    if not snippet or snippet.lower() == "nan":
        score += 1

    # ── Page title present = site is indexable and active (+1) ───────────────
    title = str(row.get("page_title", "")).strip()
    if title and title.lower() != "nan" and len(title) > 30:
        score += 1

    return min(score, 10)


def assign_label(score: int) -> str:
    """
    Convert a numeric score to a priority label.

    Args:
        score: Integer score 0-10.

    Returns:
        'High Priority', 'Medium Priority', or 'Low Priority'.
    """
    if score >= 8:
        return "High Priority"
    if score >= 5:
        return "Medium Priority"
    return "Low Priority"


# ── Main Pipeline Function ────────────────────────────────────────────────────

def score_leads() -> int:
    """
    Load cleaned leads, apply scoring, and write sorted output.

    Applies score_lead() row-by-row via df.apply(), assigns priority labels,
    sorts by score descending, and logs distribution stats.

    Returns:
        Number of rows written to scored output.

    Raises:
        FileNotFoundError: If cleaned_leads.csv does not exist.
    """
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input not found: {INPUT_PATH}. Run cleaner.py first."
        )

    df = pd.read_csv(INPUT_PATH, dtype={"phone": str, "website": str, "maps_url": str})
    logger.info("Loaded %d rows for scoring from %s", len(df), INPUT_PATH)

    df["score"]       = df.apply(score_lead, axis=1)
    df["score_label"] = df["score"].apply(assign_label)

    df = df.sort_values("score", ascending=False).reset_index(drop=True)

    # ── Distribution logging ──────────────────────────────────────────────────
    label_counts = df["score_label"].value_counts().to_dict()
    score_dist   = df["score"].value_counts().sort_index().to_dict()
    avg_score    = round(df["score"].mean(), 2)
    avg_rating   = round(pd.to_numeric(df["rating"], errors="coerce").mean(), 2)

    logger.info("Score label breakdown: %s", label_counts)
    logger.info("Score distribution (score: count): %s", score_dist)
    logger.info("Average score: %s | Average rating: %s", avg_score, avg_rating)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    logger.info("Scored output written to %s (%d rows)", OUTPUT_PATH, len(df))

    return len(df)


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    score_leads()