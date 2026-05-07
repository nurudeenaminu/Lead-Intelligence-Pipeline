"""
main.py — Pipeline Orchestrator

Runs all 5 pipeline stages in sequence:
    1. collect_all_leads()    — collector.py
    2. enrich_leads()         — enricher.py
    3. clean_leads()          — cleaner.py
    4. score_leads()          — scorer.py
    5. export_to_sheets()     — sheets_exporter.py

Each stage is wrapped in its own try/except so a single stage failure
logs the error and continues where possible, rather than aborting the run.

Usage:
    python main.py            # single run
    python scheduler.py       # runs main.py every 24 hours
"""

import logging
import sys
import time
from pathlib import Path

Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("main")

from collector import collect_all_leads
from cleaner import clean_leads
from enricher import enrich_leads
from scorer import score_leads
from sheets_exporter import export_to_sheets


def run_pipeline() -> bool:
    """
    Execute all pipeline stages in sequence.

    Stages 1-4 are hard dependencies — if collection or cleaning fails,
    later stages cannot run. The Sheets export (stage 5) is treated as
    non-critical: a failure there doesn't invalidate the scored data.

    Returns:
        True if all stages completed without error, False if any stage failed.
    """
    start = time.time()
    logger.info("=" * 60)
    logger.info("Pipeline run started")
    logger.info("=" * 60)

    failed_stages = []

   # ── Stage 1: Collection ───────────────────────────────────────────────────
    try:
        logger.info("Stage 1/5 - Collection starting")
        from collector import SerpAPILimitError
        total = collect_all_leads()
        logger.info("Stage 1/5 - Collection complete: %d leads", total)
    except SerpAPILimitError as exc:
        logger.critical(
            "Stage 1/5 - SerpAPI monthly quota exhausted: %s. "
            "Continuing pipeline with partial data if any was collected.",
            exc,
        )
        # Don't abort — partial data may still be worth enriching and scoring
        from pathlib import Path
        if not Path("data/raw/raw_leads.csv").exists():
            logger.critical("No raw data at all - aborting run")
            return False
    except Exception as exc:
        logger.error("Stage 1/5 - Collection FAILED: %s", exc, exc_info=True)
        logger.critical("Cannot continue without collected data - aborting run")
        return False
    
    # ── Stage 2: Enrichment ───────────────────────────────────────────────────
    try:
        logger.info("Stage 2/5 — Enrichment starting")
        enriched = enrich_leads()
        logger.info("Stage 2/5 — Enrichment complete: %d successful", enriched)
    except Exception as exc:
        logger.error("Stage 2/5 — Enrichment FAILED: %s", exc, exc_info=True)
        logger.warning("Continuing with unenriched data")
        failed_stages.append("enrichment")

    # ── Stage 3: Cleaning ─────────────────────────────────────────────────────
    try:
        logger.info("Stage 3/5 — Cleaning starting")
        cleaned = clean_leads()
        logger.info("Stage 3/5 — Cleaning complete: %d rows", cleaned)
    except Exception as exc:
        logger.error("Stage 3/5 — Cleaning FAILED: %s", exc, exc_info=True)
        logger.critical("Cannot score without cleaned data — aborting run")
        return False

    # ── Stage 4: Scoring ──────────────────────────────────────────────────────
    try:
        logger.info("Stage 4/5 — Scoring starting")
        scored = score_leads()
        logger.info("Stage 4/5 — Scoring complete: %d rows", scored)
    except Exception as exc:
        logger.error("Stage 4/5 — Scoring FAILED: %s", exc, exc_info=True)
        logger.critical("Cannot export without scored data — aborting run")
        return False

    # ── Stage 5: Sheets Export ────────────────────────────────────────────────
    try:
        logger.info("Stage 5/5 — Sheets export starting")
        url = export_to_sheets()
        logger.info("Stage 5/5 — Export complete: %s", url)
    except Exception as exc:
        logger.error("Stage 5/5 — Sheets export FAILED: %s", exc, exc_info=True)
        logger.warning("Scored data is valid — only Sheets export failed")
        failed_stages.append("sheets_export")

    elapsed = round(time.time() - start, 1)
    if failed_stages:
        logger.warning(
            "Pipeline finished with failures in: %s (%.1fs)",
            ", ".join(failed_stages), elapsed,
        )
        return False

    logger.info("=" * 60)
    logger.info("Pipeline run complete in %.1fs — all stages succeeded", elapsed)
    logger.info("=" * 60)
    return True


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)