"""
collector.py — Stage 1: Business Lead Collection

Queries SerpAPI's Google Maps engine for digital marketing agencies
across 5 US cities. Writes results incrementally to data/raw/raw_leads.csv
so partial data is preserved if a later city fails.

Usage:
    python collector.py          # standalone run
    collect_all_leads()          # called by main.py orchestrator
"""

import csv
import logging
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

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
logger = logging.getLogger("collector")

# ── Config ────────────────────────────────────────────────────────────────────
SERPAPI_KEY: str | None = os.getenv("SERPAPI_KEY")
RAW_OUTPUT_PATH = Path("data/raw/raw_leads.csv")
SERPAPI_URL = "https://serpapi.com/search"

CITIES = [
    "New York, NY",
    "Los Angeles, CA",
    "Chicago, IL",
    "Houston, TX",
    "Austin, TX",
    "Phoenix, AZ",
    "Philadelphia, PA",
    "San Antonio, TX",
    "San Diego, CA",
    "Dallas, TX",
    "San Jose, CA",
    "San Francisco, CA",
    "Seattle, WA",
    "Denver, CO",
    "Nashville, TN",
    "Atlanta, GA",
    "Miami, FL",
    "Minneapolis, MN",
    "Portland, OR",
    "Las Vegas, NV",
]

class SerpAPILimitError(Exception):
    """
    Raised when SerpAPI reports the monthly search quota is exhausted.
    Signals main.py to abort collection entirely — retrying other cities
    will not help and wastes no further API credits.
    """
    pass

RESULTS_PER_CITY = 20
MAX_RETRIES = 3
INTER_CITY_DELAY = 1.5  # seconds — polite gap between city requests

FIELDNAMES = [
    "name",
    "full_address",
    "city",
    "phone",
    "website",
    "rating",
    "review_count",
    "category",
    "maps_url",
]


# ── Core Functions ────────────────────────────────────────────────────────────

def _parse_business(biz: dict, city: str) -> dict:
    """
    Normalise a single SerpAPI local_result entry into a flat lead dict.

    Args:
        biz:  Raw dict from SerpAPI local_results list.
        city: City label injected as a separate column.

    Returns:
        Flat dict aligned to FIELDNAMES. Missing keys default to empty string.
    """
    return {
        "name":         biz.get("title", ""),
        "full_address": biz.get("address", ""),
        "city":         city,
        "phone":        biz.get("phone", ""),
        "website":      biz.get("website", ""),
        "rating":       biz.get("rating", ""),
        "review_count": biz.get("reviews", ""),
        "category":     biz.get("type", ""),
        # place_id_search is the stable deep-link; fall back to generic link
        "maps_url":     biz.get("place_id_search", biz.get("link", "")),
    }


def fetch_city_leads(city: str, api_key: str, num_results: int = 20) -> list[dict]:
    """
    Fetch digital marketing agency listings for a single city from SerpAPI.

    Retries up to MAX_RETRIES times using exponential backoff (2s, 4s, 8s)
    for transient network and server errors only.

    Monthly quota exhaustion is detected immediately and raises SerpAPILimitError
    so the orchestrator can abort the full run without wasting further credits.

    Args:
        city:        City string sent in the search query (e.g. "Austin, TX").
        api_key:     SerpAPI authentication key.
        num_results: How many results to request per query (default 20).

    Returns:
        List of normalised lead dicts. Empty list on recoverable failure.

    Raises:
        SerpAPILimitError: If SerpAPI reports monthly quota is exhausted.
    """
    params = {
        "engine":  "google_maps",
        "q":       f"digital marketing agency {city}",
        "type":    "search",
        "api_key": api_key,
        "num":     num_results,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("[%s] Attempt %d/%d - requesting SerpAPI", city, attempt, MAX_RETRIES)
            response = requests.get(SERPAPI_URL, params=params, timeout=20)

            # Check for quota exhaustion before raising for status —
            # SerpAPI returns 200 with an error key when quota is hit
            if response.status_code == 200:
                data = response.json()

                # Detect monthly quota exhaustion
                error_msg = data.get("error", "").lower()
                if any(phrase in error_msg for phrase in [
                    "run out of searches",
                    "out of searches",
                    "search limit",
                    "quota exceeded",
                    "plan limit",
                    "upgrade your plan",
                ]):
                    logger.critical(
                        "SerpAPI monthly quota exhausted: '%s' — aborting all city collection",
                        data.get("error", ""),
                    )
                    raise SerpAPILimitError(data.get("error", "Monthly quota exhausted"))

                raw_results = data.get("local_results", [])
                if not raw_results:
                    logger.warning("[%s] SerpAPI returned 0 local_results", city)
                    return []

                leads = [_parse_business(biz, city) for biz in raw_results]
                logger.info("[%s] Collected %d leads", city, len(leads))
                return leads

            response.raise_for_status()

        except SerpAPILimitError:
            raise  # never swallow quota errors — let them propagate immediately

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            logger.error("[%s] HTTP %s on attempt %d", city, status, attempt)
            if status == 401:
                logger.critical("SerpAPI key rejected (401) - check SERPAPI_KEY in .env")
                return []
            if status == 429:
                logger.warning("[%s] Rate limited (429) - backing off", city)

        except requests.exceptions.RequestException as exc:
            logger.error("[%s] Request error on attempt %d: %s", city, attempt, exc)

        wait = 2 ** attempt
        logger.info("[%s] Backing off %ds before retry", city, wait)
        time.sleep(wait)

    logger.error("[%s] All %d attempts failed - skipping city", city, MAX_RETRIES)
    return []


def collect_all_leads() -> int:
    """
    Orchestrate lead collection across all cities defined in CITIES.

    Writes rows incrementally (flush after each city) so partial runs
    are recoverable. Aborts immediately if SerpAPI monthly quota is hit
    rather than wasting credits on further city requests.

    Returns:
        Total number of lead rows written to RAW_OUTPUT_PATH.

    Raises:
        EnvironmentError:    If SERPAPI_KEY is missing from environment.
        SerpAPILimitError:   If monthly search quota is exhausted mid-run.
    """
    if not SERPAPI_KEY:
        logger.critical("SERPAPI_KEY not found in environment - aborting collection")
        raise EnvironmentError("Missing SERPAPI_KEY. Set it in your .env file.")

    RAW_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    total_collected  = 0
    total_failed     = 0
    quota_exhausted  = False

    with open(RAW_OUTPUT_PATH, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        writer.writeheader()

        for city in CITIES:
            try:
                leads = fetch_city_leads(city, SERPAPI_KEY, RESULTS_PER_CITY)
            except SerpAPILimitError as exc:
                logger.critical(
                    "Monthly quota hit after %d leads from %d cities - "
                    "partial data saved to %s",
                    total_collected,
                    CITIES.index(city),
                    RAW_OUTPUT_PATH,
                )
                quota_exhausted = True
                break  # stop immediately — partial data is still usable

            if not leads:
                total_failed += 1
            else:
                for lead in leads:
                    writer.writerow({k: lead.get(k, "") for k in FIELDNAMES})
                csv_file.flush()
                total_collected += len(leads)

            time.sleep(INTER_CITY_DELAY)

    logger.info(
        "Collection %s - %d leads written (%d/%d cities failed)",
        "PARTIAL - quota exhausted" if quota_exhausted else "complete",
        total_collected,
        total_failed,
        len(CITIES),
    )

    if quota_exhausted:
        raise SerpAPILimitError(
            f"Quota exhausted mid-run. {total_collected} leads collected and saved."
        )

    return total_collected


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    collect_all_leads()