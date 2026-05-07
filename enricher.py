"""
enricher.py — Stage 2: Website Enrichment via Playwright

Visits each business website and extracts:
    - Page title
    - Meta description
    - Services/about snippet (first 300 chars from relevant elements)

Improvements over v1:
    - playwright-stealth patches browser to hide automation signals (reduces 403s)
    - Two-attempt retry: fast first attempt, longer timeout + networkidle on retry
    - Execution context destruction caught and handled explicitly
    - Navigation redirect settling via waitForLoadState before DOM queries

Usage:
    python enricher.py          # standalone run
    enrich_leads()              # called by main.py orchestrator
"""

import asyncio
import logging
import random
import re
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout


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
logger = logging.getLogger("enricher")

# ── Paths ─────────────────────────────────────────────────────────────────────
CLEANED_PATH = Path("data/cleaned/cleaned_leads.csv")
RAW_PATH     = Path("data/raw/raw_leads.csv")
OUTPUT_PATH  = Path("data/raw/enriched_leads.csv")

# ── Config ────────────────────────────────────────────────────────────────────
# Attempt 1: fast, domcontentloaded
TIMEOUT_ATTEMPT_1_MS = 12_000
# Attempt 2: slower, networkidle — gives JS-heavy sites more time
TIMEOUT_ATTEMPT_2_MS = 18_000

MAX_CONCURRENCY    = 5
SERVICES_MAX_CHARS = 300

PARKED_SIGNALS = [
    "this domain is for sale",
    "domain for sale",
    "parked domain",
    "buy this domain",
    "this domain may be for sale",
    "godaddy.com",
    "sedoparking.com",
    "hugedomains.com",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

SERVICES_SELECTORS = [
    "[id*='service']",
    "[class*='service']",
    "[id*='about']",
    "[class*='about']",
    "[id*='what-we-do']",
    "[class*='what-we-do']",
    "[id*='solutions']",
    "[class*='solutions']",
]

# Errors that indicate the page navigated mid-scrape — worth retrying
NAVIGATION_ERROR_SIGNALS = [
    "execution context was destroyed",
    "cannot find context with specified id",
    "target closed",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_parked_domain(text: str) -> bool:
    """
    Check if page text matches known parked domain patterns.

    Args:
        text: Lowercased full page text content.

    Returns:
        True if the page looks like a parked/for-sale domain.
    """
    return any(signal in text for signal in PARKED_SIGNALS)


def _is_navigation_error(exc: Exception) -> bool:
    """
    Check if an exception was caused by a mid-scrape navigation/redirect.

    These are worth retrying with a longer wait strategy, unlike 403s or
    DNS failures which won't change on retry.

    Args:
        exc: The caught exception.

    Returns:
        True if the error message matches known navigation destruction patterns.
    """
    msg = str(exc).lower()
    return any(signal in msg for signal in NAVIGATION_ERROR_SIGNALS)


def _extract_services_snippet(element_texts: list[str]) -> str:
    """
    Return the first substantive services/about element text, capped at SERVICES_MAX_CHARS.

    Args:
        element_texts: List of text content strings from matched CSS selectors.

    Returns:
        Cleaned snippet string up to 300 characters. Empty string if nothing found.
    """
    for text in element_texts:
        cleaned = re.sub(r"\s+", " ", text).strip()
        if len(cleaned) > 30:
            return cleaned[:SERVICES_MAX_CHARS]
    return ""


# ── Single Page Scrape ────────────────────────────────────────────────────────

async def _scrape_page(page, url: str, timeout_ms: int, wait_until: str) -> dict:
    """
    Navigate to a URL and extract enrichment fields.

    Applies stealth patches, sets realistic headers, waits for load state
    to settle before querying the DOM. Separated from retry logic so each
    attempt is a clean function call.

    Args:
        page:       Playwright Page object with stealth already applied.
        url:        Target URL.
        timeout_ms: Navigation timeout in milliseconds.
        wait_until: Playwright wait strategy — 'domcontentloaded' or 'networkidle'.

    Returns:
        Dict with enrichment fields and enrichment_status='success', or raises.
    """
    await page.set_extra_http_headers({
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language":           "en-US,en;q=0.9",
        "Accept-Encoding":           "gzip, deflate, br",
        "DNT":                       "1",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    })

    response = await page.goto(url, timeout=timeout_ms, wait_until=wait_until)

    if response is None or not (200 <= response.status < 300):
        status = response.status if response else "no_response"
        raise ValueError(f"HTTP_{status}")

    # Wait for any post-navigation redirects to settle before querying DOM
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=5_000)
    except PlaywrightTimeout:
        pass  # page is loaded enough — proceed anyway

    body_text = await page.inner_text("body")
    body_lower = body_text.lower()

    if _is_parked_domain(body_lower):
        raise ValueError("PARKED_DOMAIN")

    # Page title
    page_title = re.sub(r"\s+", " ", await page.title()).strip()[:200]

    # Meta description — prefer name="description", fall back to og:description
    meta_desc = ""
    for selector in ['meta[name="description"]', 'meta[property="og:description"]']:
        meta_el = await page.query_selector(selector)
        if meta_el:
            meta_desc = (await meta_el.get_attribute("content") or "").strip()[:500]
            break

    # Services/about snippet
    element_texts = []
    for selector in SERVICES_SELECTORS:
        elements = await page.query_selector_all(selector)
        for el in elements[:3]:
            try:
                text = await el.inner_text()
                if text.strip():
                    element_texts.append(text)
            except Exception:
                continue
        if element_texts:
            break

    return {
        "meta_description":  meta_desc,
        "page_title":        page_title,
        "services_snippet":  _extract_services_snippet(element_texts),
        "enrichment_status": "success",
    }


async def _enrich_single(browser, url: str, business_name: str) -> dict:
    """
    Enrich a single business URL with two-attempt retry strategy.

    Attempt 1: domcontentloaded, 12s timeout — fast path for normal sites.
    Attempt 2: networkidle, 18s timeout — for JS-heavy sites and mid-scrape
               navigation errors that need more time to settle.

    A fresh browser context is created for each attempt so cookies, cache,
    and navigation state don't carry over from a failed first attempt.

    Args:
        browser:       Playwright Browser object shared across all tasks.
        url:           Business website URL to visit.
        business_name: Used only for logging context.

    Returns:
        Dict with enrichment fields. enrichment_status is 'success' or
        'enrichment_failed'. Never raises.
    """
    failed_result = {
        "meta_description":  "",
        "page_title":        "",
        "services_snippet":  "",
        "enrichment_status": "enrichment_failed",
    }

    attempts = [
        {"timeout_ms": TIMEOUT_ATTEMPT_1_MS, "wait_until": "domcontentloaded"},
        {"timeout_ms": TIMEOUT_ATTEMPT_2_MS, "wait_until": "networkidle"},
    ]

    for attempt_num, attempt_cfg in enumerate(attempts, start=1):
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
        )
        page = await context.new_page()

        # Patch browser automation signals directly — no external dependency needed
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            window.chrome = {runtime: {}};
            Object.defineProperty(navigator, 'permissions', {
                get: () => ({query: () => Promise.resolve({state: 'granted'})})
            });
        """)

        try:
            result = await _scrape_page(
                page,
                url,
                attempt_cfg["timeout_ms"],
                attempt_cfg["wait_until"],
            )
            logger.info(
                "[%s] Attempt %d succeeded — title: %d chars, meta: %d chars, services: %d chars",
                business_name, attempt_num,
                len(result["page_title"]),
                len(result["meta_description"]),
                len(result["services_snippet"]),
            )
            return result

        except PlaywrightTimeout:
            logger.warning(
                "[%s] Attempt %d timed out after %dms",
                business_name, attempt_num, attempt_cfg["timeout_ms"],
            )

        except ValueError as exc:
            msg = str(exc)
            if msg.startswith("HTTP_"):
                logger.warning("[%s] Attempt %d — %s — marking failed", business_name, attempt_num, msg)
                return failed_result  # HTTP errors won't improve on retry
            if msg == "PARKED_DOMAIN":
                logger.warning("[%s] Parked domain detected — skipping", business_name)
                return failed_result  # retrying a parked domain is pointless
            logger.warning("[%s] Attempt %d — ValueError: %s", business_name, attempt_num, msg)

        except Exception as exc:
            if _is_navigation_error(exc):
                logger.warning(
                    "[%s] Attempt %d — navigation destroyed mid-scrape — %s",
                    business_name, attempt_num,
                    "retrying with networkidle" if attempt_num == 1 else "giving up",
                )
                # Fall through to attempt 2 with networkidle strategy
            else:
                logger.error(
                    "[%s] Attempt %d — unexpected error: %s — marking failed",
                    business_name, attempt_num, exc,
                )
                return failed_result  # unknown errors won't improve on retry

        finally:
            await context.close()

    # Both attempts exhausted
    logger.warning("[%s] Both attempts failed — marking enrichment_failed", business_name)
    return failed_result


# ── Async Orchestration ───────────────────────────────────────────────────────

async def _run_enrichment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run async enrichment across all rows using a shared browser instance.

    A semaphore caps concurrent page visits at MAX_CONCURRENCY to avoid
    overwhelming the local machine or triggering rate limits.

    Args:
        df: DataFrame with at least 'website' and 'name' columns.

    Returns:
        Original DataFrame with enrichment columns appended.
    """
    results = [None] * len(df)
    semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        async def process_row(idx: int, row: pd.Series):
            url  = str(row.get("website", "")).strip()
            name = str(row.get("name", f"row_{idx}"))

            if not url or url.lower() == "nan":
                results[idx] = {
                    "meta_description":  "",
                    "page_title":        "",
                    "services_snippet":  "",
                    "enrichment_status": "no_website",
                }
                return

            async with semaphore:
                results[idx] = await _enrich_single(browser, url, name)

        await asyncio.gather(*[process_row(i, row) for i, row in df.iterrows()])
        await browser.close()

    cols_to_drop = [c for c in [
        "meta_description", "page_title", "services_snippet",
        "enrichment_status", "data_quality"
    ] if c in df.columns]
    df = df.drop(columns=cols_to_drop)

    enrichment_df = pd.DataFrame(results)
    return pd.concat([df.reset_index(drop=True), enrichment_df], axis=1)

# ── Main Pipeline Function ────────────────────────────────────────────────────

def enrich_leads() -> int:
    """
    Load cleaned leads, enrich each with website data, save enriched output.

    Prefers cleaned_leads.csv as input; falls back to raw_leads.csv.
    Output is saved to data/raw/enriched_leads.csv so cleaner.py picks it
    up automatically on next run.

    Returns:
        Number of rows with enrichment_status == 'success'.

    Raises:
        FileNotFoundError: If neither input file exists.
    """
    input_path = CLEANED_PATH if CLEANED_PATH.exists() else RAW_PATH

    if not input_path.exists():
        raise FileNotFoundError(f"No input file found. Run collector.py first. Looked for: {input_path}")

    df = pd.read_csv(input_path, dtype={"phone": str, "website": str, "maps_url": str})
    logger.info("Loaded %d rows for enrichment from %s", len(df), input_path)

    enriched_df = asyncio.run(_run_enrichment(df))

    status_counts = enriched_df["enrichment_status"].value_counts().to_dict()
    success_count = status_counts.get("success", 0)
    fail_count    = status_counts.get("enrichment_failed", 0)
    no_web_count  = status_counts.get("no_website", 0)

    logger.info(
        "Enrichment complete - %d succeeded, %d failed, %d had no website",
        success_count, fail_count, no_web_count,
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    enriched_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    logger.info("Enriched output written to %s", OUTPUT_PATH)

    return success_count


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    enrich_leads()