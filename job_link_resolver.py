"""
job_link_resolver.py
─────────────────────────────────────────────────────────────────────────────
Resolves aggregator/tracker redirect URLs to their final employer career page.
Uses requests.Session with a Chrome User-Agent, 5s timeout, and full error
handling for 403/404 and network failures.
─────────────────────────────────────────────────────────────────────────────
Usage:
    from job_link_resolver import get_direct_link, resolve_bulk

    final = get_direct_link("https://www.adzuna.co.uk/land/ad/...")
    results_df = resolve_bulk(list_of_urls)
"""

import requests
import pandas as pd
import time
import logging
from urllib.parse import urlparse
from typing import Optional
from dataclasses import dataclass, field

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("job_resolver")


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
TIMEOUT        = 5          # seconds per request
MAX_REDIRECTS  = 10         # safety cap on redirect chain depth
RETRY_ATTEMPTS = 2          # retry count on transient failures
RETRY_BACKOFF  = 1.5        # seconds to wait between retries

# Chrome 124 on Windows 11 — realistic UA to bypass bot detection
CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent":      CHROME_USER_AGENT,
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "DNT":             "1",
}

# Status codes we treat as a "soft" failure but still return the last URL
SOFT_FAIL_CODES = {403, 404, 410, 429, 451}

# Status codes worth retrying once
RETRYABLE_CODES = {429, 500, 502, 503, 504}


# ─────────────────────────────────────────────
# RESULT MODEL
# ─────────────────────────────────────────────
@dataclass
class ResolveResult:
    original_url:  str
    final_url:     str
    status_code:   Optional[int]
    redirect_hops: int
    resolved:      bool          # True = clean final URL reached
    error:         Optional[str] = field(default=None)

    def to_dict(self) -> dict:
        return {
            "original_url":  self.original_url,
            "final_url":     self.final_url,
            "status_code":   self.status_code,
            "redirect_hops": self.redirect_hops,
            "resolved":      self.resolved,
            "error":         self.error,
        }


# ─────────────────────────────────────────────
# SESSION FACTORY
# ─────────────────────────────────────────────
def _build_session() -> requests.Session:
    """
    Create a reusable requests.Session pre-configured with:
      - Chrome User-Agent and browser-like headers
      - Max redirect cap (prevents infinite loops)
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    session.max_redirects = MAX_REDIRECTS
    return session


# ─────────────────────────────────────────────
# CORE RESOLVER
# ─────────────────────────────────────────────
def _resolve_with_session(
    session: requests.Session,
    url: str,
) -> ResolveResult:
    """
    Internal resolver. Uses the shared session to follow redirects,
    returning a full ResolveResult dataclass.
    """
    attempt = 0
    last_error: Optional[str] = None

    while attempt <= RETRY_ATTEMPTS:
        try:
            response = session.get(
                url,
                timeout=TIMEOUT,
                allow_redirects=True,   # follow full redirect chain
                stream=False,
            )

            hops         = len(response.history)   # number of intermediate redirects
            final_url    = response.url             # requests resolves this automatically
            status_code  = response.status_code

            # ── 403 / 404 handling ──────────────────────────────────────────
            if status_code == 403:
                log.warning(f"403 Forbidden — site blocked bot access: {final_url}")
                return ResolveResult(
                    original_url=url,
                    final_url=str(final_url),
                    status_code=403,
                    redirect_hops=hops,
                    resolved=False,
                    error="403 Forbidden: destination blocked automated access.",
                )

            if status_code == 404:
                log.warning(f"404 Not Found — listing may have expired: {final_url}")
                return ResolveResult(
                    original_url=url,
                    final_url=str(final_url),
                    status_code=404,
                    redirect_hops=hops,
                    resolved=False,
                    error="404 Not Found: job listing has been removed or expired.",
                )

            # ── Other soft failures ─────────────────────────────────────────
            if status_code in SOFT_FAIL_CODES:
                msg = f"HTTP {status_code}: non-retryable client/server error."
                log.warning(f"{msg} | URL: {final_url}")
                return ResolveResult(
                    original_url=url,
                    final_url=str(final_url),
                    status_code=status_code,
                    redirect_hops=hops,
                    resolved=False,
                    error=msg,
                )

            # ── Retryable server errors ─────────────────────────────────────
            if status_code in RETRYABLE_CODES and attempt < RETRY_ATTEMPTS:
                log.info(f"HTTP {status_code} — retrying in {RETRY_BACKOFF}s ({url})")
                time.sleep(RETRY_BACKOFF)
                attempt += 1
                continue

            # ── Success ─────────────────────────────────────────────────────
            log.info(f"✅ Resolved ({hops} hops, HTTP {status_code}): {final_url}")
            return ResolveResult(
                original_url=url,
                final_url=str(final_url),
                status_code=status_code,
                redirect_hops=hops,
                resolved=True,
                error=None,
            )

        # ── Timeout ─────────────────────────────────────────────────────────
        except requests.exceptions.Timeout:
            last_error = f"Request timed out after {TIMEOUT}s."
            log.warning(f"⏱ Timeout ({attempt+1}/{RETRY_ATTEMPTS+1}): {url}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF)
                attempt += 1
                continue
            break

        # ── Too many redirects ───────────────────────────────────────────────
        except requests.exceptions.TooManyRedirects:
            last_error = f"Exceeded {MAX_REDIRECTS} redirects — possible redirect loop."
            log.error(f"↩️  Too many redirects: {url}")
            break

        # ── SSL errors ───────────────────────────────────────────────────────
        except requests.exceptions.SSLError as e:
            last_error = f"SSL certificate error: {e}"
            log.error(f"🔒 SSL error: {url} — {e}")
            break

        # ── DNS / connection failure ─────────────────────────────────────────
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connection error: {e}"
            log.error(f"🔌 Connection failed: {url}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF)
                attempt += 1
                continue
            break

        # ── Catch-all ────────────────────────────────────────────────────────
        except requests.exceptions.RequestException as e:
            last_error = f"Unexpected request error: {e}"
            log.error(f"❌ Unhandled error ({url}): {e}")
            break

    return ResolveResult(
        original_url=url,
        final_url=url,       # fall back to original if resolution failed
        status_code=None,
        redirect_hops=0,
        resolved=False,
        error=last_error or "Unknown error during resolution.",
    )


# ─────────────────────────────────────────────
# PUBLIC API — SINGLE URL
# ─────────────────────────────────────────────
def get_direct_link(url: str) -> str:
    """
    Resolve a single aggregator/tracker URL to its final employer career page.

    Args:
        url (str): The raw redirect/tracking URL (e.g. from Adzuna or Jooble).

    Returns:
        str: The final resolved URL. Returns the original URL if resolution fails.

    Example:
        >>> get_direct_link("https://www.adzuna.co.uk/land/ad/12345678")
        "https://careers.shell.com/job/piping-engineer-123"
    """
    if not url or not isinstance(url, str):
        log.warning("get_direct_link: received empty or non-string input.")
        return url

    # Basic URL sanity check
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("http", "https"):
        log.warning(f"get_direct_link: unsupported scheme '{parsed.scheme}' in {url}")
        return url

    with _build_session() as session:
        result = _resolve_with_session(session, url.strip())

    return result.final_url


# ─────────────────────────────────────────────
# PUBLIC API — BULK RESOLVER
# ─────────────────────────────────────────────
def resolve_bulk(
    urls: list[str],
    delay_between: float = 0.3,  # polite crawl delay in seconds
) -> pd.DataFrame:
    """
    Resolve a list of job URLs and return a cleaned pandas DataFrame.

    Args:
        urls            (list[str]): List of raw job URLs.
        delay_between   (float):     Seconds to wait between requests (default 0.3s).

    Returns:
        pd.DataFrame with columns:
            original_url | final_url | status_code | redirect_hops | resolved | error
    """
    if not urls:
        log.warning("resolve_bulk: received empty URL list.")
        return pd.DataFrame()

    log.info(f"Starting bulk resolution of {len(urls)} URLs...")
    records = []

    # One shared session for connection pooling across all URLs
    with _build_session() as session:
        for i, url in enumerate(urls, start=1):
            log.info(f"[{i}/{len(urls)}] Resolving: {url}")
            result = _resolve_with_session(session, url.strip())
            records.append(result.to_dict())
            if i < len(urls):
                time.sleep(delay_between)

    df = pd.DataFrame(records)

    # ── Clean up ─────────────────────────────────────────────────────────────
    df["status_code"]   = pd.to_numeric(df["status_code"], errors="coerce").astype("Int64")
    df["redirect_hops"] = pd.to_numeric(df["redirect_hops"], errors="coerce").fillna(0).astype(int)
    df["resolved"]      = df["resolved"].astype(bool)
    df["final_url"]     = df["final_url"].str.strip()
    df["original_url"]  = df["original_url"].str.strip()

    resolved_count = df["resolved"].sum()
    log.info(
        f"Bulk complete — {resolved_count}/{len(df)} URLs successfully resolved."
    )
    return df


# ─────────────────────────────────────────────
# ENTRY POINT — demo / smoke test
# ─────────────────────────────────────────────
if __name__ == "__main__":
    sample_urls = [
        "https://www.adzuna.co.uk/land/ad/REPLACE_WITH_REAL_ID",
        "https://jooble.org/desc/REPLACE_WITH_REAL_ID",
        "https://httpbin.org/redirect/3",        # public test: 3-hop redirect
        "https://httpbin.org/status/404",        # public test: 404
        "https://httpbin.org/status/403",        # public test: 403
        "https://httpbin.org/delay/10",          # public test: triggers 5s timeout
    ]

    # ── Single URL demo ───────────────────────────────────────────────────────
    print("\n── Single URL Resolution ──")
    final = get_direct_link("https://httpbin.org/redirect/3")
    print(f"Final URL: {final}")

    # ── Bulk demo ─────────────────────────────────────────────────────────────
    print("\n── Bulk Resolution ──")
    df = resolve_bulk(sample_urls)
    print(df.to_string(index=False))

    df.to_csv("resolved_job_links.csv", index=False)
    print("\n✅ Results saved to resolved_job_links.csv")
