"""
Job Aggregator: Senior Piping Engineer Roles
APIs: Adzuna + Jooble
Regions: UK, India, Singapore, Malaysia, Gulf (UAE, Saudi Arabia, Qatar)
"""

import httpx
import asyncio
import pandas as pd
import re
import streamlit as st
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIG — replace with your real API keys
# ─────────────────────────────────────────────
def _get_secrets():
    """Load API keys lazily — only when a fetch is actually triggered."""
    return {
        "adzuna_id":  st.secrets.get("adzuna", {}).get("app_id", ""),
        "adzuna_key": st.secrets.get("adzuna", {}).get("app_key", ""),
        "jooble_key": st.secrets.get("jooble", {}).get("api_key", ""),
    }

# Region definitions
ADZUNA_REGIONS = {
    "UK":        "gb",
    "India":     "in",
    "Singapore": "sg",
    "Malaysia":  "my",
    # Adzuna doesn't cover Gulf natively, handled via Jooble
}

JOOBLE_REGIONS = {
    "UK":           "gb",
    "India":        "in",
    "Singapore":    "sg",
    "Malaysia":     "my",
    "UAE":          "ae",
    "Saudi Arabia": "sa",
    "Qatar":        "qa",
}

SEARCH_QUERY   = "Senior Piping Engineer"
RESULTS_PER_PAGE = 50  # max per API call
TITLE_KEYWORDS = re.compile(r"\b(senior|lead|principal)\b", re.IGNORECASE)
EXP_PATTERN    = re.compile(
    r"(\d{1,2})\s*(?:\+|plus)?\s*years?",
    re.IGNORECASE
)
MIN_EXPERIENCE = 20


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def title_passes_filter(title: str) -> bool:
    """Must contain Senior, Lead, or Principal."""
    return bool(TITLE_KEYWORDS.search(title))


def experience_passes_filter(description: str) -> bool:
    """
    Returns True if:
      - description mentions >= MIN_EXPERIENCE years, OR
      - no experience requirement is found at all (keep it — we can't reject unknowns)
    """
    if not description:
        return True
    matches = EXP_PATTERN.findall(description)
    if not matches:
        return True  # no experience clause — do not reject
    max_exp = max(int(m) for m in matches)
    return max_exp >= MIN_EXPERIENCE


def safe_salary(job: dict, source: str) -> str:
    """Normalise salary field across APIs."""
    if source == "adzuna":
        min_s = job.get("salary_min")
        max_s = job.get("salary_max")
        if min_s or max_s:
            return f"{min_s or '?'} – {max_s or '?'}"
    elif source == "jooble":
        salary = job.get("salary", "")
        if salary and str(salary).strip() not in ("", "0"):
            return str(salary).strip()
    return "N/A"


# ─────────────────────────────────────────────
# ADZUNA FETCHER
# ─────────────────────────────────────────────
async def fetch_adzuna(client: httpx.AsyncClient, country_code: str, region_name: str) -> list[dict]:
    """Fetch jobs from Adzuna for one country."""
    url = (
        f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1"
        f"?app_id={ADZUNA_APP_ID}"
        f"&app_key={ADZUNA_APP_KEY}"
        f"&results_per_page={RESULTS_PER_PAGE}"
        f"&what={SEARCH_QUERY.replace(' ', '+')}"
        f"&content-type=application/json"
    )
    try:
        resp = await client.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("results", [])
        records = []
        for job in jobs:
            title = job.get("title", "")
            description = job.get("description", "")
            if not title_passes_filter(title):
                continue
            if not experience_passes_filter(description):
                continue
            records.append({
                "source":   "Adzuna",
                "region":   region_name,
                "title":    title,
                "company":  job.get("company", {}).get("display_name", "N/A"),
                "location": job.get("location", {}).get("display_name", "N/A"),
                "salary":   safe_salary(job, "adzuna"),
                "url":      job.get("redirect_url", "N/A"),
                "scraped_at": datetime.utcnow().strftime("%Y-%m-%d"),
            })
        print(f"  [Adzuna] {region_name}: {len(records)} qualifying jobs found.")
        return records
    except Exception as e:
        print(f"  [Adzuna] {region_name} ERROR: {e}")
        return []


# ─────────────────────────────────────────────
# JOOBLE FETCHER
# ─────────────────────────────────────────────
async def fetch_jooble(client: httpx.AsyncClient, country_code: str, region_name: str) -> list[dict]:
    """Fetch jobs from Jooble for one country."""
    url = f"https://jooble.org/api/{JOOBLE_API_KEY}"
    payload = {
        "keywords": SEARCH_QUERY,
        "location": country_code,  # Jooble accepts ISO country codes
        "page":     "1",
        "resultonpage": str(RESULTS_PER_PAGE),
    }
    try:
        resp = await client.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("jobs", [])
        records = []
        for job in jobs:
            title = job.get("title", "")
            description = job.get("snippet", "")  # Jooble uses 'snippet'
            if not title_passes_filter(title):
                continue
            if not experience_passes_filter(description):
                continue
            records.append({
                "source":   "Jooble",
                "region":   region_name,
                "title":    title,
                "company":  job.get("company", "N/A"),
                "location": job.get("location", "N/A"),
                "salary":   safe_salary(job, "jooble"),
                "url":      job.get("link", "N/A"),  # raw redirect link
                "scraped_at": datetime.utcnow().strftime("%Y-%m-%d"),
            })
        print(f"  [Jooble]  {region_name}: {len(records)} qualifying jobs found.")
        return records
    except Exception as e:
        print(f"  [Jooble]  {region_name} ERROR: {e}")
        return []


# ─────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────
async def main() -> pd.DataFrame:
    secrets = _get_secrets()                        # ← add this line
    ADZUNA_APP_ID  = secrets["adzuna_id"]           # ← add this line
    ADZUNA_APP_KEY = secrets["adzuna_key"]          # ← add this line
    JOOBLE_API_KEY = secrets["jooble_key"]  
    all_results = []

    async with httpx.AsyncClient() as client:
        # Build all tasks concurrently
        tasks = []

        for region_name, country_code in ADZUNA_REGIONS.items():
            tasks.append(fetch_adzuna(client, country_code, region_name))

        for region_name, country_code in JOOBLE_REGIONS.items():
            tasks.append(fetch_jooble(client, country_code, region_name))

        # Run all requests in parallel
        results = await asyncio.gather(*tasks)

    for batch in results:
        all_results.extend(batch)

    # ── DataFrame cleaning ───────────────────
    df = pd.DataFrame(all_results)

    if df.empty:
        print("No results returned. Check API keys and connectivity.")
        return df

    # 1. Drop full duplicates
    df.drop_duplicates(inplace=True)

    # 2. Deduplicate by (title + company + location) across sources
    df.drop_duplicates(subset=["title", "company", "location"], keep="first", inplace=True)

    # 3. Strip whitespace from string columns
    str_cols = ["title", "company", "location", "salary", "url"]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # 4. Normalise title casing
    df["title"] = df["title"].str.title()

    # 5. Replace blank strings with "N/A"
    df.replace(r"^\s*$", "N/A", regex=True, inplace=True)

    # 6. Final column selection & ordering
    df = df[["source", "region", "title", "company", "location", "salary", "url", "scraped_at"]]
    df.reset_index(drop=True, inplace=True)

    return df


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print(f"\n{'='*55}")
    print(f"  Job Aggregator — Senior Piping Engineer")
    print(f"  Run time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*55}\n")

    df = asyncio.run(main())

    if not df.empty:
        output_file = "senior_piping_engineer_jobs.csv"
        df.to_csv(output_file, index=False)
        print(f"\n✅ Done. {len(df)} jobs saved → {output_file}")
        print(df.head(10).to_string(index=False))
    else:
        print("⚠️  No qualifying jobs found.")
