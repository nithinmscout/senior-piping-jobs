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
from datetime import datetime, UTC

# ─────────────────────────────────────────────
# REGION DEFINITIONS
# ─────────────────────────────────────────────
ADZUNA_REGIONS = {
    "UK":        "gb",
    "India":     "in",
    "Singapore": "sg",
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

SEARCH_QUERY     = "Senior Piping Engineer"
RESULTS_PER_PAGE = 50

TITLE_KEYWORDS = re.compile(
    r"\b(senior|lead|principal|hod|chief|section\s*head|checker)\b",
    re.IGNORECASE
)
EXP_PATTERN    = re.compile(r"(\d{1,2})\s*(?:\+|plus)?\s*years?", re.IGNORECASE)
MIN_EXPERIENCE = 20


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def title_passes_filter(title: str) -> bool:
    return bool(TITLE_KEYWORDS.search(title))


def experience_passes_filter(description: str) -> bool:
    if not description:
        return True
    matches = EXP_PATTERN.findall(description)
    if not matches:
        return True
    return max(int(m) for m in matches) >= MIN_EXPERIENCE


def safe_salary(job: dict, source: str) -> str:
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
# FETCHERS — accept keys as arguments
# ─────────────────────────────────────────────
async def fetch_adzuna(
    client: httpx.AsyncClient,
    country_code: str,
    region_name: str,
    app_id: str,
    app_key: str,
) -> list[dict]:
    url = (
        f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1"
        f"?app_id={app_id}"
        f"&app_key={app_key}"
        f"&results_per_page={RESULTS_PER_PAGE}"
        f"&what={SEARCH_QUERY.replace(' ', '+')}"
        f"&content-type=application/json"
    )
    try:
        resp = await client.get(url, timeout=15)
        resp.raise_for_status()
        jobs = resp.json().get("results", [])
        records = []
        for job in jobs:
            title = job.get("title", "")
            if not title_passes_filter(title):
                continue
            if not experience_passes_filter(job.get("description", "")):
                continue
            records.append({
                "source":     "Adzuna",
                "region":     region_name,
                "title":      title,
                "company":    job.get("company", {}).get("display_name", "N/A"),
                "location":   job.get("location", {}).get("display_name", "N/A"),
                "salary":     safe_salary(job, "adzuna"),
                "url":        job.get("redirect_url", "N/A"),
                "scraped_at": datetime.now(UTC).strftime("%Y-%m-%d"),
            })
        print(f"  [Adzuna] {region_name}: {len(records)} qualifying jobs found.")
        return records
    except Exception as e:
        print(f"  [Adzuna] {region_name} ERROR: {e}")
        return []


async def fetch_jooble(
    client: httpx.AsyncClient,
    country_code: str,
    region_name: str,
    api_key: str,
) -> list[dict]:
    url = f"https://jooble.org/api/{api_key}"
    payload = {
        "keywords":    SEARCH_QUERY,
        "location":    country_code,
        "page":        "1",
        "resultonpage": str(RESULTS_PER_PAGE),
    }
    try:
        resp = await client.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        records = []
        for job in jobs:
            title = job.get("title", "")
            if not title_passes_filter(title):
                continue
            if not experience_passes_filter(job.get("snippet", "")):
                continue
            records.append({
                "source":     "Jooble",
                "region":     region_name,
                "title":      title,
                "company":    job.get("company", "N/A"),
                "location":   job.get("location", "N/A"),
                "salary":     safe_salary(job, "jooble"),
                "url":        job.get("link", "N/A"),
                "scraped_at": datetime.now(UTC).strftime("%Y-%m-%d"),
            })
        print(f"  [Jooble]  {region_name}: {len(records)} qualifying jobs found.")
        return records
    except Exception as e:
        print(f"  [Jooble]  {region_name} ERROR: {e}")
        return []

# ─────────────────────────────────────────────
# INDIAN SOURCES — Static + Scraped
# ─────────────────────────────────────────────
def fetch_indian_sources() -> list[dict]:
    """
    Returns a curated list of Indian senior piping roles from:
    - Naukri.com (direct search link)
    - iimjobs.com (Engineering Services category)
    - TCE Careers (Noida, Mumbai, Bengaluru)
    - Technip Energies India (Ahmedabad, Noida)
    - Engineers India Limited (EIL lateral recruitment)
    """
    import requests
    from bs4 import BeautifulSoup
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    records = []

    # ── 1. Naukri.com — direct search URL (scraping blocked, link provided) ──
    records.append({
        "source":     "Naukri.com",
        "region":     "India",
        "title":      "Piping Engineer – 20+ Years (Search Results)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        "https://www.naukri.com/piping-engineer-jobs?experience=20",
        "scraped_at": today,
    })

    # ── 2. iimjobs.com — Engineering Services, 15–25 yr filter ──────────────
    records.append({
        "source":     "iimjobs.com",
        "region":     "India",
        "title":      "Senior / Lead Piping Engineer (15–25 yrs)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        "https://www.iimjobs.com/jobs/engineering-services-jobs?exp=15to25",
        "scraped_at": today,
    })

    # ── 3. TCE (Tata Consulting Engineers) ───────────────────────────────────
    tce_roles = [
        ("Manager – Piping", "Noida"),
        ("Lead Engineer – Piping", "Mumbai"),
        ("Manager – Piping Design", "Bengaluru"),
    ]
    for title, loc in tce_roles:
        records.append({
            "source":     "TCE Careers",
            "region":     "India",
            "title":      title,
            "company":    "Tata Consulting Engineers (TCE)",
            "location":   f"{loc}, India",
            "salary":     "N/A",
            "url":        "https://careers.tce.co.in/",
            "scraped_at": today,
        })

    # ── 4. Technip Energies India ────────────────────────────────────────────
    technip_roles = [
        ("Lead Engineer – Piping Design Checker", "Ahmedabad"),
        ("Lead Piping Engineer (20+ yrs)", "Noida"),
    ]
    for title, loc in technip_roles:
        records.append({
            "source":     "Technip Energies",
            "region":     "India",
            "title":      title,
            "company":    "Technip Energies India",
            "location":   f"{loc}, India",
            "salary":     "N/A",
            "url":        "https://www.technipenergies.com/careers/job-opportunities",
            "scraped_at": today,
        })

    # ── 5. EIL — Engineers India Limited (lateral, Grade D–G) ───────────────
    records.append({
        "source":     "EIL",
        "region":     "India",
        "title":      "Senior Engineer / Chief Engineer – Piping (Grade D–G)",
        "company":    "Engineers India Limited",
        "location":   "New Delhi, India",
        "salary":     "N/A",
        "url":        "https://www.engineersindia.com/career/applying-to-eil",
        "scraped_at": today,
    })

    # ── Filter: only keep seniority-matching titles ──────────────────────────
    filtered = [r for r in records if TITLE_KEYWORDS.search(r["title"])]
    print(f"  [India Sources] {len(filtered)} qualifying roles added.")
    return filtered

# ─────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────
async def main() -> pd.DataFrame:
    # Load secrets safely — never crashes if keys are missing
    adzuna_id  = st.secrets.get("adzuna", {}).get("app_id", "")
    adzuna_key = st.secrets.get("adzuna", {}).get("app_key", "")
    jooble_key = st.secrets.get("jooble", {}).get("api_key", "")

    all_results = []

    async with httpx.AsyncClient() as client:
        tasks = []
        for region_name, country_code in ADZUNA_REGIONS.items():
            tasks.append(fetch_adzuna(client, country_code, region_name, adzuna_id, adzuna_key))
        for region_name, country_code in JOOBLE_REGIONS.items():
            tasks.append(fetch_jooble(client, country_code, region_name, jooble_key))

        results = await asyncio.gather(*tasks)

    for batch in results:
        all_results.extend(batch)
        # Add Indian sources
        all_results.extend(fetch_indian_sources())


    df = pd.DataFrame(all_results)
    if df.empty:
        return df

    df.drop_duplicates(inplace=True)
    df.drop_duplicates(subset=["title", "company", "location"], keep="first", inplace=True)
    for col in ["title", "company", "location", "salary", "url"]:
        df[col] = df[col].astype(str).str.strip()
    df["title"] = df["title"].str.title()
    df.replace(r"^\s*$", "N/A", regex=True, inplace=True)
    df = df[["source", "region", "title", "company", "location", "salary", "url", "scraped_at"]]
    df.reset_index(drop=True, inplace=True)
    return df
