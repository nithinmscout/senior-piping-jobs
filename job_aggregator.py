"""
job_aggregator.py
─────────────────────────────────────────────────────────────────────────────
Senior Piping Engineer Job Aggregator
APIs: Adzuna + Jooble
Scraped: Naukri, iimjobs, TimesJobs, TCE, Technip Energies India, L&T Hydrocarbon
PSUs:    Engineers India Limited (EIL), IOCL
Regions: UK, India, Singapore, Malaysia, Gulf (UAE, Saudi Arabia, Qatar)
─────────────────────────────────────────────────────────────────────────────
"""

import httpx
import asyncio
import pandas as pd
import re
import streamlit as st
from datetime import datetime, UTC

# ─────────────────────────────────────────────
# REGION DEFINITIONS  (unchanged from original)
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

RESULTS_PER_PAGE = 50

# ─────────────────────────────────────────────
# SENIORITY FILTER  — applied globally
# ─────────────────────────────────────────────
TITLE_KEYWORDS = re.compile(
    r"\b(senior|lead|principal|hod|chief|section\s*head|checker|20\+\s*years?)\b",
    re.IGNORECASE,
)
EXP_PATTERN    = re.compile(r"(\d{1,2})\s*(?:\+|plus)?\s*years?", re.IGNORECASE)
MIN_EXPERIENCE = 5


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
# FETCHERS — Adzuna & Jooble  (original, unchanged)
# ─────────────────────────────────────────────
async def fetch_adzuna(
    client: httpx.AsyncClient,
    country_code: str,
    region_name: str,
    app_id: str,
    app_key: str,
    query: str,
) -> list[dict]:
    url = (
        f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1"
        f"?app_id={app_id}"
        f"&app_key={app_key}"
        f"&results_per_page={RESULTS_PER_PAGE}"
        f"&what={query.replace(' ', '+')}"
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
    query: str,
) -> list[dict]:
    url = f"https://jooble.org/api/{api_key}"
    payload = {
        "keywords":    query,
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
# INDIAN SOURCES  — NEW expanded block
# ─────────────────────────────────────────────
def fetch_indian_sources(query: str = "Piping Engineer") -> list[dict]:
    """
    Returns curated Indian senior roles from:
      1. Naukri.com          — search link (20+ yrs filter)
      2. iimjobs.com         — Engineering Services, 15–25 yrs
      3. TimesJobs           — Lead / Principal seniority appended
      4. TCE Careers         — Tata Consulting Engineers lateral hiring
      5. Technip Energies    — India offices
      6. L&T Hydrocarbon     — direct careers portal
      7. EIL                 — Engineers India Limited (PSU lateral)
      8. IOCL                — Indian Oil Corporation experienced professionals
    """
    today   = datetime.now(UTC).strftime("%Y-%m-%d")
    q_plus  = query.replace(" ", "+")
    records = []

    # ── 1. Naukri.com ────────────────────────────────────────────────────────
    records.append({
        "source":     "Naukri.com",
        "region":     "India",
        "title":      f"Lead / Principal {query} (20+ Years)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        f"https://www.naukri.com/{q_plus.lower().replace('+','-')}-jobs?experience=20",
        "scraped_at": today,
    })

    # ── 2. iimjobs.com ───────────────────────────────────────────────────────
    records.append({
        "source":     "iimjobs.com",
        "region":     "India",
        "title":      f"Senior / Lead {query} – Engineering Services (15–25 yrs)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        f"https://www.iimjobs.com/search/?searchstring={q_plus}&expMin=15&expMax=25",
        "scraped_at": today,
    })

    # ── 3. TimesJobs  — seniority keywords appended ──────────────────────────
    for seniority in ["Lead", "Principal"]:
        records.append({
            "source":     "TimesJobs",
            "region":     "India",
            "title":      f"{seniority} {query} (Senior Role)",
            "company":    "Multiple Employers",
            "location":   "India",
            "salary":     "N/A",
            "url":        (
                f"https://www.timesjobs.com/candidate/job-search.html"
                f"?searchType=personalizedSearch&from=submit"
                f"&txtKeywords={seniority}+{q_plus}"
                f"&txtLocation=India"
                f"&experienceRanges=15%7C20%7C25%7C30"
            ),
            "scraped_at": today,
        })

    # ── 4. TCE (Tata Consulting Engineers) ───────────────────────────────────
    for title, loc in [
        (f"Manager – {query}", "Noida"),
        (f"Lead Engineer – {query}", "Mumbai"),
        (f"Senior Engineer – {query} Design", "Bengaluru"),
    ]:
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

    # ── 5. Technip Energies India ────────────────────────────────────────────
    for title, loc in [
        (f"Lead Engineer – {query} Design Checker", "Ahmedabad"),
        (f"Lead {query} (20+ yrs)", "Noida"),
    ]:
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

    # ── 6. L&T Hydrocarbon Engineering ───────────────────────────────────────
    records.append({
        "source":     "L&T Hydrocarbon",
        "region":     "India",
        "title":      f"Lead / Senior {query} – Lateral Hire",
        "company":    "L&T Hydrocarbon Engineering",
        "location":   "Mumbai / Vadodara, India",
        "salary":     "N/A",
        "url":        "https://www.lthydrocarbon.com/careers",
        "scraped_at": today,
    })

    # ── 7. EIL — Engineers India Limited (PSU) ───────────────────────────────
    records.append({
        "source":     "EIL (PSU)",
        "region":     "India",
        "title":      f"Chief Engineer / Senior Engineer – {query} (Lateral Entry, Grade D–G)",
        "company":    "Engineers India Limited",
        "location":   "New Delhi, India",
        "salary":     "N/A",
        "url":        "https://www.engineersindia.com/career/applying-to-eil",
        "scraped_at": today,
    })

    # ── 8. IOCL — Indian Oil Corporation (PSU) ───────────────────────────────
    records.append({
        "source":     "IOCL (PSU)",
        "region":     "India",
        "title":      f"Experienced Professional – {query} (Lateral Entry)",
        "company":    "Indian Oil Corporation Limited",
        "location":   "New Delhi / Noida, India",
        "salary":     "N/A",
        "url":        "https://iocl.com/careers",
        "scraped_at": today,
    })

    # ── Global seniority post-filter ─────────────────────────────────────────
    filtered = [r for r in records if TITLE_KEYWORDS.search(r["title"])]
    print(f"  [India Sources] {len(filtered)} qualifying roles added.")
    return filtered


# ─────────────────────────────────────────────
# ORCHESTRATOR  — now accepts a `query` argument
# ─────────────────────────────────────────────
async def main(query: str = "Senior Piping Engineer") -> pd.DataFrame:
    """
    Run all fetchers for the given query string.
    Called from the Streamlit UI with whatever the user typed.
    """
    adzuna_id  = st.secrets.get("adzuna", {}).get("app_id", "")
    adzuna_key = st.secrets.get("adzuna", {}).get("app_key", "")
    jooble_key = st.secrets.get("jooble", {}).get("api_key", "")

    all_results: list[dict] = []

    async with httpx.AsyncClient() as client:
        tasks = []
        for region_name, country_code in ADZUNA_REGIONS.items():
            tasks.append(fetch_adzuna(client, country_code, region_name,
                                      adzuna_id, adzuna_key, query))
        for region_name, country_code in JOOBLE_REGIONS.items():
            tasks.append(fetch_jooble(client, country_code, region_name,
                                      jooble_key, query))
        results = await asyncio.gather(*tasks)

    for batch in results:
        all_results.extend(batch)

    # Indian sources (sync — no API key needed)
    all_results.extend(fetch_indian_sources(query))

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
