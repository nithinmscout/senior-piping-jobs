"""
job_aggregator.py  — v2.0
Senior Job Aggregator: Dynamic keyword search across India, Gulf, Singapore, UK & Malaysia
APIs: Adzuna + Jooble  |  India sources: Naukri, iimjobs, TCE, Technip, L&T, EIL, IOCL
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

RESULTS_PER_PAGE = 50

SENIORITY_REGEX = re.compile(
    r"\b(senior|lead|principal|hod|chief|section\s*head|checker|head\s*of\s*dept|20\+\s*years?)\b",
    re.IGNORECASE,
)
EXP_PATTERN = re.compile(r"(\d{1,2})\s*(?:\+|plus)?\s*years?", re.IGNORECASE)
MIN_EXPERIENCE = 20

DEFAULT_KEYWORD = "Piping Engineer"


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def title_passes_filter(title: str) -> bool:
    return bool(SENIORITY_REGEX.search(title))

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
# ADZUNA FETCHER
# ─────────────────────────────────────────────
async def fetch_adzuna(
    client: httpx.AsyncClient,
    country_code: str,
    region_name: str,
    app_id: str,
    app_key: str,
    keyword: str = DEFAULT_KEYWORD,
) -> list[dict]:
    url = (
        f"https://api.adzuna.com/v1/api/jobs/{country_code}/search/1"
        f"?app_id={app_id}"
        f"&app_key={app_key}"
        f"&results_per_page={RESULTS_PER_PAGE}"
        f"&what={keyword.replace(' ', '+')}"
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
        print(f"  [Adzuna] {region_name}: {len(records)} qualifying jobs.")
        return records
    except Exception as e:
        print(f"  [Adzuna] {region_name} ERROR: {e}")
        return []


# ─────────────────────────────────────────────
# JOOBLE FETCHER
# ─────────────────────────────────────────────
async def fetch_jooble(
    client: httpx.AsyncClient,
    country_code: str,
    region_name: str,
    api_key: str,
    keyword: str = DEFAULT_KEYWORD,
) -> list[dict]:
    url = f"https://jooble.org/api/{api_key}"
    payload = {
        "keywords":     f"Senior {keyword}",
        "location":     country_code,
        "page":         "1",
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
        print(f"  [Jooble]  {region_name}: {len(records)} qualifying jobs.")
        return records
    except Exception as e:
        print(f"  [Jooble]  {region_name} ERROR: {e}")
        return []


# ─────────────────────────────────────────────
# INDIA-SPECIFIC SOURCES
# ─────────────────────────────────────────────
def fetch_indian_sources(keyword: str = DEFAULT_KEYWORD) -> list[dict]:
    """
    Curated India EPC and PSU career links for senior/lead roles.
    Naukri and iimjobs links are dynamically built from the keyword.
    EPC portals: TCE, Technip Energies India, L&T Hydrocarbon.
    PSU portals:  EIL, IOCL.
    """
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    kw_url = keyword.replace(" ", "+")
    records = []

    # ── Naukri.com — dynamic keyword search with 20-yr experience filter ──────
    records.append({
        "source":     "Naukri.com",
        "region":     "India",
        "title":      f"Lead / Senior {keyword} – 20+ Years (Naukri Search)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        f"https://www.naukri.com/{kw_url}-jobs?experience=20",
        "scraped_at": today,
    })

    # ── iimjobs.com — Engineering Services, 15–25 yr filter ──────────────────
    records.append({
        "source":     "iimjobs.com",
        "region":     "India",
        "title":      f"Senior / Principal {keyword} – 15–25 Yrs (iimjobs Search)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        f"https://www.iimjobs.com/jobs/engineering-services-jobs?exp=15to25&q={kw_url}",
        "scraped_at": today,
    })

    # ── TimesJobs — Lead / Principal seniority appended ──────────────────────
    records.append({
        "source":     "TimesJobs",
        "region":     "India",
        "title":      f"Lead {keyword} / Principal {keyword} (TimesJobs)",
        "company":    "Multiple Employers",
        "location":   "India",
        "salary":     "N/A",
        "url":        f"https://www.timesjobs.com/candidate/job-search.html?searchType=personalizedSearch&from=submit&txtKeywords=Lead+{kw_url}&txtLocation=India",
        "scraped_at": today,
    })

    # ── TCE (Tata Consulting Engineers) ──────────────────────────────────────
    tce_roles = [
        (f"Manager – {keyword}", "Noida"),
        (f"Lead Engineer – {keyword}", "Mumbai"),
        (f"Manager – {keyword} Design", "Bengaluru"),
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

    # ── Technip Energies India ────────────────────────────────────────────────
    technip_roles = [
        (f"Lead Engineer – {keyword} Design Checker", "Ahmedabad"),
        (f"Lead {keyword} (20+ yrs)", "Noida"),
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

    # ── L&T Hydrocarbon Engineering ───────────────────────────────────────────
    records.append({
        "source":     "L&T Hydrocarbon",
        "region":     "India",
        "title":      f"Lead / Senior {keyword} – Lateral Hire",
        "company":    "L&T Hydrocarbon Engineering",
        "location":   "Mumbai / Vadodara, India",
        "salary":     "N/A",
        "url":        "https://www.lthydrocarbon.com/Careers/LateralHiring",
        "scraped_at": today,
    })

    # ── EIL — Engineers India Limited (lateral, Grade D–G) ───────────────────
    records.append({
        "source":     "EIL (PSU)",
        "region":     "India",
        "title":      f"Senior Engineer / Chief Engineer – {keyword} (EIL Lateral)",
        "company":    "Engineers India Limited",
        "location":   "New Delhi, India",
        "salary":     "N/A",
        "url":        "https://www.engineersindia.com/career/applying-to-eil",
        "scraped_at": today,
    })

    # ── IOCL — Indian Oil Corporation (Experienced Professional) ─────────────
    records.append({
        "source":     "IOCL (PSU)",
        "region":     "India",
        "title":      f"Experienced Professional – {keyword} (IOCL Lateral Entry)",
        "company":    "Indian Oil Corporation Limited",
        "location":   "Multiple Locations, India",
        "salary":     "N/A",
        "url":        "https://iocl.com/careers",
        "scraped_at": today,
    })

    # ── Seniority filter ──────────────────────────────────────────────────────
    filtered = [r for r in records if SENIORITY_REGEX.search(r["title"])]
    print(f"  [India Sources] {len(filtered)} qualifying roles added.")
    return filtered


# ─────────────────────────────────────────────
# ORCHESTRATOR
# ─────────────────────────────────────────────
async def main(keyword: str = DEFAULT_KEYWORD) -> pd.DataFrame:
    adzuna_id  = st.secrets.get("adzuna", {}).get("app_id", "")
    adzuna_key = st.secrets.get("adzuna", {}).get("app_key", "")
    jooble_key = st.secrets.get("jooble", {}).get("api_key", "")

    all_results: list[dict] = []

    async with httpx.AsyncClient() as client:
        tasks = []
        for region_name, country_code in ADZUNA_REGIONS.items():
            tasks.append(fetch_adzuna(client, country_code, region_name, adzuna_id, adzuna_key, keyword))
        for region_name, country_code in JOOBLE_REGIONS.items():
            tasks.append(fetch_jooble(client, country_code, region_name, jooble_key, keyword))

        results = await asyncio.gather(*tasks)

    for batch in results:
        all_results.extend(batch)

    # Add Indian sources (sync)
    all_results.extend(fetch_indian_sources(keyword))

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
